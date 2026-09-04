"""In-memory fenced replay session store. Does not claim PostgreSQL durability."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field

from app.replay.actor import ActorMutation
from app.replay.canonical import canonical_sha256
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseFencedError,
    ReplaySessionLeaseStore,
    ReplaySessionScopeConflictError,
    require_write_fence,
)
from app.server_runtime.replay_session import (
    ReplayCallerScope,
    ServerReplayMutationCommit,
    ServerReplayMutationIntegrityError,
    ServerReplayRecovery,
    ServerReplaySessionRecord,
    ServerReplaySessionSpec,
    command_result_payload,
    mutation_integrity_hash,
)


@dataclass
class _CommandRow:
    fingerprint: str
    accepted: bool
    result: dict[str, object] | None
    error_code: str | None
    error_message: str | None
    mutation_hash: str
    revision: int
    event_sequence: int
    state: dict[str, object]


@dataclass
class _MutationRow:
    kind: str
    command_id: str | None
    mutation_hash: str
    revision: int
    event_sequence: int
    command_log_offset: int
    state_hash: str
    checkpoint: bytes | None
    payload: dict[str, object]
    events: tuple[dict[str, object], ...]


@dataclass
class _SessionRow:
    session_id: str
    organization_id: str
    workspace_id: str
    spec_public_ref: dict[str, object]
    checkpoint: bytes
    state: dict[str, object]
    closed: bool = False
    commands: dict[str, _CommandRow] = field(default_factory=dict)
    mutations: list[_MutationRow] = field(default_factory=list)
    outbox: list[dict[str, object]] = field(default_factory=list)
    last_checkpoint_index: int = -1


class InMemoryReplaySessionStore:
    """Atomic lease-check + mutation commit for Phase 1AD unit tests."""

    def __init__(self, lease_store: ReplaySessionLeaseStore) -> None:
        if not hasattr(lease_store, "require_active"):
            raise TypeError("lease_store must implement require_active")
        self._lease_store = lease_store
        self._sessions: dict[str, _SessionRow] = {}
        self._lock = asyncio.Lock()

    async def create_session(
        self,
        lease: ReplaySessionLease,
        spec: ServerReplaySessionSpec,
        initial_checkpoint: bytes,
        state: Mapping[str, object],
    ) -> ServerReplaySessionRecord:
        if not isinstance(spec, ServerReplaySessionSpec):
            raise TypeError("spec must be ServerReplaySessionSpec")
        if not isinstance(initial_checkpoint, (bytes, bytearray)):
            raise TypeError("initial_checkpoint must be bytes")
        if not isinstance(state, Mapping):
            raise TypeError("state must be a mapping")
        public_ref = spec.to_public_ref()
        _reject_token(public_ref, lease.lease_token)
        async with self._lock:
            current = await self._require_active_lease(lease)
            if current.session_id != spec.lease.session_id:
                raise ReplaySessionLeaseFencedError(
                    "replay session lease does not match the session spec"
                )
            if current.session_id in self._sessions:
                raise ReplayDomainError(
                    ReplayErrorCode.REVISION_CONFLICT,
                    "replay session id collision",
                )
            row = _SessionRow(
                session_id=current.session_id,
                organization_id=current.organization_id,
                workspace_id=current.workspace_id,
                spec_public_ref=dict(public_ref),
                checkpoint=bytes(initial_checkpoint),
                state=dict(state),
            )
            self._sessions[current.session_id] = row
            return self._record(row)

    async def commit_mutation(
        self,
        lease: ReplaySessionLease,
        mutation: ActorMutation,
    ) -> ServerReplayMutationCommit:
        if not isinstance(mutation, ActorMutation):
            raise TypeError("mutation must be ActorMutation")
        integrity = mutation_integrity_hash(mutation)
        async with self._lock:
            current = await self._require_active_lease(lease)
            row = self._sessions.get(current.session_id)
            if row is None or row.closed:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is not writable",
                )
            self._require_session_pins(row, current)
            if mutation.session_id != current.session_id:
                raise ReplayDomainError(
                    ReplayErrorCode.DATASET_MISMATCH,
                    "mutation session_id does not match the fenced lease",
                )
            incoming_state = dict(mutation.session_state)
            incoming_revision = int(incoming_state["revision"])
            incoming_sequence = int(incoming_state["event_sequence"])
            command_id = (
                None if mutation.command is None else mutation.command.command_id
            )
            if command_id is not None:
                fingerprint = canonical_sha256(mutation.command.to_dict())
                existing = row.commands.get(command_id)
                if existing is not None:
                    if existing.fingerprint != fingerprint:
                        raise ReplayDomainError(
                            ReplayErrorCode.COMMAND_ID_REUSED,
                            "command_id was reused with a different canonical command",
                            details={"command_id": command_id},
                        )
                    return ServerReplayMutationCommit(
                        session_id=row.session_id,
                        command_id=command_id,
                        duplicate=True,
                        mutation_hash=existing.mutation_hash,
                        state=dict(existing.state),
                        result=None
                        if existing.result is None
                        else dict(existing.result),
                    )
            self._reject_integrity_conflict(
                row,
                kind=mutation.kind,
                revision=incoming_revision,
                event_sequence=incoming_sequence,
                mutation_hash=integrity,
            )
            events = tuple(event.to_dict() for event in mutation.events)
            payload = {
                "kind": mutation.kind,
                "command_id": command_id,
                "mutation_hash": integrity,
                "session_state": incoming_state,
            }
            _reject_token(payload, lease.lease_token)
            mutation_row = _MutationRow(
                kind=mutation.kind,
                command_id=command_id,
                mutation_hash=integrity,
                revision=incoming_revision,
                event_sequence=incoming_sequence,
                command_log_offset=int(incoming_state["command_log_offset"]),
                state_hash=str(incoming_state["state_hash"]),
                checkpoint=None
                if mutation.checkpoint is None
                else bytes(mutation.checkpoint),
                payload=payload,
                events=events,
            )
            row.mutations.append(mutation_row)
            row.state = incoming_state
            if mutation.checkpoint is not None:
                row.checkpoint = bytes(mutation.checkpoint)
                row.last_checkpoint_index = len(row.mutations) - 1
            row.outbox.extend(dict(event) for event in events)
            result_payload = (
                None
                if mutation.result is None
                else command_result_payload(mutation.result)
            )
            if command_id is not None:
                row.commands[command_id] = _CommandRow(
                    fingerprint=canonical_sha256(mutation.command.to_dict()),
                    accepted=mutation.error is None,
                    result=result_payload,
                    error_code=None
                    if mutation.error is None
                    else mutation.error.code.value,
                    error_message=None
                    if mutation.error is None
                    else mutation.error.message,
                    mutation_hash=integrity,
                    revision=incoming_revision,
                    event_sequence=incoming_sequence,
                    state=incoming_state,
                )
            return ServerReplayMutationCommit(
                session_id=row.session_id,
                command_id=command_id,
                duplicate=False,
                mutation_hash=integrity,
                state=dict(row.state),
                result=result_payload,
            )

    async def load_recovery(
        self,
        session_id: str,
        lease: ReplaySessionLease,
    ) -> ServerReplayRecovery:
        async with self._lock:
            current = await self._require_active_lease(lease)
            if current.session_id != session_id:
                raise ReplaySessionLeaseFencedError(
                    "replay session lease does not match session_id"
                )
            row = self._sessions.get(current.session_id)
            if row is None:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is missing",
                )
            self._require_session_pins(row, current)
            start = max(row.last_checkpoint_index + 1, 0)
            tail = tuple(dict(item.payload) for item in row.mutations[start:])
            return ServerReplayRecovery(
                session_id=row.session_id,
                checkpoint=bytes(row.checkpoint),
                mutations=tail,
                state=dict(row.state),
            )

    async def read_session(
        self,
        session_id: str,
        caller_scope: ReplayCallerScope,
    ) -> ServerReplaySessionRecord:
        if not isinstance(caller_scope, ReplayCallerScope):
            raise TypeError("caller_scope must be ReplayCallerScope")
        async with self._lock:
            row = self._sessions.get(session_id)
            if row is None:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is missing",
                )
            if (row.organization_id, row.workspace_id) != (
                caller_scope.organization_id,
                caller_scope.workspace_id,
            ):
                raise ReplaySessionScopeConflictError(
                    "replay session organization/workspace pin does not match "
                    "the caller scope"
                )
            return self._record(row)

    async def close_session(
        self,
        lease: ReplaySessionLease,
        terminal_state: Mapping[str, object],
    ) -> ServerReplaySessionRecord:
        if not isinstance(terminal_state, Mapping):
            raise TypeError("terminal_state must be a mapping")
        async with self._lock:
            current = await self._require_active_lease(lease)
            row = self._sessions.get(current.session_id)
            if row is None:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is missing",
                )
            self._require_session_pins(row, current)
            row.state = dict(terminal_state)
            row.closed = True
            return self._record(row)

    async def _require_active_lease(
        self, lease: ReplaySessionLease
    ) -> ReplaySessionLease:
        current = await self._lease_store.require_active(lease)
        require_write_fence(current, lease)
        return current

    @staticmethod
    def _require_session_pins(row: _SessionRow, lease: ReplaySessionLease) -> None:
        if (row.organization_id, row.workspace_id) != (
            lease.organization_id,
            lease.workspace_id,
        ):
            raise ReplaySessionScopeConflictError(
                "replay session organization/workspace pin does not match "
                "the fenced lease"
            )

    @staticmethod
    def _reject_integrity_conflict(
        row: _SessionRow,
        *,
        kind: str,
        revision: int,
        event_sequence: int,
        mutation_hash: str,
    ) -> None:
        if kind != "command":
            return
        for item in row.mutations:
            if (
                item.kind == "command"
                and item.revision == revision
                and item.event_sequence == event_sequence
                and item.mutation_hash != mutation_hash
            ):
                raise ServerReplayMutationIntegrityError(
                    "mutation integrity conflict at the same revision/sequence"
                )

    @staticmethod
    def _record(row: _SessionRow) -> ServerReplaySessionRecord:
        return ServerReplaySessionRecord(
            session_id=row.session_id,
            spec_public_ref=dict(row.spec_public_ref),
            checkpoint=bytes(row.checkpoint),
            state=dict(row.state),
            closed=row.closed,
            mutation_count=len(row.mutations),
        )


def _reject_token(payload: object, token: str) -> None:
    rendered = repr(payload)
    if token in rendered:
        raise RuntimeError("lease_token must not be persisted in session state")


__all__ = ["InMemoryReplaySessionStore"]
