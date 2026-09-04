"""PostgreSQL-backed fenced replay session, mutation, checkpoint, and outbox."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.replay.actor import ActorMutation
from app.replay.canonical import canonical_sha256
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseFencedError,
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
from app.server_runtime.storage.postgres_replay_lease import (
    _SELECT_FOR_UPDATE_SQL,
    _database_now,
    _lock_session,
    _require_lease,
    _row_to_lease,
)

SESSION_TABLE = "candlescope_replay_session"
STATE_TABLE = "candlescope_replay_session_state"
MUTATION_TABLE = "candlescope_replay_mutation"
COMMAND_TABLE = "candlescope_replay_command_result"
OUTBOX_TABLE = "candlescope_replay_event_outbox"
ZERO_HASH = "0" * 64


class PostgresReplaySessionStore:
    """Fence the current lease and commit a mutation in one PostgreSQL transaction."""

    def __init__(self, dsn: str) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be a non-blank PostgreSQL connection string")
        self._dsn = dsn.strip()

    def __repr__(self) -> str:
        return "PostgresReplaySessionStore(dsn=<redacted>)"

    async def create_session(
        self,
        lease: ReplaySessionLease,
        spec: ServerReplaySessionSpec,
        initial_checkpoint: bytes,
        state: Mapping[str, object],
    ) -> ServerReplaySessionRecord:
        _require_lease(lease)
        if not isinstance(spec, ServerReplaySessionSpec):
            raise TypeError("spec must be ServerReplaySessionSpec")
        public_ref = spec.to_public_ref()
        _reject_token(public_ref, lease.lease_token)
        checkpoint = bytes(initial_checkpoint)
        state_payload = dict(state)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            current = await self._fence_active_lease(cursor, lease)
            await cursor.execute(
                f"SELECT session_id FROM {SESSION_TABLE} WHERE session_id = %s",
                (current.session_id,),
            )
            if await cursor.fetchone() is not None:
                raise ReplayDomainError(
                    ReplayErrorCode.REVISION_CONFLICT,
                    "replay session id collision",
                )
            snapshot = current.snapshot
            pin = spec.pin
            await cursor.execute(
                f"""
                INSERT INTO {SESSION_TABLE} (
                    session_id, organization_id, workspace_id, data_epoch,
                    snapshot_version, manifest_uri, manifest_sha256,
                    replay_start_ms, replay_end_time_ms, start_event_time_ms,
                    end_event_time_ms, expected_first_agg_trade_id,
                    expected_last_agg_trade_id, row_count, spec_public_json,
                    schema_version, code_version
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    current.session_id,
                    current.organization_id,
                    current.workspace_id,
                    snapshot.data_epoch,
                    snapshot.snapshot_version,
                    snapshot.manifest_uri,
                    snapshot.manifest_sha256,
                    spec.replay_start_ms,
                    spec.replay_end_time_ms,
                    pin.start_event_time_ms,
                    pin.end_event_time_ms,
                    pin.expected_first_agg_trade_id,
                    pin.expected_last_agg_trade_id,
                    pin.row_count,
                    Jsonb(public_ref),
                    spec.schema_version,
                    spec.code_version,
                ),
            )
            await cursor.execute(
                f"""
                INSERT INTO {STATE_TABLE} (
                    session_id, revision, event_sequence, command_log_offset,
                    source_sequence, state_hash, previous_hash, state_json,
                    checkpoint, checkpoint_sha256, closed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
                """,
                (
                    current.session_id,
                    int(state_payload["revision"]),
                    int(state_payload["event_sequence"]),
                    int(state_payload["command_log_offset"]),
                    int(state_payload["source_sequence"]),
                    str(state_payload["state_hash"]),
                    ZERO_HASH,
                    Jsonb(state_payload),
                    checkpoint,
                    hashlib.sha256(checkpoint).hexdigest(),
                ),
            )
            return await self._load_record(cursor, current.session_id)

    async def commit_mutation(
        self,
        lease: ReplaySessionLease,
        mutation: ActorMutation,
    ) -> ServerReplayMutationCommit:
        _require_lease(lease)
        if not isinstance(mutation, ActorMutation):
            raise TypeError("mutation must be ActorMutation")
        integrity = mutation_integrity_hash(mutation)
        incoming_state = dict(mutation.session_state)
        incoming_revision = int(incoming_state["revision"])
        incoming_sequence = int(incoming_state["event_sequence"])
        command_id = None if mutation.command is None else mutation.command.command_id
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            current = await self._fence_active_lease(cursor, lease)
            if mutation.session_id != current.session_id:
                raise ReplayDomainError(
                    ReplayErrorCode.DATASET_MISMATCH,
                    "mutation session_id does not match the fenced lease",
                )
            await cursor.execute(
                f"""
                SELECT revision, event_sequence, command_log_offset, state_hash,
                       previous_hash, closed, checkpoint
                FROM {STATE_TABLE}
                WHERE session_id = %s
                FOR UPDATE
                """,
                (current.session_id,),
            )
            state_row = await cursor.fetchone()
            if state_row is None or state_row["closed"]:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is not writable",
                )
            if command_id is not None:
                fingerprint = canonical_sha256(mutation.command.to_dict())
                await cursor.execute(
                    f"""
                    SELECT fingerprint, mutation_hash, result_json, revision,
                           event_sequence, accepted
                    FROM {COMMAND_TABLE}
                    WHERE session_id = %s AND command_id = %s
                    """,
                    (current.session_id, command_id),
                )
                existing = await cursor.fetchone()
                if existing is not None:
                    if existing["fingerprint"] != fingerprint:
                        raise ReplayDomainError(
                            ReplayErrorCode.COMMAND_ID_REUSED,
                            "command_id was reused with a different canonical command",
                            details={"command_id": command_id},
                        )
                    return ServerReplayMutationCommit(
                        session_id=current.session_id,
                        command_id=command_id,
                        duplicate=True,
                        mutation_hash=str(existing["mutation_hash"]),
                        state=incoming_state,
                        result=existing["result_json"],
                    )
            if mutation.kind == "command" and mutation.error is None:
                await cursor.execute(
                    f"""
                    SELECT mutation_hash
                    FROM {MUTATION_TABLE}
                    WHERE session_id = %s AND kind = 'command'
                      AND revision = %s AND event_sequence = %s
                      AND command_id IS NOT NULL
                    """,
                    (current.session_id, incoming_revision, incoming_sequence),
                )
                prior = await cursor.fetchone()
                if prior is not None and str(prior["mutation_hash"]) != integrity:
                    raise ServerReplayMutationIntegrityError(
                        "mutation integrity conflict at the same revision/sequence"
                    )
            payload = {
                "kind": mutation.kind,
                "command_id": command_id,
                "mutation_hash": integrity,
                "session_state": incoming_state,
            }
            if mutation.kind == "command" and mutation.command is not None:
                payload.update(
                    {
                        "command": mutation.command.to_dict(),
                        "accepted": mutation.error is None,
                        "command_log_offset": int(incoming_state["command_log_offset"]),
                        "state_hash": str(incoming_state["state_hash"]),
                    }
                )
            _reject_token(payload, lease.lease_token)
            checkpoint = (
                None if mutation.checkpoint is None else bytes(mutation.checkpoint)
            )
            await cursor.execute(
                f"""
                INSERT INTO {MUTATION_TABLE} (
                    session_id, kind, command_id, revision, event_sequence,
                    command_log_offset, mutation_hash, previous_hash, state_hash,
                    payload_json, checkpoint
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    current.session_id,
                    mutation.kind,
                    command_id,
                    incoming_revision,
                    incoming_sequence,
                    int(incoming_state["command_log_offset"]),
                    integrity,
                    str(state_row["state_hash"]),
                    str(incoming_state["state_hash"]),
                    Jsonb(payload),
                    checkpoint,
                ),
            )
            events = [event.to_dict() for event in mutation.events]
            for event in events:
                _reject_token(event, lease.lease_token)
                await cursor.execute(
                    f"""
                    INSERT INTO {OUTBOX_TABLE} (
                        session_id, sequence, event_json, mutation_hash
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (
                        current.session_id,
                        int(event["sequence"]),
                        Jsonb(event),
                        integrity,
                    ),
                )
            result_payload = (
                None
                if mutation.result is None
                else command_result_payload(mutation.result)
            )
            if command_id is not None:
                await cursor.execute(
                    f"""
                    INSERT INTO {COMMAND_TABLE} (
                        session_id, command_id, fingerprint, accepted,
                        result_json, error_code, error_message, mutation_hash,
                        revision, event_sequence
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        current.session_id,
                        command_id,
                        canonical_sha256(mutation.command.to_dict()),
                        mutation.error is None,
                        None if result_payload is None else Jsonb(result_payload),
                        None if mutation.error is None else mutation.error.code.value,
                        None if mutation.error is None else mutation.error.message,
                        integrity,
                        incoming_revision,
                        incoming_sequence,
                    ),
                )
            next_checkpoint = (
                checkpoint if checkpoint is not None else bytes(state_row["checkpoint"])
            )
            await cursor.execute(
                f"""
                UPDATE {STATE_TABLE}
                SET revision = %s,
                    event_sequence = %s,
                    command_log_offset = %s,
                    source_sequence = %s,
                    previous_hash = state_hash,
                    state_hash = %s,
                    state_json = %s,
                    checkpoint = %s,
                    checkpoint_sha256 = %s,
                    updated_at = clock_timestamp()
                WHERE session_id = %s
                """,
                (
                    incoming_revision,
                    incoming_sequence,
                    int(incoming_state["command_log_offset"]),
                    int(incoming_state["source_sequence"]),
                    str(incoming_state["state_hash"]),
                    Jsonb(incoming_state),
                    next_checkpoint,
                    hashlib.sha256(next_checkpoint).hexdigest(),
                    current.session_id,
                ),
            )
            return ServerReplayMutationCommit(
                session_id=current.session_id,
                command_id=command_id,
                duplicate=False,
                mutation_hash=integrity,
                state=incoming_state,
                result=result_payload,
            )

    async def load_recovery(
        self,
        session_id: str,
        lease: ReplaySessionLease,
    ) -> ServerReplayRecovery:
        _require_lease(lease)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            current = await self._fence_active_lease(cursor, lease)
            if current.session_id != session_id:
                raise ReplaySessionLeaseFencedError(
                    "replay session lease does not match session_id"
                )
            await cursor.execute(
                f"""
                SELECT checkpoint, state_json
                FROM {STATE_TABLE}
                WHERE session_id = %s
                FOR UPDATE
                """,
                (current.session_id,),
            )
            state_row = await cursor.fetchone()
            if state_row is None:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is missing",
                )
            await cursor.execute(
                f"""
                SELECT mutation_id, kind, payload_json, checkpoint
                FROM {MUTATION_TABLE}
                WHERE session_id = %s
                ORDER BY mutation_id ASC
                """,
                (current.session_id,),
            )
            rows = await cursor.fetchall()
            last_checkpoint_index = -1
            for index, row in enumerate(rows):
                if row["checkpoint"] is not None:
                    last_checkpoint_index = index
            tail = []
            for row in rows[last_checkpoint_index + 1 :]:
                payload = dict(row["payload_json"])
                if row["kind"] == "command":
                    tail.append(
                        {
                            "kind": "command",
                            "command": payload["command"],
                            "accepted": payload["accepted"],
                            "command_log_offset": payload["command_log_offset"],
                            "state_hash": payload["state_hash"],
                        }
                    )
                else:
                    tail.append(payload)
            return ServerReplayRecovery(
                session_id=current.session_id,
                checkpoint=bytes(state_row["checkpoint"]),
                mutations=tuple(tail),
                state=dict(state_row["state_json"]),
            )

    async def read_session(
        self,
        session_id: str,
        caller_scope: ReplayCallerScope,
    ) -> ServerReplaySessionRecord:
        if not isinstance(caller_scope, ReplayCallerScope):
            raise TypeError("caller_scope must be ReplayCallerScope")
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT organization_id, workspace_id
                FROM {SESSION_TABLE}
                WHERE session_id = %s
                """,
                (session_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ReplayDomainError(
                    ReplayErrorCode.SESSION_NOT_FOUND,
                    "replay session is missing",
                )
            if (row["organization_id"], row["workspace_id"]) != (
                caller_scope.organization_id,
                caller_scope.workspace_id,
            ):
                raise ReplaySessionScopeConflictError(
                    "replay session organization/workspace pin does not match "
                    "the caller scope"
                )
            return await self._load_record(cursor, session_id)

    async def close_session(
        self,
        lease: ReplaySessionLease,
        terminal_state: Mapping[str, object],
    ) -> ServerReplaySessionRecord:
        _require_lease(lease)
        state_payload = dict(terminal_state)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            current = await self._fence_active_lease(cursor, lease)
            await cursor.execute(
                f"""
                UPDATE {STATE_TABLE}
                SET state_json = %s,
                    revision = %s,
                    event_sequence = %s,
                    command_log_offset = %s,
                    source_sequence = %s,
                    state_hash = %s,
                    closed = TRUE,
                    updated_at = clock_timestamp()
                WHERE session_id = %s
                """,
                (
                    Jsonb(state_payload),
                    int(state_payload["revision"]),
                    int(state_payload["event_sequence"]),
                    int(state_payload["command_log_offset"]),
                    int(state_payload["source_sequence"]),
                    str(state_payload["state_hash"]),
                    current.session_id,
                ),
            )
            return await self._load_record(cursor, current.session_id)

    async def verify_runtime_privileges(self) -> None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                """
                SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication
                FROM pg_roles
                WHERE rolname = current_user
                """
            )
            role = await cursor.fetchone()
            if role is None or any(role.values()):
                raise ReplayDomainError(
                    ReplayErrorCode.PERSISTENCE_DEGRADED,
                    "replay worker database role is elevated",
                )
            await cursor.execute(
                """
                SELECT has_table_privilege(%s, 'INSERT') AS can_insert,
                       has_table_privilege(%s, 'SELECT') AS can_select
                """,
                (SESSION_TABLE, SESSION_TABLE),
            )
            privileges = await cursor.fetchone()
            if privileges is None or not (
                privileges["can_insert"] and privileges["can_select"]
            ):
                raise ReplayDomainError(
                    ReplayErrorCode.PERSISTENCE_DEGRADED,
                    "replay worker database role is missing DML privileges",
                )

    async def _fence_active_lease(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
        lease: ReplaySessionLease,
    ) -> ReplaySessionLease:
        await _lock_session(cursor, lease.session_id)
        now = await _database_now(cursor)
        await cursor.execute(_SELECT_FOR_UPDATE_SQL, (lease.session_id,))
        row = await cursor.fetchone()
        if row is None:
            raise ReplaySessionLeaseFencedError(
                "replay session lease is stale or no longer owned"
            )
        current = _row_to_lease(row)
        require_write_fence(current, lease)
        if row["lease_expires_at"] <= now:
            raise ReplaySessionLeaseFencedError("replay session lease has expired")
        return current

    async def _load_record(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
        session_id: str,
    ) -> ServerReplaySessionRecord:
        await cursor.execute(
            f"""
            SELECT s.spec_public_json, st.checkpoint, st.state_json, st.closed,
                   (
                       SELECT COUNT(*) FROM {MUTATION_TABLE} m
                       WHERE m.session_id = s.session_id
                   ) AS mutation_count
            FROM {SESSION_TABLE} s
            JOIN {STATE_TABLE} st ON st.session_id = s.session_id
            WHERE s.session_id = %s
            """,
            (session_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ReplayDomainError(
                ReplayErrorCode.SESSION_NOT_FOUND,
                "replay session is missing",
            )
        return ServerReplaySessionRecord(
            session_id=session_id,
            spec_public_ref=dict(row["spec_public_json"]),
            checkpoint=bytes(row["checkpoint"]),
            state=dict(row["state_json"]),
            closed=bool(row["closed"]),
            mutation_count=int(row["mutation_count"]),
        )

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        return await psycopg.AsyncConnection.connect(
            self._dsn,
            row_factory=dict_row,
            application_name="candlescope-replay-session",
        )


def _reject_token(payload: object, token: str) -> None:
    if token in repr(payload):
        raise RuntimeError("lease_token must not be persisted in session state")
