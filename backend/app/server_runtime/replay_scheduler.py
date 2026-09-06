"""Durable replay scheduler. Metadata and assignment only; no Actors."""

from __future__ import annotations

import enum
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from app.replay.canonical import canonical_sha256
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)
from app.server_runtime.replay_lease import (
    normalize_replay_worker_id,
)

SCHEDULER_SCHEMA_VERSION = "candlescope.replay-scheduler.v1"
TERMINAL_STATES = frozenset({"CANCELLED", "FAILED", "COMPLETED"})


class ReplayRequestState(str, enum.Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


_ALLOWED_TRANSITIONS: dict[ReplayRequestState, frozenset[ReplayRequestState]] = {
    ReplayRequestState.PENDING: frozenset(
        {
            ReplayRequestState.ASSIGNED,
            ReplayRequestState.CANCELLING,
            ReplayRequestState.FAILED,
        }
    ),
    ReplayRequestState.ASSIGNED: frozenset(
        {
            ReplayRequestState.STARTING,
            ReplayRequestState.CANCELLING,
            ReplayRequestState.FAILED,
        }
    ),
    ReplayRequestState.STARTING: frozenset(
        {
            ReplayRequestState.RUNNING,
            ReplayRequestState.CANCELLING,
            ReplayRequestState.FAILED,
        }
    ),
    ReplayRequestState.RUNNING: frozenset(
        {
            ReplayRequestState.CANCELLING,
            ReplayRequestState.COMPLETED,
            ReplayRequestState.FAILED,
        }
    ),
    ReplayRequestState.CANCELLING: frozenset({ReplayRequestState.CANCELLED}),
    ReplayRequestState.CANCELLED: frozenset(),
    ReplayRequestState.FAILED: frozenset(),
    ReplayRequestState.COMPLETED: frozenset(),
}


class ReplaySchedulerError(RuntimeError):
    """Stable scheduler failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class ReplaySchedulerQuotaError(ReplaySchedulerError):
    def __init__(self, message: str = "organization replay quota exceeded") -> None:
        super().__init__("SCHEDULER_QUOTA_EXCEEDED", message)


class ReplaySchedulerIdempotencyError(ReplaySchedulerError):
    def __init__(self, message: str = "idempotency key payload conflict") -> None:
        super().__init__("SCHEDULER_IDEMPOTENCY_CONFLICT", message)


@dataclass(frozen=True, slots=True)
class ReplaySchedulerRequest:
    request_id: str
    organization_id: str
    workspace_id: str
    idempotency_key: str
    payload_hash: str
    payload: Mapping[str, object]
    priority: int
    state: ReplayRequestState
    session_id: str | None
    attempt: int
    timeout_at_ms: int
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class ReplaySchedulerAssignment:
    assignment_id: str
    request_id: str
    worker_id: str
    session_id: str
    attempt: int
    active: bool = True


class ReplaySchedulerStore(Protocol):
    async def create_request(
        self, request: ReplaySchedulerRequest
    ) -> ReplaySchedulerRequest: ...

    async def get_by_idempotency(
        self, organization_id: str, idempotency_key: str
    ) -> ReplaySchedulerRequest | None: ...

    async def get_request(self, request_id: str) -> ReplaySchedulerRequest | None: ...

    async def list_requests(
        self,
        organization_id: str,
        workspace_id: str,
        *,
        limit: int,
    ) -> tuple[ReplaySchedulerRequest, ...]: ...

    async def count_open(self, organization_id: str) -> tuple[int, int]: ...

    async def heartbeat(
        self, worker_id: str, *, capacity: int, ttl_ms: int
    ) -> None: ...

    async def claim_next(
        self,
        worker_id: str,
        *,
        now_ms: int,
        organization_id: str | None,
        workspace_id: str | None,
    ) -> ReplaySchedulerAssignment | None: ...

    async def transition(
        self,
        request_id: str,
        target: ReplayRequestState,
        *,
        now_ms: int,
    ) -> ReplaySchedulerRequest: ...

    async def expire_workers_and_timeouts(self, *, now_ms: int) -> int: ...


class ReplayScheduler:
    """Assign sessions to Workers. Never owns an Actor or market reader."""

    def __init__(
        self,
        store: ReplaySchedulerStore,
        *,
        max_pending_per_org: int = 4,
        max_active_per_org: int = 2,
        default_timeout_ms: int = 60_000,
        heartbeat_ttl_ms: int = 60_000,
        clock_ms,
    ) -> None:
        self._store = store
        self._max_pending_per_org = max_pending_per_org
        self._max_active_per_org = max_active_per_org
        self._default_timeout_ms = default_timeout_ms
        self._heartbeat_ttl_ms = heartbeat_ttl_ms
        self._clock_ms = clock_ms

    def __repr__(self) -> str:
        return "ReplayScheduler(store=<redacted>)"

    async def create_request(
        self,
        *,
        organization_id: str,
        workspace_id: str,
        idempotency_key: str,
        payload: Mapping[str, object],
        priority: int = 0,
        timeout_ms: int | None = None,
    ) -> ReplaySchedulerRequest:
        organization_id = normalize_organization_id(organization_id)
        workspace_id = normalize_workspace_id(workspace_id)
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ReplaySchedulerError(
                "SCHEDULER_INVALID_IDEMPOTENCY_KEY",
                "idempotency_key must be a non-blank string",
            )
        if "lease_token" in payload:
            raise ReplaySchedulerError(
                "SCHEDULER_SECRET_PAYLOAD",
                "scheduler payload cannot include lease_token",
            )
        payload_hash = canonical_sha256(dict(payload))
        existing = await self._store.get_by_idempotency(
            organization_id, idempotency_key.strip()
        )
        if existing is not None:
            if existing.payload_hash != payload_hash:
                raise ReplaySchedulerIdempotencyError()
            return existing
        pending, active = await self._store.count_open(organization_id)
        if pending >= self._max_pending_per_org:
            raise ReplaySchedulerQuotaError("pending replay quota exceeded")
        if active >= self._max_active_per_org:
            raise ReplaySchedulerQuotaError("active replay quota exceeded")
        now = self._clock_ms()
        timeout = self._default_timeout_ms if timeout_ms is None else timeout_ms
        request = ReplaySchedulerRequest(
            request_id=f"req-{uuid.uuid4().hex[:16]}",
            organization_id=organization_id,
            workspace_id=workspace_id,
            idempotency_key=idempotency_key.strip(),
            payload_hash=payload_hash,
            payload=dict(payload),
            priority=_bounded_priority(priority),
            state=ReplayRequestState.PENDING,
            session_id=None,
            attempt=0,
            timeout_at_ms=now + timeout,
            created_at_ms=now,
        )
        return await self._store.create_request(request)

    async def heartbeat(self, worker_id: str, *, capacity: int = 1) -> None:
        await self._store.heartbeat(
            normalize_replay_worker_id(worker_id),
            capacity=capacity,
            ttl_ms=self._heartbeat_ttl_ms,
        )

    async def claim(
        self,
        worker_id: str,
        *,
        organization_id: str | None = None,
        workspace_id: str | None = None,
    ) -> ReplaySchedulerAssignment | None:
        worker_id = normalize_replay_worker_id(worker_id)
        if (organization_id is None) != (workspace_id is None):
            raise ReplaySchedulerError(
                "SCHEDULER_INVALID_WORKER_SCOPE",
                "worker organization and workspace must be supplied together",
            )
        if organization_id is not None:
            organization_id = normalize_organization_id(organization_id)
            workspace_id = normalize_workspace_id(workspace_id)
        await self._store.expire_workers_and_timeouts(now_ms=self._clock_ms())
        return await self._store.claim_next(
            worker_id,
            now_ms=self._clock_ms(),
            organization_id=organization_id,
            workspace_id=workspace_id,
        )

    async def cancel(self, request_id: str) -> ReplaySchedulerRequest:
        current = await self._require(request_id)
        if current.state in TERMINAL_STATES:
            return current
        if current.state is ReplayRequestState.CANCELLING:
            return await self._store.transition(
                request_id,
                ReplayRequestState.CANCELLED,
                now_ms=self._clock_ms(),
            )
        await self._store.transition(
            request_id,
            ReplayRequestState.CANCELLING,
            now_ms=self._clock_ms(),
        )
        return await self._store.transition(
            request_id,
            ReplayRequestState.CANCELLED,
            now_ms=self._clock_ms(),
        )

    async def mark_running(self, request_id: str) -> ReplaySchedulerRequest:
        current = await self._require(request_id)
        if current.state is ReplayRequestState.PENDING:
            current = await self._store.transition(
                request_id, ReplayRequestState.ASSIGNED, now_ms=self._clock_ms()
            )
        if current.state is ReplayRequestState.ASSIGNED:
            current = await self._store.transition(
                request_id, ReplayRequestState.STARTING, now_ms=self._clock_ms()
            )
        return await self._store.transition(
            request_id, ReplayRequestState.RUNNING, now_ms=self._clock_ms()
        )

    async def complete(self, request_id: str) -> ReplaySchedulerRequest:
        return await self._store.transition(
            request_id, ReplayRequestState.COMPLETED, now_ms=self._clock_ms()
        )

    async def fail(self, request_id: str) -> ReplaySchedulerRequest:
        return await self._store.transition(
            request_id, ReplayRequestState.FAILED, now_ms=self._clock_ms()
        )

    async def scan_timeouts(self) -> int:
        return await self._store.expire_workers_and_timeouts(now_ms=self._clock_ms())

    async def get_request(self, request_id: str) -> ReplaySchedulerRequest | None:
        return await self._store.get_request(request_id)

    async def list_requests(
        self,
        *,
        organization_id: str,
        workspace_id: str,
        limit: int = 100,
    ) -> tuple[ReplaySchedulerRequest, ...]:
        organization_id = normalize_organization_id(organization_id)
        workspace_id = normalize_workspace_id(workspace_id)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
        ):
            raise ReplaySchedulerError(
                "SCHEDULER_INVALID_LIST_LIMIT",
                "list limit must be between 1 and 100",
            )
        return await self._store.list_requests(
            organization_id,
            workspace_id,
            limit=limit,
        )

    async def get_by_session(self, session_id: str) -> ReplaySchedulerRequest | None:
        getter = getattr(self._store, "get_by_session", None)
        if getter is None:
            return None
        return await getter(session_id)

    async def get_assignment(self, session_id: str) -> ReplaySchedulerAssignment | None:
        getter = getattr(self._store, "get_assignment", None)
        if getter is None:
            return None
        return await getter(session_id)

    async def enqueue_command(
        self, session_id: str, payload: Mapping[str, object]
    ) -> None:
        await self._store.enqueue_command(session_id, payload)

    async def list_commands(self, session_id: str) -> tuple[Mapping[str, object], ...]:
        return await self._store.list_commands(session_id)

    async def live_worker_count(self) -> int:
        counter = getattr(self._store, "live_worker_count", None)
        if counter is None:
            return 0
        return int(await counter())

    async def assignment_is_recovering(self, session_id: str) -> bool:
        probe = getattr(self._store, "assignment_is_recovering", None)
        if probe is None:
            return False
        return bool(await probe(session_id))

    async def drop_claim(self, assignment: ReplaySchedulerAssignment) -> None:
        drop = getattr(self._store, "drop_claim", None)
        if drop is None:
            return
        await drop(assignment)

    async def _require(self, request_id: str) -> ReplaySchedulerRequest:
        current = await self._store.get_request(request_id)
        if current is None:
            raise ReplaySchedulerError("SCHEDULER_NOT_FOUND", "request not found")
        return current


def validate_transition(
    current: ReplayRequestState, target: ReplayRequestState
) -> None:
    if target not in _ALLOWED_TRANSITIONS[current]:
        raise ReplaySchedulerError(
            "SCHEDULER_ILLEGAL_TRANSITION",
            f"cannot transition {current.value} -> {target.value}",
        )


def _bounded_priority(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReplaySchedulerError(
            "SCHEDULER_INVALID_PRIORITY", "priority must be an integer"
        )
    if value < 0 or value > 100:
        raise ReplaySchedulerError(
            "SCHEDULER_INVALID_PRIORITY", "priority must be between 0 and 100"
        )
    return value
