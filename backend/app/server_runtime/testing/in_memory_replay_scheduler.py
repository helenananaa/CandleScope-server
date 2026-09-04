"""In-memory scheduler store with production-equivalent assignment rules."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import replace

from app.server_runtime.replay_scheduler import (
    ReplayRequestState,
    ReplaySchedulerAssignment,
    ReplaySchedulerError,
    ReplaySchedulerRequest,
    validate_transition,
)


class InMemoryReplaySchedulerStore:
    def __init__(self, *, clock_ms: Callable[[], int]) -> None:
        self._clock_ms = clock_ms
        self._lock = asyncio.Lock()
        self._requests: dict[str, ReplaySchedulerRequest] = {}
        self._by_idempotency: dict[tuple[str, str], str] = {}
        self._workers: dict[str, dict[str, int]] = {}
        self._assignments: dict[str, ReplaySchedulerAssignment] = {}
        self._active_sessions: set[str] = set()
        self.postgres_down = False

    async def create_request(
        self, request: ReplaySchedulerRequest
    ) -> ReplaySchedulerRequest:
        self._require_available()
        async with self._lock:
            self._requests[request.request_id] = request
            self._by_idempotency[(request.organization_id, request.idempotency_key)] = (
                request.request_id
            )
            return request

    async def get_by_idempotency(
        self, organization_id: str, idempotency_key: str
    ) -> ReplaySchedulerRequest | None:
        self._require_available()
        request_id = self._by_idempotency.get((organization_id, idempotency_key))
        if request_id is None:
            return None
        return self._requests[request_id]

    async def get_request(self, request_id: str) -> ReplaySchedulerRequest | None:
        self._require_available()
        return self._requests.get(request_id)

    async def count_open(self, organization_id: str) -> tuple[int, int]:
        self._require_available()
        pending = 0
        active = 0
        for request in self._requests.values():
            if request.organization_id != organization_id:
                continue
            if request.state is ReplayRequestState.PENDING:
                pending += 1
            elif request.state in {
                ReplayRequestState.ASSIGNED,
                ReplayRequestState.STARTING,
                ReplayRequestState.RUNNING,
                ReplayRequestState.CANCELLING,
            }:
                active += 1
        return pending, active

    async def heartbeat(self, worker_id: str, *, capacity: int, ttl_ms: int) -> None:
        self._require_available()
        now = self._clock_ms()
        async with self._lock:
            current = self._workers.get(worker_id, {"active_sessions": 0})
            self._workers[worker_id] = {
                "capacity": capacity,
                "active_sessions": int(current["active_sessions"]),
                "expires_at_ms": now + ttl_ms,
            }

    async def claim_next(
        self, worker_id: str, *, now_ms: int
    ) -> ReplaySchedulerAssignment | None:
        self._require_available()
        async with self._lock:
            self._expire_locked(now_ms)
            worker = self._workers.get(worker_id)
            if worker is None or worker["expires_at_ms"] <= now_ms:
                return None
            if worker["active_sessions"] >= worker["capacity"]:
                return None
            pending = [
                request
                for request in self._requests.values()
                if request.state is ReplayRequestState.PENDING
            ]
            pending.sort(
                key=lambda item: (
                    -(item.priority + min((now_ms - item.created_at_ms) // 1_000, 20)),
                    item.created_at_ms,
                )
            )
            if not pending:
                return None
            request = pending[0]
            session_id = f"sess-{uuid.uuid4().hex[:12]}"
            if session_id in self._active_sessions:
                return None
            assignment = ReplaySchedulerAssignment(
                assignment_id=f"asg-{uuid.uuid4().hex[:12]}",
                request_id=request.request_id,
                worker_id=worker_id,
                session_id=session_id,
                attempt=request.attempt + 1,
            )
            self._assignments[assignment.assignment_id] = assignment
            self._active_sessions.add(session_id)
            worker["active_sessions"] += 1
            self._requests[request.request_id] = replace(
                request,
                state=ReplayRequestState.ASSIGNED,
                session_id=session_id,
                attempt=assignment.attempt,
            )
            return assignment

    async def transition(
        self,
        request_id: str,
        target: ReplayRequestState,
        *,
        now_ms: int,
    ) -> ReplaySchedulerRequest:
        self._require_available()
        async with self._lock:
            current = self._requests[request_id]
            validate_transition(current.state, target)
            updated = replace(current, state=target)
            self._requests[request_id] = updated
            if target in {
                ReplayRequestState.COMPLETED,
                ReplayRequestState.FAILED,
                ReplayRequestState.CANCELLED,
            }:
                if current.session_id in self._active_sessions:
                    self._active_sessions.discard(current.session_id)
                for worker in self._workers.values():
                    if worker["active_sessions"] > 0 and current.session_id:
                        worker["active_sessions"] = max(
                            0, worker["active_sessions"] - 1
                        )
                        break
            del now_ms
            return updated

    async def expire_workers_and_timeouts(self, *, now_ms: int) -> int:
        self._require_available()
        async with self._lock:
            return self._expire_locked(now_ms)

    def _expire_locked(self, now_ms: int) -> int:
        expired = 0
        stale_workers = [
            worker_id
            for worker_id, worker in self._workers.items()
            if worker["expires_at_ms"] <= now_ms
        ]
        for worker_id in stale_workers:
            self._workers.pop(worker_id, None)
        for request_id, request in list(self._requests.items()):
            if (
                request.state is ReplayRequestState.PENDING
                and request.timeout_at_ms <= now_ms
            ):
                self._requests[request_id] = replace(
                    request, state=ReplayRequestState.FAILED
                )
                expired += 1
        return expired

    def _require_available(self) -> None:
        if self.postgres_down:
            raise ReplaySchedulerError(
                "SCHEDULER_UNAVAILABLE",
                "PostgreSQL control plane is unavailable",
            )
