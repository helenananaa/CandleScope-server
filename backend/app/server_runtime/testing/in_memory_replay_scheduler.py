"""In-memory scheduler store with production-equivalent assignment rules."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping
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
        self._assignments_by_request: dict[str, str] = {}
        self._active_sessions: set[str] = set()
        self._commands: dict[str, list[dict[str, object]]] = {}
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
            candidates = []
            for request in self._requests.values():
                if request.state is ReplayRequestState.PENDING:
                    candidates.append(request)
                    continue
                if request.state in {
                    ReplayRequestState.ASSIGNED,
                    ReplayRequestState.STARTING,
                    ReplayRequestState.RUNNING,
                }:
                    assignment_id = self._assignments_by_request.get(request.request_id)
                    assignment = (
                        None
                        if assignment_id is None
                        else self._assignments.get(assignment_id)
                    )
                    if assignment is not None and assignment.active is False:
                        candidates.append(request)
            candidates.sort(
                key=lambda item: (
                    -(item.priority + min((now_ms - item.created_at_ms) // 1_000, 20)),
                    item.created_at_ms,
                )
            )
            if not candidates:
                return None
            request = candidates[0]
            session_id = request.session_id or f"sess-{uuid.uuid4().hex[:12]}"
            assignment = ReplaySchedulerAssignment(
                assignment_id=self._assignments_by_request.get(
                    request.request_id, f"asg-{uuid.uuid4().hex[:12]}"
                ),
                request_id=request.request_id,
                worker_id=worker_id,
                session_id=session_id,
                attempt=request.attempt + 1,
                active=True,
            )
            self._assignments[assignment.assignment_id] = assignment
            self._assignments_by_request[request.request_id] = assignment.assignment_id
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
        stale_workers = {
            worker_id
            for worker_id, worker in self._workers.items()
            if worker["expires_at_ms"] <= now_ms
        }
        for worker_id in stale_workers:
            self._workers[worker_id]["active_sessions"] = 0
        for assignment_id, assignment in list(self._assignments.items()):
            if assignment.worker_id in stale_workers and assignment.active:
                self._assignments[assignment_id] = replace(assignment, active=False)
                expired += 1
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

    async def get_by_session(
        self, session_id: str
    ) -> ReplaySchedulerRequest | None:
        self._require_available()
        for request in self._requests.values():
            if request.session_id == session_id:
                return request
        return None

    async def get_assignment(
        self, session_id: str
    ) -> ReplaySchedulerAssignment | None:
        self._require_available()
        for assignment in self._assignments.values():
            if assignment.session_id == session_id:
                return assignment
        return None

    async def enqueue_command(
        self, session_id: str, payload: Mapping[str, object]
    ) -> None:
        self._require_available()
        queued = self._commands.setdefault(session_id, [])
        command_id = str(payload["command_id"])
        if any(item.get("command_id") == command_id for item in queued):
            return
        queued.append(dict(payload))

    async def list_commands(
        self, session_id: str
    ) -> tuple[Mapping[str, object], ...]:
        self._require_available()
        return tuple(self._commands.get(session_id, ()))

    async def live_worker_count(self) -> int:
        self._require_available()
        now = self._clock_ms()
        return sum(
            1
            for worker in self._workers.values()
            if worker["expires_at_ms"] > now and worker["capacity"] > 0
        )

    async def drop_claim(self, assignment: ReplaySchedulerAssignment) -> None:
        self._require_available()
        async with self._lock:
            current = self._assignments.get(assignment.assignment_id)
            if current is None or current.active is False:
                return
            self._assignments[assignment.assignment_id] = replace(
                current, active=False
            )
            worker = self._workers.get(assignment.worker_id)
            if worker is not None:
                worker["active_sessions"] = max(0, worker["active_sessions"] - 1)

    async def assignment_is_recovering(self, session_id: str) -> bool:
        self._require_available()
        assignment = await self.get_assignment(session_id)
        if assignment is None:
            return False
        if assignment.active is False:
            return True
        worker = self._workers.get(assignment.worker_id)
        if worker is None:
            return True
        return worker["expires_at_ms"] <= self._clock_ms()

    def _require_available(self) -> None:
        if self.postgres_down:
            raise ReplaySchedulerError(
                "SCHEDULER_UNAVAILABLE",
                "PostgreSQL control plane is unavailable",
            )
