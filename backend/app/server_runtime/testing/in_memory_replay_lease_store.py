"""Deterministic replay-session lease double with production-equivalent fencing."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import replace

from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseBusyError,
    ReplaySessionLeaseFencedError,
    ReplaySessionScopeConflictError,
    ReplaySessionSnapshotConflictError,
    normalize_replay_session_id,
    require_write_fence,
)


class InMemoryReplaySessionLeaseStore:
    """Model exclusive session ownership without claiming PostgreSQL durability."""

    def __init__(self, *, clock_ms: Callable[[], int] | None = None) -> None:
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._leases: dict[str, ReplaySessionLease] = {}
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        *,
        session_id: str,
        worker_id: str,
        snapshot: MarketDataSnapshotRef,
        organization_id: str,
        workspace_id: str,
        lease_ttl_ms: int,
    ) -> ReplaySessionLease:
        if not isinstance(snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        session_key = normalize_replay_session_id(session_id)
        organization_key = normalize_organization_id(organization_id)
        workspace_key = normalize_workspace_id(workspace_id)
        async with self._lock:
            now = self._clock_ms()
            current = self._leases.get(session_key)
            if current is not None and current.lease_expires_at_ms > now:
                raise ReplaySessionLeaseBusyError(
                    f"session {session_key!r} is leased by {current.worker_id!r}"
                )
            if current is not None and current.snapshot != snapshot:
                raise ReplaySessionSnapshotConflictError(
                    "replay session snapshot pin does not match the fenced lease"
                )
            if current is not None and (
                current.organization_id,
                current.workspace_id,
            ) != (organization_key, workspace_key):
                raise ReplaySessionScopeConflictError(
                    "replay session organization/workspace pin does not match "
                    "the fenced lease"
                )
            pinned = current.snapshot if current is not None else snapshot
            epoch = 0 if current is None else current.fencing_epoch + 1
            acquired = ReplaySessionLease(
                session_id=session_key,
                worker_id=worker_id,
                fencing_epoch=epoch,
                lease_token=str(uuid.uuid4()),
                lease_expires_at_ms=now + lease_ttl_ms,
                snapshot=pinned,
                organization_id=organization_key,
                workspace_id=workspace_key,
            )
            self._leases[session_key] = acquired
            return acquired

    async def renew(
        self,
        lease: ReplaySessionLease,
        *,
        lease_ttl_ms: int,
    ) -> ReplaySessionLease:
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        async with self._lock:
            current = self._require_active(lease)
            renewed = replace(
                current,
                lease_expires_at_ms=self._clock_ms() + lease_ttl_ms,
            )
            self._leases[lease.session_id] = renewed
            return renewed

    async def release(self, lease: ReplaySessionLease) -> None:
        async with self._lock:
            current = self._require_fence(lease)
            self._leases[lease.session_id] = replace(
                current,
                lease_expires_at_ms=self._clock_ms(),
            )

    async def inspect(self, session_id: str) -> ReplaySessionLease | None:
        session_key = normalize_replay_session_id(session_id)
        async with self._lock:
            return self._leases.get(session_key)

    async def require_active(self, lease: ReplaySessionLease) -> ReplaySessionLease:
        async with self._lock:
            return self._require_active(lease)

    def _require_fence(self, lease: ReplaySessionLease) -> ReplaySessionLease:
        if not isinstance(lease, ReplaySessionLease):
            raise TypeError("lease must be a ReplaySessionLease")
        current = self._leases.get(lease.session_id)
        if current is None:
            raise ReplaySessionLeaseFencedError(
                "replay session lease is stale or no longer owned"
            )
        require_write_fence(current, lease)
        return current

    def _require_active(self, lease: ReplaySessionLease) -> ReplaySessionLease:
        current = self._require_fence(lease)
        if current.lease_expires_at_ms <= self._clock_ms():
            raise ReplaySessionLeaseFencedError("replay session lease has expired")
        return current


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value
