"""Fenced single-writer lease for one replay session. Not a worker pool."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)

REPLAY_SESSION_LEASE_SCHEMA_VERSION = "candlescope.replay-session-lease.v2"
_RESERVED_IDS = frozenset(
    {
        "*",
        "all",
        "any",
        "default",
        "global",
        "public",
        "shared",
        "wildcard",
    }
)
_SAFE_ID = frozenset("abcdefghijklmnopqrstuvwxyz0123456789._:-")


class ReplaySessionLeaseError(RuntimeError):
    """Base class for replay session ownership failures."""


class ReplaySessionLeaseBusyError(ReplaySessionLeaseError):
    """Another non-expired worker already owns the session."""


class ReplaySessionLeaseFencedError(ReplaySessionLeaseError):
    """A stale worker attempted to mutate or renew the session."""


class ReplaySessionSnapshotConflictError(ReplaySessionLeaseError):
    """Takeover tried to re-pin a session to a different snapshot."""


class ReplaySessionScopeConflictError(ReplaySessionLeaseError):
    """Takeover tried to move a session to a different organization or workspace."""


@dataclass(frozen=True, slots=True)
class ReplaySessionLease:
    """One fenced worker plus the immutable snapshot the session is pinned to."""

    session_id: str
    worker_id: str
    fencing_epoch: int
    lease_token: str
    lease_expires_at_ms: int
    snapshot: MarketDataSnapshotRef
    organization_id: str
    workspace_id: str
    schema_version: str = REPLAY_SESSION_LEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REPLAY_SESSION_LEASE_SCHEMA_VERSION:
            raise ValueError("replay session lease schema_version has drifted")
        object.__setattr__(
            self,
            "session_id",
            normalize_replay_session_id(self.session_id),
        )
        object.__setattr__(
            self,
            "worker_id",
            normalize_replay_worker_id(self.worker_id),
        )
        fencing_epoch = _non_negative_int(self.fencing_epoch, field="fencing_epoch")
        object.__setattr__(self, "fencing_epoch", fencing_epoch)
        object.__setattr__(
            self,
            "lease_expires_at_ms",
            _non_negative_int(self.lease_expires_at_ms, field="lease_expires_at_ms"),
        )
        try:
            token = str(uuid.UUID(self.lease_token))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("lease_token must be a UUID") from exc
        object.__setattr__(self, "lease_token", token)
        if not isinstance(self.snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        object.__setattr__(
            self,
            "organization_id",
            normalize_organization_id(self.organization_id),
        )
        object.__setattr__(
            self,
            "workspace_id",
            normalize_workspace_id(self.workspace_id),
        )

    def to_public_ref(self) -> dict[str, object]:
        snapshot = self.snapshot
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "worker_id": self.worker_id,
            "fencing_epoch": self.fencing_epoch,
            "lease_expires_at_ms": self.lease_expires_at_ms,
            "organization_id": self.organization_id,
            "workspace_id": self.workspace_id,
            "snapshot": {
                "data_epoch": snapshot.data_epoch,
                "snapshot_version": snapshot.snapshot_version,
                "manifest_uri": snapshot.manifest_uri,
                "manifest_sha256": snapshot.manifest_sha256,
            },
        }


@runtime_checkable
class ReplaySessionLeaseStore(Protocol):
    """Ownership port for one replay session."""

    async def acquire(
        self,
        *,
        session_id: str,
        worker_id: str,
        snapshot: MarketDataSnapshotRef,
        organization_id: str,
        workspace_id: str,
        lease_ttl_ms: int,
    ) -> ReplaySessionLease: ...

    async def renew(
        self,
        lease: ReplaySessionLease,
        *,
        lease_ttl_ms: int,
    ) -> ReplaySessionLease: ...

    async def release(self, lease: ReplaySessionLease) -> None: ...

    async def require_active(self, lease: ReplaySessionLease) -> ReplaySessionLease: ...


def normalize_replay_session_id(value: object) -> str:
    return _normalize_id(value, field="session_id")


def normalize_replay_worker_id(value: object) -> str:
    return _normalize_id(value, field="worker_id")


def require_write_fence(
    current: ReplaySessionLease,
    presented: ReplaySessionLease,
) -> None:
    """Reject stale epoch/token before any session mutation."""

    if not isinstance(current, ReplaySessionLease):
        raise TypeError("current must be a ReplaySessionLease")
    if not isinstance(presented, ReplaySessionLease):
        raise TypeError("presented must be a ReplaySessionLease")
    if (
        current.session_id,
        current.worker_id,
        current.fencing_epoch,
        current.lease_token,
    ) != (
        presented.session_id,
        presented.worker_id,
        presented.fencing_epoch,
        presented.lease_token,
    ):
        raise ReplaySessionLeaseFencedError(
            "replay session lease is stale or no longer owned"
        )
    if current.snapshot != presented.snapshot:
        raise ReplaySessionSnapshotConflictError(
            "replay session snapshot pin does not match the fenced lease"
        )
    if (current.organization_id, current.workspace_id) != (
        presented.organization_id,
        presented.workspace_id,
    ):
        raise ReplaySessionScopeConflictError(
            "replay session organization/workspace pin does not match the fenced lease"
        )


def _normalize_id(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    normalized = value.strip().lower()
    if (
        len(normalized) > 64
        or not normalized.isascii()
        or normalized[0] not in "abcdefghijklmnopqrstuvwxyz0123456789"
        or any(character not in _SAFE_ID for character in normalized)
    ):
        raise ValueError(f"{field} must use a bounded lower-case identifier")
    if normalized in _RESERVED_IDS:
        raise ValueError(f"{field} cannot be a wildcard or reserved name")
    return normalized


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


__all__ = [
    "REPLAY_SESSION_LEASE_SCHEMA_VERSION",
    "ReplaySessionLease",
    "ReplaySessionLeaseBusyError",
    "ReplaySessionLeaseError",
    "ReplaySessionLeaseFencedError",
    "ReplaySessionLeaseStore",
    "ReplaySessionScopeConflictError",
    "ReplaySessionSnapshotConflictError",
    "normalize_replay_session_id",
    "normalize_replay_worker_id",
    "require_write_fence",
]
