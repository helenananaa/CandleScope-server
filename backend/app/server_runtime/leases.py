"""Durable single-writer lease and checkpoint contracts for market streams."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from app.server_contracts import MarketEventEnvelopeV1


class StreamLeaseError(RuntimeError):
    """Base class for durable stream ownership failures."""


class StreamLeaseBusyError(StreamLeaseError):
    """Raised when another non-expired owner holds the stream lease."""


class StreamLeaseFencedError(StreamLeaseError):
    """Raised when a stale owner attempts to mutate durable stream state."""


class StreamCheckpointError(StreamLeaseError):
    """Raised when pending or acknowledged continuity would be corrupted."""


@dataclass(frozen=True, slots=True)
class StreamLease:
    """One fenced owner plus its durable publish/checkpoint state."""

    partition_key: str
    owner_id: str
    producer_epoch: int
    lease_token: str
    lease_expires_at_ms: int
    last_sequence: int | None = None
    last_event_id: str | None = None
    last_partition_offset: int | None = None
    pending_envelope: MarketEventEnvelopeV1 | None = None

    def __post_init__(self) -> None:
        for field in ("partition_key", "owner_id"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a non-blank string")
            object.__setattr__(self, field, value.strip())
        try:
            token = str(uuid.UUID(self.lease_token))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("lease_token must be a UUID") from exc
        object.__setattr__(self, "lease_token", token)
        for field in ("producer_epoch", "lease_expires_at_ms"):
            _require_non_negative_int(getattr(self, field), field=field)
        optional_ints = ("last_sequence", "last_partition_offset")
        for field in optional_ints:
            value = getattr(self, field)
            if value is not None:
                _require_non_negative_int(value, field=field)
        if (self.last_sequence is None) != (self.last_event_id is None):
            raise ValueError("last_sequence and last_event_id must be present together")
        if self.last_event_id is not None:
            try:
                normalized = str(uuid.UUID(self.last_event_id))
            except (AttributeError, TypeError, ValueError) as exc:
                raise ValueError("last_event_id must be a UUID") from exc
            object.__setattr__(self, "last_event_id", normalized)
        if self.last_sequence is None and self.last_partition_offset is not None:
            raise ValueError("last_partition_offset requires a last_sequence")
        pending = self.pending_envelope
        if pending is not None:
            if not isinstance(pending, MarketEventEnvelopeV1):
                raise TypeError("pending_envelope must be a MarketEventEnvelopeV1")
            if pending.partition_key != self.partition_key:
                raise ValueError("pending_envelope must match the leased partition")
            if pending.previous_sequence != self.last_sequence:
                raise ValueError("pending_envelope must follow the durable checkpoint")


@runtime_checkable
class StreamLeaseStore(Protocol):
    """Persistence port for lease fencing and pre-publish intent."""

    async def acquire(
        self,
        *,
        partition_key: str,
        owner_id: str,
        lease_ttl_ms: int,
    ) -> StreamLease: ...

    async def renew(
        self,
        lease: StreamLease,
        *,
        lease_ttl_ms: int,
    ) -> StreamLease: ...

    async def stage_pending(
        self,
        lease: StreamLease,
        envelope: MarketEventEnvelopeV1,
    ) -> StreamLease: ...

    async def checkpoint(
        self,
        lease: StreamLease,
        envelope: MarketEventEnvelopeV1,
        *,
        partition_offset: int,
    ) -> StreamLease: ...

    async def release(self, lease: StreamLease) -> None: ...


def require_stageable_envelope(
    lease: StreamLease,
    envelope: MarketEventEnvelopeV1,
) -> None:
    """Validate the only Phase 1B transition allowed before publication."""

    if envelope.partition_key != lease.partition_key:
        raise StreamCheckpointError("event does not belong to the leased partition")
    if envelope.sequence_start is None or envelope.sequence_end is None:
        raise StreamCheckpointError("checkpointed events require a sequence")
    if envelope.sequence_start != envelope.sequence_end:
        raise StreamCheckpointError("Phase 1B only accepts one sequence per event")
    if envelope.previous_sequence != lease.last_sequence:
        raise StreamCheckpointError("event does not follow the durable checkpoint")
    if (
        lease.last_sequence is not None
        and envelope.sequence_start != lease.last_sequence + 1
    ):
        raise StreamCheckpointError("event sequence is not contiguous")
    if lease.pending_envelope is None and (
        envelope.producer_id != lease.owner_id
        or envelope.producer_epoch != lease.producer_epoch
    ):
        raise StreamCheckpointError("new pending event has a stale producer fence")


def require_same_envelope(
    expected: MarketEventEnvelopeV1,
    candidate: MarketEventEnvelopeV1,
) -> None:
    """Fail closed if one durable event identity changes any wire field."""

    if expected.to_wire() != candidate.to_wire():
        raise StreamCheckpointError("pending event identity has conflicting content")


def _require_non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value
