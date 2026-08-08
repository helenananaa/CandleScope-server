"""Deterministic lease-store test double with production-equivalent fencing."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import replace

from app.server_contracts import MarketEventEnvelopeV1
from app.server_runtime.leases import (
    StreamCheckpointError,
    StreamLease,
    StreamLeaseBusyError,
    StreamLeaseFencedError,
    require_same_envelope,
    require_stageable_envelope,
)


class InMemoryStreamLeaseStore:
    """Model the PostgreSQL lease state machine without claiming durability."""

    def __init__(self, *, clock_ms: Callable[[], int] | None = None) -> None:
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._leases: dict[str, StreamLease] = {}
        self._lock = asyncio.Lock()

    async def acquire(
        self,
        *,
        partition_key: str,
        owner_id: str,
        lease_ttl_ms: int,
    ) -> StreamLease:
        partition_key = _required_text(partition_key, field="partition_key")
        owner_id = _required_text(owner_id, field="owner_id")
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        async with self._lock:
            now = self._clock_ms()
            current = self._leases.get(partition_key)
            if current is not None and current.lease_expires_at_ms > now:
                raise StreamLeaseBusyError(
                    f"stream {partition_key!r} is leased by {current.owner_id!r}"
                )
            epoch = 0 if current is None else current.producer_epoch + 1
            acquired = StreamLease(
                partition_key=partition_key,
                owner_id=owner_id,
                producer_epoch=epoch,
                lease_token=str(uuid.uuid4()),
                lease_expires_at_ms=now + lease_ttl_ms,
                last_sequence=current.last_sequence if current else None,
                last_event_id=current.last_event_id if current else None,
                last_partition_offset=(
                    current.last_partition_offset if current else None
                ),
                pending_envelope=current.pending_envelope if current else None,
            )
            self._leases[partition_key] = acquired
            return acquired

    async def renew(
        self,
        lease: StreamLease,
        *,
        lease_ttl_ms: int,
    ) -> StreamLease:
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        async with self._lock:
            current = self._require_active(lease)
            renewed = replace(
                current,
                lease_expires_at_ms=self._clock_ms() + lease_ttl_ms,
            )
            self._leases[lease.partition_key] = renewed
            return renewed

    async def stage_pending(
        self,
        lease: StreamLease,
        envelope: MarketEventEnvelopeV1,
    ) -> StreamLease:
        if not isinstance(envelope, MarketEventEnvelopeV1):
            raise TypeError("envelope must be a MarketEventEnvelopeV1")
        async with self._lock:
            current = self._require_active(lease)
            require_stageable_envelope(current, envelope)
            if current.pending_envelope is not None:
                require_same_envelope(current.pending_envelope, envelope)
                return current
            staged = replace(current, pending_envelope=envelope)
            self._leases[lease.partition_key] = staged
            return staged

    async def checkpoint(
        self,
        lease: StreamLease,
        envelope: MarketEventEnvelopeV1,
        *,
        partition_offset: int,
    ) -> StreamLease:
        if not isinstance(envelope, MarketEventEnvelopeV1):
            raise TypeError("envelope must be a MarketEventEnvelopeV1")
        partition_offset = _non_negative_int(
            partition_offset,
            field="partition_offset",
        )
        async with self._lock:
            current = self._require_active(lease)
            if (
                current.pending_envelope is None
                and current.last_sequence == envelope.sequence_end
                and current.last_event_id == envelope.event_id
                and current.last_partition_offset == partition_offset
            ):
                return current
            if current.pending_envelope is None:
                raise StreamCheckpointError(
                    "no staged event is available to checkpoint"
                )
            require_same_envelope(current.pending_envelope, envelope)
            if (
                current.last_partition_offset is not None
                and partition_offset <= current.last_partition_offset
            ):
                raise StreamCheckpointError(
                    "partition offset must advance beyond the durable checkpoint"
                )
            checkpointed = replace(
                current,
                last_sequence=envelope.sequence_end,
                last_event_id=envelope.event_id,
                last_partition_offset=partition_offset,
                pending_envelope=None,
            )
            self._leases[lease.partition_key] = checkpointed
            return checkpointed

    async def release(self, lease: StreamLease) -> None:
        async with self._lock:
            current = self._require_fence(lease)
            self._leases[lease.partition_key] = replace(
                current,
                lease_expires_at_ms=self._clock_ms(),
            )

    async def inspect(self, partition_key: str) -> StreamLease | None:
        """Return the current immutable state for assertions only."""

        async with self._lock:
            return self._leases.get(partition_key)

    def _require_fence(self, lease: StreamLease) -> StreamLease:
        if not isinstance(lease, StreamLease):
            raise TypeError("lease must be a StreamLease")
        current = self._leases.get(lease.partition_key)
        if current is None or (
            current.owner_id,
            current.producer_epoch,
            current.lease_token,
        ) != (lease.owner_id, lease.producer_epoch, lease.lease_token):
            raise StreamLeaseFencedError("stream lease is stale or no longer owned")
        return current

    def _require_active(self, lease: StreamLease) -> StreamLease:
        current = self._require_fence(lease)
        if current.lease_expires_at_ms <= self._clock_ms():
            raise StreamLeaseFencedError("stream lease has expired")
        return current


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _positive_int(value: object, *, field: str) -> int:
    value = _non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be positive")
    return value
