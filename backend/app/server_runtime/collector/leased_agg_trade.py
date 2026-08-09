"""Durably leased coordinator for the one Phase 1B aggregate-trade stream."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace

from app.data_engine.ingestion.models import MarketEvent
from app.server_contracts import (
    MarketEventEnvelopeV1,
    MarketEventPublisher,
    PublishReceipt,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.leases import StreamLease, StreamLeaseError, StreamLeaseStore
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import PHASE1B_PARTITION_KEY

from .agg_trade import AggTradeCollector


class LeasedCollectorFailedError(RuntimeError):
    """Raised after ownership or durable checkpoint state becomes ambiguous."""


class LeasedAggTradeCollector:
    """Fence, stage, publish, then checkpoint one Binance aggTrade event."""

    def __init__(
        self,
        *,
        lease_store: StreamLeaseStore,
        lease: StreamLease,
        publisher: MarketEventPublisher,
        lease_ttl_ms: int,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if not isinstance(lease, StreamLease):
            raise TypeError("lease must be a StreamLease")
        if lease.partition_key != PHASE1B_PARTITION_KEY:
            raise ValueError("lease must own the frozen Phase 1B partition")
        if isinstance(lease_ttl_ms, bool) or not isinstance(lease_ttl_ms, int):
            raise TypeError("lease_ttl_ms must be an integer")
        if lease_ttl_ms <= 0:
            raise ValueError("lease_ttl_ms must be positive")
        self._lease_store = lease_store
        self._lease = lease
        self._lease_ttl_ms = lease_ttl_ms
        self._terminal_error: BaseException | None = None
        # Event publication is serialized, while lease renewal deliberately is not.
        # A broker round trip may stall for longer than one heartbeat interval.
        self._lock = asyncio.Lock()
        self._lease_state_lock = asyncio.Lock()
        self._renew_lock = asyncio.Lock()
        self._collector = AggTradeCollector(
            adapter=AggTradeEnvelopeAdapter(
                ProducerIdentity(lease.owner_id, lease.producer_epoch)
            ),
            publisher=publisher,
            previous_sequence=lease.last_sequence,
            initial_pending=lease.pending_envelope,
            before_publish=self._stage_pending,
            clock_ms=clock_ms,
        )

    @classmethod
    async def acquire(
        cls,
        *,
        lease_store: StreamLeaseStore,
        publisher: MarketEventPublisher,
        owner_id: str,
        lease_ttl_ms: int,
        clock_ms: Callable[[], int] | None = None,
    ) -> LeasedAggTradeCollector:
        lease = await lease_store.acquire(
            partition_key=PHASE1B_PARTITION_KEY,
            owner_id=owner_id,
            lease_ttl_ms=lease_ttl_ms,
        )
        return cls(
            lease_store=lease_store,
            lease=lease,
            publisher=publisher,
            lease_ttl_ms=lease_ttl_ms,
            clock_ms=clock_ms,
        )

    @property
    def lease(self) -> StreamLease:
        return self._lease

    @property
    def failed(self) -> bool:
        return self._terminal_error is not None

    @property
    def terminal_error(self) -> BaseException | None:
        return self._terminal_error

    @property
    def pending_event_id(self) -> str | None:
        return self._collector.pending_event_id

    async def handle(
        self,
        event: MarketEvent,
        *,
        published_at_ms: int | None = None,
    ) -> PublishReceipt:
        async with self._lock:
            self._require_healthy()
            try:
                receipt = await self._collector.handle(
                    event,
                    published_at_ms=published_at_ms,
                )
            except StreamLeaseError as exc:
                self._mark_failed(exc)
                raise
            # A heartbeat can fence this collector while Kafka is accepting the
            # event. Never checkpoint after ownership has become ambiguous.
            self._require_healthy()
            envelope = self._collector.last_envelope
            if envelope is None:  # pragma: no cover - valid receipt sets it
                error = RuntimeError("collector acknowledged no envelope")
                self._mark_failed(error)
                raise error
            offset = dict(receipt.partition_offsets)[PHASE1B_PARTITION_KEY]
            lease = await self._lease_snapshot()
            if (
                lease.pending_envelope is None
                and lease.last_event_id == envelope.event_id
                and lease.last_sequence == envelope.sequence_end
                and lease.last_partition_offset == offset
            ):
                return receipt
            try:
                checkpointed = await self._lease_store.checkpoint(
                    lease,
                    envelope,
                    partition_offset=offset,
                )
            except BaseException as exc:
                self._mark_failed(exc)
                raise
            await self._replace_lease(checkpointed)
            return receipt

    async def renew(self) -> StreamLease:
        """Renew ownership independently from a possibly blocked publication."""

        async with self._renew_lock:
            self._require_healthy()
            lease = await self._lease_snapshot()
            try:
                renewed = await self._lease_store.renew(
                    lease,
                    lease_ttl_ms=self._lease_ttl_ms,
                )
            except BaseException as exc:
                self._mark_failed(exc)
                raise
            return await self._merge_renewal(renewed)

    async def release(self) -> None:
        async with self._lock, self._renew_lock:
            self._require_healthy()
            lease = await self._lease_snapshot()
            try:
                await self._lease_store.release(lease)
            except BaseException as exc:
                self._mark_failed(exc)
                raise

    async def _stage_pending(self, envelope: MarketEventEnvelopeV1) -> None:
        lease = await self._lease_snapshot()
        try:
            staged = await self._lease_store.stage_pending(
                lease,
                envelope,
            )
        except BaseException as exc:
            self._mark_failed(exc)
            raise
        await self._replace_lease(staged)

    async def _lease_snapshot(self) -> StreamLease:
        async with self._lease_state_lock:
            return self._lease

    async def _replace_lease(self, lease: StreamLease) -> None:
        async with self._lease_state_lock:
            self._lease = lease

    async def _merge_renewal(self, renewed: StreamLease) -> StreamLease:
        async with self._lease_state_lock:
            current = self._lease
            if (
                current.lease_token != renewed.lease_token
                or current.producer_epoch != renewed.producer_epoch
            ):
                error = LeasedCollectorFailedError(
                    "renewal returned a different ownership fence"
                )
                self._mark_failed(error)
                raise error
            # A stage/checkpoint can finish while renew() is awaiting storage.
            # Only the expiry belongs to the renewal; keep newer durable fields.
            self._lease = replace(
                current,
                lease_expires_at_ms=renewed.lease_expires_at_ms,
            )
            return self._lease

    def _mark_failed(self, error: BaseException) -> None:
        if self._terminal_error is None:
            self._terminal_error = error

    def _require_healthy(self) -> None:
        if self._terminal_error is not None:
            raise LeasedCollectorFailedError(
                "collector is terminal after a durable ownership failure"
            ) from self._terminal_error
