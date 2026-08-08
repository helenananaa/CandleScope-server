"""Durably leased coordinator for the one Phase 1B aggregate-trade stream."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

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
        self._lock = asyncio.Lock()
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
                self._terminal_error = exc
                raise
            envelope = self._collector.last_envelope
            if envelope is None:  # pragma: no cover - valid receipt sets it
                error = RuntimeError("collector acknowledged no envelope")
                self._terminal_error = error
                raise error
            offset = dict(receipt.partition_offsets)[PHASE1B_PARTITION_KEY]
            if (
                self._lease.pending_envelope is None
                and self._lease.last_event_id == envelope.event_id
                and self._lease.last_sequence == envelope.sequence_end
                and self._lease.last_partition_offset == offset
            ):
                return receipt
            try:
                self._lease = await self._lease_store.checkpoint(
                    self._lease,
                    envelope,
                    partition_offset=offset,
                )
            except BaseException as exc:
                self._terminal_error = exc
                raise
            return receipt

    async def renew(self) -> StreamLease:
        async with self._lock:
            self._require_healthy()
            try:
                self._lease = await self._lease_store.renew(
                    self._lease,
                    lease_ttl_ms=self._lease_ttl_ms,
                )
            except BaseException as exc:
                self._terminal_error = exc
                raise
            return self._lease

    async def release(self) -> None:
        async with self._lock:
            self._require_healthy()
            try:
                await self._lease_store.release(self._lease)
            except BaseException as exc:
                self._terminal_error = exc
                raise

    async def _stage_pending(self, envelope: MarketEventEnvelopeV1) -> None:
        try:
            self._lease = await self._lease_store.stage_pending(
                self._lease,
                envelope,
            )
        except BaseException as exc:
            self._terminal_error = exc
            raise

    def _require_healthy(self) -> None:
        if self._terminal_error is not None:
            raise LeasedCollectorFailedError(
                "collector is terminal after a durable ownership failure"
            ) from self._terminal_error
