"""Single-stream Phase 1A collector with fail-closed continuity semantics."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from app.data_engine.ingestion.models import MarketEvent
from app.server_contracts import (
    MarketEventEnvelopeV1,
    MarketEventPublisher,
    PublishReceipt,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity


class CollectorContinuityError(RuntimeError):
    """Raised before publication when the aggregate-trade sequence is unsafe."""


class CollectorIntegrityError(RuntimeError):
    """Raised when one durable event identity resolves to different content."""


class PendingPublishError(RuntimeError):
    """Raised when a different event arrives while an acknowledgement is pending."""


class InvalidPublishReceiptError(RuntimeError):
    """Raised when a publisher does not acknowledge the exact single event."""


class AggTradeCollector:
    """Serialize, publish, and checkpoint one Binance BTCUSDT aggTrade stream.

    The in-memory checkpoint is deliberately only a Phase 1A seam. A durable
    producer lease and checkpoint store must replace it before a server runtime
    can be declared deployable.
    """

    def __init__(
        self,
        *,
        adapter: AggTradeEnvelopeAdapter,
        publisher: MarketEventPublisher,
        previous_sequence: int | None = None,
        initial_pending: MarketEventEnvelopeV1 | None = None,
        before_publish: (
            Callable[[MarketEventEnvelopeV1], Awaitable[None]] | None
        ) = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if previous_sequence is not None and (
            isinstance(previous_sequence, bool)
            or not isinstance(previous_sequence, int)
            or previous_sequence < 0
        ):
            raise ValueError("previous_sequence must be a non-negative integer")
        self._adapter = adapter
        self._publisher = publisher
        self._last_sequence = previous_sequence
        self._last_envelope: MarketEventEnvelopeV1 | None = None
        self._last_receipt: PublishReceipt | None = None
        if initial_pending is not None:
            if not isinstance(initial_pending, MarketEventEnvelopeV1):
                raise TypeError("initial_pending must be a MarketEventEnvelopeV1")
            if initial_pending.previous_sequence != previous_sequence:
                raise ValueError("initial_pending must follow previous_sequence")
        self._pending = initial_pending
        self._before_publish = before_publish
        self._clock_ms = clock_ms or _system_clock_ms
        self._lock = asyncio.Lock()

    @property
    def last_sequence(self) -> int | None:
        return self._last_sequence

    @property
    def pending_event_id(self) -> str | None:
        return self._pending.event_id if self._pending is not None else None

    @property
    def last_envelope(self) -> MarketEventEnvelopeV1 | None:
        return self._last_envelope

    async def handle(
        self,
        event: MarketEvent,
        *,
        published_at_ms: int | None = None,
    ) -> PublishReceipt:
        async with self._lock:
            sequence = self._adapter.sequence(event)
            if self._pending is not None:
                if sequence != self._pending.sequence_start:
                    raise PendingPublishError(
                        "a different event arrived before the pending publish was acknowledged"
                    )
                pending_adapter = AggTradeEnvelopeAdapter(
                    ProducerIdentity(
                        self._pending.producer_id,
                        self._pending.producer_epoch,
                    )
                )
                candidate = pending_adapter.adapt(
                    event,
                    published_at_ms=self._pending.published_at_ms,
                    previous_sequence=self._pending.previous_sequence,
                )
                _require_same_envelope(candidate, self._pending)
                return await self._publish_pending()

            if self._last_envelope is not None and sequence == self._last_sequence:
                candidate = self._adapter.adapt(
                    event,
                    published_at_ms=self._last_envelope.published_at_ms,
                    previous_sequence=self._last_envelope.previous_sequence,
                )
                _require_same_envelope(candidate, self._last_envelope)
                if self._last_receipt is None:
                    raise InvalidPublishReceiptError(
                        "the last acknowledged event has no retained receipt"
                    )
                return self._last_receipt

            previous_sequence = self._last_sequence
            if previous_sequence is not None:
                if sequence <= previous_sequence:
                    raise CollectorContinuityError(
                        f"aggTrade sequence regressed from {previous_sequence} to {sequence}"
                    )
                if sequence != previous_sequence + 1:
                    raise CollectorContinuityError(
                        f"aggTrade gap after {previous_sequence}: received {sequence}"
                    )

            published_at_ms = (
                self._clock_ms() if published_at_ms is None else published_at_ms
            )
            self._pending = self._adapter.adapt(
                event,
                published_at_ms=published_at_ms,
                previous_sequence=previous_sequence,
            )
            return await self._publish_pending()

    async def _publish_pending(self) -> PublishReceipt:
        if self._pending is None:
            raise RuntimeError("no pending event to publish")
        pending = self._pending
        if self._before_publish is not None:
            await self._before_publish(pending)
        receipt = await self._publisher.publish((pending,))
        _validate_receipt(pending, receipt)
        self._last_sequence = pending.sequence_end
        self._last_envelope = pending
        self._last_receipt = receipt
        self._pending = None
        return receipt


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _require_same_envelope(
    candidate: MarketEventEnvelopeV1,
    expected: MarketEventEnvelopeV1,
) -> None:
    if candidate.to_wire() != expected.to_wire():
        raise CollectorIntegrityError(
            f"event identity {expected.event_id} resolved to different envelope content"
        )


def _validate_receipt(
    event: MarketEventEnvelopeV1,
    receipt: PublishReceipt,
) -> None:
    if not isinstance(receipt, PublishReceipt):
        raise InvalidPublishReceiptError("publisher returned an invalid receipt type")
    if receipt.accepted_count != 1:
        raise InvalidPublishReceiptError("publisher did not acknowledge one event")
    if len(receipt.partition_offsets) != 1:
        raise InvalidPublishReceiptError(
            "publisher returned an ambiguous partition receipt"
        )
    partition_key, offset = receipt.partition_offsets[0]
    if partition_key != event.partition_key:
        raise InvalidPublishReceiptError("publisher acknowledged the wrong partition")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise InvalidPublishReceiptError("publisher offset must be non-negative")
