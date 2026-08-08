from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from app.data_engine.ingestion.config import IngestionConfig
from app.data_engine.ingestion.models import (
    DataSource,
    MarketEvent,
    RawMessage,
    StreamDescriptor,
    StreamType,
)
from app.exchanges.plugins.binance.normalizer import BinanceNormalizer
from app.server_contracts import PublishReceipt
from app.server_runtime import ProducerIdentity
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.collector import (
    AggTradeCollector,
    CollectorContinuityError,
    CollectorIntegrityError,
    InvalidPublishReceiptError,
    PendingPublishError,
)
from app.server_runtime.testing import (
    InMemoryMarketEventLog,
    MarketEventIdentityConflictError,
)

EVENT_TIME_MS = 1_700_000_000_010
TRADE_TIME_MS = 1_700_000_000_000
RECEIVED_AT_MS = 1_700_000_000_020
PUBLISHED_AT_MS = 1_700_000_000_030


def _event(
    agg_trade_id: int = 42,
    *,
    price_text: str = "100000.1000",
    quantity_text: str = "0.02500000",
    event_type: StreamType = StreamType.AGG_TRADE,
    exchange: str = "binance",
    market_type: str = "futures",
    symbol: str = "BTCUSDT",
    source: DataSource = DataSource.WEBSOCKET,
) -> MarketEvent:
    return MarketEvent(
        event_type=event_type,
        symbol=symbol,
        exchange=exchange,
        event_time_ms=EVENT_TIME_MS,
        received_at_ms=RECEIVED_AT_MS,
        source=source,
        data={
            "agg_trade_id": agg_trade_id,
            "price": float(price_text),
            "quantity": float(quantity_text),
            "price_text": price_text,
            "quantity_text": quantity_text,
            "first_trade_id": agg_trade_id * 10,
            "last_trade_id": agg_trade_id * 10 + 2,
            "trade_time_ms": TRADE_TIME_MS,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=agg_trade_id,
        market_type=market_type,
    )


def _adapter(epoch: int = 7) -> AggTradeEnvelopeAdapter:
    return AggTradeEnvelopeAdapter(
        ProducerIdentity("collector-binance-futures-01", epoch)
    )


def _raw_message(payload: dict[str, Any], source: DataSource) -> RawMessage:
    return RawMessage(
        payload=payload,
        source=source,
        stream_type=StreamType.AGG_TRADE,
        received_at_ms=RECEIVED_AT_MS,
    )


@pytest.mark.parametrize(
    ("source", "payload"),
    [
        (
            DataSource.WEBSOCKET,
            {
                "e": "aggTrade",
                "E": EVENT_TIME_MS,
                "a": 42,
                "p": "100000.1000",
                "q": "0.02500000",
                "f": 420,
                "l": 422,
                "T": TRADE_TIME_MS,
                "m": False,
            },
        ),
        (
            DataSource.HTTP_BACKFILL,
            {
                "a": 42,
                "p": "100000.1000",
                "q": "0.02500000",
                "f": 420,
                "l": 422,
                "T": TRADE_TIME_MS,
                "m": False,
            },
        ),
    ],
)
def test_binance_agg_trade_normalizer_preserves_exact_decimal_text(
    source: DataSource,
    payload: dict[str, Any],
) -> None:
    normalizer = BinanceNormalizer(
        IngestionConfig(proxy_mode="none"),
        StreamDescriptor(
            "BTCUSDT",
            StreamType.AGG_TRADE,
            market_type="futures",
        ),
    )

    event = normalizer.parse(_raw_message(payload, source))

    assert event is not None
    assert event.data["price"] == 100000.1
    assert event.data["quantity"] == 0.025
    assert event.data["price_text"] == "100000.1000"
    assert event.data["quantity_text"] == "0.02500000"


def test_agg_trade_adapter_builds_frozen_wire_mapping() -> None:
    envelope = _adapter().adapt(
        _event(),
        previous_sequence=41,
        published_at_ms=PUBLISHED_AT_MS,
    )

    assert envelope.event_id == "282a4d5e-df27-5bf7-a69f-a17c88ec6341"
    assert uuid.UUID(envelope.event_id).version == 5
    assert envelope.partition_key == "binance:futures:BTCUSDT@agg_trade"
    assert envelope.source == "websocket"
    assert envelope.source_event_id == "42"
    assert envelope.sequence_start == 42
    assert envelope.sequence_end == 42
    assert envelope.previous_sequence == 41
    assert envelope.producer_id == "collector-binance-futures-01"
    assert envelope.producer_epoch == 7
    assert dict(envelope.payload) == {
        "agg_trade_id": 42,
        "buyer_is_maker": False,
        "first_trade_id": 420,
        "last_trade_id": 422,
        "price": "100000.1000",
        "quantity": "0.02500000",
        "trade_time_ms": TRADE_TIME_MS,
    }


def test_agg_trade_adapter_event_id_is_stable_across_producer_epochs() -> None:
    first = _adapter(7).adapt(
        _event(),
        previous_sequence=41,
        published_at_ms=PUBLISHED_AT_MS,
    )
    restarted = _adapter(8).adapt(
        _event(),
        previous_sequence=41,
        published_at_ms=PUBLISHED_AT_MS + 1,
    )

    assert restarted.event_id == first.event_id
    assert restarted.producer_epoch == 8


@pytest.mark.parametrize(
    "event",
    [
        _event(event_type=StreamType.TRADE),
        _event(exchange="okx"),
        _event(market_type="spot"),
        _event(symbol="ETHUSDT"),
        _event(source=DataSource.MOCK),
    ],
)
def test_agg_trade_adapter_rejects_events_outside_phase1a(event: MarketEvent) -> None:
    with pytest.raises(ValueError):
        _adapter().adapt(
            event,
            previous_sequence=None,
            published_at_ms=PUBLISHED_AT_MS,
        )


def test_agg_trade_adapter_requires_exact_decimal_fields() -> None:
    event = _event()
    event.data.pop("price_text")

    with pytest.raises(TypeError, match="price_text"):
        _adapter().adapt(
            event,
            previous_sequence=None,
            published_at_ms=PUBLISHED_AT_MS,
        )


def test_agg_trade_adapter_rejects_decimal_and_legacy_value_mismatch() -> None:
    event = _event()
    event.data["price_text"] = "100000.11"

    with pytest.raises(ValueError, match="does not match"):
        _adapter().adapt(
            event,
            previous_sequence=None,
            published_at_ms=PUBLISHED_AT_MS,
        )


def test_agg_trade_adapter_requires_sequence_identity_match() -> None:
    event = _event()
    event.sequence = 43

    with pytest.raises(ValueError, match="event.sequence"):
        _adapter().adapt(
            event,
            previous_sequence=None,
            published_at_ms=PUBLISHED_AT_MS,
        )


def test_collector_publishes_contiguous_events_and_deduplicates_last_event() -> None:
    async def run() -> None:
        event_log = InMemoryMarketEventLog()
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=event_log,
            previous_sequence=41,
        )

        first = await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)
        second = await collector.handle(_event(43), published_at_ms=PUBLISHED_AT_MS + 1)
        duplicate = await collector.handle(
            _event(43),
            published_at_ms=PUBLISHED_AT_MS + 999,
        )

        assert first.partition_offsets == (("binance:futures:BTCUSDT@agg_trade", 0),)
        assert second.partition_offsets == (("binance:futures:BTCUSDT@agg_trade", 1),)
        assert duplicate == second
        assert collector.last_sequence == 43
        assert collector.pending_event_id is None
        assert len(event_log.events) == 2
        assert event_log.events[1].previous_sequence == 42

    asyncio.run(run())


def test_collector_fails_closed_on_gap_without_publication() -> None:
    async def run() -> None:
        event_log = InMemoryMarketEventLog()
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=event_log,
            previous_sequence=41,
        )
        await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)

        with pytest.raises(CollectorContinuityError, match="gap"):
            await collector.handle(_event(44), published_at_ms=PUBLISHED_AT_MS + 1)

        assert collector.last_sequence == 42
        assert len(event_log.events) == 1

    asyncio.run(run())


def test_collector_fails_closed_on_regression() -> None:
    async def run() -> None:
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=InMemoryMarketEventLog(),
            previous_sequence=42,
        )

        with pytest.raises(CollectorContinuityError, match="regressed"):
            await collector.handle(_event(41), published_at_ms=PUBLISHED_AT_MS)

    asyncio.run(run())


def test_collector_rejects_conflicting_duplicate_identity() -> None:
    async def run() -> None:
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=InMemoryMarketEventLog(),
            previous_sequence=41,
        )
        await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)

        with pytest.raises(CollectorIntegrityError):
            await collector.handle(
                _event(42, price_text="100000.1100"),
                published_at_ms=PUBLISHED_AT_MS + 1,
            )

    asyncio.run(run())


def test_collector_retries_the_exact_pending_envelope_after_lost_receipt() -> None:
    class AcceptThenTimeout:
        def __init__(self) -> None:
            self.inner = InMemoryMarketEventLog()
            self.attempts: list[dict[str, Any]] = []

        async def publish(self, events: Any) -> PublishReceipt:
            event = events[0]
            self.attempts.append(event.to_wire())
            receipt = await self.inner.publish(events)
            if len(self.attempts) == 1:
                raise TimeoutError("receipt lost after durable accept")
            return receipt

    async def run() -> None:
        publisher = AcceptThenTimeout()
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=publisher,
            previous_sequence=41,
        )

        with pytest.raises(TimeoutError):
            await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)
        assert collector.last_sequence == 41
        assert collector.pending_event_id is not None

        receipt = await collector.handle(
            _event(42),
            published_at_ms=PUBLISHED_AT_MS + 999,
        )

        assert receipt.accepted_count == 1
        assert publisher.attempts[0] == publisher.attempts[1]
        assert len(publisher.inner.events) == 1
        assert collector.last_sequence == 42
        assert collector.pending_event_id is None

    asyncio.run(run())


def test_collector_rejects_a_different_event_while_publish_is_pending() -> None:
    class AlwaysTimeout:
        async def publish(self, events: Any) -> PublishReceipt:
            raise TimeoutError("unavailable")

    async def run() -> None:
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=AlwaysTimeout(),
            previous_sequence=41,
        )
        with pytest.raises(TimeoutError):
            await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)

        with pytest.raises(PendingPublishError):
            await collector.handle(_event(43), published_at_ms=PUBLISHED_AT_MS + 1)

    asyncio.run(run())


def test_collector_keeps_event_pending_on_invalid_receipt() -> None:
    class InvalidReceiptPublisher:
        async def publish(self, events: Any) -> PublishReceipt:
            return PublishReceipt(accepted_count=0, partition_offsets=())

    async def run() -> None:
        collector = AggTradeCollector(
            adapter=_adapter(),
            publisher=InvalidReceiptPublisher(),
            previous_sequence=41,
        )

        with pytest.raises(InvalidPublishReceiptError):
            await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)

        assert collector.last_sequence == 41
        assert collector.pending_event_id is not None

    asyncio.run(run())


def test_in_memory_event_log_is_idempotent_and_rejects_identity_conflict() -> None:
    async def run() -> None:
        event_log = InMemoryMarketEventLog()
        original = _adapter().adapt(
            _event(42),
            previous_sequence=41,
            published_at_ms=PUBLISHED_AT_MS,
        )
        conflicting = _adapter().adapt(
            _event(42, quantity_text="0.02600000"),
            previous_sequence=41,
            published_at_ms=PUBLISHED_AT_MS,
        )

        first = await event_log.publish((original,))
        retry = await event_log.publish((original, original))
        with pytest.raises(MarketEventIdentityConflictError):
            await event_log.publish((conflicting,))

        assert first.accepted_count == 1
        assert retry.accepted_count == 2
        assert first.partition_offsets == retry.partition_offsets
        assert len(event_log.events) == 1

    asyncio.run(run())


def test_producer_identity_is_strict() -> None:
    assert ProducerIdentity(" collector-1 ", 0).producer_id == "collector-1"
    with pytest.raises(ValueError):
        ProducerIdentity(" ", 0)
    with pytest.raises(TypeError):
        ProducerIdentity("collector-1", True)  # type: ignore[arg-type]
