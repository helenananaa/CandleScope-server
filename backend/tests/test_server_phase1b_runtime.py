from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_runtime import (
    ProducerIdentity,
    StreamCheckpointError,
    StreamLeaseBusyError,
    StreamLeaseFencedError,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.collector import (
    LeasedAggTradeCollector,
    LeasedCollectorFailedError,
)
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    PHASE1B_PARTITION_KEY,
    KafkaMarketEventPublisher,
    KafkaPublisherStateError,
    KafkaTopicContractError,
    canonical_envelope_bytes,
)
from app.server_runtime.testing import (
    InMemoryMarketEventLog,
    InMemoryStreamLeaseStore,
)

EVENT_TIME_MS = 1_700_000_000_010
RECEIVED_AT_MS = 1_700_000_000_020
PUBLISHED_AT_MS = 1_700_000_000_030


def _event(agg_trade_id: int = 42, *, quantity: str = "0.02500000") -> MarketEvent:
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=EVENT_TIME_MS,
        received_at_ms=RECEIVED_AT_MS,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": agg_trade_id,
            "price": 100000.1,
            "quantity": float(quantity),
            "price_text": "100000.1000",
            "quantity_text": quantity,
            "first_trade_id": agg_trade_id * 10,
            "last_trade_id": agg_trade_id * 10 + 2,
            "trade_time_ms": EVENT_TIME_MS - 10,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=agg_trade_id,
        market_type="futures",
    )


def _envelope(
    *,
    owner: str,
    epoch: int,
    sequence: int = 42,
    previous_sequence: int | None = None,
    quantity: str = "0.02500000",
) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity(owner, epoch)).adapt(
        _event(sequence, quantity=quantity),
        previous_sequence=previous_sequence,
        published_at_ms=PUBLISHED_AT_MS + sequence,
    )


def test_in_memory_lease_fences_active_owner_and_preserves_pending_on_takeover() -> (
    None
):
    async def run() -> None:
        now = [1_000]
        store = InMemoryStreamLeaseStore(clock_ms=lambda: now[0])
        first = await store.acquire(
            partition_key=PHASE1B_PARTITION_KEY,
            owner_id="collector-a",
            lease_ttl_ms=500,
        )
        assert first.producer_epoch == 0
        with pytest.raises(StreamLeaseBusyError):
            await store.acquire(
                partition_key=PHASE1B_PARTITION_KEY,
                owner_id="collector-b",
                lease_ttl_ms=500,
            )

        pending = _envelope(owner="collector-a", epoch=0)
        first = await store.stage_pending(first, pending)
        now[0] += 500
        second = await store.acquire(
            partition_key=PHASE1B_PARTITION_KEY,
            owner_id="collector-b",
            lease_ttl_ms=500,
        )

        assert second.producer_epoch == 1
        assert second.pending_envelope == pending
        with pytest.raises(StreamLeaseFencedError):
            await store.renew(first, lease_ttl_ms=500)

    asyncio.run(run())


def test_lease_checkpoint_requires_staged_contiguous_event_and_advancing_offset() -> (
    None
):
    async def run() -> None:
        store = InMemoryStreamLeaseStore(clock_ms=lambda: 1_000)
        lease = await store.acquire(
            partition_key=PHASE1B_PARTITION_KEY,
            owner_id="collector-a",
            lease_ttl_ms=500,
        )
        first = _envelope(owner="collector-a", epoch=0)

        with pytest.raises(StreamCheckpointError, match="no staged"):
            await store.checkpoint(lease, first, partition_offset=0)
        lease = await store.stage_pending(lease, first)
        lease = await store.checkpoint(lease, first, partition_offset=0)
        assert lease.last_sequence == 42
        assert lease.pending_envelope is None
        assert await store.checkpoint(lease, first, partition_offset=0) == lease

        gap = _envelope(
            owner="collector-a",
            epoch=0,
            sequence=44,
            previous_sequence=42,
        )
        with pytest.raises(StreamCheckpointError, match="contiguous"):
            await store.stage_pending(lease, gap)

    asyncio.run(run())


def test_leased_collector_recovers_exact_pending_envelope_after_lost_receipt() -> None:
    class AcceptThenTimeout:
        def __init__(self, inner: InMemoryMarketEventLog) -> None:
            self.inner = inner
            self.attempt: dict[str, Any] | None = None

        async def publish(self, events: Any) -> Any:
            event = next(iter(events))
            self.attempt = event.to_wire()
            await self.inner.publish((event,))
            raise TimeoutError("receipt lost after durable accept")

    async def run() -> None:
        now = [1_000]
        store = InMemoryStreamLeaseStore(clock_ms=lambda: now[0])
        event_log = InMemoryMarketEventLog()
        lost_receipt = AcceptThenTimeout(event_log)
        first = await LeasedAggTradeCollector.acquire(
            lease_store=store,
            publisher=lost_receipt,
            owner_id="collector-a",
            lease_ttl_ms=500,
        )

        with pytest.raises(TimeoutError):
            await first.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)
        durable_pending = await store.inspect(PHASE1B_PARTITION_KEY)
        assert durable_pending is not None
        assert durable_pending.pending_envelope is not None
        assert durable_pending.pending_envelope.to_wire() == lost_receipt.attempt

        now[0] += 500
        recovered = await LeasedAggTradeCollector.acquire(
            lease_store=store,
            publisher=event_log,
            owner_id="collector-b",
            lease_ttl_ms=500,
        )
        retry = await recovered.handle(
            _event(42),
            published_at_ms=PUBLISHED_AT_MS + 999,
        )
        following = await recovered.handle(
            _event(43),
            published_at_ms=PUBLISHED_AT_MS + 1_000,
        )

        assert retry.partition_offsets == ((PHASE1B_PARTITION_KEY, 0),)
        assert following.partition_offsets == ((PHASE1B_PARTITION_KEY, 1),)
        assert len(event_log.events) == 2
        assert event_log.events[0].to_wire() == lost_receipt.attempt
        assert event_log.events[0].producer_epoch == 0
        assert event_log.events[1].producer_epoch == 1
        assert recovered.lease.last_sequence == 43
        assert recovered.lease.last_partition_offset == 1
        assert recovered.lease.pending_envelope is None

    asyncio.run(run())


def test_leased_collector_becomes_terminal_when_checkpoint_loses_lease() -> None:
    class ExpiringPublisher:
        def __init__(self, now: list[int]) -> None:
            self.now = now
            self.inner = InMemoryMarketEventLog()

        async def publish(self, events: Any) -> Any:
            receipt = await self.inner.publish(events)
            self.now[0] += 501
            return receipt

    async def run() -> None:
        now = [1_000]
        store = InMemoryStreamLeaseStore(clock_ms=lambda: now[0])
        collector = await LeasedAggTradeCollector.acquire(
            lease_store=store,
            publisher=ExpiringPublisher(now),
            owner_id="collector-a",
            lease_ttl_ms=500,
        )

        with pytest.raises(StreamLeaseFencedError, match="expired"):
            await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)
        assert collector.failed is True
        with pytest.raises(LeasedCollectorFailedError):
            await collector.handle(_event(42), published_at_ms=PUBLISHED_AT_MS)

    asyncio.run(run())


class _FakeProducer:
    def __init__(self, **config: Any) -> None:
        self.config = config
        self.started = False
        self.stopped = False
        self.partitions: set[int] | None = {0}
        self.records: list[dict[str, Any]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def partitions_for(self, topic: str) -> set[int] | None:
        assert topic == MARKET_EVENTS_TOPIC
        return self.partitions

    async def send_and_wait(self, topic: str, **record: Any) -> Any:
        self.records.append({"topic": topic, **record})
        return SimpleNamespace(topic=topic, partition=0, offset=len(self.records) - 1)


def test_kafka_publisher_locks_strong_ack_key_partition_and_canonical_bytes() -> None:
    async def run() -> None:
        fake = _FakeProducer()
        publisher = KafkaMarketEventPublisher(
            bootstrap_servers="redpanda:9092",
            client_id="collector-a",
            producer_factory=lambda **config: _capture_config(fake, config),
        )
        envelope = _envelope(owner="collector-a", epoch=0)

        with pytest.raises(KafkaPublisherStateError):
            await publisher.publish((envelope,))
        await publisher.start()
        with pytest.raises(ValueError, match="one event per call"):
            await publisher.publish((envelope, envelope))
        receipt = await publisher.publish((envelope,))
        await publisher.stop()

        assert fake.config["acks"] == "all"
        assert fake.config["enable_idempotence"] is True
        assert receipt.partition_offsets == ((PHASE1B_PARTITION_KEY, 0),)
        assert fake.records == [
            {
                "topic": MARKET_EVENTS_TOPIC,
                "value": canonical_envelope_bytes(envelope),
                "key": PHASE1B_PARTITION_KEY.encode(),
                "partition": 0,
                "headers": [
                    ("event-id", envelope.event_id.encode()),
                    ("schema-version", envelope.schema_version.encode()),
                ],
            }
        ]
        assert json.loads(fake.records[0]["value"]) == envelope.to_wire()
        assert fake.stopped is True

    asyncio.run(run())


def test_kafka_publisher_rejects_topic_partition_drift_and_stops() -> None:
    async def run() -> None:
        fake = _FakeProducer()
        fake.partitions = {0, 1}
        publisher = KafkaMarketEventPublisher(
            bootstrap_servers="redpanda:9092",
            client_id="collector-a",
            producer_factory=lambda **config: _capture_config(fake, config),
        )

        with pytest.raises(KafkaTopicContractError, match="exactly partition 0"):
            await publisher.start()
        assert fake.stopped is True
        assert publisher.started is False

    asyncio.run(run())


def _capture_config(fake: _FakeProducer, config: dict[str, Any]) -> _FakeProducer:
    fake.config = config
    return fake
