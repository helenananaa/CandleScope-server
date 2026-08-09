from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka.structs import TopicPartition
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_runtime import (
    ClickHouseWriterConfigurationError,
    ClickHouseWriterService,
    ClickHouseWriterSettings,
    ClickHouseWriterState,
    MarketEventRecordError,
    decode_kafka_market_event,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.consumers.kafka import KafkaConsumerOffsetError
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.projection import KafkaMarketEventRecord
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    PHASE1B_PARTITION_KEY,
    canonical_envelope_bytes,
)
from app.server_runtime.testing import InMemoryMarketEventProjector


def _settings(owner_id: str = "writer-a") -> ClickHouseWriterSettings:
    return ClickHouseWriterSettings(
        kafka_bootstrap_servers=("localhost:9092",),
        owner_id=owner_id,
        clickhouse_url="http://localhost:8123",
        clickhouse_user="candlescope",
        clickhouse_password="secret",
        batch_size=10,
        poll_timeout_ms=10,
        kafka_session_timeout_ms=300,
        kafka_heartbeat_interval_ms=100,
        shutdown_timeout_ms=1_000,
    )


def _market_event(sequence: int, *, quantity: str = "0.02500000") -> MarketEvent:
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=1_700_000_000_000 + sequence,
        received_at_ms=1_700_000_000_100 + sequence,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": sequence,
            "price": 100000.1,
            "quantity": float(quantity),
            "price_text": "100000.1000",
            "quantity_text": quantity,
            "first_trade_id": sequence * 10,
            "last_trade_id": sequence * 10 + 2,
            "trade_time_ms": 1_700_000_000_000 + sequence,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=sequence,
        market_type="futures",
    )


def _envelope(
    sequence: int,
    *,
    quantity: str = "0.02500000",
    previous_sequence: int | None = None,
) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        _market_event(sequence, quantity=quantity),
        previous_sequence=previous_sequence,
        published_at_ms=1_700_000_001_000 + sequence,
    )


def _record(
    offset: int,
    envelope: Any,
) -> KafkaMarketEventRecord:
    return decode_kafka_market_event(
        topic=MARKET_EVENTS_TOPIC,
        partition=0,
        offset=offset,
        key=PHASE1B_PARTITION_KEY.encode(),
        value=canonical_envelope_bytes(envelope),
        headers=(
            ("event-id", envelope.event_id.encode("ascii")),
            ("schema-version", envelope.schema_version.encode("ascii")),
        ),
    )


def test_writer_settings_are_strict_and_redact_clickhouse_password() -> None:
    environment = {
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_BOOTSTRAP_SERVERS": (
            "kafka-a:9092,kafka-b:9092"
        ),
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_OWNER_ID": "writer-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_URL": "http://ch:8123",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_USER": "candlescope",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_PASSWORD": "secret-value",
    }
    settings = ClickHouseWriterSettings.from_env(environment)
    assert settings.kafka_bootstrap_servers == ("kafka-a:9092", "kafka-b:9092")
    assert "secret-value" not in repr(settings)
    with pytest.raises(ClickHouseWriterConfigurationError, match="KAFKA"):
        ClickHouseWriterSettings.from_env({})
    with pytest.raises(ClickHouseWriterConfigurationError, match="one third"):
        ClickHouseWriterSettings.from_env(
            {
                **environment,
                "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_SESSION_TIMEOUT_MS": "300",
                "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_HEARTBEAT_INTERVAL_MS": "101",
            }
        )


def test_decode_rejects_key_header_and_noncanonical_wire_drift() -> None:
    envelope = _envelope(42)
    value = canonical_envelope_bytes(envelope)
    record = _record(0, envelope)
    assert record.envelope == envelope

    with pytest.raises(MarketEventRecordError, match="key"):
        decode_kafka_market_event(
            topic=MARKET_EVENTS_TOPIC,
            partition=0,
            offset=0,
            key=b"wrong",
            value=value,
            headers=(),
        )
    with pytest.raises(MarketEventRecordError, match="canonical"):
        decode_kafka_market_event(
            topic=MARKET_EVENTS_TOPIC,
            partition=0,
            offset=0,
            key=PHASE1B_PARTITION_KEY.encode(),
            value=b" " + value,
            headers=(
                ("event-id", envelope.event_id.encode()),
                ("schema-version", envelope.schema_version.encode()),
            ),
        )
    with pytest.raises(MarketEventRecordError, match="headers"):
        decode_kafka_market_event(
            topic=MARKET_EVENTS_TOPIC,
            partition=0,
            offset=0,
            key=PHASE1B_PARTITION_KEY.encode(),
            value=value,
            headers=(("event-id", envelope.event_id.encode()),),
        )


def test_kafka_consumer_uses_manual_commit_and_frozen_partition() -> None:
    class FakeConsumer:
        def __init__(self, *topics: str, **options: Any) -> None:
            self.topics = topics
            self.options = options
            self.commits: list[Any] = []

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def partitions_for_topic(self, topic: str) -> set[int]:
            assert topic == MARKET_EVENTS_TOPIC
            return {0}

        async def getmany(self, **_: Any) -> dict[Any, list[Any]]:
            envelope = _envelope(42)
            return {
                TopicPartition(MARKET_EVENTS_TOPIC, 0): [
                    SimpleNamespace(
                        topic=MARKET_EVENTS_TOPIC,
                        partition=0,
                        offset=7,
                        key=PHASE1B_PARTITION_KEY.encode(),
                        value=canonical_envelope_bytes(envelope),
                        headers=[
                            ("event-id", envelope.event_id.encode()),
                            ("schema-version", envelope.schema_version.encode()),
                        ],
                    )
                ]
            }

        async def committed(self, partition: TopicPartition) -> int:
            assert partition == TopicPartition(MARKET_EVENTS_TOPIC, 0)
            return 7

        async def commit(self, offsets: Any) -> None:
            self.commits.append(offsets)

    async def run() -> None:
        fake: FakeConsumer | None = None

        def factory(*topics: str, **options: Any) -> FakeConsumer:
            nonlocal fake
            fake = FakeConsumer(*topics, **options)
            return fake

        consumer = KafkaMarketEventBatchConsumer(
            bootstrap_servers="localhost:9092",
            group_id="writer-v1",
            client_id="writer-a",
            consumer_factory=factory,
        )
        await consumer.start()
        assert fake is not None
        assert fake.options["enable_auto_commit"] is False
        assert fake.options["isolation_level"] == "read_committed"
        records = await consumer.poll(timeout_ms=10, max_records=10)
        await consumer.commit_through(records[-1])
        assert fake.commits == [{TopicPartition(MARKET_EVENTS_TOPIC, 0): 8}]
        await consumer.stop()

    asyncio.run(run())


def test_kafka_consumer_fails_closed_when_history_does_not_start_at_commit() -> None:
    class FakeConsumer:
        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def partitions_for_topic(self, _: str) -> set[int]:
            return {0}

        async def committed(self, _: TopicPartition) -> None:
            return None

        async def getmany(self, **_: Any) -> dict[Any, list[Any]]:
            envelope = _envelope(42)
            return {
                TopicPartition(MARKET_EVENTS_TOPIC, 0): [
                    SimpleNamespace(
                        topic=MARKET_EVENTS_TOPIC,
                        partition=0,
                        offset=1,
                        key=PHASE1B_PARTITION_KEY.encode(),
                        value=canonical_envelope_bytes(envelope),
                        headers=[
                            ("event-id", envelope.event_id.encode()),
                            ("schema-version", envelope.schema_version.encode()),
                        ],
                    )
                ]
            }

    async def run() -> None:
        consumer = KafkaMarketEventBatchConsumer(
            bootstrap_servers="localhost:9092",
            group_id="writer-v1",
            client_id="writer-a",
            consumer_factory=lambda *_, **__: FakeConsumer(),
        )
        await consumer.start()
        with pytest.raises(KafkaConsumerOffsetError, match="expected 0, received 1"):
            await consumer.poll(timeout_ms=10, max_records=10)
        await consumer.stop()

    asyncio.run(run())


def test_projector_classifies_new_duplicate_and_conflicting_identity() -> None:
    async def run() -> None:
        original = _envelope(42)
        conflict = _envelope(42, quantity="0.03000000")
        following = _envelope(43, previous_sequence=42)
        assert original.event_id == conflict.event_id
        projector = InMemoryMarketEventProjector()
        await projector.start()
        result = await projector.apply_batch(
            (
                _record(0, original),
                _record(1, original),
                _record(2, conflict),
                _record(3, following),
            )
        )
        assert (
            result.inserted_count,
            result.duplicate_count,
            result.conflict_count,
        ) == (
            2,
            1,
            1,
        )
        assert len(projector.identities) == 2
        assert len(projector.conflicts) == 1
        await projector.stop()

    asyncio.run(run())


class _BatchConsumer:
    def __init__(
        self,
        batch: tuple[KafkaMarketEventRecord, ...],
        *,
        stop_after_commit: asyncio.Event | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.batch = batch
        self.stop_after_commit = stop_after_commit
        self.order = order
        self.delivered = False
        self.committed_next_offsets: list[int] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        if self.order is not None:
            self.order.append("consumer.stop")

    async def poll(self, *, timeout_ms: int, max_records: int) -> Any:
        del timeout_ms, max_records
        if not self.delivered:
            self.delivered = True
            return self.batch
        await asyncio.sleep(0)
        return ()

    async def commit_through(self, record: KafkaMarketEventRecord) -> None:
        self.committed_next_offsets.append(record.offset + 1)
        if self.stop_after_commit is not None:
            self.stop_after_commit.set()


def test_restart_after_projection_before_offset_commit_is_idempotent() -> None:
    async def run() -> None:
        original = _envelope(42)
        conflict = _envelope(42, quantity="0.03000000")
        following = _envelope(43, previous_sequence=42)
        batch = (
            _record(0, original),
            _record(1, original),
            _record(2, conflict),
            _record(3, following),
        )
        projector = InMemoryMarketEventProjector()
        first_consumer = _BatchConsumer(batch)

        async def crash_after_projection(_: Any) -> None:
            raise RuntimeError("simulated crash before Kafka commit")

        first = ClickHouseWriterService(
            settings=_settings("writer-a"),
            consumer=first_consumer,
            projector=projector,
            after_project_before_commit=crash_after_projection,
        )
        with pytest.raises(RuntimeError, match="before Kafka commit"):
            await first.run()
        assert first_consumer.committed_next_offsets == []
        assert len(projector.identities) == 2
        assert len(projector.conflicts) == 1

        stop = asyncio.Event()
        second_consumer = _BatchConsumer(batch, stop_after_commit=stop)
        second = ClickHouseWriterService(
            settings=_settings("writer-b"),
            consumer=second_consumer,
            projector=projector,
        )
        health = await second.run(stop)
        assert second_consumer.committed_next_offsets == [4]
        assert len(projector.identities) == 2
        assert len(projector.conflicts) == 1
        assert health.committed_next_offset == 4
        assert health.inserted_events == 0
        assert health.duplicate_events == 3
        assert health.conflict_events == 1
        assert health.state is ClickHouseWriterState.STOPPED

    asyncio.run(run())


def test_writer_shutdown_stops_consumer_before_projector() -> None:
    async def run() -> None:
        order: list[str] = []
        stop = asyncio.Event()
        stop.set()
        consumer = _BatchConsumer((), order=order)
        projector = InMemoryMarketEventProjector(order=order)
        service = ClickHouseWriterService(
            settings=_settings(),
            consumer=consumer,
            projector=projector,
        )
        health = await service.run(stop)
        assert order == ["consumer.stop", "projector.stop"]
        assert health.state is ClickHouseWriterState.STOPPED

    asyncio.run(run())
