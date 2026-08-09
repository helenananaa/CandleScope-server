from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aiokafka.structs import TopicPartition
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_contracts import MarketEventCursor, parse_manifest_bytes
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.archive_settings import (
    ArchiveWriterConfigurationError,
    ArchiveWriterSettings,
)
from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.projection import (
    KafkaMarketEventRecord,
    decode_kafka_market_event,
)
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    PHASE1B_PARTITION_KEY,
    canonical_envelope_bytes,
)
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
    MarketEventParquetCodec,
    ParquetArchiveConflictError,
    ParquetArchiveIntegrityError,
    ParquetMarketEventQuery,
    manifest_key,
    segment_key,
)
from app.server_runtime.testing import InMemoryImmutableObjectStore
from jsonschema import Draft202012Validator


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


def _record(offset: int, envelope: Any) -> KafkaMarketEventRecord:
    return decode_kafka_market_event(
        topic=MARKET_EVENTS_TOPIC,
        partition=0,
        offset=offset,
        key=PHASE1B_PARTITION_KEY.encode(),
        value=canonical_envelope_bytes(envelope),
        headers=(
            ("event-id", envelope.event_id.encode()),
            ("schema-version", envelope.schema_version.encode()),
        ),
    )


def _records(
    start_offset: int, start_sequence: int
) -> tuple[KafkaMarketEventRecord, ...]:
    return tuple(
        _record(
            start_offset + index,
            _envelope(
                start_sequence + index,
                previous_sequence=(
                    None if start_sequence + index == 42 else start_sequence + index - 1
                ),
            ),
        )
        for index in range(4)
    )


def _settings() -> ArchiveWriterSettings:
    return ArchiveWriterSettings(
        kafka_bootstrap_servers=("localhost:9092",),
        owner_id="archiver-a",
        data_epoch="binance-futures-test-1",
        s3_endpoint_url="http://localhost:9000",
        s3_region="us-east-1",
        s3_bucket="candlescope-test",
        s3_prefix="market-data",
        s3_access_key_id="access",
        s3_secret_access_key="secret",
        segment_event_count=4,
        kafka_session_timeout_ms=300,
        kafka_heartbeat_interval_ms=100,
    )


def test_archive_settings_are_strict_and_redact_s3_credentials() -> None:
    environment = {
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_OWNER_ID": "archiver-a",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH": "epoch-1",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ENDPOINT_URL": "http://minio:9000",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ACCESS_KEY_ID": "access-value",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_SECRET_ACCESS_KEY": "secret-value",
    }
    settings = ArchiveWriterSettings.from_env(environment)
    assert settings.segment_event_count == 10_000
    assert "access-value" not in repr(settings)
    assert "secret-value" not in repr(settings)
    with pytest.raises(ArchiveWriterConfigurationError, match="DATA_EPOCH"):
        ArchiveWriterSettings.from_env(
            {
                key: value
                for key, value in environment.items()
                if "DATA_EPOCH" not in key
            }
        )
    with pytest.raises(ArchiveWriterConfigurationError, match="path-safe"):
        ArchiveWriterSettings.from_env(
            {
                **environment,
                "CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH": "../escape",
            }
        )


def test_parquet_codec_is_deterministic_and_round_trips_canonical_envelopes() -> None:
    records = _records(0, 42)
    codec = MarketEventParquetCodec()
    first = codec.encode(records)
    second = codec.encode(records)
    assert first == second
    rows = codec.validate_records(first, records)
    assert [row.kafka_offset for row in rows] == [0, 1, 2, 3]
    assert [row.envelope.sequence_end for row in rows] == [42, 43, 44, 45]


def test_archive_replay_reuses_objects_and_manifest_is_schema_valid() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        archive = ImmutableParquetMarketEventArchive(
            object_store=store,
            segment_event_count=4,
        )
        await archive.initialize()
        records = _records(0, 42)
        first = await archive.append_records(records, data_epoch="epoch-1")
        replay = await archive.append_records(records, data_epoch="epoch-1")
        assert first.object_created is True
        assert first.manifest_created is True
        assert replay.object_created is False
        assert replay.manifest_created is False
        assert replay.commit == first.commit
        assert len(store.objects) == 2

        stored = await store.get(manifest_key("epoch-1", 4))
        manifest = parse_manifest_bytes(stored.data)
        assert manifest.sha256() == first.commit.snapshot.manifest_sha256
        schema_path = (
            Path(__file__).resolve().parents[2]
            / "docs/server/contracts/market-data-manifest-v1.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(manifest.to_wire())

    asyncio.run(run())


def test_archive_rejects_same_offset_segment_with_different_events() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        archive = ImmutableParquetMarketEventArchive(
            object_store=store,
            segment_event_count=4,
        )
        await archive.initialize()
        original = _records(0, 42)
        await archive.append_records(original, data_epoch="epoch-1")
        conflicting = (
            _record(0, _envelope(42, quantity="0.03000000")),
            *original[1:],
        )
        with pytest.raises(ParquetArchiveConflictError, match="different"):
            await archive.append_records(conflicting, data_epoch="epoch-1")

    asyncio.run(run())


def test_snapshot_query_pages_verified_manifest_chain_and_binds_cursor() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        archive = ImmutableParquetMarketEventArchive(
            object_store=store,
            segment_event_count=4,
        )
        await archive.initialize()
        first = await archive.append_records(_records(0, 42), data_epoch="epoch-1")
        second = await archive.append_records(_records(4, 46), data_epoch="epoch-1")
        query = ParquetMarketEventQuery(archive=archive, max_page_rows=4)
        stream = _envelope(42).stream
        page_one = await query.query(
            snapshot=second.commit.snapshot,
            stream=stream,
            start_event_time_ms=1_700_000_000_000,
            end_event_time_ms=1_700_000_001_000,
            limit=3,
        )
        assert [event.sequence_end for event in page_one.events] == [42, 43, 44]
        assert page_one.next_cursor is not None
        page_two = await query.query(
            snapshot=second.commit.snapshot,
            stream=stream,
            start_event_time_ms=1_700_000_000_000,
            end_event_time_ms=1_700_000_001_000,
            limit=3,
            cursor=page_one.next_cursor,
        )
        assert [event.sequence_end for event in page_two.events] == [45, 46, 47]
        assert page_two.next_cursor is not None
        page_three = await query.query(
            snapshot=second.commit.snapshot,
            stream=stream,
            start_event_time_ms=1_700_000_000_000,
            end_event_time_ms=1_700_000_001_000,
            limit=3,
            cursor=page_two.next_cursor,
        )
        assert [event.sequence_end for event in page_three.events] == [48, 49]
        assert page_three.next_cursor is None

        with pytest.raises(ParquetArchiveIntegrityError, match="another manifest"):
            await query.query(
                snapshot=first.commit.snapshot,
                stream=stream,
                start_event_time_ms=1_700_000_000_000,
                end_event_time_ms=1_700_000_001_000,
                limit=3,
                cursor=page_one.next_cursor,
            )
        forged = MarketEventCursor(
            value=page_one.next_cursor.value[:-1] + "A",
            manifest_sha256=second.commit.snapshot.manifest_sha256,
        )
        with pytest.raises(ParquetArchiveIntegrityError, match="malformed"):
            await query.query(
                snapshot=second.commit.snapshot,
                stream=stream,
                start_event_time_ms=1_700_000_000_000,
                end_event_time_ms=1_700_000_001_000,
                limit=3,
                cursor=forged,
            )

    asyncio.run(run())


def test_query_detects_parquet_bytes_tampered_after_manifest_publication() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        archive = ImmutableParquetMarketEventArchive(
            object_store=store,
            segment_event_count=4,
        )
        await archive.initialize()
        result = await archive.append_records(_records(0, 42), data_epoch="epoch-1")
        key = segment_key("epoch-1", 0, 3)
        original = store.objects[key]
        store.objects[key] = type(original)(
            data=original.data + b"tampered",
            metadata=original.metadata,
            content_type=original.content_type,
        )
        query = ParquetMarketEventQuery(archive=archive)
        with pytest.raises(ParquetArchiveIntegrityError, match="byte size|SHA-256"):
            await query.query(
                snapshot=result.commit.snapshot,
                stream=_envelope(42).stream,
                start_event_time_ms=0,
                end_event_time_ms=2_000_000_000_000,
                limit=10,
            )

    asyncio.run(run())


def test_kafka_consumer_buffers_until_exact_archive_segment_is_complete() -> None:
    records = _records(0, 42)

    class FakeConsumer:
        def __init__(self, *_: Any, **options: Any) -> None:
            self.options = options
            self.calls = 0
            self.commits: list[Any] = []

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        def partitions_for_topic(self, _: str) -> set[int]:
            return {0}

        async def committed(self, _: TopicPartition) -> int:
            return 0

        async def getmany(self, **options: Any) -> dict[Any, list[Any]]:
            expected_max = 4 if self.calls == 0 else 2
            assert options["max_records"] == expected_max
            selected = records[:2] if self.calls == 0 else records[2:]
            self.calls += 1
            return {
                TopicPartition(MARKET_EVENTS_TOPIC, 0): [
                    SimpleNamespace(
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        key=PHASE1B_PARTITION_KEY.encode(),
                        value=record.envelope_bytes,
                        headers=[
                            ("event-id", record.envelope.event_id.encode()),
                            (
                                "schema-version",
                                record.envelope.schema_version.encode(),
                            ),
                        ],
                    )
                    for record in selected
                ]
            }

        async def commit(self, offsets: Any) -> None:
            self.commits.append(offsets)

    async def run() -> None:
        fake: FakeConsumer | None = None

        def factory(*args: Any, **kwargs: Any) -> FakeConsumer:
            nonlocal fake
            fake = FakeConsumer(*args, **kwargs)
            return fake

        consumer = KafkaMarketEventBatchConsumer(
            bootstrap_servers="localhost:9092",
            group_id="archiver-v1",
            client_id="archiver-a",
            exact_batch_size=4,
            consumer_factory=factory,
        )
        await consumer.start()
        assert await consumer.poll(timeout_ms=10, max_records=4) == ()
        complete = await consumer.poll(timeout_ms=10, max_records=4)
        assert [record.offset for record in complete] == [0, 1, 2, 3]
        await consumer.commit_through(complete[-1])
        assert fake is not None
        assert fake.commits == [{TopicPartition(MARKET_EVENTS_TOPIC, 0): 4}]
        await consumer.stop()

    asyncio.run(run())
