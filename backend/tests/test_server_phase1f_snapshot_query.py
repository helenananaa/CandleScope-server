from __future__ import annotations

import asyncio
import json
from typing import Any, Self

import httpx
import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventPage,
    MarketEventRange,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
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
from app.server_runtime.query_api import create_snapshot_query_app
from app.server_runtime.query_cursor import KafkaProjectionCursorReader
from app.server_runtime.query_router import (
    HotProjectionParityError,
    HotProjectionUnavailableError,
    QueryPreference,
    SnapshotQueryRouter,
)
from app.server_runtime.query_settings import (
    QueryServiceConfigurationError,
    QueryServiceSettings,
)
from app.server_runtime.storage.clickhouse_query import (
    ClickHouseSnapshotMarketEventQuery,
)
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
    ParquetMarketEventQuery,
)
from app.server_runtime.testing import InMemoryImmutableObjectStore


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


def _envelope(sequence: int, *, quantity: str = "0.02500000") -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        _market_event(sequence, quantity=quantity),
        previous_sequence=None if sequence == 42 else sequence - 1,
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


def _snapshot(version: int = 4) -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="epoch-1",
        snapshot_version=version,
        manifest_uri=f"s3://archive/snapshot-{version}.json",
        manifest_sha256="a" * 64,
    )


def _page(
    envelope: Any, *, snapshot: MarketDataSnapshotRef | None = None
) -> MarketEventPage:
    selected_snapshot = snapshot or _snapshot()
    return MarketEventPage(
        snapshot=selected_snapshot,
        events=(envelope,),
        covered_range=MarketEventRange(
            partition_key=envelope.partition_key,
            start_event_time_ms=envelope.event_time_ms,
            end_event_time_ms=envelope.event_time_ms,
            event_count=1,
            sequence_start=envelope.sequence_start,
            sequence_end=envelope.sequence_end,
        ),
        next_cursor=None,
    )


class _FakeQuery:
    def __init__(self, page: MarketEventPage) -> None:
        self.page = page
        self.calls = 0
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def query(self, **_: Any) -> MarketEventPage:
        self.calls += 1
        return self.page


class _FakeCursor:
    def __init__(self, value: int | None) -> None:
        self.value = value
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def committed_next_offset(self) -> int | None:
        return self.value


def test_cold_query_applies_first_fact_duplicate_and_conflict_semantics() -> None:
    async def run() -> None:
        original = _envelope(42)
        conflict = _envelope(42, quantity="0.03000000")
        following = _envelope(43)
        assert original.event_id == conflict.event_id
        archive = ImmutableParquetMarketEventArchive(
            object_store=InMemoryImmutableObjectStore(),
            segment_event_count=4,
        )
        await archive.initialize()
        result = await archive.append_records(
            (
                _record(0, original),
                _record(1, original),
                _record(2, conflict),
                _record(3, following),
            ),
            data_epoch="epoch-1",
        )
        page = await ParquetMarketEventQuery(archive=archive).query(
            snapshot=result.commit.snapshot,
            stream=original.stream,
            start_event_time_ms=0,
            end_event_time_ms=2_000_000_000_000,
            limit=10,
        )
        assert [event.sequence_end for event in page.events] == [42, 43]
        assert page.events[0] == original

    asyncio.run(run())


def test_router_uses_cold_on_lag_and_requires_exact_parity_for_hot() -> None:
    async def run() -> None:
        envelope = _envelope(42)
        page = _page(envelope)
        cold = _FakeQuery(page)
        hot = _FakeQuery(page)
        cursor = _FakeCursor(3)
        router = SnapshotQueryRouter(
            cold_query=cold,
            hot_query=hot,
            projection_cursor=cursor,
        )
        await router.start()
        arguments = {
            "snapshot": page.snapshot,
            "stream": envelope.stream,
            "start_event_time_ms": 0,
            "end_event_time_ms": 2_000_000_000_000,
            "limit": 10,
        }
        lagged = await router.query(**arguments)
        assert lagged.backend == "cold"
        assert lagged.hot_committed_next_offset == 3
        assert hot.calls == 0
        with pytest.raises(HotProjectionUnavailableError):
            await router.query(**arguments, preference=QueryPreference.HOT)

        cursor.value = 4
        verified = await router.query(**arguments)
        assert verified.backend == "hot"
        assert verified.parity_verified is True
        hot.page = _page(_envelope(42, quantity="0.03000000"))
        with pytest.raises(HotProjectionParityError):
            await router.query(**arguments)
        await router.stop()

    asyncio.run(run())


def test_query_http_boundary_is_strict_and_reports_route_metrics() -> None:
    async def run() -> None:
        envelope = _envelope(42)
        page = _page(envelope)
        router = SnapshotQueryRouter(
            cold_query=_FakeQuery(page),
            hot_query=_FakeQuery(page),
            projection_cursor=_FakeCursor(4),
        )
        app = create_snapshot_query_app(
            router=router,
            max_concurrent_queries=2,
            query_queue_timeout_ms=100,
        )
        body = {
            "snapshot": {
                "data_epoch": page.snapshot.data_epoch,
                "snapshot_version": page.snapshot.snapshot_version,
                "manifest_uri": page.snapshot.manifest_uri,
                "manifest_sha256": page.snapshot.manifest_sha256,
            },
            "stream": envelope.stream.to_dict(),
            "start_event_time_ms": 0,
            "end_event_time_ms": 2_000_000_000_000,
            "limit": 10,
            "preference": "auto",
        }
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
        ):
            ready = await client.get("/health/ready")
            assert ready.status_code == 200
            response = await client.post(
                "/api/v1/server/market-events/query",
                json=body,
            )
            assert response.status_code == 200
            assert response.json()["backend"] == "hot"
            assert response.json()["parity_verified"] is True
            invalid = await client.post(
                "/api/v1/server/market-events/query",
                json={**body, "unknown": True},
            )
            assert invalid.status_code == 422
            metrics = (await client.get("/metrics")).json()
            assert metrics["requests_total"] == 1
            assert metrics["hot_responses_total"] == 1

    asyncio.run(run())


def test_clickhouse_query_validates_canonical_rows_and_snapshot_cutoff() -> None:
    envelope = _envelope(42)
    responses = [
        json.dumps({"table_count": 1}) + "\n",
        "\n".join(
            json.dumps({"name": name, "type": type_name})
            for name, type_name in (
                ("envelope_sha256", "FixedString(64)"),
                ("envelope_json", "String"),
                ("first_kafka_partition", "UInt16"),
                ("first_kafka_offset", "UInt64"),
            )
        )
        + "\n",
        json.dumps(
            {
                "envelope_sha256": _record(0, envelope).envelope_sha256,
                "envelope_json": canonical_envelope_bytes(envelope).decode(),
                "first_kafka_partition": 0,
                "first_kafka_offset": 0,
            }
        )
        + "\n",
    ]

    class FakeResponse:
        status = 200

        def __init__(self, body: str) -> None:
            self.body = body

        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *_: object) -> None:
            return None

        async def text(self) -> str:
            return self.body

    class FakeSession:
        def __init__(self, **_: Any) -> None:
            self.calls: list[dict[str, Any]] = []
            self.closed = False

        def post(self, _: str, **options: Any) -> FakeResponse:
            self.calls.append(options)
            return FakeResponse(responses.pop(0))

        async def close(self) -> None:
            self.closed = True

    async def run() -> None:
        sessions: list[FakeSession] = []

        def factory(**options: Any) -> FakeSession:
            session = FakeSession(**options)
            sessions.append(session)
            return session

        query = ClickHouseSnapshotMarketEventQuery(
            url="http://clickhouse:8123",
            database="candlescope",
            user="query",
            password="secret",
            session_factory=factory,
        )
        await query.start()
        page = await query.query(
            snapshot=_snapshot(),
            stream=envelope.stream,
            start_event_time_ms=0,
            end_event_time_ms=2_000_000_000_000,
            limit=10,
        )
        assert page.events == (envelope,)
        assert sessions[0].calls[2]["params"]["param_snapshot_version"] == "4"
        await query.stop()
        assert sessions[0].closed is True

    asyncio.run(run())


def test_projection_cursor_reader_is_observational_and_settings_redact_secrets() -> (
    None
):
    class FakeConsumer:
        def __init__(self, **options: Any) -> None:
            self.options = options
            self.stopped = False

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            self.stopped = True

        async def describe_topics(self, _: list[str]) -> list[dict[str, Any]]:
            return [
                {
                    "topic": MARKET_EVENTS_TOPIC,
                    "partitions": [{"partition": 0}],
                }
            ]

        async def list_consumer_group_offsets(
            self,
            _: str,
            *,
            partitions: list[Any],
        ) -> dict[Any, Any]:
            return {partitions[0]: type("Offset", (), {"offset": 9})()}

    async def run() -> None:
        fake: FakeConsumer | None = None

        def factory(**options: Any) -> FakeConsumer:
            nonlocal fake
            fake = FakeConsumer(**options)
            return fake

        reader = KafkaProjectionCursorReader(
            bootstrap_servers="kafka:9092",
            group_id="writer-v1",
            client_id="query-a",
            admin_factory=factory,
        )
        await reader.start()
        assert fake is not None
        assert "group_id" not in fake.options
        assert await reader.committed_next_offset() == 9
        await reader.stop()

    asyncio.run(run())

    environment = {
        "CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL": "http://clickhouse:8123",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER": "query",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD": "clickhouse-secret",
        "CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL": "http://minio:9000",
        "CANDLESCOPE_SERVER_QUERY_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID": "minio-access",
        "CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY": "minio-secret",
    }
    settings = QueryServiceSettings.from_env(environment)
    assert "clickhouse-secret" not in repr(settings)
    assert "minio-access" not in repr(settings)
    assert "minio-secret" not in repr(settings)
    with pytest.raises(QueryServiceConfigurationError, match="S3_BUCKET"):
        QueryServiceSettings.from_env(
            {key: value for key, value in environment.items() if "S3_BUCKET" not in key}
        )
