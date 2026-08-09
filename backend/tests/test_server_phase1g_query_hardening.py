from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import httpx
import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_contracts import (
    MarketDataManifestV1,
    MarketDataSnapshotRef,
    MarketEventPage,
    MarketEventRange,
    ParquetArchiveSegmentV1,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    canonical_envelope_bytes,
)
from app.server_runtime.query_api import create_snapshot_query_app
from app.server_runtime.query_router import (
    HotProjectionQuarantinedError,
    QueryPreference,
    SnapshotQueryRouter,
)
from app.server_runtime.query_security import (
    BearerTokenAuthenticator,
    QueryAuditEvent,
)
from app.server_runtime.storage.parquet_archive import (
    PARQUET_SCHEMA_SHA256,
    ArchivedMarketEventRow,
    ImmutableParquetMarketEventArchive,
    ParquetMarketEventQuery,
)
from app.server_runtime.testing import InMemoryImmutableObjectStore

AUTH_TOKEN = "phase1g-hardening-token-000000000000"


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


def _snapshot() -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="epoch-1",
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256="a" * 64,
    )


def _page(envelope: Any) -> MarketEventPage:
    return MarketEventPage(
        snapshot=_snapshot(),
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

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def query(self, **_: Any) -> MarketEventPage:
        return self.page


class _FakeCursor:
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def committed_next_offset(self) -> int:
        return 4


class _AuditSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.events: list[QueryAuditEvent] = []

    async def emit(self, event: QueryAuditEvent) -> None:
        if self.fail:
            raise RuntimeError("audit unavailable")
        self.events.append(event)


def _router(page: MarketEventPage, *, interval_ms: int = 30_000):
    return SnapshotQueryRouter(
        cold_query=_FakeQuery(page),
        hot_query=_FakeQuery(page),
        projection_cursor=_FakeCursor(),
        parity_sample_interval_ms=interval_ms,
    )


def _body(envelope: Any) -> dict[str, Any]:
    snapshot = _snapshot()
    return {
        "snapshot": {
            "data_epoch": snapshot.data_epoch,
            "snapshot_version": snapshot.snapshot_version,
            "manifest_uri": snapshot.manifest_uri,
            "manifest_sha256": snapshot.manifest_sha256,
        },
        "stream": envelope.stream.to_dict(),
        "start_event_time_ms": 0,
        "end_event_time_ms": 2_000_000_000_000,
        "limit": 10,
        "preference": "auto",
    }


def test_query_and_metrics_require_bearer_auth_and_emit_redacted_audit() -> None:
    async def run() -> None:
        envelope = _envelope(42)
        audit = _AuditSink()
        app = create_snapshot_query_app(
            router=_router(_page(envelope)),
            authenticator=BearerTokenAuthenticator(
                token=AUTH_TOKEN,
                principal="gateway-a",
            ),
            audit_sink=audit,
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
        ):
            denied = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope),
            )
            assert denied.status_code == 401
            assert denied.headers["www-authenticate"] == "Bearer"
            assert (await client.get("/metrics")).status_code == 401
            invalid_id = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope),
                headers={
                    "Authorization": f"Bearer {AUTH_TOKEN}",
                    "X-Request-ID": " unsafe ",
                },
            )
            assert invalid_id.status_code == 400
            oversized = _body(envelope)
            oversized["stream"] = {
                **oversized["stream"],
                "params": {f"p{index}": "v" for index in range(33)},
            }
            bounded = await client.post(
                "/api/v1/server/market-events/query",
                json=oversized,
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            )
            assert bounded.status_code == 422
            success = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope),
                headers={
                    "Authorization": f"Bearer {AUTH_TOKEN}",
                    "X-Request-ID": "gateway-a:request-42",
                },
            )
            assert success.status_code == 200
            assert success.headers["x-request-id"] == "gateway-a:request-42"
            query_audit = audit.events[-1]
            assert query_audit.principal == "gateway-a"
            assert query_audit.outcome == "success"
            assert query_audit.manifest_sha256 == "a" * 64
            assert "payload" not in query_audit.to_wire()
            assert AUTH_TOKEN not in str(query_audit.to_wire())

    asyncio.run(run())


def test_query_fails_closed_when_audit_sink_is_unavailable() -> None:
    async def run() -> None:
        envelope = _envelope(42)
        app = create_snapshot_query_app(
            router=_router(_page(envelope)),
            authenticator=BearerTokenAuthenticator(
                token=AUTH_TOKEN,
                principal="gateway-a",
            ),
            audit_sink=_AuditSink(fail=True),
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
        ):
            response = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope),
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            )
            assert response.status_code == 503
            assert response.json()["detail"]["code"] == "QUERY_AUDIT_UNAVAILABLE"

    asyncio.run(run())


def test_background_parity_mismatch_latches_hot_quarantine() -> None:
    async def run() -> None:
        original = _envelope(42)
        cold = _FakeQuery(_page(original))
        hot = _FakeQuery(_page(original))
        router = SnapshotQueryRouter(
            cold_query=cold,
            hot_query=hot,
            projection_cursor=_FakeCursor(),
            parity_sample_interval_ms=10,
        )
        await router.start()
        arguments = {
            "snapshot": _snapshot(),
            "stream": original.stream,
            "start_event_time_ms": 0,
            "end_event_time_ms": 2_000_000_000_000,
            "limit": 10,
        }
        first = await router.query(**arguments)
        assert first.backend == "hot"
        assert router.parity_status().registered_probes == 1
        hot.page = _page(_envelope(42, quantity="0.03000000"))
        deadline = asyncio.get_running_loop().time() + 1
        while not router.parity_status().hot_quarantined:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("background parity did not quarantine hot")
            await asyncio.sleep(0.01)
        status = router.parity_status()
        assert status.samples_failed == 1
        fallback = await router.query(**arguments)
        assert fallback.backend == "cold"
        assert fallback.hot_quarantined is True
        with pytest.raises(HotProjectionQuarantinedError):
            await router.query(**arguments, preference=QueryPreference.HOT)
        await router.stop()

    asyncio.run(run())


def test_cold_query_prunes_only_segments_from_other_logical_streams() -> None:
    envelope = _envelope(42)
    envelope_bytes = canonical_envelope_bytes(envelope)
    envelope_sha256 = hashlib.sha256(envelope_bytes).hexdigest()

    def manifest(
        *,
        version: int,
        partition_key: str,
        offset: int,
        parent_snapshot: MarketDataSnapshotRef | None = None,
    ):
        return MarketDataManifestV1(
            data_epoch="epoch-1",
            snapshot_version=version,
            parent_snapshot=parent_snapshot,
            segment=ParquetArchiveSegmentV1(
                object_uri=f"s3://archive/segment-{offset}.parquet",
                content_sha256="b" * 64,
                byte_size=1,
                parquet_schema_sha256=PARQUET_SCHEMA_SHA256,
                row_count=1,
                partition_key=partition_key,
                kafka_topic=MARKET_EVENTS_TOPIC,
                kafka_partition=0,
                first_kafka_offset=offset,
                last_kafka_offset=offset,
                start_event_time_ms=envelope.event_time_ms,
                end_event_time_ms=envelope.event_time_ms,
                envelope_sha256s=(envelope_sha256,),
                sequence_start=42,
                sequence_end=42,
            ),
        )

    other = manifest(version=1, partition_key="other:stream@agg_trade", offset=0)
    target = manifest(
        version=2,
        partition_key=envelope.partition_key,
        offset=1,
        parent_snapshot=MarketDataSnapshotRef(
            data_epoch="epoch-1",
            snapshot_version=1,
            manifest_uri="s3://archive/manifest-1.json",
            manifest_sha256=other.sha256(),
        ),
    )

    class CountingArchive(ImmutableParquetMarketEventArchive):
        def __init__(self) -> None:
            super().__init__(object_store=InMemoryImmutableObjectStore())
            self.loaded: list[str] = []

        async def load_chain(self, _: Any):
            return (other, target)

        async def load_segment_rows(self, selected: MarketDataManifestV1):
            self.loaded.append(selected.segment.partition_key)
            if selected is other:
                raise AssertionError("other logical stream must be pruned")
            return (
                ArchivedMarketEventRow(
                    kafka_topic=MARKET_EVENTS_TOPIC,
                    kafka_partition=0,
                    kafka_offset=1,
                    envelope_sha256=envelope_sha256,
                    envelope=envelope,
                ),
            )

    async def run() -> None:
        archive = CountingArchive()
        page = await ParquetMarketEventQuery(archive=archive).query(
            snapshot=_snapshot(),
            stream=envelope.stream,
            start_event_time_ms=0,
            end_event_time_ms=2_000_000_000_000,
            limit=10,
        )
        assert page.events == (envelope,)
        assert archive.loaded == [envelope.partition_key]

    asyncio.run(run())
