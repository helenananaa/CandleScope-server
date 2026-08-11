from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

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
from app.server_runtime.query_api import create_snapshot_query_app
from app.server_runtime.query_control import (
    ClearHotProjectionQuarantineCommand,
    HotProjectionControlState,
    HotProjectionGenerationConflictError,
    HotProjectionNotQuarantinedError,
)
from app.server_runtime.query_router import (
    HotProjectionParityError,
    HotProjectionQuarantinedError,
    QueryPreference,
    SnapshotQueryRouter,
)
from app.server_runtime.query_security import (
    BearerTokenAuthenticator,
    QueryAuditEvent,
)
from app.server_runtime.query_settings import (
    QueryServiceConfigurationError,
    QueryServiceSettings,
)

QUERY_TOKEN = "phase1h-query-token-0000000000000000"
CONTROL_TOKEN = "phase1h-control-token-00000000000000"


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


@dataclass(slots=True)
class _SharedState:
    state: HotProjectionControlState


class _SharedControlStore:
    def __init__(self, shared: _SharedState, *, instance_id: str) -> None:
        self.shared = shared
        self.instance_id = instance_id
        self.started = False
        self.events: list[QueryAuditEvent] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def status(self) -> HotProjectionControlState:
        assert self.started
        return self.shared.state

    async def latch(self, reason: str) -> HotProjectionControlState:
        assert self.started
        if not self.shared.state.active:
            self.shared.state = HotProjectionControlState(
                backend_id=self.shared.state.backend_id,
                generation=self.shared.state.generation + 1,
                active=True,
                reason=reason,
                latched_by=self.instance_id,
                latched_at_ms=time.time_ns() // 1_000_000,
                cleared_by=None,
                clear_reason_code=None,
                cleared_at_ms=None,
            )
            self.events.append(
                QueryAuditEvent(
                    request_id="shared-latch",
                    principal=self.instance_id,
                    action="latch_hot_projection_quarantine",
                    outcome="quarantine_latched",
                    status_code=503,
                    timestamp_ms=time.time_ns() // 1_000_000,
                    latency_ms=0,
                    backend=self.shared.state.backend_id,
                    control_generation=self.shared.state.generation,
                    reason_code="hot_cold_parity_mismatch",
                )
            )
        return self.shared.state

    async def clear(
        self,
        command: ClearHotProjectionQuarantineCommand,
    ) -> HotProjectionControlState:
        assert self.started
        if not self.shared.state.active:
            raise HotProjectionNotQuarantinedError("not quarantined")
        if command.expected_generation != self.shared.state.generation:
            raise HotProjectionGenerationConflictError("stale generation")
        current = self.shared.state
        self.shared.state = HotProjectionControlState(
            backend_id=current.backend_id,
            generation=current.generation,
            active=False,
            reason=current.reason,
            latched_by=current.latched_by,
            latched_at_ms=current.latched_at_ms,
            cleared_by=command.principal,
            clear_reason_code=command.reason_code,
            cleared_at_ms=command.timestamp_ms,
        )
        self.events.append(
            QueryAuditEvent(
                request_id=command.request_id,
                principal=command.principal,
                action="clear_hot_projection_quarantine",
                outcome="quarantine_cleared",
                status_code=200,
                timestamp_ms=command.timestamp_ms,
                latency_ms=0,
                backend=current.backend_id,
                control_generation=current.generation,
                reason_code=command.reason_code,
            )
        )
        return self.shared.state

    async def emit(self, event: QueryAuditEvent) -> None:
        assert self.started
        self.events.append(event)


def _router(
    page: MarketEventPage,
    store: _SharedControlStore,
) -> tuple[SnapshotQueryRouter, _FakeQuery]:
    hot = _FakeQuery(page)
    return (
        SnapshotQueryRouter(
            cold_query=_FakeQuery(page),
            hot_query=hot,
            projection_cursor=_FakeCursor(),
            quarantine_store=store,
        ),
        hot,
    )


def _arguments(envelope: Any) -> dict[str, Any]:
    return {
        "snapshot": _snapshot(),
        "stream": envelope.stream,
        "start_event_time_ms": 0,
        "end_event_time_ms": 2_000_000_000_000,
        "limit": 10,
    }


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


def test_two_routers_share_generation_fenced_quarantine_and_manual_clear() -> None:
    async def run() -> None:
        original = _envelope(42)
        shared = _SharedState(
            HotProjectionControlState.initial("clickhouse-market-events-v1")
        )
        store_a = _SharedControlStore(shared, instance_id="query-a")
        store_b = _SharedControlStore(shared, instance_id="query-b")
        router_a, hot_a = _router(_page(original), store_a)
        router_b, _ = _router(_page(original), store_b)
        await router_a.start()
        await router_b.start()
        try:
            assert (await router_a.query(**_arguments(original))).backend == "hot"
            hot_a.page = _page(_envelope(42, quantity="0.03000000"))
            with pytest.raises(HotProjectionParityError):
                await router_a.query(**_arguments(original))
            shared_fallback = await router_b.query(**_arguments(original))
            assert shared_fallback.backend == "cold"
            assert shared_fallback.hot_quarantined is True
            with pytest.raises(HotProjectionQuarantinedError):
                await router_b.query(
                    **_arguments(original),
                    preference=QueryPreference.HOT,
                )
            with pytest.raises(HotProjectionGenerationConflictError):
                await router_b.clear_hot_quarantine(
                    ClearHotProjectionQuarantineCommand(
                        expected_generation=2,
                        principal="operator-a",
                        reason_code="projection_repaired",
                        request_id="clear-stale",
                        timestamp_ms=time.time_ns() // 1_000_000,
                    )
                )
            cleared = await router_b.clear_hot_quarantine(
                ClearHotProjectionQuarantineCommand(
                    expected_generation=1,
                    principal="operator-a",
                    reason_code="projection_repaired",
                    request_id="clear-current",
                    timestamp_ms=time.time_ns() // 1_000_000,
                )
            )
            assert cleared.active is False
            assert cleared.generation == 1
            assert (await router_b.query(**_arguments(original))).backend == "hot"
        finally:
            await router_b.stop()
            await router_a.stop()

    asyncio.run(run())


def test_control_endpoint_requires_separate_token_and_generation() -> None:
    async def run() -> None:
        envelope = _envelope(42)
        shared = _SharedState(
            HotProjectionControlState.initial("clickhouse-market-events-v1")
        )
        store = _SharedControlStore(shared, instance_id="query-a")
        router, _ = _router(_page(envelope), store)
        app = create_snapshot_query_app(
            router=router,
            authenticator=BearerTokenAuthenticator(
                token=QUERY_TOKEN,
                principal="gateway-a",
            ),
            control_authenticator=BearerTokenAuthenticator(
                token=CONTROL_TOKEN,
                principal="operator-a",
            ),
            audit_sink=store,
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
        ):
            await store.latch("background hot/cold page mismatch")
            denied = await client.get(
                "/api/v1/server/control/hot-projection",
                headers={"Authorization": f"Bearer {QUERY_TOKEN}"},
            )
            assert denied.status_code == 401
            control_headers = {"Authorization": f"Bearer {CONTROL_TOKEN}"}
            status = await client.get(
                "/api/v1/server/control/hot-projection",
                headers=control_headers,
            )
            assert status.status_code == 200
            assert status.json()["hot_projection"]["generation"] == 1
            stale = await client.post(
                "/api/v1/server/control/hot-projection/clear",
                headers=control_headers,
                json={
                    "expected_generation": 2,
                    "reason_code": "projection_repaired",
                },
            )
            assert stale.status_code == 409
            assert (
                stale.json()["detail"]["code"] == "HOT_PROJECTION_GENERATION_CONFLICT"
            )
            cleared = await client.post(
                "/api/v1/server/control/hot-projection/clear",
                headers={
                    **control_headers,
                    "X-Request-ID": "operator-a:clear-1",
                },
                json={
                    "expected_generation": 1,
                    "reason_code": "projection_repaired",
                },
            )
            assert cleared.status_code == 200
            assert cleared.json()["hot_projection"]["active"] is False
            success = await client.post(
                "/api/v1/server/market-events/query",
                headers={"Authorization": f"Bearer {QUERY_TOKEN}"},
                json=_body(envelope),
            )
            assert success.status_code == 200
            assert any(
                event.action == "clear_hot_projection_quarantine"
                and event.outcome == "quarantine_cleared"
                and event.request_id == "operator-a:clear-1"
                for event in store.events
            )
            assert QUERY_TOKEN not in str([event.to_wire() for event in store.events])
            assert CONTROL_TOKEN not in str([event.to_wire() for event in store.events])

    asyncio.run(run())


def test_postgres_control_settings_are_fail_closed_and_redact_credentials() -> None:
    base = {
        "CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL": "http://clickhouse:8123",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER": "query",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD": "clickhouse-secret",
        "CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL": "http://minio:9000",
        "CANDLESCOPE_SERVER_QUERY_S3_BUCKET": "archive",
        "CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID": "minio-access",
        "CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY": "minio-secret",
        "CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN": QUERY_TOKEN,
        "CANDLESCOPE_SERVER_QUERY_INSTANCE_ID": "query-a",
    }
    with pytest.raises(QueryServiceConfigurationError):
        QueryServiceSettings.from_env(base)
    configured = QueryServiceSettings.from_env(
        {
            **base,
            "CANDLESCOPE_SERVER_QUERY_POSTGRES_DSN": (
                "postgresql://query-control:postgres-secret@postgres/candlescope"
            ),
            "CANDLESCOPE_SERVER_QUERY_CONTROL_BEARER_TOKEN": CONTROL_TOKEN,
        }
    )
    rendered = repr(configured)
    assert configured.control_backend == "postgres"
    assert configured.instance_id == "query-a"
    assert "postgres-secret" not in rendered
    assert QUERY_TOKEN not in rendered
    assert CONTROL_TOKEN not in rendered
    with pytest.raises(QueryServiceConfigurationError):
        QueryServiceSettings.from_env(
            {
                **base,
                "CANDLESCOPE_SERVER_QUERY_POSTGRES_DSN": "postgresql://postgres/db",
                "CANDLESCOPE_SERVER_QUERY_CONTROL_BEARER_TOKEN": QUERY_TOKEN,
            }
        )


def test_audit_v2_encodes_large_identifiers_as_ijson_decimal_strings() -> None:
    event = QueryAuditEvent(
        request_id="large-identifiers",
        action="snapshot_market_event_query",
        outcome="success",
        status_code=200,
        timestamp_ms=1_700_000_000_000,
        latency_ms=1,
        snapshot_version=18_446_744_073_709_551_615,
        control_generation=9_223_372_036_854_775_807,
    )
    wire = event.to_wire()
    assert wire["snapshot_version"] == "18446744073709551615"
    assert wire["control_generation"] == "9223372036854775807"
    with pytest.raises(ValueError, match="safe ASCII"):
        ClearHotProjectionQuarantineCommand(
            expected_generation=1,
            principal="operator-a",
            reason_code="επισκευή",
            request_id="unicode-reason",
            timestamp_ms=1_700_000_000_000,
        )
