from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventPage,
    MarketEventRange,
)
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.query_api import create_snapshot_query_app
from app.server_runtime.query_identity import (
    QueryCallerIdentity,
    QueryWorkspaceScopeError,
    normalize_workspace_id,
    require_workspace_scope,
)
from app.server_runtime.query_router import SnapshotQueryRouter
from app.server_runtime.query_security import BearerTokenAuthenticator, QueryAuditEvent
from app.server_runtime.query_settings import (
    QueryServiceConfigurationError,
    QueryServiceSettings,
)

AUTH_TOKEN = "phase1y-workspace-token-000000000000"


def _envelope(sequence: int = 42) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        MarketEvent(
            event_type=StreamType.AGG_TRADE,
            symbol="BTCUSDT",
            exchange="binance",
            event_time_ms=1_700_000_000_000 + sequence,
            received_at_ms=1_700_000_000_100 + sequence,
            source=DataSource.WEBSOCKET,
            data={
                "agg_trade_id": sequence,
                "price": 100000.1,
                "quantity": 0.025,
                "price_text": "100000.1000",
                "quantity_text": "0.02500000",
                "first_trade_id": sequence * 10,
                "last_trade_id": sequence * 10 + 2,
                "trade_time_ms": 1_700_000_000_000 + sequence,
                "is_buyer_maker": False,
            },
            stream_key="futures:BTCUSDT@aggTrade",
            sequence=sequence,
            market_type="futures",
        ),
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
    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def __init__(self, page: MarketEventPage) -> None:
        self.page = page

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
    def __init__(self) -> None:
        self.events: list[QueryAuditEvent] = []

    async def emit(self, event: QueryAuditEvent) -> None:
        self.events.append(event)


def _router(page: MarketEventPage) -> SnapshotQueryRouter:
    return SnapshotQueryRouter(
        cold_query=_FakeQuery(page),
        hot_query=_FakeQuery(page),
        projection_cursor=_FakeCursor(),
    )


def _body(
    envelope: Any,
    *,
    organization_id: str | None = "org-alpha",
    workspace_id: str | None = "ws-research",
) -> dict[str, Any]:
    snapshot = _snapshot()
    payload: dict[str, Any] = {
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
    if organization_id is not None:
        payload["organization_id"] = organization_id
    if workspace_id is not None:
        payload["workspace_id"] = workspace_id
    return payload


def _settings(**overrides: Any) -> QueryServiceSettings:
    values: dict[str, Any] = {
        "kafka_bootstrap_servers": ("kafka:9092",),
        "clickhouse_url": "http://clickhouse:8123",
        "clickhouse_user": "query",
        "clickhouse_password": "clickhouse-secret",
        "s3_endpoint_url": "http://minio:9000",
        "s3_bucket": "archive",
        "s3_access_key_id": "minio-access",
        "s3_secret_access_key": "minio-secret",
        "auth_bearer_token": AUTH_TOKEN,
        "postgres_dsn": None,
        "control_bearer_token": None,
        "instance_id": "query-a",
        "control_backend": "process",
        "auth_organization_id": "org-alpha",
        "auth_workspace_id": "ws-research",
    }
    values.update(overrides)
    return QueryServiceSettings(**values)


def test_workspace_id_rejects_wildcards_and_requires_token_match() -> None:
    identity = QueryCallerIdentity(
        principal="gateway-a",
        organization_id="org-alpha",
        workspace_id="Ws-Research",
    )
    assert identity.workspace_id == "ws-research"
    assert require_workspace_scope(identity, "ws-research") == "ws-research"
    with pytest.raises(ValueError, match="identifier"):
        normalize_workspace_id("*")
    with pytest.raises(ValueError, match="reserved"):
        QueryCallerIdentity(
            principal="gateway-a",
            organization_id="org-alpha",
            workspace_id="all",
        )
    with pytest.raises(QueryWorkspaceScopeError) as missing:
        require_workspace_scope(identity, None)
    assert missing.value.code == "WORKSPACE_SCOPE_REQUIRED"
    with pytest.raises(QueryWorkspaceScopeError) as denied:
        require_workspace_scope(identity, "ws-trading")
    assert denied.value.code == "WORKSPACE_SCOPE_DENIED"
    with pytest.raises(QueryWorkspaceScopeError) as invalid:
        require_workspace_scope(identity, "all")
    assert invalid.value.code == "WORKSPACE_SCOPE_INVALID"


def test_scoped_query_requires_matching_workspace_and_audits_it() -> None:
    async def run() -> None:
        envelope = _envelope()
        audit = _AuditSink()
        app = create_snapshot_query_app(
            router=_router(_page(envelope)),
            authenticator=BearerTokenAuthenticator(
                token=AUTH_TOKEN,
                principal="gateway-a",
                organization_id="org-alpha",
                workspace_id="ws-research",
            ),
            audit_sink=audit,
        )
        headers = {"Authorization": f"Bearer {AUTH_TOKEN}"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
        ):
            missing = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(
                    envelope,
                    organization_id="org-alpha",
                    workspace_id=None,
                ),
                headers=headers,
            )
            assert missing.status_code == 422
            assert missing.json()["detail"]["code"] == "WORKSPACE_SCOPE_REQUIRED"
            denied = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope, workspace_id="ws-trading"),
                headers=headers,
            )
            assert denied.status_code == 403
            assert denied.json()["detail"]["code"] == "WORKSPACE_SCOPE_DENIED"
            success = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope),
                headers=headers,
            )
            assert success.status_code == 200
            assert audit.events[-1].organization_id == "org-alpha"
            assert audit.events[-1].workspace_id == "ws-research"
            assert AUTH_TOKEN not in str(audit.events[-1].to_wire())
            metrics = await client.get("/metrics", headers=headers)
            assert metrics.status_code == 200
            payload = metrics.json()
            assert payload["workspace_scope_denied_total"] == 2
            assert payload["organization_scope_denied_total"] == 0

    asyncio.run(run())


def test_unscoped_token_cannot_self_assert_a_workspace() -> None:
    async def run() -> None:
        envelope = _envelope()
        app = create_snapshot_query_app(
            router=_router(_page(envelope)),
            authenticator=BearerTokenAuthenticator(
                token=AUTH_TOKEN,
                principal="gateway-a",
            ),
            audit_sink=_AuditSink(),
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client,
        ):
            asserted = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(
                    envelope,
                    organization_id=None,
                    workspace_id="ws-research",
                ),
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            )
            assert asserted.status_code == 422
            assert asserted.json()["detail"]["code"] == "WORKSPACE_SCOPE_NOT_BOUND"
            legacy = await client.post(
                "/api/v1/server/market-events/query",
                json=_body(envelope, organization_id=None, workspace_id=None),
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            )
            assert legacy.status_code == 200

    asyncio.run(run())


def test_organization_and_workspace_must_be_bound_together() -> None:
    with pytest.raises(ValueError, match="together"):
        BearerTokenAuthenticator(
            token=AUTH_TOKEN,
            principal="gateway-a",
            organization_id="org-alpha",
        )
    with pytest.raises(ValueError, match="together"):
        BearerTokenAuthenticator(
            token=AUTH_TOKEN,
            principal="gateway-a",
            workspace_id="ws-research",
        )
    with pytest.raises(QueryServiceConfigurationError, match="together"):
        _settings(auth_workspace_id=None)
    with pytest.raises(QueryServiceConfigurationError, match="together"):
        _settings(auth_organization_id=None)
    with pytest.raises(QueryServiceConfigurationError, match="reserved"):
        _settings(auth_workspace_id="all")
    settings = _settings()
    assert settings.auth_organization_id == "org-alpha"
    assert settings.auth_workspace_id == "ws-research"


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
