from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.replay.sources.trade_source import TradeReplaySource
from app.server_contracts import MarketDataSnapshotRef, MarketEventPage
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import canonical_envelope_bytes
from app.server_runtime.query_pagination import SnapshotQueryRow, paginate_snapshot_rows
from app.server_runtime.replay_lease import (
    ReplaySessionLeaseFencedError,
    ReplaySessionSnapshotConflictError,
)
from app.server_runtime.replay_leased import (
    LEASED_SERVER_SNAPSHOT_REPLAY_SCHEMA_VERSION,
    load_leased_server_snapshot,
)
from app.server_runtime.replay_snapshot import (
    ReplayServerSnapshotPin,
    ServerSnapshotTradeReader,
)
from app.server_runtime.testing import InMemoryReplaySessionLeaseStore

START_MS = 1_700_000_000_000
AUTH_ORG = "org-alpha"
AUTH_WS = "ws-research"


def _envelope(sequence: int) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        MarketEvent(
            event_type=StreamType.AGG_TRADE,
            symbol="BTCUSDT",
            exchange="binance",
            event_time_ms=START_MS + sequence,
            received_at_ms=START_MS + 100 + sequence,
            source=DataSource.WEBSOCKET,
            data={
                "agg_trade_id": sequence,
                "price": 100000.1,
                "quantity": 0.025,
                "price_text": "100000.1000",
                "quantity_text": "0.02500000",
                "first_trade_id": sequence * 10,
                "last_trade_id": sequence * 10 + 2,
                "trade_time_ms": START_MS + sequence,
                "is_buyer_maker": False,
            },
            stream_key="futures:BTCUSDT@aggTrade",
            sequence=sequence,
            market_type="futures",
        ),
        previous_sequence=None if sequence == 42 else sequence - 1,
        published_at_ms=START_MS + 1_000 + sequence,
    )


def _snapshot(*, digest: str = "a" * 64) -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="epoch-1",
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256=digest,
    )


def _pin(snapshot: MarketDataSnapshotRef | None = None) -> ReplayServerSnapshotPin:
    return ReplayServerSnapshotPin(
        snapshot=snapshot or _snapshot(),
        start_event_time_ms=START_MS + 42,
        end_event_time_ms=START_MS + 44,
        expected_first_agg_trade_id=42,
        expected_last_agg_trade_id=44,
        row_count=3,
    )


class _PagingColdQuery:
    def __init__(self, snapshot: MarketDataSnapshotRef, envelopes: list[Any]) -> None:
        self.snapshot = snapshot
        self.rows = [
            SnapshotQueryRow(
                envelope=envelope,
                envelope_sha256=hashlib.sha256(
                    canonical_envelope_bytes(envelope)
                ).hexdigest(),
                envelope_bytes=canonical_envelope_bytes(envelope),
                kafka_partition=0,
                kafka_offset=index,
            )
            for index, envelope in enumerate(envelopes)
        ]
        self.calls: list[dict[str, Any]] = []

    async def query(self, **kwargs: Any) -> MarketEventPage:
        self.calls.append(kwargs)
        return paginate_snapshot_rows(
            self.rows,
            snapshot=self.snapshot,
            stream=kwargs["stream"],
            start_event_time_ms=kwargs["start_event_time_ms"],
            end_event_time_ms=kwargs["end_event_time_ms"],
            limit=kwargs["limit"],
            max_page_rows=kwargs["limit"],
            cursor=kwargs.get("cursor"),
        )


async def _acquire(
    store: InMemoryReplaySessionLeaseStore, snapshot: MarketDataSnapshotRef
):
    return await store.acquire(
        session_id="sess-alpha",
        worker_id="worker-a",
        snapshot=snapshot,
        organization_id=AUTH_ORG,
        workspace_id=AUTH_WS,
        lease_ttl_ms=500,
    )


def test_leased_load_requires_active_matching_lease_then_materializes_trades() -> None:
    async def run() -> None:
        pin = _pin()
        query = _PagingColdQuery(
            pin.snapshot,
            [_envelope(42), _envelope(43), _envelope(44)],
        )
        now = [1_000]
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        lease = await _acquire(store, pin.snapshot)
        loaded = await load_leased_server_snapshot(
            store,
            lease,
            query,
            pin,
            query_page_limit=10,
            page_rows=10,
        )
        assert loaded.schema_version == LEASED_SERVER_SNAPSHOT_REPLAY_SCHEMA_VERSION
        assert query.calls
        assert all("preference" not in call for call in query.calls)
        source = TradeReplaySource(loaded.reader)
        trades = [source.next() for _ in range(3)]
        assert [item.agg_trade_id for item in trades] == [42, 43, 44]
        assert source.exhausted()
        wire = loaded.to_public_ref()
        assert wire["lease"]["session_id"] == "sess-alpha"
        assert wire["lease"]["organization_id"] == AUTH_ORG
        assert "lease_token" not in wire["lease"]
        assert lease.lease_token not in str(wire)

    asyncio.run(run())


def test_expired_or_stale_lease_never_reaches_the_query_port() -> None:
    async def run() -> None:
        pin = _pin()
        query = _PagingColdQuery(
            pin.snapshot, [_envelope(42), _envelope(43), _envelope(44)]
        )
        now = [1_000]
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        lease = await _acquire(store, pin.snapshot)
        now[0] += 500
        with pytest.raises(ReplaySessionLeaseFencedError, match="expired"):
            await load_leased_server_snapshot(store, lease, query, pin)
        assert query.calls == []
        takeover = await store.acquire(
            session_id="sess-alpha",
            worker_id="worker-b",
            snapshot=pin.snapshot,
            organization_id=AUTH_ORG,
            workspace_id=AUTH_WS,
            lease_ttl_ms=500,
        )
        with pytest.raises(ReplaySessionLeaseFencedError):
            await load_leased_server_snapshot(store, lease, query, pin)
        assert query.calls == []
        loaded = await load_leased_server_snapshot(store, takeover, query, pin)
        assert loaded.lease.worker_id == "worker-b"
        assert query.calls

    asyncio.run(run())


def test_snapshot_mismatch_does_not_query_and_unleased_1q_path_remains() -> None:
    async def run() -> None:
        pin = _pin()
        other = _pin(snapshot=_snapshot(digest="b" * 64))
        query = _PagingColdQuery(
            pin.snapshot,
            [_envelope(42), _envelope(43), _envelope(44)],
        )
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: 1_000)
        lease = await _acquire(store, pin.snapshot)
        with pytest.raises(ReplaySessionSnapshotConflictError):
            await load_leased_server_snapshot(store, lease, query, other)
        assert query.calls == []
        reader = await ServerSnapshotTradeReader.load(query, pin)
        assert len(query.calls) >= 1
        assert reader.snapshot_pin.snapshot == pin.snapshot

    asyncio.run(run())


def test_leased_bind_is_not_a_worker_pool_and_fastapi_stays_locked() -> None:
    from pathlib import Path

    source = Path(
        Path(__file__).resolve().parents[1]
        / "app"
        / "server_runtime"
        / "replay_leased.py"
    ).read_text(encoding="utf-8")
    assert "ReplayService" not in source
    assert "start_replay_runtime" not in source
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    settings.require_runtime_support()
