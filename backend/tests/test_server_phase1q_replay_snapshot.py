from __future__ import annotations

import ast
import asyncio
import hashlib
from pathlib import Path
from typing import Any

import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.data_engine.market_data import MarketChannel, MarketStreamKey
from app.deployment.profile import (
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.replay.errors import ReplayErrorCode
from app.replay.sources.trade_source import TradeReplaySource
from app.server_contracts import MarketDataSnapshotRef, MarketEventPage
from app.server_runtime.adapters import (
    BINANCE_AGG_TRADE_PAYLOAD_SCHEMA,
    AggTradeEnvelopeAdapter,
)
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import canonical_envelope_bytes
from app.server_runtime.query_pagination import (
    SnapshotQueryRow,
    paginate_snapshot_rows,
)
from app.server_runtime.replay_snapshot import (
    REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION,
    SERVER_SNAPSHOT_SOURCE_QUALITY,
    SERVER_SNAPSHOT_TRADE_SOURCE,
    ReplayServerSnapshotPin,
    ServerSnapshotReplayError,
    ServerSnapshotTradeReader,
    replay_trade_from_envelope,
)

BACKEND_ROOT = Path(__file__).parents[1]
REPLAY_ROOT = BACKEND_ROOT / "app" / "replay"
START_MS = 1_700_000_000_000


def _market_event(sequence: int, *, quantity: str = "0.02500000") -> MarketEvent:
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=START_MS + sequence,
        received_at_ms=START_MS + 100 + sequence,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": sequence,
            "price": 100000.1,
            "quantity": float(quantity),
            "price_text": "100000.1000",
            "quantity_text": quantity,
            "first_trade_id": sequence * 10,
            "last_trade_id": sequence * 10 + 2,
            "trade_time_ms": START_MS + sequence,
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
        published_at_ms=START_MS + 1_000 + sequence,
    )


def _snapshot(version: int = 4, digest: str = "a" * 64) -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="epoch-1",
        snapshot_version=version,
        manifest_uri=f"s3://archive/snapshot-{version}.json",
        manifest_sha256=digest,
    )


def _pin(
    *,
    first: int = 42,
    last: int = 44,
    snapshot: MarketDataSnapshotRef | None = None,
    stream: MarketStreamKey | None = None,
) -> ReplayServerSnapshotPin:
    kwargs: dict[str, Any] = {
        "snapshot": snapshot or _snapshot(),
        "start_event_time_ms": START_MS + first,
        "end_event_time_ms": START_MS + last,
        "expected_first_agg_trade_id": first,
        "expected_last_agg_trade_id": last,
        "row_count": last - first + 1,
    }
    if stream is not None:
        kwargs["stream"] = stream
    return ReplayServerSnapshotPin(**kwargs)


def _row(offset: int, envelope: Any) -> SnapshotQueryRow:
    blob = canonical_envelope_bytes(envelope)
    return SnapshotQueryRow(
        envelope=envelope,
        envelope_sha256=hashlib.sha256(blob).hexdigest(),
        envelope_bytes=blob,
        kafka_partition=0,
        kafka_offset=offset,
    )


class _PagingColdQuery:
    def __init__(
        self,
        snapshot: MarketDataSnapshotRef,
        envelopes: list[Any],
        *,
        page_size: int = 2,
        snapshot_override: MarketDataSnapshotRef | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.rows = [_row(index, envelope) for index, envelope in enumerate(envelopes)]
        self.page_size = page_size
        self.snapshot_override = snapshot_override
        self.calls: list[dict[str, Any]] = []

    async def query(self, **kwargs: Any) -> MarketEventPage:
        self.calls.append(kwargs)
        page = paginate_snapshot_rows(
            self.rows,
            snapshot=self.snapshot,
            stream=kwargs["stream"],
            start_event_time_ms=kwargs["start_event_time_ms"],
            end_event_time_ms=kwargs["end_event_time_ms"],
            limit=min(kwargs["limit"], self.page_size),
            max_page_rows=kwargs["limit"],
            cursor=kwargs.get("cursor"),
        )
        if self.snapshot_override is None:
            return page
        return MarketEventPage(
            snapshot=self.snapshot_override,
            events=page.events,
            covered_range=page.covered_range,
            next_cursor=page.next_cursor,
        )


class _BrokenQuery:
    async def query(self, **_: Any) -> MarketEventPage:
        raise RuntimeError("object store unavailable")


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            modules.append(node.module or "")
    return modules


def test_pin_rejects_any_stream_outside_the_frozen_phase1a_agg_trade() -> None:
    with pytest.raises(ServerSnapshotReplayError, match="BTCUSDT agg_trade") as exc:
        _pin(
            stream=MarketStreamKey.build(
                "binance",
                "futures",
                "ETHUSDT",
                MarketChannel.AGG_TRADE,
            )
        )
    assert exc.value.code is ReplayErrorCode.UNSUPPORTED_SOURCE


def test_pin_rejects_zero_row_count_and_implicit_latest_snapshot() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        MarketDataSnapshotRef(
            data_epoch="epoch-1",
            snapshot_version=0,
            manifest_uri="s3://archive/snapshot-0.json",
            manifest_sha256="a" * 64,
        )
    with pytest.raises(ValueError, match="greater than zero"):
        ReplayServerSnapshotPin(
            snapshot=_snapshot(),
            start_event_time_ms=START_MS,
            end_event_time_ms=START_MS,
            expected_first_agg_trade_id=1,
            expected_last_agg_trade_id=1,
            row_count=0,
        )


def test_reader_materializes_paged_cold_query_into_existing_trade_source() -> None:
    async def run() -> None:
        pin = _pin()
        query = _PagingColdQuery(
            pin.snapshot,
            [_envelope(42), _envelope(43), _envelope(44)],
            page_size=2,
        )
        reader = await ServerSnapshotTradeReader.load(
            query,
            pin,
            query_page_limit=2,
            page_rows=2,
        )
        assert len(query.calls) == 2
        assert all("preference" not in call for call in query.calls)
        assert {call["snapshot"] for call in query.calls} == {pin.snapshot}
        source = TradeReplaySource(reader)
        trades = [source.next() for _ in range(3)]
        assert [item.agg_trade_id for item in trades] == [42, 43, 44]
        assert trades[0] is not None
        assert trades[0].source == SERVER_SNAPSHOT_TRADE_SOURCE
        assert trades[0].price == "100000.1"
        assert trades[0].quantity == "0.025"
        assert trades[0].quote_quantity == "2500.0025"
        assert trades[0].is_buyer_maker is False
        assert source.exhausted()
        reference = source.snapshot_ref()
        assert reference["source_quality"] == SERVER_SNAPSHOT_SOURCE_QUALITY
        assert reference["completeness"] == "exact"
        assert reference["server_snapshot"]["schema_version"] == (
            REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION
        )
        assert reference["server_snapshot"]["manifest_sha256"] == (
            pin.snapshot.manifest_sha256
        )
        assert reference["server_snapshot"]["snapshot_version"] == 4

    asyncio.run(run())


def test_reader_pages_and_positions_without_rescanning_live_data() -> None:
    async def run() -> None:
        pin = _pin()
        reader = await ServerSnapshotTradeReader.load(
            _PagingColdQuery(
                pin.snapshot,
                [_envelope(42), _envelope(43), _envelope(44)],
                page_size=1,
            ),
            pin,
            query_page_limit=1,
            page_rows=1,
        )
        first = reader.read_page(limit=1)
        assert [item.agg_trade_id for item in first.trades] == [42]
        assert first.exhausted is False
        second = reader.read_page(first.next_cursor, limit=1)
        assert [item.agg_trade_id for item in second.trades] == [43]
        source = TradeReplaySource(reader)
        source.next()
        positioned = source.fork_at_sequence(
            1,
            last_event_time_ms=START_MS + 42,
        )
        replayed = positioned.next()
        assert replayed is not None
        assert replayed.agg_trade_id == 43
        assert source.cursor().source_sequence == 1

    asyncio.run(run())


def test_load_fails_closed_on_snapshot_drift_gap_and_query_failure() -> None:
    async def run() -> None:
        pin = _pin()
        with pytest.raises(ServerSnapshotReplayError) as snapshot_exc:
            await ServerSnapshotTradeReader.load(
                _PagingColdQuery(
                    pin.snapshot,
                    [_envelope(42), _envelope(43), _envelope(44)],
                    page_size=8,
                    snapshot_override=_snapshot(version=8, digest="b" * 64),
                ),
                pin,
            )
        assert snapshot_exc.value.code is ReplayErrorCode.DATASET_MISMATCH

        with pytest.raises(ServerSnapshotReplayError) as gap_exc:
            await ServerSnapshotTradeReader.load(
                _PagingColdQuery(pin.snapshot, [_envelope(42), _envelope(44)]),
                pin,
            )
        assert gap_exc.value.code is ReplayErrorCode.DATA_GAP

        with pytest.raises(ServerSnapshotReplayError) as incomplete_exc:
            await ServerSnapshotTradeReader.load(
                _PagingColdQuery(pin.snapshot, [_envelope(42), _envelope(43)]),
                pin,
            )
        assert incomplete_exc.value.code is ReplayErrorCode.DATA_GAP

        with pytest.raises(ServerSnapshotReplayError) as degraded_exc:
            await ServerSnapshotTradeReader.load(_BrokenQuery(), pin)
        assert degraded_exc.value.code is ReplayErrorCode.ARCHIVE_DEGRADED

    asyncio.run(run())


def test_mapper_rejects_payload_schema_stream_and_window_escape() -> None:
    pin = _pin()
    envelope = _envelope(42)
    other_stream = envelope.build(
        event_id=envelope.event_id,
        stream=MarketStreamKey.build(
            "binance",
            "futures",
            "ETHUSDT",
            MarketChannel.AGG_TRADE,
        ),
        delivery_class=envelope.delivery_class,
        source=envelope.source,
        source_event_id=envelope.source_event_id,
        sequence_start=envelope.sequence_start,
        sequence_end=envelope.sequence_end,
        previous_sequence=envelope.previous_sequence,
        producer_id=envelope.producer_id,
        producer_epoch=envelope.producer_epoch,
        event_time_ms=envelope.event_time_ms,
        received_at_ms=envelope.received_at_ms,
        published_at_ms=envelope.published_at_ms,
        payload_schema=envelope.payload_schema,
        payload=dict(envelope.payload),
    )
    with pytest.raises(ServerSnapshotReplayError) as stream_exc:
        replay_trade_from_envelope(pin, other_stream)
    assert stream_exc.value.code is ReplayErrorCode.DATASET_MISMATCH

    wrong_schema = envelope.build(
        event_id=envelope.event_id,
        stream=envelope.stream,
        delivery_class=envelope.delivery_class,
        source=envelope.source,
        source_event_id=envelope.source_event_id,
        sequence_start=envelope.sequence_start,
        sequence_end=envelope.sequence_end,
        previous_sequence=envelope.previous_sequence,
        producer_id=envelope.producer_id,
        producer_epoch=envelope.producer_epoch,
        event_time_ms=envelope.event_time_ms,
        received_at_ms=envelope.received_at_ms,
        published_at_ms=envelope.published_at_ms,
        payload_schema="binance.kline.normalized.v1",
        payload=dict(envelope.payload),
    )
    with pytest.raises(ServerSnapshotReplayError, match="payload schema"):
        replay_trade_from_envelope(pin, wrong_schema)

    outside = envelope.build(
        event_id=envelope.event_id,
        stream=envelope.stream,
        delivery_class=envelope.delivery_class,
        source=envelope.source,
        source_event_id=envelope.source_event_id,
        sequence_start=envelope.sequence_start,
        sequence_end=envelope.sequence_end,
        previous_sequence=envelope.previous_sequence,
        producer_id=envelope.producer_id,
        producer_epoch=envelope.producer_epoch,
        event_time_ms=pin.end_event_time_ms + 1,
        received_at_ms=envelope.received_at_ms,
        published_at_ms=envelope.published_at_ms,
        payload_schema=BINANCE_AGG_TRADE_PAYLOAD_SCHEMA,
        payload=dict(envelope.payload),
    )
    with pytest.raises(ServerSnapshotReplayError, match="query window"):
        replay_trade_from_envelope(pin, outside)


def test_load_rejects_scan_and_page_budgets() -> None:
    async def run() -> None:
        pin = _pin()
        with pytest.raises(ServerSnapshotReplayError) as scan_exc:
            await ServerSnapshotTradeReader.load(
                _PagingColdQuery(
                    pin.snapshot,
                    [_envelope(42), _envelope(43), _envelope(44)],
                ),
                pin,
                max_scan_rows=2,
            )
        assert scan_exc.value.code is ReplayErrorCode.SCAN_LIMIT_EXCEEDED

        with pytest.raises(ServerSnapshotReplayError) as page_exc:
            await ServerSnapshotTradeReader.load(
                _PagingColdQuery(
                    pin.snapshot,
                    [_envelope(42), _envelope(43), _envelope(44)],
                    page_size=1,
                ),
                pin,
                query_page_limit=1,
                max_query_pages=2,
            )
        assert page_exc.value.code is ReplayErrorCode.SCAN_LIMIT_EXCEEDED

    asyncio.run(run())


def test_reader_has_no_live_or_latest_fallback_surface() -> None:
    async def run() -> None:
        pin = _pin()
        reader = await ServerSnapshotTradeReader.load(
            _PagingColdQuery(
                pin.snapshot,
                [_envelope(42), _envelope(43), _envelope(44)],
            ),
            pin,
        )
        for name in (
            "latest_snapshot",
            "live_source",
            "hot_query",
            "fallback_archive",
            "personal_archive",
        ):
            assert not hasattr(reader, name)
        assert reader.snapshot_pin.snapshot == pin.snapshot

    asyncio.run(run())


def test_replay_package_does_not_import_server_runtime() -> None:
    violations = []
    for path in sorted(REPLAY_ROOT.rglob("*.py")):
        for module in _imports(path):
            if module == "app.server_runtime" or module.startswith(
                "app.server_runtime."
            ):
                violations.append(f"{path.relative_to(REPLAY_ROOT)}:{module}")
    assert violations == []


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.profile is DeploymentProfile.SERVER
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
