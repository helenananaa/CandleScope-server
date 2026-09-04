from __future__ import annotations

import asyncio
import hashlib
import inspect
from pathlib import Path
from typing import Any

import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.deployment import load_deployment_settings
from app.replay.broker.models import BrokerConfig, BrokerLimits, InstrumentFilters
from app.replay.constants import REPLAY_PROTOCOL, CommandType, QualityMode, SourceKind
from app.replay.models import (
    FeeModel,
    ReplayCommand,
    ReplaySessionConfig,
    SlippageModel,
)
from app.server_contracts import MarketDataSnapshotRef, MarketEventPage
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.health_http import HealthHttpBindError
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import canonical_envelope_bytes
from app.server_runtime.query_pagination import SnapshotQueryRow, paginate_snapshot_rows
from app.server_runtime.replay_lease import ReplaySessionLeaseFencedError
from app.server_runtime.replay_runtime_migrations import (
    REPLAY_RUNTIME_MIGRATION_SHA256,
    PostgresReplayRuntimeMigrator,
)
from app.server_runtime.replay_session import ServerReplaySessionSpec
from app.server_runtime.replay_snapshot import ReplayServerSnapshotPin
from app.server_runtime.replay_worker import ReplayWorker
from app.server_runtime.replay_worker_settings import (
    ReplayWorkerConfigurationError,
    ReplayWorkerSettings,
)
from app.server_runtime.storage.postgres_replay_session import (
    COMMAND_TABLE,
    MUTATION_TABLE,
    OUTBOX_TABLE,
    SESSION_TABLE,
    STATE_TABLE,
    PostgresReplaySessionStore,
)
from app.server_runtime.testing import (
    InMemoryReplaySessionLeaseStore,
    InMemoryReplaySessionStore,
)

START_MS = 1_710_000_000_000
INTERVAL_MS = 60_000
REPLAY_END_MS = START_MS + INTERVAL_MS - 1
AUTH_ORG = "org-alpha"
AUTH_WS = "ws-research"
MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "deploy"
    / "server"
    / "postgres"
    / "migrations"
    / "002_replay_runtime.sql"
)


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
        data_epoch="sha256:" + ("c" * 64),
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


def _config() -> ReplaySessionConfig:
    return ReplaySessionConfig(
        protocol=REPLAY_PROTOCOL,
        source_kind=SourceKind.AGG_TRADE,
        exchange="binance",
        market_type="futures",
        symbol="BTCUSDT",
        base_interval="1m",
        display_interval="1m",
        start_policy="manual",  # type: ignore[arg-type]
        requested_start_ms=START_MS,
        warmup_bars=0,
        horizon_ms=INTERVAL_MS,
        random_seed=7,
        quality_mode=QualityMode.EXACT,
        blind_mode=False,
        initial_equity="10000",
        quote_asset="USDT",
        execution_model="paper_linear_v1",  # type: ignore[arg-type]
        fee_model=FeeModel("2", "5"),
        slippage_model=SlippageModel("fixed_bps", "1"),  # type: ignore[arg-type]
        max_leverage="3",
        pause_on_controller_loss=True,
    )


def _broker_config() -> BrokerConfig:
    return BrokerConfig(
        initial_equity="10000",
        quote_asset="USDT",
        maker_bps="2",
        taker_bps="5",
        market_slippage_bps="1",
        initial_mark_price="100000.1",
        instrument=InstrumentFilters(
            price_tick="0.1",
            quantity_step="0.00000001",
            min_quantity="0.00000001",
            max_quantity="1000000000",
            min_notional="0.01",
            max_notional="30000",
            quote_step="0.00000001",
        ),
        limits=BrokerLimits(
            max_leverage="3",
            max_position_notional="30000",
            max_order_quantity="1000000000",
            max_open_orders=256,
            max_orders=4_096,
            max_fills=8_192,
            max_ledger_entries=65_536,
            max_warnings=4_096,
        ),
    )


class _PagingColdQuery:
    def __init__(self, snapshot: MarketDataSnapshotRef) -> None:
        envelopes = [_envelope(42), _envelope(43), _envelope(44)]
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

    async def query(self, **kwargs: Any) -> MarketEventPage:
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


def _settings(worker_id: str = "worker-a") -> ReplayWorkerSettings:
    return ReplayWorkerSettings(
        worker_id=worker_id,
        postgres_dsn="postgresql://replay:worker-secret@localhost:25432/candlescope",
        query_credential="query-token-aaaa",
        worker_control_token="control-token-bbbb",
        lease_ttl_ms=3_000,
        renew_interval_ms=1_000,
        shutdown_timeout_ms=1_000,
        health_bind="127.0.0.1:18221",
    )


def _command(
    command_id: str,
    command_type: CommandType,
    revision: int,
    payload: dict[str, object] | None = None,
) -> ReplayCommand:
    return ReplayCommand(
        protocol=REPLAY_PROTOCOL,
        command_id=command_id,
        client_instance_id="worker-client",
        expected_revision=revision,
        type=command_type,
        payload=payload or {},
    )


async def _spec(lease_store, snapshot, worker_id: str = "worker-a"):
    lease = await lease_store.acquire(
        session_id="sess-alpha",
        worker_id=worker_id,
        snapshot=snapshot,
        organization_id=AUTH_ORG,
        workspace_id=AUTH_WS,
        lease_ttl_ms=3_000,
    )
    return ServerReplaySessionSpec(
        lease=lease,
        pin=_pin(snapshot),
        config=_config(),
        broker_config=_broker_config(),
        replay_start_ms=START_MS,
        replay_end_time_ms=REPLAY_END_MS,
        command_queue_size=32,
        event_buffer_size=64,
        max_emit_fps=30,
        controller_ttl_seconds=5.0,
        checkpoint_event_interval=1,
        checkpoint_virtual_ms=300_000,
        max_closed_bars=16,
        shutdown_timeout_seconds=1.0,
    )


def test_worker_settings_reject_short_ttl_and_redact_secrets() -> None:
    with pytest.raises(ReplayWorkerConfigurationError, match="lease_ttl_ms"):
        ReplayWorkerSettings(
            worker_id="worker-a",
            postgres_dsn="postgresql://replay:secret@localhost/db",
            query_credential="query-token",
            worker_control_token="control-token",
            lease_ttl_ms=1_000,
            renew_interval_ms=500,
        )
    with pytest.raises(HealthHttpBindError):
        ReplayWorkerSettings(
            worker_id="worker-a",
            postgres_dsn="postgresql://replay:secret@localhost/db",
            query_credential="query-token",
            worker_control_token="control-token",
            health_bind="0.0.0.0:80",
        )
    settings = _settings()
    rendered = repr(settings)
    assert "worker-secret" not in rendered
    assert "query-token-aaaa" not in rendered
    assert "control-token-bbbb" not in rendered
    store = PostgresReplaySessionStore(
        "postgresql://replay:super-secret@localhost:5432/candlescope"
    )
    assert "super-secret" not in repr(store)
    migrator = PostgresReplayRuntimeMigrator(
        "postgresql://admin:super-secret@localhost:5432/candlescope",
        migration_path=MIGRATION_PATH,
        runtime_login_role="candlescope_replay_app",
    )
    assert "super-secret" not in repr(migrator)


def test_migration_sql_pins_tables_and_checksum() -> None:
    sql = MIGRATION_PATH.read_text(encoding="utf-8")
    digest = hashlib.sha256(MIGRATION_PATH.read_bytes()).hexdigest()
    assert digest == REPLAY_RUNTIME_MIGRATION_SHA256
    for table in (
        SESSION_TABLE,
        STATE_TABLE,
        MUTATION_TABLE,
        COMMAND_TABLE,
        OUTBOX_TABLE,
        "candlescope_replay_session_lease",
    ):
        assert table in sql
    assert "pg_advisory" not in sql or "candlescope_replay_runtime" in sql
    assert "CREATE ROLE candlescope_replay_runtime" in sql
    assert "NOSUPERUSER" in sql
    source = inspect.getsource(PostgresReplaySessionStore.commit_mutation)
    assert "FOR UPDATE" in inspect.getsource(PostgresReplaySessionStore)
    assert (
        "_fence_active_lease" in source
        or "_fence_active_lease" in inspect.getsource(PostgresReplaySessionStore)
    )


def test_in_memory_worker_runs_step_and_recovers_after_abandon() -> None:
    async def run() -> None:
        snapshot = _snapshot()
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        query = _PagingColdQuery(snapshot)
        worker_a = ReplayWorker(
            _settings("worker-a"),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            clock_ms=lambda: now[0],
        )
        spec_a = await _spec(lease_store, snapshot, "worker-a")
        await worker_a.start_new(spec_a)
        assert worker_a.health().ready is True
        acquired = await worker_a.submit(
            _command("acquire", CommandType.ACQUIRE_CONTROLLER, 0)
        )
        first = await worker_a.submit(
            _command("step-1", CommandType.STEP, acquired.revision, {"count": 1})
        )
        assert first.cursor.source_sequence == 1
        before = await worker_a.session.durable_state()  # type: ignore[union-attr]
        old_lease = spec_a.lease
        await worker_a.abandon()
        now[0] += 3_000
        worker_b = ReplayWorker(
            _settings("worker-b"),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            clock_ms=lambda: now[0],
        )
        spec_b = await _spec(lease_store, snapshot, "worker-b")
        recovered = await worker_b.recover(spec_b)
        assert recovered["state_hash"] == before["state_hash"]
        acquired_b = await worker_b.submit(
            _command(
                "acquire-b",
                CommandType.ACQUIRE_CONTROLLER,
                int(recovered["revision"]),
            )
        )
        with pytest.raises(ReplaySessionLeaseFencedError):
            await session_store.commit_mutation(
                old_lease,
                __import__(
                    "app.replay.actor", fromlist=["ActorMutation"]
                ).ActorMutation(
                    kind="command",
                    session_id="sess-alpha",
                    session_state=before,
                    checkpoint=b"late",
                    events=(),
                    source_events=(),
                    component_state={"journal": []},
                    command=_command(
                        "late", CommandType.STEP, int(before["revision"]), {"count": 1}
                    ),
                ),
            )
        second = await worker_b.submit(
            _command(
                "step-2",
                CommandType.STEP,
                acquired_b.revision,
                {"count": 1},
            )
        )
        assert second.cursor.source_sequence == 2
        await worker_b.stop()

    asyncio.run(run())


def test_server_profile_remains_refused() -> None:
    from app.deployment import FastAPISqliteBootError, refuse_server_sqlite_boot

    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.runtime_supported is True
    settings.require_runtime_support()
    with pytest.raises(FastAPISqliteBootError):
        refuse_server_sqlite_boot(settings)
