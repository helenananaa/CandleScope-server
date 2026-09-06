from __future__ import annotations

import ast
import asyncio
import inspect
import json
from pathlib import Path

import pytest
from app.deployment import (
    PRODUCTION_READY_BLOCKERS,
    DeploymentProfile,
    FastAPISqliteBootError,
    load_deployment_settings,
    refuse_server_sqlite_boot,
)
from app.deployment.fastapi_sqlite_boot import FASTAPI_SQLITE_BOOT_PATHS
from app.replay.broker.models import BrokerConfig, BrokerLimits, InstrumentFilters
from app.replay.constants import REPLAY_PROTOCOL, CommandType, QualityMode, SourceKind
from app.replay.models import (
    FeeModel,
    ReplayCommand,
    ReplaySessionConfig,
    SlippageModel,
)
from app.server_runtime.access_identity import ServerPrincipal
from app.server_runtime.application import STATUS, attach_server_profile
from app.server_runtime.composition import (
    fastapi_unlock_status,
    load_server_data_plane_composition,
)
from app.server_runtime.replay_api_service import ServerReplayApiService
from app.server_runtime.replay_event_stream import ReplayEventStream
from app.server_runtime.replay_scheduler import ReplayScheduler
from app.server_runtime.replay_worker_pool import (
    ReplayWorkerPoolLoop,
    server_query_from_payload,
)
from app.server_runtime.replay_worker_settings import ReplayWorkerSettings
from app.server_runtime.testing import (
    FrozenAggTradeQuery,
    InMemoryReplaySchedulerStore,
    InMemoryReplaySessionLeaseStore,
    InMemoryReplaySessionStore,
)
from fastapi import FastAPI
from scripts import server_composition_check

BACKEND_ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = BACKEND_ROOT / "app" / "main.py"
PERSONAL_PATH = BACKEND_ROOT / "app" / "deployment" / "personal_runtime.py"
SERVER_PATH = BACKEND_ROOT / "app" / "deployment" / "server_runtime.py"
APPLICATION_PATH = BACKEND_ROOT / "app" / "server_runtime" / "application.py"
START_MS = 1_710_000_000_000
INTERVAL_MS = 60_000
REPLAY_END_MS = START_MS + INTERVAL_MS - 1
TOKEN_A = "a" * 32
TOKEN_B = "b" * 32


def _complete_env(**overrides: str) -> dict[str, str]:
    values = {
        "CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN": "postgresql://collector@localhost:15432/candlescope",
        "CANDLESCOPE_SERVER_COLLECTOR_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_COLLECTOR_OWNER_ID": "collector-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_OWNER_ID": "writer-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_URL": "http://localhost:18123",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_USER": "candlescope",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_PASSWORD": "writer-secret",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_OWNER_ID": "archiver-a",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH": "epoch-1",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ENDPOINT_URL": "http://localhost:19000",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ACCESS_KEY_ID": "access",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_SECRET_ACCESS_KEY": "secret",
        "CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL": "http://localhost:18123",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER": "query",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD": "query-secret",
        "CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL": "http://localhost:19000",
        "CANDLESCOPE_SERVER_QUERY_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID": "access",
        "CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY": "secret",
        "CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN": TOKEN_A,
        "CANDLESCOPE_SERVER_QUERY_AUTH_ORGANIZATION_ID": "org-alpha",
        "CANDLESCOPE_SERVER_QUERY_AUTH_WORKSPACE_ID": "ws-research",
        "CANDLESCOPE_SERVER_QUERY_CONTROL_BEARER_TOKEN": TOKEN_B,
        "CANDLESCOPE_SERVER_QUERY_POSTGRES_DSN": "postgresql://query@localhost:15432/candlescope",
        "CANDLESCOPE_SERVER_QUERY_INSTANCE_ID": "query-a",
    }
    values.update(overrides)
    return values


def _function_def(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name == name
        ):
            return node
    raise AssertionError(f"missing function {name}")


def _ordered_call_names(function: ast.AST) -> list[str]:
    names: list[str] = []
    for statement in getattr(function, "body", ()):
        for child in ast.walk(statement):
            if not isinstance(child, ast.Call):
                continue
            func = child.func
            if isinstance(func, ast.Name):
                names.append(func.id)
            elif isinstance(func, ast.Attribute):
                names.append(func.attr)
    return names


def test_server_profile_runtime_supported_but_not_production_ready() -> None:
    personal = load_deployment_settings({})
    assert personal.profile is DeploymentProfile.PERSONAL
    assert personal.runtime_supported is True
    server = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert server.profile is DeploymentProfile.SERVER
    assert server.runtime_supported is True
    server.require_runtime_support()
    assert PRODUCTION_READY_BLOCKERS == ("twenty_four_hour_public_continuity",)
    assert STATUS == "SERVER_PROFILE_RUNTIME_COMPLETE_NOT_PRODUCTION_READY"


def test_refuse_server_sqlite_boot_is_negative_inventory() -> None:
    personal = load_deployment_settings({})
    refuse_server_sqlite_boot(personal)
    server = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    with pytest.raises(FastAPISqliteBootError) as missing:
        refuse_server_sqlite_boot(server)
    assert missing.value.code == "FASTAPI_SQLITE_CONTROL_OR_MARKET_PATH"
    composition = load_server_data_plane_composition(_complete_env())
    refuse_server_sqlite_boot(server, composition=composition)
    wire = composition.to_public_wire()
    assert wire["fastapi_runtime_supported"] is True
    assert wire["production_ready"] is False
    assert "writer-secret" not in json.dumps(wire)


def test_startup_dispatches_after_loading_settings() -> None:
    startup = _function_def(MAIN_PATH, "startup_event")
    names = _ordered_call_names(startup)
    required = [
        "load_deployment_settings",
        "require_runtime_support",
        "refuse_server_sqlite_boot",
        "start_personal_runtime",
        "start_server_runtime",
    ]
    indexes = [names.index(name) for name in required]
    assert indexes == sorted(indexes)
    personal = PERSONAL_PATH.read_text(encoding="utf-8")
    assert "init_klines_storage" in personal
    assert "ReplaySQLiteStore" not in SERVER_PATH.read_text(encoding="utf-8")
    assert "init_klines_storage" not in SERVER_PATH.read_text(encoding="utf-8")
    assert "init_klines_storage" not in APPLICATION_PATH.read_text(encoding="utf-8")
    assert [path.module for path in FASTAPI_SQLITE_BOOT_PATHS[:4]] == [
        "app.deployment.personal_runtime"
    ] * 4


def test_cli_fastapi_unlock_succeeds_when_composition_is_complete(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for key, value in _complete_env().items():
        monkeypatch.setenv(key, value)
    assert server_composition_check.main(["fastapi-unlock"]) == 0
    unlocked = json.loads(capsys.readouterr().out)
    assert unlocked["fastapi_runtime_supported"] is True
    assert unlocked["production_ready"] is False
    assert fastapi_unlock_status(load_server_data_plane_composition(_complete_env()))[
        "production_ready_blockers"
    ] == list(PRODUCTION_READY_BLOCKERS)


def test_server_startup_does_not_open_sqlite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app import main as main_module
    from app.deployment import personal_runtime as personal_module

    storage_calls: list[str] = []
    monkeypatch.setenv("CANDLESCOPE_PROFILE", "server")
    for key, value in _complete_env().items():
        monkeypatch.setenv(key, value)

    async def _server_start(app, settings, composition, **_kwargs) -> None:
        del app, settings, composition

    monkeypatch.setattr(main_module, "start_server_runtime", _server_start)
    monkeypatch.setattr(
        personal_module, "init_klines_storage", lambda: storage_calls.append("klines")
    )
    asyncio.run(main_module.startup_event())
    assert storage_calls == []
    assert main_module.app.state.deployment_profile == "server"


def _payload() -> dict[str, object]:
    config = ReplaySessionConfig(
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
    broker = BrokerConfig(
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
    return {
        "idempotency_key": "unit-1",
        "source_kind": "agg_trade",
        "organization_id": "org-alpha",
        "workspace_id": "ws-research",
        "snapshot": {
            "data_epoch": "sha256:" + ("c" * 64),
            "snapshot_version": 4,
            "manifest_uri": "s3://archive/snapshot-4.json",
            "manifest_sha256": "a" * 64,
        },
        "pin": {
            "start_event_time_ms": START_MS + 42,
            "end_event_time_ms": START_MS + 44,
            "expected_first_agg_trade_id": 42,
            "expected_last_agg_trade_id": 44,
            "row_count": 3,
        },
        "config": config.to_dict(),
        "broker_config": broker.to_dict(),
        "replay_start_ms": START_MS,
        "replay_end_time_ms": REPLAY_END_MS,
    }


def test_in_memory_worker_pool_serves_authenticated_command(tmp_path: Path) -> None:
    query_path = tmp_path / "frozen-query.json"
    query_path.write_text(
        json.dumps(
            {
                "start_ms": START_MS,
                "sequences": [42, 43, 44],
                "snapshot": {
                    "data_epoch": "sha256:" + ("c" * 64),
                    "snapshot_version": 4,
                    "manifest_uri": "s3://archive/snapshot-4.json",
                    "manifest_sha256": "a" * 64,
                },
            }
        ),
        encoding="utf-8",
    )
    now = [1_000]

    async def _run() -> None:
        store = InMemoryReplaySchedulerStore(clock_ms=lambda: now[0])
        scheduler = ReplayScheduler(store, clock_ms=lambda: now[0])
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        settings = ReplayWorkerSettings(
            worker_id="worker-a",
            postgres_dsn="postgresql://replay:worker-secret@localhost:25432/candlescope",
            query_credential="query-token-aaaa",
            worker_control_token="control-token-bbbb",
            organization_id="org-alpha",
            workspace_id="ws-research",
            lease_ttl_ms=30_000,
            renew_interval_ms=5_000,
            poll_interval_ms=50,
            shutdown_timeout_ms=1_000,
        )
        pool = ReplayWorkerPoolLoop(
            settings,
            scheduler=scheduler,
            lease_store=lease_store,
            session_store=session_store,
            query_factory=lambda _payload: FrozenAggTradeQuery(query_path),
        )
        principal = ServerPrincipal(
            subject="trader",
            organization_id="org-alpha",
            workspace_id="ws-research",
            principal_type="user",
            role="trader",
            credential_id="cred-trader",
        )
        service = ServerReplayApiService(
            scheduler,
            event_stream=ReplayEventStream(session_store),
            session_store=session_store,
            command_wait_ms=4_000,
        )
        from app.server_runtime.application import ServerProfileRuntime

        runtime = ServerProfileRuntime(
            settings=load_deployment_settings({"CANDLESCOPE_PROFILE": "server"}),
            composition=load_server_data_plane_composition(_complete_env()),
            scheduler=scheduler,
            service=service,
            ready=True,
        )
        live = runtime.liveness()
        assert live["runtime_status"] == STATUS
        assert live["production_ready"] is False
        task = asyncio.create_task(pool.run())
        try:
            created = await service.create_run(_payload(), principal=principal)
            session_id = None
            for _ in range(80):
                fetched = await service.get_run(created["run_id"], principal=principal)
                if fetched.get("state") == "RUNNING" and fetched.get("session_id"):
                    session_id = fetched["session_id"]
                    break
                await asyncio.sleep(0.05)
            assert session_id is not None
            acquire = ReplayCommand(
                protocol=REPLAY_PROTOCOL,
                command_id="acquire",
                client_instance_id="ui",
                expected_revision=0,
                type=CommandType.ACQUIRE_CONTROLLER,
                payload={},
            )
            acquired = await service.submit_command(
                session_id, acquire, principal=principal
            )
            assert acquired.revision >= 1
        finally:
            pool.request_stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            await pool.stop()

    asyncio.run(_run())


def test_application_module_does_not_import_sqlite_runtime() -> None:
    source = APPLICATION_PATH.read_text(encoding="utf-8")
    assert "ReplaySQLiteStore" not in source
    assert "init_klines_storage" not in source
    assert inspect.getsource(refuse_server_sqlite_boot)


def test_worker_query_scope_must_match_scheduler_payload() -> None:
    settings = ReplayWorkerSettings(
        worker_id="worker-a",
        postgres_dsn="postgresql://replay:worker-secret@localhost/db",
        query_credential="query-token-aaaa",
        worker_control_token="control-token-bbbb",
        organization_id="org-alpha",
        workspace_id="ws-research",
    )
    with pytest.raises(ValueError, match="outside the Worker query scope"):
        server_query_from_payload(
            settings,
            {"organization_id": "org-beta", "workspace_id": "ws-research"},
        )


def test_attach_server_profile_replaces_personal_routes() -> None:
    app = FastAPI()

    @app.put("/api/v1/settings/proxy")
    async def personal_settings() -> dict[str, bool]:
        return {"personal": True}

    @app.get("/debug/snapshot")
    async def personal_debug() -> dict[str, bool]:
        return {"personal": True}

    environment = _complete_env(
        CANDLESCOPE_SERVER_REPLAY_SCHEDULER_POSTGRES_DSN=(
            "postgresql://replay@localhost:15432/candlescope"
        ),
        CANDLESCOPE_SERVER_API_STATIC_TOKENS_JSON=json.dumps(
            {
                "server-token": {
                    "subject": "server-user",
                    "organization_id": "org-alpha",
                    "workspace_id": "ws-research",
                    "role": "admin",
                    "credential_id": "credential-a",
                }
            }
        ),
    )

    async def _attach() -> None:
        runtime = await attach_server_profile(
            app,
            load_deployment_settings({"CANDLESCOPE_PROFILE": "server"}),
            load_server_data_plane_composition(environment),
            environment=environment,
        )
        paths = {route.path for route in app.routes}
        assert "/api/v1/settings/proxy" not in paths
        assert "/debug/snapshot" not in paths
        assert "/api/v1/replay/capabilities" in paths
        assert "/health/ready" in paths
        await runtime.stop()

    asyncio.run(_attach())
