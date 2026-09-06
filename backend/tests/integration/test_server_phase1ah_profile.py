from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import psycopg
import pytest
from aiohttp import web
from app.deployment import DeploymentProfile, load_deployment_settings
from app.replay.broker.models import BrokerConfig, BrokerLimits, InstrumentFilters
from app.replay.constants import REPLAY_PROTOCOL, CommandType, QualityMode, SourceKind
from app.replay.models import (
    FeeModel,
    ReplayCommand,
    ReplaySessionConfig,
    SlippageModel,
)
from app.server_runtime.access_identity import (
    ServerPrincipal,
    StaticTokenIdentityVerifier,
)
from app.server_runtime.application import (
    STATUS,
    ServerProfileRuntime,
    build_server_application,
)
from app.server_runtime.composition import load_server_data_plane_composition
from app.server_runtime.replay_api_service import ServerReplayApiService
from app.server_runtime.replay_event_stream import ReplayEventStream
from app.server_runtime.replay_runtime_migrations import (
    DEFAULT_REPLAY_MIGRATION_PATH,
    PostgresReplayRuntimeMigrator,
)
from app.server_runtime.replay_scheduler import ReplayScheduler
from app.server_runtime.storage.postgres_replay_lease import (
    REPLAY_SESSION_LEASE_TABLE,
    PostgresReplaySessionLeaseStore,
)
from app.server_runtime.storage.postgres_replay_scheduler import (
    DEFAULT_SCHEDULER_MIGRATION_PATH,
    PostgresReplaySchedulerStore,
    apply_scheduler_migration,
)
from app.server_runtime.storage.postgres_replay_session import (
    PostgresReplaySessionStore,
)
from app.server_runtime.testing import FrozenAggTradeQuery
from httpx import ASGITransport, AsyncClient
from psycopg import sql

ADMIN_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1AH_POSTGRES_DSN",
    "postgresql://candlescope:phase1ah-local-only@localhost:28432/candlescope",
)
RUNTIME_ROLE = "candlescope_replay_app"
RUNTIME_PASSWORD = "phase1ah-runtime-local-only"
RUNTIME_DSN = (
    f"postgresql://{RUNTIME_ROLE}:{RUNTIME_PASSWORD}@localhost:28432/candlescope"
)
START_MS = 1_710_000_000_000
INTERVAL_MS = 60_000
REPLAY_END_MS = START_MS + INTERVAL_MS - 1
WORKER_A_BIND = "127.0.0.1:18321"
WORKER_B_BIND = "127.0.0.1:18322"
CONTROL_A = "phase1ah-control-token-aaaa"
CONTROL_B = "phase1ah-control-token-bbbb"
QUERY_CREDENTIAL = "phase1ah-query-credential-0000000000"
QUERY_URL = "http://127.0.0.1:18310"
SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
WORKER_SCRIPT = SCRIPTS / "server_replay_worker.py"
PYTHON = Path(__file__).resolve().parents[2] / ".venv" / "bin" / "python"

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1AH_INTEGRATION") != "1",
    reason="requires the explicit Phase 1AH PostgreSQL/Redpanda/ClickHouse/MinIO stack",
)


def test_server_profile_authenticated_replay_and_worker_takeover(
    tmp_path: Path,
) -> None:
    asyncio.run(_run(tmp_path))


async def _run(tmp_path: Path) -> None:
    _require_explicit_local_reset()
    await _prepare_database()
    query_path = tmp_path / "frozen-query.json"
    _write_frozen_query(query_path)
    health = await _start_role_health(query_path)
    worker_a = await _start_pool_worker(
        worker_id="worker-a", bind=WORKER_A_BIND, token=CONTROL_A
    )
    worker_b: asyncio.subprocess.Process | None = None
    try:
        store = PostgresReplaySchedulerStore(RUNTIME_DSN)
        scheduler = ReplayScheduler(store, clock_ms=lambda: time.time_ns() // 1_000_000)
        session_store = PostgresReplaySessionStore(RUNTIME_DSN)
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
            command_wait_ms=12_000,
        )
        composition = load_server_data_plane_composition(_composition_env())
        runtime = ServerProfileRuntime(
            settings=load_deployment_settings({"CANDLESCOPE_PROFILE": "server"}),
            composition=composition,
            scheduler=scheduler,
            service=service,
        )
        app = build_server_application(
            service,
            StaticTokenIdentityVerifier({"token-trader": principal}),
            runtime,
        )
        transport = ASGITransport(app=app)
        headers = {"Authorization": "Bearer token-trader"}
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            live = (await client.get("/health/live")).json()
            assert live["runtime_status"] == STATUS
            assert live["production_ready"] is False
            created = await client.post(
                "/api/v1/replay/runs",
                json=_payload(),
                headers=headers,
            )
            assert created.status_code == 200
            run_id = created.json()["run_id"]
            listed = await client.get("/api/v1/replay/runs", headers=headers)
            assert listed.status_code == 200
            assert [item["run_id"] for item in listed.json()["runs"]] == [run_id]
            session_id = await _wait_running(client, run_id)
            acquired = await client.post(
                f"/api/v1/replay/runs/session/{session_id}/commands",
                json=_command("acquire", CommandType.ACQUIRE_CONTROLLER, 0, {}),
                headers=headers,
            )
            assert acquired.status_code == 200
            first_revision = int(acquired.json()["revision"])
            stepped = await client.post(
                f"/api/v1/replay/runs/session/{session_id}/commands",
                json=_command("step-1", CommandType.STEP, first_revision, {"count": 1}),
                headers=headers,
            )
            assert stepped.status_code == 200
            assert stepped.json()["revision"] > first_revision
            worker_a.kill()
            await asyncio.wait_for(worker_a.wait(), timeout=5)
            recovering = False
            for _ in range(80):
                session = (
                    await client.get(
                        f"/api/v1/replay/runs/session/{session_id}",
                        headers=headers,
                    )
                ).json()
                if session.get("state") == "RECOVERING":
                    recovering = True
                    break
                await asyncio.sleep(0.1)
            assert recovering is True
            worker_b = await _start_pool_worker(
                worker_id="worker-b", bind=WORKER_B_BIND, token=CONTROL_B
            )
            for _ in range(100):
                session = (
                    await client.get(
                        f"/api/v1/replay/runs/session/{session_id}",
                        headers=headers,
                    )
                ).json()
                if session.get("state") == "RUNNING":
                    break
                await asyncio.sleep(0.1)
            else:
                raise TimeoutError("worker B did not take over")
            acquired_b = await client.post(
                f"/api/v1/replay/runs/session/{session_id}/commands",
                json=_command(
                    "acquire-b",
                    CommandType.ACQUIRE_CONTROLLER,
                    int(stepped.json()["revision"]),
                    {},
                ),
                headers=headers,
            )
            assert acquired_b.status_code == 200, acquired_b.text
            continued = await client.post(
                f"/api/v1/replay/runs/session/{session_id}/commands",
                json=_command(
                    "step-2",
                    CommandType.STEP,
                    int(acquired_b.json()["revision"]),
                    {"count": 1},
                ),
                headers=headers,
            )
            assert continued.status_code == 200, continued.text
            assert continued.json()["revision"] > int(acquired_b.json()["revision"])
            events = []
            async for event in service.subscribe_events(
                session_id, after_sequence=None, principal=principal
            ):
                events.append(event)
                if len(events) >= 1:
                    break
            assert events, "durable outbox must project at least one replay event"
        restarted = build_server_application(
            ServerReplayApiService(
                scheduler,
                event_stream=ReplayEventStream(session_store),
                session_store=session_store,
                command_wait_ms=8_000,
            ),
            StaticTokenIdentityVerifier({"token-trader": principal}),
            runtime,
        )
        async with AsyncClient(
            transport=ASGITransport(app=restarted), base_url="http://test"
        ) as restarted_client:
            persisted = await restarted_client.get(
                f"/api/v1/replay/runs/{run_id}",
                headers=headers,
            )
            assert persisted.status_code == 200
            assert persisted.json()["session_id"] == session_id
            assert "sqlite" not in json.dumps(persisted.json()).lower()
        result = await session_store.get_command_result(session_id, "step-2")
        assert result is not None
        default = load_deployment_settings({})
        assert default.profile is DeploymentProfile.PERSONAL
    finally:
        await _terminate(worker_a)
        if worker_b is not None:
            await _terminate(worker_b)
        await health.cleanup()


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1AH_ALLOW_TEST_RESET") != "1":
        raise RuntimeError("CANDLESCOPE_PHASE1AH_ALLOW_TEST_RESET=1 is required")
    parsed = urlparse(ADMIN_DSN)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port != 28432:
        raise RuntimeError("Phase 1AH reset is limited to localhost:28432")


async def _prepare_database() -> None:
    async with (
        await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (RUNTIME_ROLE,)
        )
        if await cursor.fetchone() is None:
            await cursor.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} INHERIT "
                    "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(RUNTIME_ROLE), sql.Literal(RUNTIME_PASSWORD))
            )
        for table in (
            "candlescope_replay_scheduler_audit",
            "candlescope_replay_scheduler_command",
            "candlescope_replay_scheduler_assignment",
            "candlescope_replay_scheduler_worker",
            "candlescope_replay_scheduler_request",
            "candlescope_replay_event_outbox",
            "candlescope_replay_command_result",
            "candlescope_replay_mutation",
            "candlescope_replay_session_state",
            "candlescope_replay_session",
            "candlescope_replay_schema_migration",
            REPLAY_SESSION_LEASE_TABLE,
        ):
            await cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    await PostgresReplaySessionLeaseStore(ADMIN_DSN).initialize_schema()
    await PostgresReplayRuntimeMigrator(
        ADMIN_DSN,
        migration_path=DEFAULT_REPLAY_MIGRATION_PATH,
        runtime_login_role=RUNTIME_ROLE,
    ).apply()
    await apply_scheduler_migration(
        ADMIN_DSN,
        migration_path=DEFAULT_SCHEDULER_MIGRATION_PATH,
        runtime_login_role=RUNTIME_ROLE,
    )


def _write_frozen_query(path: Path) -> None:
    path.write_text(
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
        "idempotency_key": "int-1ah",
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


def _command(
    command_id: str,
    command_type: CommandType,
    revision: int,
    payload: dict[str, object],
) -> dict[str, object]:
    return ReplayCommand(
        protocol=REPLAY_PROTOCOL,
        command_id=command_id,
        client_instance_id="ui",
        expected_revision=revision,
        type=command_type,
        payload=payload,
    ).to_dict()


def _composition_env() -> dict[str, str]:
    return {
        "CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN": ADMIN_DSN,
        "CANDLESCOPE_SERVER_COLLECTOR_KAFKA_BOOTSTRAP_SERVERS": "localhost:59092",
        "CANDLESCOPE_SERVER_COLLECTOR_OWNER_ID": "collector-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "localhost:59092",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_OWNER_ID": "writer-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_URL": "http://localhost:58123",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_USER": "candlescope",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_PASSWORD": "phase1ah-local-only",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "localhost:59092",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_OWNER_ID": "archiver-a",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH": "epoch-1",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ENDPOINT_URL": "http://localhost:59000",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ACCESS_KEY_ID": "candlescope",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_SECRET_ACCESS_KEY": "phase1ah-local-secret",
        "CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS": "localhost:59092",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL": "http://localhost:58123",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER": "query",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD": "query-secret",
        "CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL": "http://localhost:59000",
        "CANDLESCOPE_SERVER_QUERY_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID": "candlescope",
        "CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY": "phase1ah-local-secret",
        "CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN": QUERY_CREDENTIAL,
        "CANDLESCOPE_SERVER_QUERY_AUTH_ORGANIZATION_ID": "org-alpha",
        "CANDLESCOPE_SERVER_QUERY_AUTH_WORKSPACE_ID": "ws-research",
        "CANDLESCOPE_SERVER_QUERY_CONTROL_BEARER_TOKEN": "b" * 32,
        "CANDLESCOPE_SERVER_QUERY_POSTGRES_DSN": ADMIN_DSN,
        "CANDLESCOPE_SERVER_QUERY_INSTANCE_ID": "query-a",
        "CANDLESCOPE_SERVER_QUERY_BIND_HOST": "127.0.0.1",
        "CANDLESCOPE_SERVER_QUERY_BIND_PORT": "18310",
        "CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND": "127.0.0.1:18311",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_HEALTH_BIND": "127.0.0.1:18312",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_HEALTH_BIND": "127.0.0.1:18313",
    }


async def _start_role_health(query_path: Path) -> web.AppRunner:
    app = web.Application()
    frozen_query = FrozenAggTradeQuery(query_path)

    async def ready(_request: web.Request) -> web.Response:
        return web.json_response({"status": "ready", "ready": True})

    async def query(request: web.Request) -> web.Response:
        if request.headers.get("Authorization") != f"Bearer {QUERY_CREDENTIAL}":
            raise web.HTTPUnauthorized()
        body = await request.json()
        assert body["organization_id"] == "org-alpha"
        assert body["workspace_id"] == "ws-research"
        assert body["preference"] == "cold"
        from app.data_engine.market_data import MarketStreamKey
        from app.server_contracts import MarketDataSnapshotRef, MarketEventCursor

        snapshot = MarketDataSnapshotRef(**body["snapshot"])
        stream = MarketStreamKey.build(**body["stream"])
        cursor = None if body["cursor"] is None else MarketEventCursor(**body["cursor"])
        page = await frozen_query.query(
            snapshot=snapshot,
            stream=stream,
            start_event_time_ms=body["start_event_time_ms"],
            end_event_time_ms=body["end_event_time_ms"],
            limit=body["limit"],
            cursor=cursor,
        )
        covered = page.covered_range
        return web.json_response(
            {
                "backend": "cold",
                "hot_committed_next_offset": None,
                "parity_verified": False,
                "hot_quarantined": False,
                "page": {
                    "snapshot": {
                        "data_epoch": page.snapshot.data_epoch,
                        "snapshot_version": page.snapshot.snapshot_version,
                        "manifest_uri": page.snapshot.manifest_uri,
                        "manifest_sha256": page.snapshot.manifest_sha256,
                    },
                    "events": [event.to_wire() for event in page.events],
                    "covered_range": {
                        "partition_key": covered.partition_key,
                        "start_event_time_ms": covered.start_event_time_ms,
                        "end_event_time_ms": covered.end_event_time_ms,
                        "event_count": covered.event_count,
                        "sequence_start": covered.sequence_start,
                        "sequence_end": covered.sequence_end,
                    },
                    "next_cursor": (
                        None
                        if page.next_cursor is None
                        else {
                            "value": page.next_cursor.value,
                            "manifest_sha256": page.next_cursor.manifest_sha256,
                        }
                    ),
                },
            }
        )

    app.router.add_get("/health/ready", ready)
    app.router.add_get("/health", ready)
    app.router.add_post("/api/v1/server/market-events/query", query)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    for port in (18310, 18311, 18312, 18313):
        await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner


async def _start_pool_worker(
    *, worker_id: str, bind: str, token: str
) -> asyncio.subprocess.Process:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            "CANDLESCOPE_SERVER_REPLAY_WORKER_WORKER_ID": worker_id,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_POSTGRES_DSN": RUNTIME_DSN,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_QUERY_CREDENTIAL": QUERY_CREDENTIAL,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_ORGANIZATION_ID": "org-alpha",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_WORKSPACE_ID": "ws-research",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_QUERY_URL": QUERY_URL,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_CONTROL_TOKEN": token,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_LEASE_TTL_MS": "3000",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_RENEW_INTERVAL_MS": "1000",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_SHUTDOWN_TIMEOUT_MS": "1000",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_POLL_INTERVAL_MS": "100",
            "CANDLESCOPE_SERVER_REPLAY_SCHEDULER_HEARTBEAT_TTL_MS": "2000",
        }
    )
    return await asyncio.create_subprocess_exec(
        str(PYTHON),
        str(WORKER_SCRIPT),
        "--pool",
        "--control-bind",
        bind,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )


async def _wait_running(client: AsyncClient, run_id: str) -> str:
    for _ in range(100):
        fetched = await client.get(
            f"/api/v1/replay/runs/{run_id}",
            headers={"Authorization": "Bearer token-trader"},
        )
        body = fetched.json()
        if body.get("state") == "RUNNING" and body.get("session_id"):
            return str(body["session_id"])
        await asyncio.sleep(0.1)
    raise TimeoutError("session did not become RUNNING")


async def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        await process.wait()
