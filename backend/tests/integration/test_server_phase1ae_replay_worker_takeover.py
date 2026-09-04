from __future__ import annotations

import asyncio
import json
import os
import signal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import psycopg
import pytest
from app.replay.broker.models import BrokerConfig, BrokerLimits, InstrumentFilters
from app.replay.constants import REPLAY_PROTOCOL, QualityMode, SourceKind
from app.replay.models import FeeModel, ReplaySessionConfig, SlippageModel
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseFencedError,
)
from app.server_runtime.replay_runtime_migrations import (
    DEFAULT_REPLAY_MIGRATION_PATH,
    PostgresReplayRuntimeMigrator,
)
from app.server_runtime.storage.postgres_replay_lease import (
    REPLAY_SESSION_LEASE_TABLE,
    PostgresReplaySessionLeaseStore,
)
from app.server_runtime.storage.postgres_replay_session import (
    MUTATION_TABLE,
    PostgresReplaySessionStore,
)
from psycopg import sql
from psycopg.rows import dict_row

ADMIN_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1AE_POSTGRES_DSN",
    "postgresql://candlescope:phase1ae-local-only@localhost:25432/candlescope",
)
RUNTIME_ROLE = "candlescope_replay_app"
RUNTIME_PASSWORD = "phase1ae-runtime-local-only"
RUNTIME_DSN = (
    f"postgresql://{RUNTIME_ROLE}:{RUNTIME_PASSWORD}@localhost:25432/candlescope"
)
START_MS = 1_710_000_000_000
INTERVAL_MS = 60_000
REPLAY_END_MS = START_MS + INTERVAL_MS - 1
AUTH_ORG = "org-alpha"
AUTH_WS = "ws-research"
WORKER_A_BIND = "127.0.0.1:18221"
WORKER_B_BIND = "127.0.0.1:18222"
CONTROL_A = "phase1ae-control-token-aaaa"
CONTROL_B = "phase1ae-control-token-bbbb"
QUERY_CREDENTIAL = "phase1ae-query-credential"
SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
WORKER_SCRIPT = SCRIPTS / "server_replay_worker.py"
PYTHON = Path(__file__).resolve().parents[2] / ".venv" / "bin" / "python"

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1AE_INTEGRATION") != "1",
    reason="requires the explicit Phase 1AE PostgreSQL/Redpanda/ClickHouse/MinIO stack",
)


def test_worker_sigkill_takeover_restores_checkpoint_and_fences_old_token(
    tmp_path: Path,
) -> None:
    asyncio.run(_run_gate(tmp_path))


async def _run_gate(tmp_path: Path) -> None:
    _require_explicit_local_reset()
    await _prepare_database()
    query_path = tmp_path / "frozen-query.json"
    assignment_path = tmp_path / "assignment.json"
    _write_frozen_query(query_path)
    _write_assignment(assignment_path, query_path)
    worker_log = tmp_path / "worker-stderr.log"
    worker_a = await _start_worker(
        worker_id="worker-a",
        mode="new",
        bind=WORKER_A_BIND,
        token=CONTROL_A,
        assignment=assignment_path,
        log_path=worker_log,
    )
    worker_b: asyncio.subprocess.Process | None = None
    try:
        try:
            await _wait_ready(WORKER_A_BIND)
        except TimeoutError as exc:
            raw = worker_log.read_text(encoding="utf-8") if worker_log.is_file() else ""
            for secret in (RUNTIME_PASSWORD, CONTROL_A, CONTROL_B, QUERY_CREDENTIAL):
                raw = raw.replace(secret, "<redacted>")
            raise TimeoutError(f"{exc}; worker-stderr={raw[-2000:]}") from exc
        acquired = await _command(
            WORKER_A_BIND,
            CONTROL_A,
            {
                "command_id": "acquire",
                "type": "acquire_controller",
                "expected_revision": 0,
                "payload": {},
            },
        )
        first = await _command(
            WORKER_A_BIND,
            CONTROL_A,
            {
                "command_id": "step-1",
                "type": "step",
                "expected_revision": acquired["revision"],
                "payload": {"count": 1},
            },
        )
        second = first
        assert second["cursor"]["source_sequence"] == 1
        old_lease = await _load_lease()
        mutation_count = await _mutation_count()
        worker_a.kill()
        await asyncio.wait_for(worker_a.wait(), timeout=5)
        await asyncio.sleep(3.2)
        worker_b = await _start_worker(
            worker_id="worker-b",
            mode="recover",
            bind=WORKER_B_BIND,
            token=CONTROL_B,
            assignment=assignment_path,
            log_path=worker_log,
        )
        await _wait_ready(WORKER_B_BIND)
        recovered = await _snapshot(WORKER_B_BIND, CONTROL_B)
        assert recovered["state_hash"] == second["state_hash"]
        acquired_b = await _command(
            WORKER_B_BIND,
            CONTROL_B,
            {
                "command_id": "acquire-b",
                "type": "acquire_controller",
                "expected_revision": recovered["revision"],
                "payload": {},
            },
        )
        continued = await _command(
            WORKER_B_BIND,
            CONTROL_B,
            {
                "command_id": "step-2",
                "type": "step",
                "expected_revision": acquired_b["revision"],
                "payload": {"count": 1},
            },
        )
        assert continued["cursor"]["source_sequence"] == 2
        assert continued["cursor"]["last_agg_trade_id"] == 43
        store = PostgresReplaySessionStore(RUNTIME_DSN)
        after_takeover = await _mutation_count()
        assert after_takeover > mutation_count
        with pytest.raises(ReplaySessionLeaseFencedError):
            await store.commit_mutation(
                old_lease,
                _late_mutation(old_lease.session_id, int(second["revision"])),
            )
        assert await _mutation_count() == after_takeover
        new_lease = await _load_lease()
        assert new_lease.worker_id == "worker-b"
        assert new_lease.fencing_epoch == 1
        retry = await _command(
            WORKER_B_BIND,
            CONTROL_B,
            {
                "command_id": "step-2",
                "type": "step",
                "expected_revision": acquired_b["revision"],
                "payload": {"count": 1},
            },
        )
        assert retry["state_hash"] == continued["state_hash"]
        worker_b.send_signal(signal.SIGTERM)
        await asyncio.wait_for(worker_b.wait(), timeout=8)
    finally:
        await _terminate(worker_a)
        if worker_b is not None:
            await _terminate(worker_b)


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1AE_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1AE_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its local PostgreSQL schema"
        )
    parsed = urlparse(ADMIN_DSN)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port != 25432:
        raise RuntimeError("Phase 1AE reset is limited to localhost:25432")


async def _prepare_database() -> None:
    async with (
        await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection,
        connection.cursor() as cursor,
    ):
        for table in (
            "candlescope_replay_event_outbox",
            "candlescope_replay_command_result",
            "candlescope_replay_mutation",
            "candlescope_replay_session_state",
            "candlescope_replay_session",
            "candlescope_replay_schema_migration",
            REPLAY_SESSION_LEASE_TABLE,
        ):
            await cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        await cursor.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s",
            (RUNTIME_ROLE,),
        )
        if await cursor.fetchone() is None:
            await cursor.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} INHERIT "
                    "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(RUNTIME_ROLE), sql.Literal(RUNTIME_PASSWORD))
            )
    await PostgresReplaySessionLeaseStore(ADMIN_DSN).initialize_schema()
    await PostgresReplayRuntimeMigrator(
        ADMIN_DSN,
        migration_path=DEFAULT_REPLAY_MIGRATION_PATH,
        runtime_login_role=RUNTIME_ROLE,
    ).apply()


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


def _write_assignment(path: Path, query_path: Path) -> None:
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
    path.write_text(
        json.dumps(
            {
                "session_id": "sess-alpha",
                "organization_id": AUTH_ORG,
                "workspace_id": AUTH_WS,
                "query_path": str(query_path),
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
        ),
        encoding="utf-8",
    )


async def _start_worker(
    *,
    worker_id: str,
    mode: str,
    bind: str,
    token: str,
    assignment: Path,
    log_path: Path | None = None,
) -> asyncio.subprocess.Process:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            "CANDLESCOPE_SERVER_REPLAY_WORKER_WORKER_ID": worker_id,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_POSTGRES_DSN": RUNTIME_DSN,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_QUERY_CREDENTIAL": QUERY_CREDENTIAL,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_CONTROL_TOKEN": token,
            "CANDLESCOPE_SERVER_REPLAY_WORKER_LEASE_TTL_MS": "3000",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_RENEW_INTERVAL_MS": "1000",
            "CANDLESCOPE_SERVER_REPLAY_WORKER_SHUTDOWN_TIMEOUT_MS": "1000",
        }
    )
    stderr: int | None = asyncio.subprocess.DEVNULL
    log_fd: int | None = None
    if log_path is not None:
        log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        stderr = log_fd
    try:
        return await asyncio.create_subprocess_exec(
            str(PYTHON),
            str(WORKER_SCRIPT),
            "--assignment",
            str(assignment),
            "--mode",
            mode,
            "--control-bind",
            bind,
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=stderr,
        )
    finally:
        if log_fd is not None:
            os.close(log_fd)


async def _wait_ready(bind: str, *, timeout: float = 15.0) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    async with aiohttp.ClientSession() as session:
        while asyncio.get_running_loop().time() < deadline:
            try:
                async with session.get(f"http://{bind}/health") as response:
                    payload = await response.json()
                    if payload.get("ready") is True:
                        return payload
            except aiohttp.ClientError:
                await asyncio.sleep(0.1)
                continue
            await asyncio.sleep(0.1)
    raise TimeoutError(f"worker at {bind} did not become ready")


async def _command(bind: str, token: str, body: dict[str, Any]) -> dict[str, Any]:
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            f"http://{bind}/commands",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        ) as response,
    ):
        if response.content_type.startswith("application/json"):
            payload = await response.json()
        else:
            payload = {"text": await response.text()}
        if response.status != 200:
            raise RuntimeError(f"command failed: {payload}")
        return payload


async def _snapshot(bind: str, token: str) -> dict[str, Any]:
    async with (
        aiohttp.ClientSession() as session,
        session.get(
            f"http://{bind}/snapshot",
            headers={"Authorization": f"Bearer {token}"},
        ) as response,
    ):
        payload = await response.json()
        if response.status != 200:
            raise RuntimeError(f"snapshot failed: {payload}")
        return payload


async def _load_lease() -> ReplaySessionLease:
    async with (
        await psycopg.AsyncConnection.connect(
            ADMIN_DSN, row_factory=dict_row
        ) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"""
                SELECT session_id, worker_id, fencing_epoch, lease_token,
                       lease_expires_at, organization_id, workspace_id,
                       data_epoch, snapshot_version, manifest_uri, manifest_sha256
                FROM {REPLAY_SESSION_LEASE_TABLE}
                WHERE session_id = %s
                """,
            ("sess-alpha",),
        )
        row = await cursor.fetchone()
    assert row is not None
    expires = row["lease_expires_at"]
    return ReplaySessionLease(
        session_id=row["session_id"],
        worker_id=row["worker_id"],
        fencing_epoch=int(row["fencing_epoch"]),
        lease_token=str(row["lease_token"]),
        lease_expires_at_ms=int(expires.timestamp() * 1000),
        snapshot=MarketDataSnapshotRef(
            data_epoch=row["data_epoch"],
            snapshot_version=int(row["snapshot_version"]),
            manifest_uri=row["manifest_uri"],
            manifest_sha256=row["manifest_sha256"],
        ),
        organization_id=row["organization_id"],
        workspace_id=row["workspace_id"],
    )


async def _mutation_count() -> int:
    async with (
        await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT COUNT(*) FROM {MUTATION_TABLE} WHERE session_id = %s",
            ("sess-alpha",),
        )
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


def _late_mutation(session_id: str, revision: int):
    from app.replay.actor import ActorMutation
    from app.replay.constants import CommandType
    from app.replay.models import ReplayCommand

    return ActorMutation(
        kind="command",
        session_id=session_id,
        session_state={
            "state": "PAUSED",
            "revision": revision,
            "event_sequence": 1,
            "source_sequence": 1,
            "command_log_offset": 1,
            "state_hash": "sha256:" + ("d" * 64),
        },
        checkpoint=b"late-write",
        events=(),
        source_events=(),
        component_state={"journal": []},
        command=ReplayCommand(
            protocol=REPLAY_PROTOCOL,
            command_id="late",
            client_instance_id="old-worker",
            expected_revision=revision,
            type=CommandType.STEP,
            payload={"count": 1},
        ),
    )


async def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), timeout=3)
    except TimeoutError:
        process.kill()
        await process.wait()
