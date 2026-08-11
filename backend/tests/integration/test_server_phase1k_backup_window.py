from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import psycopg
import pytest
from app.server_runtime.query_control import (
    QueryBackupFenceError,
    QueryControlWriteFencedError,
)
from app.server_runtime.query_migrations import (
    QUERY_AUDITOR_GROUP_ROLE,
    QUERY_CONTROL_MIGRATION_TABLE,
    QUERY_RUNTIME_GROUP_ROLE,
    PostgresQueryControlMigrator,
)
from app.server_runtime.query_security import QueryAuditEvent
from app.server_runtime.storage.postgres_query_control import (
    HOT_PROJECTION_QUARANTINE_TABLE,
    QUERY_AUDIT_EVENT_TABLE,
    QUERY_AUDIT_HEAD_TABLE,
    QUERY_BACKUP_WRITE_FENCE_LOCK,
    PostgresQueryAuditVerifier,
    PostgresQueryBackupFence,
    PostgresQueryControlStore,
)
from psycopg import sql

ROOT = Path(__file__).resolve().parents[3]
MIGRATION_PATH = ROOT / "deploy/server/postgres/migrations/001_query_control.sql"
WINDOW_SCRIPT = ROOT / "backend/scripts/server_query_backup_window.py"
ADMIN_DSN = "postgresql://candlescope:phase1j-local-only@localhost:15432/candlescope"
RUNTIME_ROLE = "candlescope_query_app"
RUNTIME_PASSWORD = "phase1j-runtime-local-only"
RUNTIME_DSN = (
    "postgresql://candlescope_query_app:phase1j-runtime-local-only"
    "@localhost:15432/candlescope"
)
AUDITOR_ROLE = "candlescope_query_audit_reader"
AUDITOR_PASSWORD = "phase1j-auditor-local-only"
AUDITOR_DSN = (
    "postgresql://candlescope_query_audit_reader:phase1j-auditor-local-only"
    "@localhost:15432/candlescope"
)


@pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1K_INTEGRATION") != "1",
    reason="requires the explicit Phase 1K PostgreSQL backup-window stack",
)
def test_real_multi_instance_query_write_drain_and_fence() -> None:
    asyncio.run(_run_gate())


async def _run_gate() -> None:
    _require_explicit_local_reset()
    await _reset_postgres()
    await PostgresQueryControlMigrator(
        ADMIN_DSN,
        migration_path=MIGRATION_PATH,
        runtime_login_role=RUNTIME_ROLE,
        auditor_login_role=AUDITOR_ROLE,
        backend_ids=("clickhouse-market-events-v1",),
    ).apply()

    runtime_a = PostgresQueryControlStore(RUNTIME_DSN, instance_id="phase1k-a")
    runtime_b = PostgresQueryControlStore(RUNTIME_DSN, instance_id="phase1k-b")
    await runtime_a.start()
    await runtime_b.start()
    try:
        await runtime_a.emit(_event("phase1k-before-fence", 1_700_000_000_001))
        blocker = await psycopg.AsyncConnection.connect(RUNTIME_DSN)
        try:
            await blocker.execute(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(%s, 0))",
                (QUERY_BACKUP_WRITE_FENCE_LOCK,),
            )
            fence = PostgresQueryBackupFence(
                AUDITOR_DSN,
                operator_id="phase1k-direct",
                drain_timeout_ms=2_000,
            )
            acquiring = asyncio.create_task(fence.acquire())
            await asyncio.sleep(0.1)
            assert acquiring.done() is False
            await blocker.commit()
            receipt = await asyncio.wait_for(acquiring, timeout=2)
        finally:
            await blocker.close()

        assert receipt.operator_id == "phase1k-direct"
        await fence.heartbeat()
        await _require_fence("phase1k-direct")
        contender = PostgresQueryBackupFence(
            AUDITOR_DSN,
            operator_id="phase1k-contender",
            drain_timeout_ms=100,
        )
        with pytest.raises(QueryBackupFenceError, match="could not be acquired"):
            await contender.acquire()
        with pytest.raises(QueryControlWriteFencedError, match="drained backup"):
            await runtime_a.emit(_event("phase1k-fenced-a", 1_700_000_000_002))
        with pytest.raises(QueryControlWriteFencedError, match="drained backup"):
            await runtime_b.emit(_event("phase1k-fenced-b", 1_700_000_000_003))
        with pytest.raises(QueryControlWriteFencedError, match="drained backup"):
            await runtime_a.latch("phase1k-fenced-latch")
        assert (await _verify()).record_count == 1
        await fence.release()
        with pytest.raises(QueryBackupFenceError, match="is not held"):
            await _require_fence("phase1k-direct")

        await runtime_a.emit(_event("phase1k-after-direct-a", 1_700_000_000_004))
        await runtime_b.emit(_event("phase1k-after-direct-b", 1_700_000_000_005))
        assert (await _verify()).record_count == 3

        process = await _start_window_cli()
        await _wait_for_cli_fence(process)
        with pytest.raises(QueryControlWriteFencedError, match="drained backup"):
            await runtime_b.emit(_event("phase1k-fenced-cli", 1_700_000_000_006))
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode(errors="replace")
        receipt_wire = json.loads(stdout)
        assert receipt_wire["operator_id"] == "phase1k-cli"
        assert receipt_wire["command_executable"] == Path(sys.executable).name
        assert receipt_wire["command_exit_code"] == 0
        await runtime_b.emit(_event("phase1k-after-cli", 1_700_000_000_007))
        final = await _verify()
        assert final.record_count == 4
        assert final.head_audit_sequence == 4
    finally:
        await runtime_b.stop()
        await runtime_a.stop()


async def _start_window_cli() -> asyncio.subprocess.Process:
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": str(ROOT / "backend"),
            "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_POSTGRES_AUDITOR_DSN": (
                AUDITOR_DSN
            ),
            "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_OPERATOR_ID": "phase1k-cli",
            "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_CONNECT_TIMEOUT_MS": "2000",
            "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_DRAIN_TIMEOUT_MS": "2000",
            "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_MAXIMUM_RUNTIME_MS": "2000",
            "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_HEARTBEAT_INTERVAL_MS": "50",
        }
    )
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(WINDOW_SCRIPT),
        "--",
        sys.executable,
        "-c",
        "import time; time.sleep(0.4)",
        cwd=ROOT,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _wait_for_cli_fence(process: asyncio.subprocess.Process) -> None:
    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        if process.returncode is not None:
            stdout, stderr = await process.communicate()
            raise AssertionError(
                {
                    "returncode": process.returncode,
                    "stdout": stdout.decode(errors="replace"),
                    "stderr": stderr.decode(errors="replace"),
                }
            )
        async with await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection:
            result = await connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity AS activity
                    JOIN pg_locks AS lock ON lock.pid = activity.pid
                    WHERE activity.application_name = %s
                      AND lock.locktype = 'advisory'
                      AND lock.mode = 'ExclusiveLock'
                      AND lock.granted
                )
                """,
                ("candlescope-query-backup-fence:phase1k-cli",),
            )
            row = await result.fetchone()
        assert row is not None
        if row[0] is True:
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("backup-window CLI did not acquire the exclusive fence")


async def _verify():
    verifier = PostgresQueryAuditVerifier(AUDITOR_DSN, verifier_id="phase1k-gate")
    await verifier.start()
    try:
        return await verifier.verify_audit_chain(max_records=100)
    finally:
        await verifier.stop()


async def _require_fence(operator_id: str) -> None:
    verifier = PostgresQueryAuditVerifier(
        AUDITOR_DSN,
        verifier_id="phase1k-fence-verifier",
    )
    await verifier.start()
    try:
        await verifier.require_backup_fence(operator_id)
    finally:
        await verifier.stop()


def _event(request_id: str, timestamp_ms: int) -> QueryAuditEvent:
    return QueryAuditEvent(
        request_id=request_id,
        principal="phase1k-gateway",
        action="snapshot_market_event_query",
        outcome="success",
        status_code=200,
        timestamp_ms=timestamp_ms,
        latency_ms=1,
        backend="hot",
    )


async def _reset_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection:
        for table in (
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            QUERY_CONTROL_MIGRATION_TABLE,
        ):
            await connection.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table))
            )
        async with connection.cursor() as cursor:
            for role_name, password in (
                (RUNTIME_ROLE, RUNTIME_PASSWORD),
                (AUDITOR_ROLE, AUDITOR_PASSWORD),
            ):
                await cursor.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname = %s",
                    (role_name,),
                )
                command = "CREATE" if await cursor.fetchone() is None else "ALTER"
                await cursor.execute(
                    sql.SQL(
                        f"{command} ROLE {{}} LOGIN PASSWORD {{}} INHERIT "
                        "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOREPLICATION NOBYPASSRLS"
                    ).format(sql.Identifier(role_name), sql.Literal(password))
                )
            await cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                ([QUERY_RUNTIME_GROUP_ROLE, QUERY_AUDITOR_GROUP_ROLE],),
            )
            for (group_role,) in await cursor.fetchall():
                for login_role in (RUNTIME_ROLE, AUDITOR_ROLE):
                    await cursor.execute(
                        sql.SQL("REVOKE {} FROM {}").format(
                            sql.Identifier(group_role),
                            sql.Identifier(login_role),
                        )
                    )


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1K_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1K_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds the local Phase 1K PostgreSQL state"
        )
