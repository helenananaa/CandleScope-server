from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import psycopg
import pytest
from app.server_runtime.replay_lease import ReplaySessionLeaseBusyError
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
from psycopg import sql

ADMIN_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1AF_POSTGRES_DSN",
    "postgresql://candlescope:phase1af-local-only@localhost:26432/candlescope",
)
RUNTIME_ROLE = "candlescope_replay_app"
RUNTIME_PASSWORD = "phase1af-runtime-local-only"
RUNTIME_DSN = (
    f"postgresql://{RUNTIME_ROLE}:{RUNTIME_PASSWORD}@localhost:26432/candlescope"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1AF_INTEGRATION") != "1",
    reason="requires the explicit Phase 1AF PostgreSQL stack",
)


def test_two_workers_exclusive_assignment_and_lease(tmp_path: Path) -> None:
    del tmp_path
    asyncio.run(_run())


async def _run() -> None:
    _require_explicit_local_reset()
    await _prepare()
    store = PostgresReplaySchedulerStore(RUNTIME_DSN)
    scheduler = ReplayScheduler(store, clock_ms=lambda: time.time_ns() // 1_000_000)
    await scheduler.heartbeat("worker-a", capacity=1)
    await scheduler.heartbeat("worker-b", capacity=1)
    request = await scheduler.create_request(
        organization_id="org-alpha",
        workspace_id="ws-research",
        idempotency_key="job-1",
        payload={"source_kind": "agg_trade"},
        priority=5,
    )
    first = await scheduler.claim("worker-a")
    second = await scheduler.claim("worker-b")
    assert first is not None
    assert first.request_id == request.request_id
    assert second is None
    lease_store = PostgresReplaySessionLeaseStore(RUNTIME_DSN)
    from app.server_contracts import MarketDataSnapshotRef

    snapshot = MarketDataSnapshotRef(
        data_epoch="sha256:" + ("c" * 64),
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256="a" * 64,
    )
    lease = await lease_store.acquire(
        session_id=first.session_id,
        worker_id="worker-a",
        snapshot=snapshot,
        organization_id="org-alpha",
        workspace_id="ws-research",
        lease_ttl_ms=5_000,
    )
    with pytest.raises(ReplaySessionLeaseBusyError):
        await lease_store.acquire(
            session_id=first.session_id,
            worker_id="worker-b",
            snapshot=snapshot,
            organization_id="org-alpha",
            workspace_id="ws-research",
            lease_ttl_ms=5_000,
        )
    assert lease.worker_id == "worker-a"
    assert lease.fencing_epoch == 0


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1AF_ALLOW_TEST_RESET") != "1":
        raise RuntimeError("CANDLESCOPE_PHASE1AF_ALLOW_TEST_RESET=1 is required")
    parsed = urlparse(ADMIN_DSN)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port != 26432:
        raise RuntimeError("Phase 1AF reset is limited to localhost:26432")


async def _prepare() -> None:
    async with (
        await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection,
        connection.cursor() as cursor,
    ):
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
        ADMIN_DSN, migration_path=DEFAULT_SCHEDULER_MIGRATION_PATH
    )
