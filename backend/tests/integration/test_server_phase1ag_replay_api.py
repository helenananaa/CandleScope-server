from __future__ import annotations

import asyncio
import os
import time
from urllib.parse import urlparse

import psycopg
import pytest
from app.server_runtime.access_identity import (
    ServerPrincipal,
    StaticTokenIdentityVerifier,
)
from app.server_runtime.replay_api_composition import build_server_replay_app
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
from fastapi.testclient import TestClient
from psycopg import sql

ADMIN_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1AG_POSTGRES_DSN",
    "postgresql://candlescope:phase1ag-local-only@localhost:27432/candlescope",
)
RUNTIME_ROLE = "candlescope_replay_app"
RUNTIME_PASSWORD = "phase1ag-runtime-local-only"
RUNTIME_DSN = (
    f"postgresql://{RUNTIME_ROLE}:{RUNTIME_PASSWORD}@localhost:27432/candlescope"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1AG_INTEGRATION") != "1",
    reason="requires the explicit Phase 1AG PostgreSQL stack",
)


class _EmptyOutbox:
    async def events_after(self, session_id: str, after_sequence: int):
        del session_id, after_sequence
        return ()


def test_authenticated_create_run_on_postgres_scheduler() -> None:
    asyncio.run(_prepare())
    store = PostgresReplaySchedulerStore(RUNTIME_DSN)
    scheduler = ReplayScheduler(store, clock_ms=lambda: time.time_ns() // 1_000_000)
    principal = ServerPrincipal(
        subject="trader",
        organization_id="org-alpha",
        workspace_id="ws-research",
        principal_type="user",
        role="trader",
        credential_id="cred-trader",
    )
    app = build_server_replay_app(
        ServerReplayApiService(
            scheduler, event_stream=ReplayEventStream(_EmptyOutbox())
        ),
        StaticTokenIdentityVerifier({"token-trader": principal}),
    )
    client = TestClient(app)
    created = client.post(
        "/api/v1/replay/runs",
        json={"idempotency_key": "int-1", "source_kind": "agg_trade"},
        headers={"Authorization": "Bearer token-trader"},
    )
    assert created.status_code == 200
    body = created.json()
    fetched = client.get(
        f"/api/v1/replay/runs/{body['run_id']}",
        headers={"Authorization": "Bearer token-trader"},
    )
    assert fetched.status_code == 200
    assert fetched.json()["run_id"] == body["run_id"]


async def _prepare() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1AG_ALLOW_TEST_RESET") != "1":
        raise RuntimeError("CANDLESCOPE_PHASE1AG_ALLOW_TEST_RESET=1 is required")
    parsed = urlparse(ADMIN_DSN)
    if parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.port != 27432:
        raise RuntimeError("Phase 1AG reset is limited to localhost:27432")
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
