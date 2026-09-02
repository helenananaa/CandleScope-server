from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

import pytest
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.replay_lease import (
    REPLAY_SESSION_LEASE_SCHEMA_VERSION,
    ReplaySessionScopeConflictError,
)
from app.server_runtime.storage import (
    CREATE_REPLAY_SESSION_LEASE_TABLE_SQL,
    REPLAY_SESSION_LEASE_TABLE,
    PostgresReplaySessionLeaseStore,
)
from app.server_runtime.storage.postgres_replay_lease import (
    _SELECT_FOR_UPDATE_SQL,
    REPLAY_SESSION_LOCK_PREFIX,
)
from app.server_runtime.testing import InMemoryReplaySessionLeaseStore

BACKEND_ROOT = Path(__file__).resolve().parents[1]


def _snapshot(*, digest: str = "a" * 64) -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="epoch-1",
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256=digest,
    )


def test_postgres_schema_pins_snapshot_and_tenant_and_fences_rows() -> None:
    sql = CREATE_REPLAY_SESSION_LEASE_TABLE_SQL
    assert REPLAY_SESSION_LEASE_TABLE == "candlescope_replay_session_lease"
    assert REPLAY_SESSION_LEASE_TABLE in sql
    for column in (
        "session_id",
        "worker_id",
        "fencing_epoch",
        "lease_token",
        "organization_id",
        "workspace_id",
        "data_epoch",
        "snapshot_version",
        "manifest_uri",
        "manifest_sha256",
    ):
        assert column in sql
    assert "FOR UPDATE" in _SELECT_FOR_UPDATE_SQL
    assert REPLAY_SESSION_LOCK_PREFIX == "replay-session:"
    source = inspect.getsource(PostgresReplaySessionLeaseStore)
    module = (
        BACKEND_ROOT / "app" / "server_runtime" / "storage" / "postgres_replay_lease.py"
    ).read_text(encoding="utf-8")
    assert "pg_advisory_xact_lock" in module
    assert "ReplayService" not in source
    assert "fencing_epoch + 1" in source
    assert REPLAY_SESSION_LEASE_SCHEMA_VERSION == "candlescope.replay-session-lease.v2"


def test_postgres_dsn_is_required_and_redacted() -> None:
    with pytest.raises(ValueError, match="dsn"):
        PostgresReplaySessionLeaseStore("  ")
    store = PostgresReplaySessionLeaseStore(
        "postgresql://query:super-secret@localhost:5432/candlescope"
    )
    assert "super-secret" not in repr(store)
    assert "super-secret" not in str(store)


def test_takeover_cannot_rebind_organization_or_workspace() -> None:
    async def run() -> None:
        now = [1_000]
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        first = await store.acquire(
            session_id="sess-alpha",
            worker_id="worker-a",
            snapshot=_snapshot(),
            organization_id="org-alpha",
            workspace_id="ws-research",
            lease_ttl_ms=500,
        )
        assert first.organization_id == "org-alpha"
        now[0] += 500
        with pytest.raises(ReplaySessionScopeConflictError):
            await store.acquire(
                session_id="sess-alpha",
                worker_id="worker-b",
                snapshot=_snapshot(),
                organization_id="org-beta",
                workspace_id="ws-research",
                lease_ttl_ms=500,
            )
        with pytest.raises(ReplaySessionScopeConflictError):
            await store.acquire(
                session_id="sess-alpha",
                worker_id="worker-b",
                snapshot=_snapshot(),
                organization_id="org-alpha",
                workspace_id="ws-trading",
                lease_ttl_ms=500,
            )
        second = await store.acquire(
            session_id="sess-alpha",
            worker_id="worker-b",
            snapshot=_snapshot(),
            organization_id="ORG-ALPHA",
            workspace_id="WS-RESEARCH",
            lease_ttl_ms=500,
        )
        assert second.fencing_epoch == 1
        assert second.organization_id == "org-alpha"
        assert second.workspace_id == "ws-research"

    asyncio.run(run())


def test_postgres_store_is_not_a_worker_pool_and_fastapi_stays_locked() -> None:
    path = (
        BACKEND_ROOT / "app" / "server_runtime" / "storage" / "postgres_replay_lease.py"
    )
    source = path.read_text(encoding="utf-8")
    assert "ReplayService" not in source
    assert "start_replay_runtime" not in source
    assert "app.replay" not in source
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
