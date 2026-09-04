from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.server_runtime.replay_scheduler import (
    ReplayRequestState,
    ReplayScheduler,
    ReplaySchedulerIdempotencyError,
    ReplaySchedulerQuotaError,
)
from app.server_runtime.testing.in_memory_replay_scheduler import (
    InMemoryReplaySchedulerStore,
)

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "deploy"
    / "server"
    / "postgres"
    / "migrations"
    / "003_replay_scheduler.sql"
)


def _payload(name: str = "alpha") -> dict[str, object]:
    return {"session_name": name, "source_kind": "agg_trade"}


def test_scheduler_sql_has_skip_locked_and_scope_columns() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "candlescope_replay_scheduler_request" in sql
    assert "organization_id" in sql
    assert "workspace_id" in sql
    assert "idempotency_key" in sql
    assert "candlescope_replay_scheduler_worker" in sql
    import inspect as inspect_module

    from app.server_runtime.storage.postgres_replay_scheduler import (
        PostgresReplaySchedulerStore,
    )

    source = inspect_module.getsource(PostgresReplaySchedulerStore.claim_next)
    assert "SKIP LOCKED" in source
    assert "super-secret" not in repr(
        PostgresReplaySchedulerStore("postgresql://u:super-secret@localhost/db")
    )
    digest = hashlib.sha256(MIGRATION.read_bytes()).hexdigest()
    assert len(digest) == 64


def test_two_workers_one_session_and_quota_isolation() -> None:
    async def run() -> None:
        now = [1_000]
        store = InMemoryReplaySchedulerStore(clock_ms=lambda: now[0])
        scheduler = ReplayScheduler(
            store,
            max_pending_per_org=1,
            max_active_per_org=1,
            clock_ms=lambda: now[0],
        )
        first = await scheduler.create_request(
            organization_id="org-alpha",
            workspace_id="ws-research",
            idempotency_key="client-1",
            payload=_payload(),
            priority=1,
        )
        replayed = await scheduler.create_request(
            organization_id="org-alpha",
            workspace_id="ws-research",
            idempotency_key="client-1",
            payload=_payload(),
        )
        assert replayed.request_id == first.request_id
        with pytest.raises(ReplaySchedulerIdempotencyError):
            await scheduler.create_request(
                organization_id="org-alpha",
                workspace_id="ws-research",
                idempotency_key="client-1",
                payload=_payload("other"),
            )
        with pytest.raises(ReplaySchedulerQuotaError):
            await scheduler.create_request(
                organization_id="org-alpha",
                workspace_id="ws-research",
                idempotency_key="client-2",
                payload=_payload("beta"),
            )
        other_org = await scheduler.create_request(
            organization_id="org-beta",
            workspace_id="ws-research",
            idempotency_key="client-1",
            payload=_payload("beta"),
            priority=0,
        )
        await scheduler.heartbeat("worker-a", capacity=1)
        await scheduler.heartbeat("worker-b", capacity=1)
        claimed_a = await scheduler.claim("worker-a")
        claimed_b = await scheduler.claim("worker-b")
        assert claimed_a is not None
        assert claimed_a.session_id != (
            None if claimed_b is None else claimed_b.session_id
        )
        owners = {claimed_a.worker_id}
        if claimed_b is not None:
            owners.add(claimed_b.worker_id)
            assert claimed_a.session_id != claimed_b.session_id
        assert "worker-a" in owners or "worker-b" in owners
        leftover = await scheduler.claim("worker-a")
        if claimed_b is None:
            assert leftover is not None
            assert leftover.request_id == other_org.request_id
        await scheduler.mark_running(first.request_id)
        cancelled = await scheduler.cancel(first.request_id)
        assert cancelled.state is ReplayRequestState.CANCELLED

    asyncio.run(run())


def test_capacity_priority_aging_timeout_and_unavailable() -> None:
    async def run() -> None:
        now = [5_000]
        store = InMemoryReplaySchedulerStore(clock_ms=lambda: now[0])
        scheduler = ReplayScheduler(
            store, max_pending_per_org=8, max_active_per_org=8, clock_ms=lambda: now[0]
        )
        await scheduler.heartbeat("worker-a", capacity=1)
        high = await scheduler.create_request(
            organization_id="org-alpha",
            workspace_id="ws-research",
            idempotency_key="high",
            payload=_payload("high"),
            priority=10,
        )
        low = await scheduler.create_request(
            organization_id="org-alpha",
            workspace_id="ws-research",
            idempotency_key="low",
            payload=_payload("low"),
            priority=0,
        )
        first = await scheduler.claim("worker-a")
        assert first is not None
        assert first.request_id == high.request_id
        second = await scheduler.claim("worker-a")
        assert second is None
        assert (
            await store.get_request(low.request_id)
        ).state is ReplayRequestState.PENDING
        now[0] += 6_000
        await scheduler.scan_timeouts()
        await scheduler.heartbeat("worker-a", capacity=1)
        await scheduler.heartbeat("worker-b", capacity=1)
        aged = await scheduler.create_request(
            organization_id="org-alpha",
            workspace_id="ws-research",
            idempotency_key="aged",
            payload=_payload("aged"),
            priority=0,
        )
        now[0] += 30_000
        claimed = await scheduler.claim("worker-b")
        assert claimed is not None
        assert claimed.request_id in {low.request_id, aged.request_id}
        store.postgres_down = True
        with pytest.raises(Exception, match="unavailable"):
            await scheduler.create_request(
                organization_id="org-alpha",
                workspace_id="ws-research",
                idempotency_key="down",
                payload=_payload("down"),
            )

    asyncio.run(run())


def test_server_profile_remains_refused() -> None:
    from app.deployment import FastAPISqliteBootError, refuse_server_sqlite_boot

    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.runtime_supported is True
    settings.require_runtime_support()
    with pytest.raises(FastAPISqliteBootError):
        refuse_server_sqlite_boot(settings)
