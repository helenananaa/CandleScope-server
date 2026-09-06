from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

import pytest
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.replay.runtime import start_replay_runtime
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.replay_lease import (
    REPLAY_SESSION_LEASE_SCHEMA_VERSION,
    ReplaySessionLease,
    ReplaySessionLeaseBusyError,
    ReplaySessionLeaseFencedError,
    ReplaySessionSnapshotConflictError,
    normalize_replay_session_id,
    require_write_fence,
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


def _acquire_kwargs(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "session_id": "sess-alpha",
        "worker_id": "worker-a",
        "snapshot": _snapshot(),
        "organization_id": "org-alpha",
        "workspace_id": "ws-research",
        "lease_ttl_ms": 500,
    }
    values.update(overrides)
    return values


def test_session_and_worker_ids_reject_wildcards() -> None:
    with pytest.raises(ValueError, match="identifier"):
        normalize_replay_session_id("*")
    with pytest.raises(ValueError, match="reserved"):
        ReplaySessionLease(
            session_id="all",
            worker_id="worker-a",
            fencing_epoch=0,
            lease_token="00000000-0000-4000-8000-000000000001",
            lease_expires_at_ms=1_000,
            snapshot=_snapshot(),
            organization_id="org-alpha",
            workspace_id="ws-research",
        )
    lease = ReplaySessionLease(
        session_id="Sess-Alpha",
        worker_id="Worker-A",
        fencing_epoch=0,
        lease_token="00000000-0000-4000-8000-000000000001",
        lease_expires_at_ms=1_000,
        snapshot=_snapshot(),
        organization_id="Org-Alpha",
        workspace_id="Ws-Research",
    )
    assert lease.session_id == "sess-alpha"
    assert lease.worker_id == "worker-a"
    assert lease.organization_id == "org-alpha"
    assert lease.workspace_id == "ws-research"
    wire = lease.to_public_ref()
    assert wire["schema_version"] == REPLAY_SESSION_LEASE_SCHEMA_VERSION
    assert wire["organization_id"] == "org-alpha"
    assert wire["workspace_id"] == "ws-research"
    assert "lease_token" not in wire
    assert lease.lease_token not in str(wire)


def test_active_session_is_exclusive_and_same_worker_must_renew() -> None:
    async def run() -> None:
        now = [1_000]
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        first = await store.acquire(**_acquire_kwargs())
        assert first.fencing_epoch == 0
        with pytest.raises(ReplaySessionLeaseBusyError):
            await store.acquire(**_acquire_kwargs(worker_id="worker-b"))
        with pytest.raises(ReplaySessionLeaseBusyError):
            await store.acquire(**_acquire_kwargs())
        renewed = await store.renew(first, lease_ttl_ms=500)
        assert renewed.fencing_epoch == 0
        assert renewed.lease_token == first.lease_token
        assert renewed.lease_expires_at_ms == 1_500
        require_write_fence(renewed, first)

    asyncio.run(run())


def test_expired_session_takeover_increments_epoch_and_keeps_snapshot() -> None:
    async def run() -> None:
        now = [1_000]
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        first = await store.acquire(**_acquire_kwargs())
        now[0] += 500
        with pytest.raises(ReplaySessionSnapshotConflictError):
            await store.acquire(
                **_acquire_kwargs(
                    worker_id="worker-b",
                    snapshot=_snapshot(digest="b" * 64),
                )
            )
        second = await store.acquire(**_acquire_kwargs(worker_id="worker-b"))
        assert second.fencing_epoch == 1
        assert second.worker_id == "worker-b"
        assert second.snapshot == first.snapshot
        assert second.lease_token != first.lease_token
        with pytest.raises(ReplaySessionLeaseFencedError):
            await store.renew(first, lease_ttl_ms=500)
        with pytest.raises(ReplaySessionLeaseFencedError):
            require_write_fence(second, first)

    asyncio.run(run())


def test_release_and_expired_renew_are_fenced() -> None:
    async def run() -> None:
        now = [1_000]
        store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        first = await store.acquire(**_acquire_kwargs())
        await store.release(first)
        with pytest.raises(ReplaySessionLeaseFencedError):
            await store.renew(first, lease_ttl_ms=500)
        second = await store.acquire(**_acquire_kwargs(worker_id="worker-b"))
        assert second.fencing_epoch == 1
        now[0] += 500
        with pytest.raises(ReplaySessionLeaseFencedError, match="expired"):
            await store.renew(second, lease_ttl_ms=500)

    asyncio.run(run())


def test_lease_is_not_a_worker_pool_and_fastapi_stays_locked() -> None:
    source = Path(
        BACKEND_ROOT / "app" / "server_runtime" / "replay_lease.py"
    ).read_text(encoding="utf-8")
    assert "ReplayService" not in source
    assert "start_replay_runtime" not in source
    runtime_source = inspect.getsource(start_replay_runtime)
    assert "ReplaySessionLease" not in runtime_source
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    settings.require_runtime_support()
