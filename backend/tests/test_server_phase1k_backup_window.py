from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import replace

import pytest
from app.server_runtime.query_control import QueryBackupFenceReceipt
from scripts import server_query_backup_bundle, server_query_backup_window

FENCE_ID = "11111111-2222-4333-8444-55555555555a"


def test_backup_fence_receipt_is_canonical_and_wire_safe() -> None:
    receipt = QueryBackupFenceReceipt(
        fence_id=FENCE_ID,
        operator_id="phase1k-backup-operator",
        acquired_at_ms=1_700_000_000_123,
    )
    assert receipt.to_wire() == {
        "fence_id": FENCE_ID,
        "operator_id": "phase1k-backup-operator",
        "acquired_at_ms": 1_700_000_000_123,
    }
    with pytest.raises(ValueError, match="canonical UUID"):
        replace(receipt, fence_id=FENCE_ID.upper())
    with pytest.raises(ValueError, match="non-negative"):
        replace(receipt, acquired_at_ms=-1)
    with pytest.raises(ValueError, match="lower-case safe ASCII"):
        replace(receipt, operator_id="Phase 1K")
    with pytest.raises(ValueError, match="at most 32"):
        replace(receipt, operator_id="a" * 33)


def test_backup_window_runs_child_with_receipt_and_releases(
    monkeypatch,
) -> None:
    fence = _FakeFence()
    _configure(monkeypatch, fence)
    child = (
        sys.executable,
        "-c",
        "import os,sys; "
        "sys.exit(0 if os.environ.get("
        "'CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_FENCE_ID') == "
        f"'{FENCE_ID}' else 9)",
        "secret-argument-must-not-be-echoed",
    )
    result = asyncio.run(server_query_backup_window.run(child))
    assert result["command_exit_code"] == 0
    assert result["command_executable"] == os.path.basename(sys.executable)
    assert "secret-argument" not in str(result)
    assert fence.heartbeats >= 1
    assert fence.released is True


def test_backup_window_preserves_child_failure_and_releases(monkeypatch) -> None:
    fence = _FakeFence()
    _configure(monkeypatch, fence)
    result = asyncio.run(
        server_query_backup_window.run((sys.executable, "-c", "raise SystemExit(7)"))
    )
    assert result["command_exit_code"] == 7
    assert fence.released is True


def test_backup_window_terminates_timeout_and_releases(monkeypatch) -> None:
    fence = _FakeFence()
    _configure(monkeypatch, fence)
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_MAXIMUM_RUNTIME_MS",
        "50",
    )
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_HEARTBEAT_INTERVAL_MS",
        "10",
    )
    with pytest.raises(TimeoutError, match="runtime bound"):
        asyncio.run(
            server_query_backup_window.run(
                (sys.executable, "-c", "import time; time.sleep(10)")
            )
        )
    assert fence.heartbeats >= 1
    assert fence.released is True


def test_backup_window_rejects_heartbeat_not_shorter_than_drain(
    monkeypatch,
) -> None:
    fence = _FakeFence()
    _configure(monkeypatch, fence)
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_HEARTBEAT_INTERVAL_MS",
        "100",
    )
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_DRAIN_TIMEOUT_MS",
        "100",
    )
    with pytest.raises(RuntimeError, match="shorter than drain timeout"):
        asyncio.run(server_query_backup_window.run((sys.executable, "-c", "pass")))
    assert fence.released is False


def test_backup_bundle_publishes_anchor_then_fence_bound_backup(
    tmp_path,
    monkeypatch,
) -> None:
    calls: list[object] = []

    async def anchor(*, action: str, anchor_uri: str | None):
        calls.append(("anchor", action, anchor_uri))
        return {"anchor_uri": "s3://bucket/prefix/anchors/v1/key/anchor.json"}

    async def backup(**arguments):
        calls.append(("backup", arguments))
        return {
            "write_fence_id": FENCE_ID,
            "write_fence_acquired_at_ms": 1_700_000_000_123,
            "recovery_target_time": "2026-08-11 13:00:00.123456+00",
            "recovery_target_lsn": "0/3000200",
            "recovery_target_wal_filename": "000000010000000000000003",
            "wal_segment_size_bytes": 16 * 1024 * 1024,
        }

    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_FENCE_ID",
        FENCE_ID,
    )
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_FENCE_ACQUIRED_AT_MS",
        "1700000000123",
    )
    monkeypatch.setattr(server_query_backup_bundle, "run_audit_anchor", anchor)
    monkeypatch.setattr(server_query_backup_bundle, "run_backup_publish", backup)
    result = asyncio.run(
        server_query_backup_bundle.run(
            backup_directory=tmp_path,
            backup_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        )
    )
    assert calls[0] == ("anchor", "publish", None)
    backup_call = calls[1]
    assert isinstance(backup_call, tuple)
    assert backup_call[1]["audit_anchor_uri"].endswith("anchor.json")
    assert result["recovery_target_lsn"] == "0/3000200"
    assert result["fence_id"] == FENCE_ID
    assert result["anchor"]["anchor_uri"].endswith("anchor.json")


def test_backup_bundle_requires_window_receipt(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_FENCE_ID",
        raising=False,
    )
    with pytest.raises(RuntimeError, match="FENCE_ID"):
        asyncio.run(
            server_query_backup_bundle.run(
                backup_directory=tmp_path,
                backup_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            )
        )


class _FakeFence:
    def __init__(self) -> None:
        self.receipt = QueryBackupFenceReceipt(
            fence_id=FENCE_ID,
            operator_id="phase1k-backup-operator",
            acquired_at_ms=1_700_000_000_123,
        )
        self.heartbeats = 0
        self.released = False

    async def acquire(self) -> QueryBackupFenceReceipt:
        return self.receipt

    async def heartbeat(self) -> None:
        self.heartbeats += 1

    async def release(self) -> None:
        self.released = True


def _configure(monkeypatch, fence: _FakeFence) -> None:
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_POSTGRES_AUDITOR_DSN",
        "postgresql://unused",
    )
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_OPERATOR_ID",
        "phase1k-backup-operator",
    )
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_MAXIMUM_RUNTIME_MS",
        "1000",
    )
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_HEARTBEAT_INTERVAL_MS",
        "10",
    )
    monkeypatch.setattr(
        server_query_backup_window,
        "PostgresQueryBackupFence",
        lambda *args, **kwargs: fence,
    )
