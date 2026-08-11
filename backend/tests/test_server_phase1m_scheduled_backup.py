from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

from scripts import server_query_backup_failure, server_query_backup_job

RUN_ID = "11111111-2222-4333-8444-555555555555"
FENCE_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
ROOT = Path(__file__).resolve().parents[2]


def test_scheduled_job_runs_basebackup_then_window_and_emits_receipt(
    tmp_path,
    monkeypatch,
) -> None:
    staging_root = _configure(tmp_path, monkeypatch)
    calls: list[tuple[str, ...]] = []

    async def process(command, **arguments):
        calls.append(command)
        assert arguments["run_id"] == RUN_ID
        if arguments["phase"] == "basebackup":
            assert set(arguments["environment"]) >= {
                "PGHOST",
                "PGPORT",
                "PGDATABASE",
                "PGUSER",
                "PGPASSFILE",
            }
            assert not any(
                name.startswith("CANDLESCOPE_") for name in arguments["environment"]
            )
            assert "--no-password" in command
            assert not any("secret" in value for value in command)
            directory = Path(command[command.index("--pgdata") + 1])
            for name in ("backup_manifest", "base.tar.gz", "pg_wal.tar.gz"):
                (directory / name).write_bytes(name.encode())
            return server_query_backup_job._ProcessResult(0, b"")
        assert "PGPASSFILE" not in arguments["environment"]
        assert command[-1] == RUN_ID
        assert command[-2] == "--backup-id"
        return server_query_backup_job._ProcessResult(0, _window_output())

    monkeypatch.setattr(server_query_backup_job, "_run_process", process)
    receipt = asyncio.run(server_query_backup_job.run(run_id=RUN_ID))

    assert len(calls) == 2
    assert calls[0][0] == "/usr/bin/true"
    assert calls[1][1].endswith("server_query_backup_window.py")
    assert receipt["schema_version"] == ("candlescope.query-backup-job-receipt.v1")
    assert receipt["run_id"] == RUN_ID
    assert receipt["manifest_sha256"] == "b" * 64
    assert receipt["wal_coverage_segment_count"] == 2
    assert list(staging_root.iterdir()) == []


def test_scheduled_job_rejects_basebackup_failure_and_cleans_staging(
    tmp_path,
    monkeypatch,
) -> None:
    staging_root = _configure(tmp_path, monkeypatch)

    async def process(_command, **_arguments):
        return server_query_backup_job._ProcessResult(7, b"")

    monkeypatch.setattr(server_query_backup_job, "_run_process", process)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "BASEBACKUP_FAILED"
    assert captured.value.child_exit_code == 7
    assert list(staging_root.iterdir()) == []


def test_scheduled_job_rejects_incomplete_artifacts_and_bad_window_receipt(
    tmp_path,
    monkeypatch,
) -> None:
    _configure(tmp_path, monkeypatch)
    calls = 0

    async def incomplete(command, **arguments):
        nonlocal calls
        calls += 1
        directory = Path(command[command.index("--pgdata") + 1])
        (directory / "backup_manifest").write_bytes(b"manifest")
        return server_query_backup_job._ProcessResult(0, b"")

    monkeypatch.setattr(server_query_backup_job, "_run_process", incomplete)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "BASEBACKUP_ARTIFACTS_INVALID"
    assert calls == 1

    async def malformed(command, **arguments):
        if arguments["phase"] == "basebackup":
            directory = Path(command[command.index("--pgdata") + 1])
            for name in ("backup_manifest", "base.tar.gz", "pg_wal.tar.gz"):
                (directory / name).write_bytes(b"x")
            return server_query_backup_job._ProcessResult(0, b"")
        return server_query_backup_job._ProcessResult(0, b"{}\n")

    monkeypatch.setattr(server_query_backup_job, "_run_process", malformed)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "WINDOW_RECEIPT_INVALID"

    async def crlf(command, **arguments):
        if arguments["phase"] == "basebackup":
            directory = Path(command[command.index("--pgdata") + 1])
            for name in ("backup_manifest", "base.tar.gz", "pg_wal.tar.gz"):
                (directory / name).write_bytes(b"x")
            return server_query_backup_job._ProcessResult(0, b"")
        return server_query_backup_job._ProcessResult(
            0,
            _window_output().replace(b"\n", b"\r\n"),
        )

    monkeypatch.setattr(server_query_backup_job, "_run_process", crlf)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "WINDOW_RECEIPT_INVALID"


def test_same_host_kernel_lock_rejects_overlap(tmp_path, monkeypatch) -> None:
    _configure(tmp_path, monkeypatch)
    first = server_query_backup_job._acquire_lock(RUN_ID)
    try:
        with pytest.raises(
            server_query_backup_job.QueryBackupJobAlreadyRunningError
        ) as captured:
            server_query_backup_job._acquire_lock(
                "22222222-3333-4444-8555-666666666666"
            )
        assert captured.value.code == "JOB_ALREADY_RUNNING"
    finally:
        os.close(first)


def test_job_rejects_ambient_password_and_insecure_passfile(
    tmp_path,
    monkeypatch,
) -> None:
    _configure(tmp_path, monkeypatch)
    monkeypatch.setenv("PGPASSWORD", "must-not-be-used")
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "CONFIGURATION_INVALID"
    monkeypatch.delenv("PGPASSWORD")

    passfile = Path(os.environ["PGPASSFILE"])
    passfile.chmod(0o644)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "CONFIGURATION_INVALID"


def test_job_rejects_shared_staging_and_lock_roots(tmp_path, monkeypatch) -> None:
    staging_root = _configure(tmp_path, monkeypatch)
    staging_root.chmod(0o750)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "CONFIGURATION_INVALID"

    staging_root.chmod(0o700)
    lock_root = Path(os.environ["CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_LOCK_PATH"])
    lock_root.parent.chmod(0o750)
    with pytest.raises(server_query_backup_job.QueryBackupJobError) as captured:
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))
    assert captured.value.code == "CONFIGURATION_INVALID"


def test_child_runtime_and_output_are_bounded() -> None:
    async def run() -> None:
        with pytest.raises(server_query_backup_job.QueryBackupJobError) as timeout:
            await server_query_backup_job._run_process(
                (sys.executable, "-c", "import time; time.sleep(10)"),
                timeout_ms=20,
                max_output_bytes=1024,
                run_id=RUN_ID,
                phase="test-timeout",
            )
        assert timeout.value.code == "CHILD_TIMEOUT"

        with pytest.raises(server_query_backup_job.QueryBackupJobError) as output:
            await server_query_backup_job._run_process(
                (sys.executable, "-c", "print('x' * 2000)"),
                timeout_ms=1_000,
                max_output_bytes=100,
                run_id=RUN_ID,
                phase="test-output",
            )
        assert output.value.code == "CHILD_OUTPUT_EXCEEDED"

    asyncio.run(run())


def test_cleanup_failure_still_releases_kernel_lock(tmp_path, monkeypatch) -> None:
    _configure(tmp_path, monkeypatch)

    async def process(command, **arguments):
        if arguments["phase"] == "basebackup":
            directory = Path(command[command.index("--pgdata") + 1])
            for name in ("backup_manifest", "base.tar.gz", "pg_wal.tar.gz"):
                (directory / name).write_bytes(b"x")
            return server_query_backup_job._ProcessResult(0, b"")
        return server_query_backup_job._ProcessResult(0, _window_output())

    def fail_cleanup(_directory) -> None:
        raise OSError("injected cleanup failure")

    monkeypatch.setattr(server_query_backup_job, "_run_process", process)
    monkeypatch.setattr(server_query_backup_job.shutil, "rmtree", fail_cleanup)
    with pytest.raises(OSError, match="injected cleanup failure"):
        asyncio.run(server_query_backup_job.run(run_id=RUN_ID))

    descriptor = server_query_backup_job._acquire_lock(
        "22222222-3333-4444-8555-666666666666"
    )
    os.close(descriptor)


def test_failure_signal_and_systemd_templates_are_bounded() -> None:
    signal = server_query_backup_failure.run("candlescope-query-backup.service")
    assert signal["schema_version"] == ("candlescope.query-backup-failure-signal.v1")
    assert signal["operator_action_required"] is True
    with pytest.raises(ValueError, match="systemd identifier"):
        server_query_backup_failure.run("bad unit\nsecret")

    systemd = ROOT / "deploy/server/systemd"
    service = (systemd / "candlescope-query-backup.service").read_text()
    timer = (systemd / "candlescope-query-backup.timer").read_text()
    failure = (systemd / "candlescope-query-backup-failure@.service").read_text()
    assert "OnFailure=candlescope-query-backup-failure@%n.service" in service
    assert "ProtectSystem=strict" in service
    assert "MemoryDenyWriteExecute=yes" in service
    assert "Persistent=yes" in timer
    assert "RandomizedDelaySec=15min" in timer
    assert "PrivateNetwork=yes" in failure


def _configure(tmp_path: Path, monkeypatch) -> Path:
    staging_root = tmp_path / "staging"
    staging_root.mkdir(exist_ok=True)
    staging_root.chmod(0o700)
    lock_root = tmp_path / "locks"
    lock_root.mkdir(exist_ok=True)
    lock_root.chmod(0o700)
    passfile = tmp_path / "pgpass"
    passfile.write_text("host:5432:db:user:password\n", encoding="utf-8")
    passfile.chmod(0o600)
    values = {
        "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_STAGING_ROOT": str(staging_root),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_LOCK_PATH": str(lock_root / "job.lock"),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_PG_BASEBACKUP_EXECUTABLE": (
            "/usr/bin/true"
        ),
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGDATABASE": "candlescope",
        "PGUSER": "candlescope_backup",
        "PGPASSFILE": str(passfile),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    for name in ("PGPASSWORD", "PGSERVICE", "PGSERVICEFILE"):
        monkeypatch.delenv(name, raising=False)
    return staging_root


def _window_output() -> bytes:
    backup = {
        "backup_id": RUN_ID,
        "manifest_uri": f"s3://bucket/backups/v3/{RUN_ID}/manifest.json",
        "manifest_sha256": "b" * 64,
        "wal_coverage_segment_count": 2,
    }
    anchor = {
        "anchor_uri": "s3://bucket/anchors/v1/key/anchor.json",
        "content_sha256": "c" * 64,
    }
    bundle = {
        "anchor": anchor,
        "backup": backup,
        "fence_acquired_at_ms": 1_700_000_000_000,
        "fence_id": FENCE_ID,
        "recovery_target_lsn": "0/3000028",
        "recovery_target_time": "2026-08-11 13:00:00.123456+00",
        "recovery_target_wal_filename": "000000010000000000000003",
        "wal_segment_size_bytes": 16 * 1024 * 1024,
    }
    window = {
        "command_executable": Path(sys.executable).name,
        "command_exit_code": 0,
        "duration_ms": 100,
        "fence_acquired_at_ms": 1_700_000_000_000,
        "fence_id": FENCE_ID,
        "operator_id": "scheduled-backup",
    }
    return (
        json.dumps(bundle, sort_keys=True, separators=(",", ":"))
        + "\n"
        + json.dumps(window, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode()
