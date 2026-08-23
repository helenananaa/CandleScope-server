from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import sys
import tempfile
from pathlib import Path

import psycopg
import pytest

from scripts import server_query_backup_job

RUN_ID = "11111111-2222-4333-8444-555555555555"
FENCE_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
ROOT = Path(__file__).resolve().parents[3]
COMPOSE_PATH = ROOT / "deploy/server/compose.phase1j.yml"
ADMIN_DSN = "postgresql://candlescope:phase1j-local-only@localhost:15432/candlescope"


@pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1M_INTEGRATION") != "1",
    reason="requires the explicit Phase 1M PostgreSQL pg_basebackup stack",
)
def test_real_pg_basebackup_job_staging_and_receipt(monkeypatch) -> None:
    if os.environ.get("CANDLESCOPE_PHASE1M_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1M_ALLOW_TEST_RESET=1 is required because the "
            "gate creates a temporary replication role and pg_hba rule"
        )
    asyncio.run(_run_gate(monkeypatch))


async def _run_gate(monkeypatch) -> None:
    await _configure_replication_role()
    with tempfile.TemporaryDirectory(prefix="candlescope-phase1m-") as root_name:
        root = Path(root_name)
        executable = _pg_basebackup_wrapper(root)
        staging = root / "staging"
        locks = root / "locks"
        staging.mkdir()
        staging.chmod(0o700)
        locks.mkdir()
        locks.chmod(0o700)
        inventory = root / "inventory"
        inventory.mkdir()
        inventory.chmod(0o700)
        passfile = root / "pgpass"
        passfile.write_text(
            "localhost:15432:*:candlescope_backup:phase1m-backup-local-only\n",
            encoding="utf-8",
        )
        passfile.chmod(0o600)
        settings = {
            "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_STAGING_ROOT": str(staging),
            "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_LOCK_PATH": str(locks / "job.lock"),
            "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_PG_BASEBACKUP_EXECUTABLE": str(
                executable
            ),
            "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_BASEBACKUP_TIMEOUT_MS": "60000",
            "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_INVENTORY_ROOT": str(inventory),
            "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_CLUSTER_ID": ("phase1p-primary"),
            "PGHOST": "localhost",
            "PGPORT": "15432",
            "PGDATABASE": "candlescope",
            "PGUSER": "candlescope_backup",
            "PGPASSFILE": str(passfile),
        }
        for name, value in settings.items():
            monkeypatch.setenv(name, value)
        for name in ("PGPASSWORD", "PGSERVICE", "PGSERVICEFILE"):
            monkeypatch.delenv(name, raising=False)

        original = server_query_backup_job._run_process

        async def process(command, **arguments):
            if arguments["phase"] == "basebackup":
                return await original(command, **arguments)
            directory = Path(command[command.index("--backup-directory") + 1])
            assert {entry.name for entry in directory.iterdir()} == {
                "backup_manifest",
                "base.tar.gz",
                "pg_wal.tar.gz",
            }
            assert all(entry.stat().st_size > 0 for entry in directory.iterdir())
            return server_query_backup_job._ProcessResult(0, _window_output())

        monkeypatch.setattr(server_query_backup_job, "_run_process", process)

        async def publish_history(receipt):
            return {
                "schema_version": "candlescope.query-backup-run-history.v1",
                "cluster_id": "phase1p-primary",
                "backup_id": receipt["backup_id"],
                "completed_at_ms": receipt["completed_at_ms"],
                "history_uri": "s3://history/success.json",
                "history_sha256": "d" * 64,
                "created": True,
            }

        monkeypatch.setattr(
            server_query_backup_job.server_query_backup_history,
            "publish_receipt",
            publish_history,
        )
        receipt = await server_query_backup_job.run(run_id=RUN_ID)
        assert receipt["status"] == "succeeded"
        assert receipt["schema_version"] == "candlescope.query-backup-job-result.v2"
        assert receipt["backup_id"] == RUN_ID
        assert receipt["manifest_sha256"] == "b" * 64
        assert list(staging.iterdir()) == []
        inventory_files = list(inventory.iterdir())
        assert len(inventory_files) == 1
        assert inventory_files[0].name.endswith(f"-{RUN_ID}.json")
        assert inventory_files[0].stat().st_mode & 0o777 == 0o600


async def _configure_replication_role() -> None:
    async with await psycopg.AsyncConnection.connect(
        ADMIN_DSN,
        autocommit=True,
    ) as connection:
        await connection.execute("DROP ROLE IF EXISTS candlescope_backup")
        await connection.execute(
            "CREATE ROLE candlescope_backup LOGIN REPLICATION "
            "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
            "PASSWORD 'phase1m-backup-local-only'"
        )
        result = await connection.execute(
            "SELECT rolreplication, rolsuper, rolcreatedb, rolcreaterole "
            "FROM pg_roles WHERE rolname = 'candlescope_backup'"
        )
        row = await result.fetchone()
    assert row == (True, False, False, False)
    process = await asyncio.create_subprocess_exec(
        _docker(),
        "compose",
        "--file",
        str(COMPOSE_PATH),
        "exec",
        "--no-TTY",
        "--user",
        "postgres",
        "postgres",
        "sh",
        "-ceu",
        "printf '%s\\n' "
        "'host replication candlescope_backup samenet scram-sha-256' "
        '>> "$PGDATA/pg_hba.conf"; pg_ctl reload',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, (stdout, stderr)


def _docker() -> str:
    executable = shutil.which("docker")
    if executable is None:
        pytest.skip("docker is unavailable")
    return executable


def _pg_basebackup_wrapper(root: Path) -> Path:
    executable = root / "pg_basebackup-18-wrapper"
    docker = shlex.quote(_docker())
    compose = shlex.quote(str(COMPOSE_PATH))
    executable.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "destination=\n"
        "seen=0\n"
        'while test "$#" -gt 0; do\n'
        '  case "$1" in\n'
        '    --pgdata) shift; destination="$1" ;;\n'
        "    --format=tar|--gzip|--wal-method=stream|--checkpoint=fast|"
        "--manifest-checksums=SHA256|--no-password) seen=$((seen + 1)) ;;\n"
        "    *) exit 64 ;;\n"
        "  esac\n"
        "  shift\n"
        "done\n"
        'test -n "$destination"\n'
        'test "$seen" -eq 6\n'
        f"{docker} compose --file {compose} exec --no-TTY --user postgres "
        "postgres sh -ceu 'rm -rf /base-backup/phase1m-job; "
        "mkdir -m 0700 /base-backup/phase1m-job; "
        "pg_basebackup --host=/var/run/postgresql --username=candlescope_backup "
        "--pgdata=/base-backup/phase1m-job --format=tar --gzip "
        "--wal-method=stream --checkpoint=fast --manifest-checksums=SHA256 "
        "--no-password'\n"
        f"{docker} compose --file {compose} cp "
        'postgres:/base-backup/phase1m-job/. "$destination"\n'
        f"{docker} compose --file {compose} exec --no-TTY --user postgres "
        "postgres rm -rf /base-backup/phase1m-job\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    return executable


def _window_output() -> bytes:
    bundle = {
        "anchor": {
            "anchor_uri": "s3://bucket/anchors/v1/key/anchor.json",
            "content_sha256": "c" * 64,
        },
        "backup": {
            "backup_id": RUN_ID,
            "manifest_uri": f"s3://bucket/backups/v3/{RUN_ID}/manifest.json",
            "manifest_sha256": "b" * 64,
            "wal_coverage_segment_count": 1,
        },
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
