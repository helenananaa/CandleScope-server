"""Run one bounded, non-overlapping query-control physical backup job."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import shutil
import signal
import stat
import sys
import tempfile
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from app.server_runtime.query_backup_catalog import REQUIRED_BACKUP_ARTIFACTS
from scripts import server_query_backup_history, server_query_backup_monitor

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_JOB_"
WINDOW_SCRIPT = Path(__file__).with_name("server_query_backup_window.py")
BUNDLE_SCRIPT = Path(__file__).with_name("server_query_backup_bundle.py")
RECEIPT_SCHEMA_VERSION = "candlescope.query-backup-job-receipt.v1"
RESULT_SCHEMA_VERSION = "candlescope.query-backup-job-result.v2"
FAILURE_SCHEMA_VERSION = "candlescope.query-backup-job-failure.v1"


class QueryBackupJobError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        phase: str,
        run_id: str,
        child_exit_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.phase = phase
        self.run_id = run_id
        self.child_exit_code = child_exit_code

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": FAILURE_SCHEMA_VERSION,
            "status": "failed",
            "run_id": self.run_id,
            "phase": self.phase,
            "code": self.code,
            "child_exit_code": self.child_exit_code,
        }


class QueryBackupJobAlreadyRunningError(QueryBackupJobError):
    """Another same-host backup job owns the kernel lock."""


@dataclass(frozen=True, slots=True)
class _ProcessResult:
    exit_code: int
    stdout: bytes


async def run(*, run_id: str | None = None) -> dict[str, object]:
    run_id = _run_id(run_id or str(uuid.uuid4()))
    _validate_postgres_environment(run_id)
    staging_root = _staging_root(run_id)
    lock_fd = _acquire_lock(run_id)
    staging_directory: Path | None = None
    started_at_ms = time.time_ns() // 1_000_000
    started = asyncio.get_running_loop().time()
    try:
        staging_directory = Path(
            tempfile.mkdtemp(
                prefix=f".candlescope-query-backup-{run_id}-",
                dir=staging_root,
            )
        )
        staging_directory.chmod(0o700)
        basebackup = await _run_process(
            _basebackup_command(staging_directory, run_id),
            timeout_ms=_positive_env("BASEBACKUP_TIMEOUT_MS", 900_000),
            max_output_bytes=_positive_env("MAX_CHILD_OUTPUT_BYTES", 1 << 20),
            run_id=run_id,
            phase="basebackup",
            environment=_basebackup_environment(),
        )
        if basebackup.exit_code != 0:
            raise QueryBackupJobError(
                "pg_basebackup failed",
                code="BASEBACKUP_FAILED",
                phase="basebackup",
                run_id=run_id,
                child_exit_code=basebackup.exit_code,
            )
        _require_backup_artifacts(staging_directory, run_id)

        window = await _run_process(
            _window_command(staging_directory, run_id),
            timeout_ms=_positive_env("WINDOW_TIMEOUT_MS", 360_000),
            max_output_bytes=_positive_env("MAX_CHILD_OUTPUT_BYTES", 1 << 20),
            run_id=run_id,
            phase="window",
            environment=_window_environment(),
        )
        if window.exit_code != 0:
            raise QueryBackupJobError(
                "backup window failed",
                code="BACKUP_WINDOW_FAILED",
                phase="window",
                run_id=run_id,
                child_exit_code=window.exit_code,
            )
        bundle_receipt, window_receipt = _parse_window_output(window.stdout, run_id)
        completed_at_ms = time.time_ns() // 1_000_000
        backup = bundle_receipt["backup"]
        anchor = bundle_receipt["anchor"]
        receipt = {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "status": "succeeded",
            "run_id": run_id,
            "backup_id": run_id,
            "started_at_ms": started_at_ms,
            "completed_at_ms": completed_at_ms,
            "duration_ms": int((asyncio.get_running_loop().time() - started) * 1_000),
            "manifest_uri": backup["manifest_uri"],
            "manifest_sha256": backup["manifest_sha256"],
            "audit_anchor_uri": anchor["anchor_uri"],
            "audit_anchor_sha256": anchor["content_sha256"],
            "recovery_target_time": bundle_receipt["recovery_target_time"],
            "recovery_target_lsn": bundle_receipt["recovery_target_lsn"],
            "recovery_target_wal_filename": bundle_receipt[
                "recovery_target_wal_filename"
            ],
            "wal_coverage_segment_count": backup["wal_coverage_segment_count"],
            "write_fence_id": bundle_receipt["fence_id"],
            "operator_id": window_receipt["operator_id"],
        }
        try:
            history = await server_query_backup_history.publish_receipt(receipt)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise QueryBackupJobError(
                "immutable backup success history could not be published",
                code="SUCCESS_HISTORY_PUBLISH_FAILED",
                phase="history",
                run_id=run_id,
            ) from exc
        result = {
            **receipt,
            "schema_version": RESULT_SCHEMA_VERSION,
            "success_history_schema_version": history["schema_version"],
            "success_history_uri": history["history_uri"],
            "success_history_sha256": history["history_sha256"],
            "success_history_created": history["created"],
        }
        try:
            server_query_backup_monitor.persist_success_reference(receipt, history)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise QueryBackupJobError(
                "host backup success reference could not be persisted",
                code="SUCCESS_INVENTORY_PERSIST_FAILED",
                phase="inventory",
                run_id=run_id,
            ) from exc
        return result
    finally:
        try:
            if staging_directory is not None:
                shutil.rmtree(staging_directory)
        finally:
            os.close(lock_fd)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one scheduled query-control physical backup job."
    )
    parser.add_argument("--run-id")
    arguments = parser.parse_args()
    run_id = str(uuid.uuid4())
    try:
        if arguments.run_id is not None:
            run_id = _run_id(arguments.run_id)
        receipt = asyncio.run(run(run_id=run_id))
    except QueryBackupJobError as exc:
        print(_canonical_json(exc.to_wire()), file=sys.stderr)
        raise SystemExit(
            75 if isinstance(exc, QueryBackupJobAlreadyRunningError) else 1
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        failure = QueryBackupJobError(
            "backup job failed before a classified receipt was available",
            code="JOB_UNCLASSIFIED_FAILURE",
            phase="job",
            run_id=run_id,
        )
        print(_canonical_json(failure.to_wire()), file=sys.stderr)
        raise SystemExit(1) from exc
    print(_canonical_json(receipt))


def _basebackup_command(staging_directory: Path, run_id: str) -> tuple[str, ...]:
    try:
        executable = _regular_executable(
            _required_env("PG_BASEBACKUP_EXECUTABLE"),
            field="PG_BASEBACKUP_EXECUTABLE",
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise QueryBackupJobError(
            "pg_basebackup executable configuration is invalid",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        ) from exc
    return (
        str(executable),
        "--pgdata",
        str(staging_directory),
        "--format=tar",
        "--gzip",
        "--wal-method=stream",
        "--checkpoint=fast",
        "--manifest-checksums=SHA256",
        "--no-password",
    )


def _window_command(staging_directory: Path, run_id: str) -> tuple[str, ...]:
    executable = Path(sys.executable).resolve(strict=True)
    if not executable.is_absolute() or not executable.is_file():
        raise QueryBackupJobError(
            "Python executable is unavailable",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )
    return (
        str(executable),
        str(WINDOW_SCRIPT.resolve(strict=True)),
        "--",
        str(executable),
        str(BUNDLE_SCRIPT.resolve(strict=True)),
        "--backup-directory",
        str(staging_directory),
        "--backup-id",
        run_id,
    )


async def _run_process(
    command: tuple[str, ...],
    *,
    timeout_ms: int,
    max_output_bytes: int,
    run_id: str,
    phase: str,
    environment: Mapping[str, str] | None = None,
) -> _ProcessResult:
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        env=environment,
    )
    assert process.stdout is not None
    assert process.stderr is not None
    stdout_task = asyncio.create_task(_read_bounded(process.stdout, max_output_bytes))
    stderr_task = asyncio.create_task(_read_bounded(process.stderr, max_output_bytes))
    wait_task = asyncio.create_task(process.wait())
    tasks = (stdout_task, stderr_task, wait_task)
    try:
        stdout, _stderr, exit_code = await asyncio.wait_for(
            asyncio.gather(*tasks),
            timeout=timeout_ms / 1_000,
        )
    except TimeoutError as exc:
        await _terminate_process_group(process)
        await asyncio.gather(*tasks, return_exceptions=True)
        raise QueryBackupJobError(
            "backup child exceeded its runtime bound",
            code="CHILD_TIMEOUT",
            phase=phase,
            run_id=run_id,
        ) from exc
    except _OutputLimitError as exc:
        await _terminate_process_group(process)
        await asyncio.gather(*tasks, return_exceptions=True)
        raise QueryBackupJobError(
            "backup child output exceeded its configured bound",
            code="CHILD_OUTPUT_EXCEEDED",
            phase=phase,
            run_id=run_id,
        ) from exc
    except BaseException:
        await _terminate_process_group(process)
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    return _ProcessResult(exit_code=exit_code, stdout=stdout)


class _OutputLimitError(RuntimeError):
    pass


async def _read_bounded(
    stream: asyncio.StreamReader,
    maximum_bytes: int,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await stream.read(min(65_536, maximum_bytes - total + 1))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum_bytes:
            raise _OutputLimitError


async def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()


def _parse_window_output(
    data: bytes,
    run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QueryBackupJobError(
            "backup window output is not UTF-8",
            code="WINDOW_RECEIPT_INVALID",
            phase="receipt",
            run_id=run_id,
        ) from exc
    lines = text.splitlines()
    if len(lines) != 2 or text != f"{lines[0]}\n{lines[1]}\n":
        raise QueryBackupJobError(
            "backup window did not return exactly two LF-terminated receipts",
            code="WINDOW_RECEIPT_INVALID",
            phase="receipt",
            run_id=run_id,
        )
    try:
        bundle = _strict_json_object(lines[0])
        window = _strict_json_object(lines[1])
        backup = bundle["backup"]
        anchor = bundle["anchor"]
        if not isinstance(backup, dict) or not isinstance(anchor, dict):
            raise TypeError("nested receipt shape")
        if backup["backup_id"] != run_id:
            raise ValueError("backup ID")
        if bundle["fence_id"] != window["fence_id"]:
            raise ValueError("fence ID")
        if bundle["fence_acquired_at_ms"] != window["fence_acquired_at_ms"]:
            raise ValueError("fence acquisition time")
        if window["command_exit_code"] != 0:
            raise ValueError("window exit code")
        _s3_uri(backup["manifest_uri"], field="manifest_uri")
        _sha256(backup["manifest_sha256"], field="manifest_sha256")
        _s3_uri(anchor["anchor_uri"], field="anchor_uri")
        _sha256(anchor["content_sha256"], field="anchor content_sha256")
        coverage_count = backup["wal_coverage_segment_count"]
        if (
            isinstance(coverage_count, bool)
            or not isinstance(coverage_count, int)
            or coverage_count <= 0
        ):
            raise ValueError("coverage count")
        _run_id(bundle["fence_id"])
        operator_id = window["operator_id"]
        if not isinstance(operator_id, str) or not operator_id:
            raise ValueError("operator ID")
        for field in (
            "recovery_target_time",
            "recovery_target_lsn",
            "recovery_target_wal_filename",
        ):
            if not isinstance(bundle[field], str) or not bundle[field]:
                raise ValueError(field)
    except (KeyError, TypeError, ValueError) as exc:
        raise QueryBackupJobError(
            "backup window receipts are inconsistent",
            code="WINDOW_RECEIPT_INVALID",
            phase="receipt",
            run_id=run_id,
        ) from exc
    return bundle, window


def _strict_json_object(value: str) -> dict[str, Any]:
    parsed = json.loads(value, object_pairs_hook=_strict_object)
    if not isinstance(parsed, dict) or _canonical_json(parsed) != value:
        raise ValueError("receipt is not canonical JSON")
    return parsed


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _require_backup_artifacts(directory: Path, run_id: str) -> None:
    if {entry.name for entry in directory.iterdir()} != set(REQUIRED_BACKUP_ARTIFACTS):
        raise QueryBackupJobError(
            "pg_basebackup produced an unexpected artifact set",
            code="BASEBACKUP_ARTIFACTS_INVALID",
            phase="basebackup",
            run_id=run_id,
        )
    for name in REQUIRED_BACKUP_ARTIFACTS:
        path = directory / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size <= 0:
            raise QueryBackupJobError(
                "pg_basebackup artifact is not a non-empty regular file",
                code="BASEBACKUP_ARTIFACTS_INVALID",
                phase="basebackup",
                run_id=run_id,
            )


def _validate_postgres_environment(run_id: str) -> None:
    if os.environ.get("PGPASSWORD"):
        raise QueryBackupJobError(
            "PGPASSWORD is not allowed; use a protected PGPASSFILE",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )
    if os.environ.get("PGSERVICE") or os.environ.get("PGSERVICEFILE"):
        raise QueryBackupJobError(
            "PGSERVICE indirection is not allowed",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )
    for name in ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSFILE"):
        if not os.environ.get(name, "").strip():
            raise QueryBackupJobError(
                "required PostgreSQL environment is missing",
                code="CONFIGURATION_INVALID",
                phase="configuration",
                run_id=run_id,
            )
    try:
        port = int(os.environ["PGPORT"])
    except ValueError as exc:
        raise QueryBackupJobError(
            "PGPORT is invalid",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        ) from exc
    if not 1 <= port <= 65_535:
        raise QueryBackupJobError(
            "PGPORT is invalid",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )
    passfile = Path(os.environ["PGPASSFILE"])
    try:
        status = passfile.lstat()
    except OSError as exc:
        raise QueryBackupJobError(
            "PGPASSFILE cannot be inspected",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        ) from exc
    if (
        not passfile.is_absolute()
        or stat.S_ISLNK(status.st_mode)
        or not stat.S_ISREG(status.st_mode)
        or status.st_uid != os.geteuid()
        or status.st_mode & 0o077
        or status.st_size <= 0
    ):
        raise QueryBackupJobError(
            "PGPASSFILE must be an owned non-empty absolute 0600 regular file",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )


def _basebackup_environment() -> dict[str, str]:
    allowed = (
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSFILE",
        "PGSSLMODE",
        "PGSSLROOTCERT",
        "PGSSLCERT",
        "PGSSLKEY",
        "PGCONNECT_TIMEOUT",
        "LANG",
        "LC_ALL",
        "TZ",
    )
    return {name: os.environ[name] for name in allowed if name in os.environ}


def _window_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in (
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSFILE",
        "PGSSLMODE",
        "PGSSLROOTCERT",
        "PGSSLCERT",
        "PGSSLKEY",
        "PGCONNECT_TIMEOUT",
    ):
        environment.pop(name, None)
    for name in tuple(environment):
        if name.startswith(
            (
                "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_",
                "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_",
            )
        ):
            environment.pop(name)
    return environment


def _staging_root(run_id: str) -> Path:
    raw = Path(_required_env("STAGING_ROOT"))
    try:
        status = raw.lstat()
        root = raw.resolve(strict=True)
    except OSError as exc:
        raise QueryBackupJobError(
            "staging root cannot be inspected",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        ) from exc
    if (
        not raw.is_absolute()
        or stat.S_ISLNK(status.st_mode)
        or raw != root
        or not stat.S_ISDIR(status.st_mode)
        or status.st_uid != os.geteuid()
        or status.st_mode & 0o077
    ):
        raise QueryBackupJobError(
            "staging root must be an owned private absolute directory without symlinks",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )
    return root


def _acquire_lock(run_id: str) -> int:
    path = Path(_required_env("LOCK_PATH"))
    try:
        parent_status = path.parent.lstat()
        resolved_parent = path.parent.resolve(strict=True)
    except OSError as exc:
        raise QueryBackupJobError(
            "lock path parent cannot be inspected",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        ) from exc
    if (
        not path.is_absolute()
        or path.parent != resolved_parent
        or stat.S_ISLNK(parent_status.st_mode)
        or not stat.S_ISDIR(parent_status.st_mode)
        or parent_status.st_uid != os.geteuid()
        or parent_status.st_mode & 0o077
    ):
        raise QueryBackupJobError(
            "lock path parent must be an owned private directory without symlinks",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        )
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise QueryBackupJobError(
            "backup job lock cannot be opened",
            code="CONFIGURATION_INVALID",
            phase="configuration",
            run_id=run_id,
        ) from exc
    try:
        status = os.fstat(descriptor)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.geteuid()
            or status.st_mode & 0o077
        ):
            raise QueryBackupJobError(
                "backup job lock must be an owned private regular file",
                code="CONFIGURATION_INVALID",
                phase="configuration",
                run_id=run_id,
            )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise QueryBackupJobAlreadyRunningError(
                "another backup job is already running",
                code="JOB_ALREADY_RUNNING",
                phase="lock",
                run_id=run_id,
            ) from exc
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _regular_executable(value: str, *, field: str) -> Path:
    path = Path(value)
    try:
        status = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{field} cannot be inspected") from exc
    if (
        not path.is_absolute()
        or stat.S_ISLNK(status.st_mode)
        or not resolved.is_file()
        or not os.access(resolved, os.X_OK)
    ):
        raise ValueError(f"{field} must be an absolute non-symlink executable")
    return resolved


def _run_id(value: object) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("run_id must be a UUID") from exc
    rendered = str(parsed)
    if value != rendered:
        raise ValueError("run_id must be a canonical UUID")
    return rendered


def _s3_uri(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.path in {"", "/"}
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field} must be a credential-free S3 URI")
    return value


def _sha256(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _required_env(suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _positive_env(suffix: str, default: int) -> int:
    name = f"{ENV_PREFIX}{suffix}"
    raw = os.environ.get(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"setting {name} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"setting {name} must be positive")
    return value


if __name__ == "__main__":
    main()
