"""Verify and immutably publish a PostgreSQL physical backup directory."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from app.server_runtime.query_audit_anchor import (
    ImmutableQueryAuditAnchorRepository,
    QueryAuditAnchorSigner,
)
from app.server_runtime.query_backup_catalog import (
    REQUIRED_BACKUP_ARTIFACTS,
    ImmutablePhysicalBackupCatalog,
    PhysicalBackupManifestSigner,
    PhysicalBackupRequest,
    parse_postgres_backup_manifest,
)
from app.server_runtime.storage.postgres_query_control import (
    PostgresQueryAuditVerifier,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_"
ANCHOR_ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_"
WINDOW_ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_"


async def run(
    *,
    backup_directory: Path,
    backup_id: str,
    created_at_ms: int,
    recovery_target_time: str,
    audit_anchor_uri: str,
) -> dict[str, Any]:
    directory = backup_directory.resolve(strict=True)
    if not directory.is_dir():
        raise RuntimeError("backup directory is not a directory")
    max_artifact_bytes = _positive_env(
        "MAX_ARTIFACT_BYTES",
        512 * 1024 * 1024,
    )
    max_total_bytes = _positive_env("MAX_TOTAL_BYTES", 1024 * 1024 * 1024)
    artifacts = _read_artifacts(
        directory,
        max_artifact_bytes=max_artifact_bytes,
        max_total_bytes=max_total_bytes,
    )
    await _verify_postgres_backup(artifacts)
    metadata = parse_postgres_backup_manifest(artifacts["backup_manifest"])
    store = _store()
    anchors = ImmutableQueryAuditAnchorRepository(
        object_store=_anchor_store(),
        signer=QueryAuditAnchorSigner(
            key_id=_required_anchor_env("HMAC_KEY_ID"),
            secret=_anchor_secret(),
        ),
    )
    verifier = PostgresQueryAuditVerifier(
        _required_env("POSTGRES_AUDITOR_DSN"),
        verifier_id=_optional_env("VERIFIER_ID", "physical-backup-publisher"),
        connect_timeout_ms=_positive_env("CONNECT_TIMEOUT_MS", 5_000),
        request_timeout_ms=_positive_env("REQUEST_TIMEOUT_MS", 30_000),
    )
    await verifier.start()
    try:
        fence_operator_id = _required_window_env("OPERATOR_ID")
        await verifier.require_backup_fence(fence_operator_id)
        database = await verifier.verify_audit_chain(
            max_records=_positive_env("MAX_AUDIT_RECORDS", 10_000)
        )
        anchor = await anchors.verify(audit_anchor_uri, database=database)
    finally:
        await verifier.stop()
    request = PhysicalBackupRequest(
        backup_id=backup_id,
        cluster_id=_required_env("CLUSTER_ID"),
        created_at_ms=created_at_ms,
        write_fence_id=_required_window_env("FENCE_ID"),
        write_fence_acquired_at_ms=_positive_window_env("FENCE_ACQUIRED_AT_MS"),
        recovery_target_time=recovery_target_time,
        postgres_version=_required_env("POSTGRES_VERSION"),
        system_identifier=metadata.system_identifier,
        timeline=metadata.timeline,
        start_lsn=metadata.start_lsn,
        end_lsn=metadata.end_lsn,
        wal_archive_prefix_uri=_required_env("WAL_ARCHIVE_PREFIX_URI"),
        audit_anchor_uri=anchor.uri,
        audit_anchor_sha256=anchor.content_sha256,
        audit_head_sequence=database.head_audit_sequence,
        audit_head_event_hash=database.head_event_hash,
        migration_version=database.migration_version,
        migration_sha256=database.migration_sha256,
    )
    catalog = ImmutablePhysicalBackupCatalog(
        object_store=store,
        signer=PhysicalBackupManifestSigner(
            key_id=_required_env("BACKUP_HMAC_KEY_ID"),
            secret=_secret("BACKUP_HMAC_SECRET_BASE64"),
        ),
        max_artifact_bytes=max_artifact_bytes,
        max_total_bytes=max_total_bytes,
    )
    result = await catalog.publish(request, artifacts)
    return {
        "backup_id": result.manifest.request.backup_id,
        "cluster_id": result.manifest.request.cluster_id,
        "manifest_uri": result.manifest_uri,
        "manifest_sha256": result.manifest_sha256,
        "created_artifacts": result.created_artifacts,
        "manifest_created": result.manifest_created,
        "write_fence_id": result.manifest.request.write_fence_id,
        "write_fence_acquired_at_ms": (
            result.manifest.request.write_fence_acquired_at_ms
        ),
        "recovery_target_time": result.manifest.request.recovery_target_time,
        "audit_anchor_uri": result.manifest.request.audit_anchor_uri,
        "audit_head_sequence": result.manifest.request.audit_head_sequence,
        "system_identifier": str(result.manifest.request.system_identifier),
        "timeline": result.manifest.request.timeline,
        "start_lsn": result.manifest.request.start_lsn,
        "end_lsn": result.manifest.request.end_lsn,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify and publish a PostgreSQL physical backup set."
    )
    parser.add_argument("--backup-directory", required=True, type=Path)
    parser.add_argument("--backup-id", required=True)
    parser.add_argument("--created-at-ms", required=True, type=int)
    parser.add_argument("--recovery-target-time", required=True)
    parser.add_argument("--audit-anchor-uri", required=True)
    arguments = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                run(
                    backup_directory=arguments.backup_directory,
                    backup_id=arguments.backup_id,
                    created_at_ms=arguments.created_at_ms,
                    recovery_target_time=arguments.recovery_target_time,
                    audit_anchor_uri=arguments.audit_anchor_uri,
                )
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )


async def _verify_postgres_backup(artifacts: dict[str, bytes]) -> None:
    executable = Path(_required_env("PG_VERIFYBACKUP_EXECUTABLE"))
    if not executable.is_absolute() or not executable.is_file():
        raise RuntimeError("PG_VERIFYBACKUP_EXECUTABLE must be an absolute file")
    with tempfile.TemporaryDirectory(prefix="candlescope-backup-verify-") as name:
        directory = Path(name)
        directory.chmod(0o700)
        for artifact_name in REQUIRED_BACKUP_ARTIFACTS:
            path = directory / artifact_name
            path.write_bytes(artifacts[artifact_name])
            path.chmod(0o600)
        process = await asyncio.create_subprocess_exec(
            str(executable),
            "--no-parse-wal",
            str(directory),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                "pg_verifybackup rejected the physical backup: "
                + (stderr or stdout).decode(errors="replace")[:1000]
            )


def _read_artifacts(
    directory: Path,
    *,
    max_artifact_bytes: int,
    max_total_bytes: int,
) -> dict[str, bytes]:
    present = {entry.name for entry in directory.iterdir()}
    if present != set(REQUIRED_BACKUP_ARTIFACTS):
        raise RuntimeError("backup directory must contain exactly three artifacts")
    total = 0
    result: dict[str, bytes] = {}
    for name in REQUIRED_BACKUP_ARTIFACTS:
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("backup artifacts must be regular non-symlink files")
        size = path.stat().st_size
        total += size
        if not 0 < size <= max_artifact_bytes or total > max_total_bytes:
            raise RuntimeError("backup artifact size is outside configured bounds")
        data = path.read_bytes()
        if len(data) != size:
            raise RuntimeError("backup artifact size changed while it was read")
        result[name] = data
    return result


def _store() -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=_required_env("S3_ENDPOINT_URL"),
        region=_optional_env("S3_REGION", "us-east-1"),
        bucket=_required_env("S3_BUCKET"),
        prefix=_required_env("S3_PREFIX"),
        access_key_id=_required_env("S3_ACCESS_KEY_ID"),
        secret_access_key=_required_env("S3_SECRET_ACCESS_KEY"),
        request_timeout_ms=_positive_env("REQUEST_TIMEOUT_MS", 30_000),
    )


def _anchor_store() -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=_required_anchor_env("S3_ENDPOINT_URL"),
        region=_optional_anchor_env("S3_REGION", "us-east-1"),
        bucket=_required_anchor_env("S3_BUCKET"),
        prefix=_required_anchor_env("S3_PREFIX"),
        access_key_id=_required_anchor_env("S3_ACCESS_KEY_ID"),
        secret_access_key=_required_anchor_env("S3_SECRET_ACCESS_KEY"),
        request_timeout_ms=_positive_anchor_env("REQUEST_TIMEOUT_MS", 30_000),
    )


def _secret(suffix: str) -> bytes:
    try:
        secret = base64.b64decode(_required_env(suffix), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(
            f"setting {ENV_PREFIX}{suffix} is not strict base64"
        ) from exc
    if len(secret) < 32:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} is shorter than 32 bytes")
    return secret


def _anchor_secret() -> bytes:
    try:
        secret = base64.b64decode(
            _required_anchor_env("HMAC_SECRET_BASE64"),
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(
            f"setting {ANCHOR_ENV_PREFIX}HMAC_SECRET_BASE64 is not strict base64"
        ) from exc
    if len(secret) < 32:
        raise RuntimeError(
            f"setting {ANCHOR_ENV_PREFIX}HMAC_SECRET_BASE64 is shorter than 32 bytes"
        )
    return secret


def _required_env(suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _optional_env(suffix: str, default: str) -> str:
    value = os.environ.get(f"{ENV_PREFIX}{suffix}", default).strip()
    if not value:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} cannot be blank")
    return value


def _positive_env(suffix: str, default: int) -> int:
    raw = os.environ.get(f"{ENV_PREFIX}{suffix}", str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be positive")
    return value


def _required_window_env(suffix: str) -> str:
    name = f"{WINDOW_ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _positive_window_env(suffix: str) -> int:
    name = f"{WINDOW_ENV_PREFIX}{suffix}"
    raw = _required_window_env(suffix)
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"setting {name} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"setting {name} must be positive")
    return value


def _required_anchor_env(suffix: str) -> str:
    name = f"{ANCHOR_ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _optional_anchor_env(suffix: str, default: str) -> str:
    value = os.environ.get(f"{ANCHOR_ENV_PREFIX}{suffix}", default).strip()
    if not value:
        raise RuntimeError(f"setting {ANCHOR_ENV_PREFIX}{suffix} cannot be blank")
    return value


def _positive_anchor_env(suffix: str, default: int) -> int:
    name = f"{ANCHOR_ENV_PREFIX}{suffix}"
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
