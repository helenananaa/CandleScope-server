"""Verify an immutable PostgreSQL physical backup and its audit anchor."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
from typing import Any

from app.server_runtime.query_audit_anchor import (
    ImmutableQueryAuditAnchorRepository,
    PublishedQueryAuditAnchor,
    QueryAuditAnchorSigner,
)
from app.server_runtime.query_backup_catalog import (
    ImmutablePhysicalBackupCatalog,
    PhysicalBackupManifestSigner,
    PublishedPhysicalBackup,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_"


async def run(
    *,
    manifest_uri: str,
    audit_anchor_uri: str,
) -> dict[str, Any]:
    store = _store()
    backup = await ImmutablePhysicalBackupCatalog(
        object_store=store,
        signer=PhysicalBackupManifestSigner(
            key_id=_required_env("BACKUP_HMAC_KEY_ID"),
            secret=_secret("BACKUP_HMAC_SECRET_BASE64"),
        ),
        max_artifact_bytes=_positive_env(
            "MAX_ARTIFACT_BYTES",
            512 * 1024 * 1024,
        ),
        max_total_bytes=_positive_env("MAX_TOTAL_BYTES", 1024 * 1024 * 1024),
    ).verify(
        manifest_uri,
        expected_audit_anchor_uri=audit_anchor_uri,
    )
    anchor = await ImmutableQueryAuditAnchorRepository(
        object_store=store,
        signer=QueryAuditAnchorSigner(
            key_id=_required_env("ANCHOR_HMAC_KEY_ID"),
            secret=_secret("ANCHOR_HMAC_SECRET_BASE64"),
        ),
    ).verify(audit_anchor_uri)
    _require_anchor_match(backup, anchor)
    request = backup.manifest.request
    return {
        "backup_id": request.backup_id,
        "cluster_id": request.cluster_id,
        "manifest_uri": backup.manifest_uri,
        "manifest_sha256": backup.manifest_sha256,
        "recovery_target_time": request.recovery_target_time,
        "audit_anchor_uri": request.audit_anchor_uri,
        "audit_anchor_sha256": request.audit_anchor_sha256,
        "audit_head_sequence": request.audit_head_sequence,
        "system_identifier": str(request.system_identifier),
        "timeline": request.timeline,
        "start_lsn": request.start_lsn,
        "end_lsn": request.end_lsn,
        "artifacts": [artifact.to_wire() for artifact in backup.manifest.artifacts],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify a physical backup set and its external audit anchor."
    )
    parser.add_argument("--manifest-uri", required=True)
    parser.add_argument("--audit-anchor-uri", required=True)
    arguments = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                run(
                    manifest_uri=arguments.manifest_uri,
                    audit_anchor_uri=arguments.audit_anchor_uri,
                )
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _require_anchor_match(
    backup: PublishedPhysicalBackup,
    anchor: PublishedQueryAuditAnchor,
) -> None:
    request = backup.manifest.request
    if (
        request.audit_anchor_uri,
        request.audit_anchor_sha256,
        request.audit_head_sequence,
        request.audit_head_event_hash,
        request.migration_version,
        request.migration_sha256,
    ) != (
        anchor.uri,
        anchor.content_sha256,
        anchor.anchor.head_audit_sequence,
        anchor.anchor.head_event_hash,
        anchor.anchor.migration_version,
        anchor.anchor.migration_sha256,
    ):
        raise RuntimeError("physical backup differs from its signed audit anchor")


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


if __name__ == "__main__":
    main()
