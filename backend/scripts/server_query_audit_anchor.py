"""Publish or verify an immutable Phase 1I query-audit anchor."""

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
    QueryAuditAnchorSigner,
)
from app.server_runtime.storage.postgres_query_control import (
    PostgresQueryAuditVerifier,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_"


async def run(*, action: str, anchor_uri: str | None) -> dict[str, Any]:
    verifier = PostgresQueryAuditVerifier(
        _required_env("POSTGRES_DSN"),
        verifier_id=_optional_env("VERIFIER_ID", "query-audit-anchor-operator"),
        connect_timeout_ms=_positive_env("CONNECT_TIMEOUT_MS", 5_000),
        request_timeout_ms=_positive_env("REQUEST_TIMEOUT_MS", 30_000),
    )
    repository = ImmutableQueryAuditAnchorRepository(
        object_store=S3ImmutableObjectStore(
            endpoint_url=_required_env("S3_ENDPOINT_URL"),
            region=_optional_env("S3_REGION", "us-east-1"),
            bucket=_required_env("S3_BUCKET"),
            prefix=_required_env("S3_PREFIX"),
            access_key_id=_required_env("S3_ACCESS_KEY_ID"),
            secret_access_key=_required_env("S3_SECRET_ACCESS_KEY"),
            request_timeout_ms=_positive_env("REQUEST_TIMEOUT_MS", 30_000),
        ),
        signer=QueryAuditAnchorSigner(
            key_id=_required_env("HMAC_KEY_ID"),
            secret=_secret(),
        ),
    )
    await verifier.start()
    try:
        database = await verifier.verify_audit_chain(
            max_records=_positive_env("MAX_RECORDS", 10_000)
        )
        if action == "publish":
            if anchor_uri is not None:
                raise ValueError("--anchor-uri is only valid with verify")
            result = await repository.publish(database)
        elif action == "verify":
            if anchor_uri is None or not anchor_uri.strip():
                raise ValueError("verify requires --anchor-uri")
            result = await repository.verify(anchor_uri.strip(), database=database)
        else:  # pragma: no cover - argparse constrains this value
            raise ValueError("unsupported action")
    finally:
        await verifier.stop()
    return {
        "action": action,
        "anchor_uri": result.uri,
        "content_sha256": result.content_sha256,
        "key_id": result.anchor.key_id,
        "record_count": result.anchor.record_count,
        "head_audit_sequence": result.anchor.head_audit_sequence,
        "head_event_hash": result.anchor.head_event_hash,
        "head_updated_at_ms": result.anchor.head_updated_at_ms,
        "migration_version": result.anchor.migration_version,
        "migration_sha256": result.anchor.migration_sha256,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish or verify an immutable query-audit chain anchor."
    )
    parser.add_argument("action", choices=("publish", "verify"))
    parser.add_argument("--anchor-uri")
    arguments = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(run(action=arguments.action, anchor_uri=arguments.anchor_uri)),
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _secret() -> bytes:
    encoded = _required_env("HMAC_SECRET_BASE64")
    try:
        secret = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(
            f"required setting {ENV_PREFIX}HMAC_SECRET_BASE64 is not strict base64"
        ) from exc
    if len(secret) < 32:
        raise RuntimeError("anchor HMAC secret must decode to at least 32 bytes")
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
