"""Publish signed backup success history and verify bounded success cadence."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import sys
import time
from typing import Any

from app.server_runtime.query_backup_history import (
    DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
    DEFAULT_MAXIMUM_GAP_MS,
    DEFAULT_MAXIMUM_HISTORIES,
    BackupRunHistorySigner,
    ImmutableBackupRunHistoryRepository,
    verify_success_cadence,
)
from app.server_runtime.query_backup_selection import BackupJobReceipt
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_"
FAILURE_SCHEMA_VERSION = "candlescope.query-backup-history-failure.v1"


async def publish_receipt(receipt_wire: dict[str, object]) -> dict[str, object]:
    receipt = BackupJobReceipt.from_wire(receipt_wire)
    published = await history_repository().publish(
        receipt,
        cluster_id=_required_env("CLUSTER_ID"),
    )
    return {
        "schema_version": published.history.schema_version,
        "cluster_id": published.history.cluster_id,
        "backup_id": published.history.receipt.backup_id,
        "completed_at_ms": published.history.receipt.completed_at_ms,
        "history_uri": published.uri,
        "history_sha256": published.content_sha256,
        "created": published.created,
    }


async def verify_cadence(
    *,
    history_uris: tuple[str, ...],
    expected_cluster_id: str,
    window_start_ms: int,
    window_end_ms: int,
    evaluated_at_ms: int | None = None,
) -> dict[str, object]:
    maximum_histories = _positive_env(
        "MAXIMUM_HISTORIES",
        DEFAULT_MAXIMUM_HISTORIES,
    )
    if not history_uris or len(history_uris) > maximum_histories:
        raise RuntimeError("history URI count is outside its configured bound")
    configured_cluster_id = _required_env("CLUSTER_ID")
    if expected_cluster_id != configured_cluster_id:
        raise RuntimeError("expected cluster differs from history configuration")
    repository = history_repository()
    histories = []
    for uri in history_uris:
        histories.append(await repository.verify(uri))
    cadence = verify_success_cadence(
        tuple(histories),
        expected_cluster_id=expected_cluster_id,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        evaluated_at_ms=(
            evaluated_at_ms
            if evaluated_at_ms is not None
            else time.time_ns() // 1_000_000
        ),
        maximum_gap_ms=_positive_env("MAXIMUM_GAP_MS", DEFAULT_MAXIMUM_GAP_MS),
        maximum_future_skew_ms=_non_negative_env(
            "MAXIMUM_FUTURE_SKEW_MS",
            DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
        ),
        maximum_histories=maximum_histories,
    )
    return cadence.to_wire()


def history_repository() -> ImmutableBackupRunHistoryRepository:
    return ImmutableBackupRunHistoryRepository(
        object_store=S3ImmutableObjectStore(
            endpoint_url=_required_env("S3_ENDPOINT_URL"),
            region=_optional_env("S3_REGION", "us-east-1"),
            bucket=_required_env("S3_BUCKET"),
            prefix=_required_env("S3_PREFIX"),
            access_key_id=_required_env("S3_ACCESS_KEY_ID"),
            secret_access_key=_required_env("S3_SECRET_ACCESS_KEY"),
            request_timeout_ms=_positive_env("REQUEST_TIMEOUT_MS", 30_000),
        ),
        signer=BackupRunHistorySigner(
            key_id=_required_env("HMAC_KEY_ID"),
            secret=_secret(),
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish or verify signed immutable backup success history."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--receipt-json", required=True)
    cadence = commands.add_parser("verify-cadence")
    cadence.add_argument("--history-uri", required=True, action="append")
    cadence.add_argument("--expected-cluster-id", required=True)
    cadence.add_argument("--window-start-ms", required=True, type=int)
    cadence.add_argument("--window-end-ms", required=True, type=int)
    arguments = parser.parse_args()
    try:
        if arguments.command == "publish":
            receipt = _strict_json_object(arguments.receipt_json)
            result = asyncio.run(publish_receipt(receipt))
        else:
            result = asyncio.run(
                verify_cadence(
                    history_uris=tuple(arguments.history_uri),
                    expected_cluster_id=arguments.expected_cluster_id,
                    window_start_ms=arguments.window_start_ms,
                    window_end_ms=arguments.window_end_ms,
                )
            )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "schema_version": FAILURE_SCHEMA_VERSION,
                    "status": "failed",
                    "code": "BACKUP_HISTORY_OPERATION_FAILED",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _strict_json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value, object_pairs_hook=_strict_object)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError("receipt JSON is invalid") from exc
    if not isinstance(parsed, dict):
        raise TypeError("receipt JSON must be an object")
    return parsed


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _secret() -> bytes:
    try:
        secret = base64.b64decode(_required_env("HMAC_SECRET_BASE64"), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RuntimeError(
            f"setting {ENV_PREFIX}HMAC_SECRET_BASE64 is not strict base64"
        ) from exc
    if len(secret) < 32:
        raise RuntimeError(
            f"setting {ENV_PREFIX}HMAC_SECRET_BASE64 is shorter than 32 bytes"
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
    value = _integer_env(suffix, default)
    if value <= 0:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be positive")
    return value


def _non_negative_env(suffix: str, default: int) -> int:
    value = _integer_env(suffix, default)
    if value < 0:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be non-negative")
    return value


def _integer_env(suffix: str, default: int) -> int:
    raw = os.environ.get(f"{ENV_PREFIX}{suffix}", str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be an integer") from exc


if __name__ == "__main__":
    main()
