"""Select and fully verify the freshest backup from explicit job receipts."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
import sys
import time
from pathlib import Path
from typing import Any

from app.server_runtime.query_backup_selection import (
    DEFAULT_MAXIMUM_AGE_MS,
    DEFAULT_MAXIMUM_CANDIDATES,
    DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
    BackupJobReceipt,
    RecoverySelectionError,
    select_recovery_candidate,
)
from scripts import server_query_backup_verify

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_SELECT_"
FAILURE_SCHEMA_VERSION = "candlescope.query-backup-recovery-selection-failure.v1"
DEFAULT_MAXIMUM_RECEIPT_BYTES = 64 * 1024


async def run(
    *,
    receipt_paths: tuple[Path, ...],
    expected_cluster_id: str,
    expected_system_identifier: int,
    expected_timeline: int,
    now_ms: int | None = None,
) -> dict[str, object]:
    maximum_candidates = _positive_env(
        "MAXIMUM_CANDIDATES",
        DEFAULT_MAXIMUM_CANDIDATES,
    )
    if not receipt_paths or len(receipt_paths) > maximum_candidates:
        raise RecoverySelectionError("receipt path count is outside its bound")
    receipts = tuple(
        _read_receipt(
            path,
            maximum_bytes=_positive_env(
                "MAXIMUM_RECEIPT_BYTES",
                DEFAULT_MAXIMUM_RECEIPT_BYTES,
            ),
        )
        for path in receipt_paths
    )
    catalog = server_query_backup_verify.backup_catalog()
    inspected = []
    for receipt in receipts:
        published = await catalog.inspect_signed_manifest(
            receipt.manifest_uri,
            expected_audit_anchor_uri=receipt.audit_anchor_uri,
        )
        inspected.append((receipt, published))
    selection = select_recovery_candidate(
        tuple(inspected),
        expected_cluster_id=expected_cluster_id,
        expected_system_identifier=expected_system_identifier,
        expected_timeline=expected_timeline,
        now_ms=now_ms if now_ms is not None else time.time_ns() // 1_000_000,
        maximum_age_ms=_positive_env("MAXIMUM_AGE_MS", DEFAULT_MAXIMUM_AGE_MS),
        maximum_future_skew_ms=_non_negative_env(
            "MAXIMUM_FUTURE_SKEW_MS",
            DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
        ),
        maximum_candidates=maximum_candidates,
    )
    receipt = selection.receipt
    verified = await server_query_backup_verify.run(
        manifest_uri=receipt.manifest_uri,
        audit_anchor_uri=receipt.audit_anchor_uri,
    )
    _require_full_verification_match(selection.to_wire()["selected"], verified)
    result = selection.to_wire()
    result["status"] = "selected-and-fully-verified"
    result["selected_backup_fully_verified"] = True
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Select the freshest candidate from explicit Phase 1M receipts and "
            "fully verify the selected physical backup."
        )
    )
    parser.add_argument("--receipt", required=True, action="append", type=Path)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--expected-system-identifier", required=True, type=int)
    parser.add_argument("--expected-timeline", required=True, type=int)
    arguments = parser.parse_args()
    try:
        result = asyncio.run(
            run(
                receipt_paths=tuple(arguments.receipt),
                expected_cluster_id=arguments.expected_cluster_id,
                expected_system_identifier=arguments.expected_system_identifier,
                expected_timeline=arguments.expected_timeline,
            )
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "schema_version": FAILURE_SCHEMA_VERSION,
                    "status": "failed",
                    "code": "RECOVERY_SELECTION_FAILED",
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _read_receipt(path: Path, *, maximum_bytes: int) -> BackupJobReceipt:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RecoverySelectionError("receipt file cannot be inspected") from exc
    if not path.is_absolute() or path != resolved:
        raise RecoverySelectionError(
            "receipt must be an owned private bounded absolute regular file"
        )
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RecoverySelectionError("receipt file cannot be opened safely") from exc
    try:
        status = os.fstat(descriptor)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.geteuid()
            or status.st_mode & 0o077
            or not 0 < status.st_size <= maximum_bytes
        ):
            raise RecoverySelectionError(
                "receipt must be an owned private bounded absolute regular file"
            )
        chunks: list[bytes] = []
        remaining = maximum_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) != status.st_size:
            raise RecoverySelectionError("receipt file changed while it was read")
    finally:
        os.close(descriptor)
    return BackupJobReceipt.from_canonical_bytes(data)


def _require_full_verification_match(
    selected: object,
    verified: dict[str, Any],
) -> None:
    if not isinstance(selected, dict):
        raise TypeError("selected recovery candidate shape is invalid")
    fields = (
        "backup_id",
        "cluster_id",
        "system_identifier",
        "timeline",
        "manifest_uri",
        "manifest_sha256",
        "audit_anchor_uri",
        "audit_anchor_sha256",
        "recovery_target_time",
        "recovery_target_lsn",
        "recovery_target_wal_filename",
        "write_fence_id",
    )
    if any(selected[field] != verified[field] for field in fields):
        raise RecoverySelectionError(
            "full backup verification differs from the selected candidate"
        )
    if selected["wal_coverage_segment_count"] != len(verified["wal_coverage"]):
        raise RecoverySelectionError(
            "full WAL verification differs from the selected candidate"
        )


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
