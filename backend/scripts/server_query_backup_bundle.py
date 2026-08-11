"""Publish one audit-anchored physical backup inside a Phase 1K fence."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from scripts.server_query_audit_anchor import run as run_audit_anchor
from scripts.server_query_backup_publish import run as run_backup_publish

WINDOW_ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_"


async def run(*, backup_directory: Path, backup_id: str) -> dict[str, Any]:
    _required_window_env("FENCE_ID")
    _required_window_env("FENCE_ACQUIRED_AT_MS")
    anchor = await run_audit_anchor(action="publish", anchor_uri=None)
    backup = await run_backup_publish(
        backup_directory=backup_directory,
        backup_id=backup_id,
        created_at_ms=time.time_ns() // 1_000_000,
        audit_anchor_uri=str(anchor["anchor_uri"]),
    )
    return {
        "fence_id": backup["write_fence_id"],
        "fence_acquired_at_ms": backup["write_fence_acquired_at_ms"],
        "recovery_target_time": backup["recovery_target_time"],
        "recovery_target_lsn": backup["recovery_target_lsn"],
        "recovery_target_wal_filename": backup["recovery_target_wal_filename"],
        "wal_segment_size_bytes": backup["wal_segment_size_bytes"],
        "anchor": anchor,
        "backup": backup,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish an audit anchor and physical backup in one fence."
    )
    parser.add_argument("--backup-directory", required=True, type=Path)
    parser.add_argument("--backup-id", required=True)
    arguments = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                run(
                    backup_directory=arguments.backup_directory,
                    backup_id=arguments.backup_id,
                )
            ),
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _required_window_env(suffix: str) -> str:
    name = f"{WINDOW_ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


if __name__ == "__main__":
    main()
