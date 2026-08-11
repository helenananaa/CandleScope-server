"""Publish one audit-anchored physical backup inside a Phase 1K fence."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import time
from pathlib import Path
from typing import Any

import psycopg
from scripts.server_query_audit_anchor import run as run_audit_anchor
from scripts.server_query_backup_publish import run as run_backup_publish

BACKUP_ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_"
WINDOW_ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_"


async def run(*, backup_directory: Path, backup_id: str) -> dict[str, Any]:
    _required_window_env("FENCE_ID")
    _required_window_env("FENCE_ACQUIRED_AT_MS")
    anchor = await run_audit_anchor(action="publish", anchor_uri=None)
    target_time = await _database_target_time()
    backup = await run_backup_publish(
        backup_directory=backup_directory,
        backup_id=backup_id,
        created_at_ms=time.time_ns() // 1_000_000,
        recovery_target_time=target_time,
        audit_anchor_uri=str(anchor["anchor_uri"]),
    )
    return {
        "fence_id": backup["write_fence_id"],
        "fence_acquired_at_ms": backup["write_fence_acquired_at_ms"],
        "recovery_target_time": target_time,
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


async def _database_target_time() -> str:
    dsn = _required_backup_env("POSTGRES_AUDITOR_DSN")
    try:
        async with await psycopg.AsyncConnection.connect(
            dsn,
            connect_timeout=max(
                1,
                math.ceil(_positive_backup_env("CONNECT_TIMEOUT_MS", 5_000) / 1_000),
            ),
            application_name="candlescope-query-backup-target-time",
        ) as connection:
            await connection.set_read_only(True)
            timeout = str(_positive_backup_env("REQUEST_TIMEOUT_MS", 30_000))
            await connection.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (timeout,),
            )
            result = await connection.execute(
                "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', "
                "'YYYY-MM-DD HH24:MI:SS.US\"+00\"')"
            )
            row = await result.fetchone()
    except psycopg.Error as exc:
        raise RuntimeError("PostgreSQL recovery target time is unavailable") from exc
    if row is None or not isinstance(row[0], str):
        raise RuntimeError("PostgreSQL recovery target time is invalid")
    return row[0]


def _required_backup_env(suffix: str) -> str:
    name = f"{BACKUP_ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _required_window_env(suffix: str) -> str:
    name = f"{WINDOW_ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _positive_backup_env(suffix: str, default: int) -> int:
    name = f"{BACKUP_ENV_PREFIX}{suffix}"
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
