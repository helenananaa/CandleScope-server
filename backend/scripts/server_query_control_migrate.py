"""Apply the immutable Phase 1I query-control PostgreSQL migration."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from app.server_runtime.query_migrations import PostgresQueryControlMigrator

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_MIGRATION_"
MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "deploy"
    / "server"
    / "postgres"
    / "migrations"
    / "001_query_control.sql"
)


async def run() -> dict[str, object]:
    migrator = PostgresQueryControlMigrator(
        _required_env("POSTGRES_DSN"),
        migration_path=MIGRATION_PATH,
        runtime_login_role=_required_env("RUNTIME_LOGIN_ROLE"),
        auditor_login_role=_required_env("AUDITOR_LOGIN_ROLE"),
        backend_ids=(
            os.environ.get(
                f"{ENV_PREFIX}HOT_BACKEND_ID",
                "clickhouse-market-events-v1",
            ),
        ),
    )
    result = await migrator.apply()
    return {
        "version": result.version,
        "name": result.name,
        "sql_sha256": result.sql_sha256,
        "applied": result.applied,
        "runtime_login_role": result.runtime_login_role,
        "auditor_login_role": result.auditor_login_role,
        "backend_ids": list(result.backend_ids),
    }


def main() -> None:
    print(json.dumps(asyncio.run(run()), sort_keys=True, separators=(",", ":")))


def _required_env(suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


if __name__ == "__main__":
    main()
