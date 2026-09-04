"""Versioned PostgreSQL migrations for replay session runtime state."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

REPLAY_RUNTIME_MIGRATION_TABLE = "candlescope_replay_schema_migration"
REPLAY_RUNTIME_MIGRATION_VERSION = 2
REPLAY_RUNTIME_MIGRATION_NAME = "002_replay_runtime"
REPLAY_RUNTIME_MIGRATION_SHA256 = (
    "12849d4fd926b69b997f7038f03fa190d3eff582dc375494d22393ed9461bbb9"
)
REPLAY_RUNTIME_GROUP_ROLE = "candlescope_replay_runtime"
REPLAY_RUNTIME_MIGRATION_LOCK = "candlescope-replay-runtime-migration-v2"
DEFAULT_REPLAY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "deploy"
    / "server"
    / "postgres"
    / "migrations"
    / "002_replay_runtime.sql"
)


class ReplayRuntimeMigrationError(RuntimeError):
    """Base class for replay-runtime migration failures."""


class ReplayRuntimeMigrationDriftError(ReplayRuntimeMigrationError):
    """Applied migration metadata differs from the compiled contract."""


@dataclass(frozen=True, slots=True)
class ReplayRuntimeMigrationResult:
    version: int
    name: str
    sql_sha256: str
    applied: bool
    runtime_login_role: str


class PostgresReplayRuntimeMigrator:
    """Apply migration 002 using an administrator connection."""

    def __init__(
        self,
        dsn: str,
        *,
        migration_path: Path,
        runtime_login_role: str,
        connect_timeout_ms: int = 5_000,
        request_timeout_ms: int = 30_000,
    ) -> None:
        self._dsn = _required_text(dsn, field="dsn", max_length=4_096)
        if not isinstance(migration_path, Path):
            raise TypeError("migration_path must be a pathlib.Path")
        self._migration_path = migration_path
        self._runtime_login_role = _role_name(
            runtime_login_role,
            field="runtime_login_role",
        )
        self._connect_timeout_ms = _positive_int(
            connect_timeout_ms,
            field="connect_timeout_ms",
        )
        self._request_timeout_ms = _positive_int(
            request_timeout_ms,
            field="request_timeout_ms",
        )

    def __repr__(self) -> str:
        return "PostgresReplayRuntimeMigrator(dsn=<redacted>)"

    async def apply(self) -> ReplayRuntimeMigrationResult:
        migration_sql = self._read_migration_sql()
        applied = False
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (REPLAY_RUNTIME_MIGRATION_LOCK,),
                )
                await cursor.execute(migration_sql)
                await cursor.execute(
                    f"""
                    SELECT version, name, sql_sha256
                    FROM {REPLAY_RUNTIME_MIGRATION_TABLE}
                    WHERE version = %s
                    FOR UPDATE
                    """,
                    (REPLAY_RUNTIME_MIGRATION_VERSION,),
                )
                current = await cursor.fetchone()
                if current is None:
                    await cursor.execute(
                        f"""
                        INSERT INTO {REPLAY_RUNTIME_MIGRATION_TABLE} (
                            version, name, sql_sha256
                        ) VALUES (%s, %s, %s)
                        """,
                        (
                            REPLAY_RUNTIME_MIGRATION_VERSION,
                            REPLAY_RUNTIME_MIGRATION_NAME,
                            REPLAY_RUNTIME_MIGRATION_SHA256,
                        ),
                    )
                    applied = True
                else:
                    _require_current_migration(current)
                await self._grant_login_role(cursor)
        except psycopg.Error as exc:
            raise ReplayRuntimeMigrationError(
                "PostgreSQL replay-runtime migration failed"
            ) from exc
        return ReplayRuntimeMigrationResult(
            version=REPLAY_RUNTIME_MIGRATION_VERSION,
            name=REPLAY_RUNTIME_MIGRATION_NAME,
            sql_sha256=REPLAY_RUNTIME_MIGRATION_SHA256,
            applied=applied,
            runtime_login_role=self._runtime_login_role,
        )

    def _read_migration_sql(self) -> str:
        try:
            raw = self._migration_path.read_bytes()
        except OSError as exc:
            raise ReplayRuntimeMigrationError("migration SQL cannot be read") from exc
        digest = hashlib.sha256(raw).hexdigest()
        if digest != REPLAY_RUNTIME_MIGRATION_SHA256:
            raise ReplayRuntimeMigrationDriftError(
                "migration SQL checksum differs from the compiled contract"
            )
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ReplayRuntimeMigrationError("migration SQL must be UTF-8") from exc

    async def _grant_login_role(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
    ) -> None:
        await _require_group_role(cursor, REPLAY_RUNTIME_GROUP_ROLE)
        await _require_login_role(cursor, self._runtime_login_role)
        await cursor.execute(
            sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
                sql.Identifier(REPLAY_RUNTIME_GROUP_ROLE),
                sql.Identifier(self._runtime_login_role),
            )
        )

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        connection = await psycopg.AsyncConnection.connect(
            self._dsn,
            row_factory=dict_row,
            connect_timeout=max(1, math.ceil(self._connect_timeout_ms / 1_000)),
            application_name="candlescope-replay-migrator",
        )
        try:
            timeout = str(self._request_timeout_ms)
            await connection.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (timeout,),
            )
            await connection.execute(
                "SELECT set_config('lock_timeout', %s, false)",
                (timeout,),
            )
            return connection
        except BaseException:
            await connection.close()
            raise


def _require_current_migration(row: dict[str, Any]) -> None:
    if (
        int(row["version"]) != REPLAY_RUNTIME_MIGRATION_VERSION
        or row["name"] != REPLAY_RUNTIME_MIGRATION_NAME
        or row["sql_sha256"] != REPLAY_RUNTIME_MIGRATION_SHA256
    ):
        raise ReplayRuntimeMigrationDriftError(
            "applied replay-runtime migration metadata has drifted"
        )


async def _require_login_role(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
    role_name: str,
) -> None:
    await cursor.execute(
        """
        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
               rolreplication, rolbypassrls
        FROM pg_roles
        WHERE rolname = %s
        """,
        (role_name,),
    )
    role = await cursor.fetchone()
    if role is None:
        raise ReplayRuntimeMigrationError(f"login role {role_name!r} does not exist")
    if not role["rolcanlogin"] or any(
        role[field]
        for field in (
            "rolsuper",
            "rolcreatedb",
            "rolcreaterole",
            "rolreplication",
            "rolbypassrls",
        )
    ):
        raise ReplayRuntimeMigrationError(
            f"login role {role_name!r} is missing or elevated"
        )


async def _require_group_role(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
    role_name: str,
) -> None:
    await cursor.execute(
        """
        SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
               rolreplication, rolbypassrls
        FROM pg_roles
        WHERE rolname = %s
        """,
        (role_name,),
    )
    role = await cursor.fetchone()
    if (
        role is None
        or role["rolcanlogin"]
        or any(
            role[field]
            for field in (
                "rolsuper",
                "rolcreatedb",
                "rolcreaterole",
                "rolreplication",
                "rolbypassrls",
            )
        )
    ):
        raise ReplayRuntimeMigrationError(
            f"group role {role_name!r} is missing, login-enabled, or elevated"
        )


def _role_name(value: object, *, field: str) -> str:
    value = _required_text(value, field=field, max_length=63)
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_"
    if value[0] not in "abcdefghijklmnopqrstuvwxyz_" or any(
        character not in allowed for character in value
    ):
        raise ValueError(f"{field} must be a lower-case PostgreSQL identifier")
    if value == REPLAY_RUNTIME_GROUP_ROLE:
        raise ValueError(f"{field} cannot reuse a group role")
    return value


def _required_text(value: object, *, field: str, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    value = value.strip()
    if len(value) > max_length:
        raise ValueError(f"{field} must contain at most {max_length} characters")
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value
