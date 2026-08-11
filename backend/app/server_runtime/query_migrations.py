"""Versioned external PostgreSQL migrations for query-control state."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

QUERY_CONTROL_MIGRATION_TABLE = "candlescope_query_schema_migration"
QUERY_CONTROL_MIGRATION_VERSION = 1
QUERY_CONTROL_MIGRATION_NAME = "001_query_control"
QUERY_CONTROL_MIGRATION_SHA256 = (
    "b2c29c8300a325248d415ffc3358879beb644a7eddd2f7182a79366bfbebfcb5"
)
QUERY_RUNTIME_GROUP_ROLE = "candlescope_query_runtime"
QUERY_AUDITOR_GROUP_ROLE = "candlescope_query_auditor"
QUERY_CONTROL_MIGRATION_LOCK = "candlescope-query-control-migration-v1"

CREATE_QUERY_CONTROL_MIGRATION_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {QUERY_CONTROL_MIGRATION_TABLE} (
    version BIGINT PRIMARY KEY CHECK (version > 0),
    name TEXT NOT NULL UNIQUE CHECK (btrim(name) <> ''),
    sql_sha256 TEXT NOT NULL CHECK (sql_sha256 ~ '^[0-9a-f]{{64}}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    applied_by TEXT NOT NULL DEFAULT current_user CHECK (btrim(applied_by) <> '')
)
"""


class QueryControlMigrationError(RuntimeError):
    """Base class for query-control migration failures."""


class QueryControlMigrationDriftError(QueryControlMigrationError):
    """Applied migration metadata differs from the compiled contract."""


@dataclass(frozen=True, slots=True)
class QueryControlMigrationResult:
    version: int
    name: str
    sql_sha256: str
    applied: bool
    runtime_login_role: str
    auditor_login_role: str
    backend_ids: tuple[str, ...]


class PostgresQueryControlMigrator:
    """Apply one immutable migration using an administrator connection."""

    def __init__(
        self,
        dsn: str,
        *,
        migration_path: Path,
        runtime_login_role: str,
        auditor_login_role: str,
        backend_ids: tuple[str, ...],
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
        self._auditor_login_role = _role_name(
            auditor_login_role,
            field="auditor_login_role",
        )
        if self._runtime_login_role == self._auditor_login_role:
            raise ValueError("runtime and auditor login roles must be distinct")
        if not isinstance(backend_ids, tuple) or not backend_ids:
            raise ValueError("backend_ids must be a non-empty tuple")
        self._backend_ids = tuple(
            _required_text(value, field="backend_id", max_length=128)
            for value in backend_ids
        )
        if len(set(self._backend_ids)) != len(self._backend_ids):
            raise ValueError("backend_ids cannot contain duplicates")
        self._connect_timeout_ms = _positive_int(
            connect_timeout_ms,
            field="connect_timeout_ms",
        )
        self._request_timeout_ms = _positive_int(
            request_timeout_ms,
            field="request_timeout_ms",
        )

    async def apply(self) -> QueryControlMigrationResult:
        migration_sql = self._read_migration_sql()
        applied = False
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (QUERY_CONTROL_MIGRATION_LOCK,),
                )
                await cursor.execute(CREATE_QUERY_CONTROL_MIGRATION_TABLE_SQL)
                await cursor.execute(
                    f"""
                    SELECT version, name, sql_sha256
                    FROM {QUERY_CONTROL_MIGRATION_TABLE}
                    WHERE version = %s
                    FOR UPDATE
                    """,
                    (QUERY_CONTROL_MIGRATION_VERSION,),
                )
                current = await cursor.fetchone()
                if current is None:
                    await cursor.execute(migration_sql)
                    await cursor.execute(
                        f"""
                        INSERT INTO {QUERY_CONTROL_MIGRATION_TABLE} (
                            version, name, sql_sha256
                        ) VALUES (%s, %s, %s)
                        """,
                        (
                            QUERY_CONTROL_MIGRATION_VERSION,
                            QUERY_CONTROL_MIGRATION_NAME,
                            QUERY_CONTROL_MIGRATION_SHA256,
                        ),
                    )
                    applied = True
                else:
                    _require_current_migration(current)
                await self._seed_backends(cursor)
                await self._grant_login_roles(cursor)
        except psycopg.Error as exc:
            raise QueryControlMigrationError(
                "PostgreSQL query-control migration failed"
            ) from exc
        return QueryControlMigrationResult(
            version=QUERY_CONTROL_MIGRATION_VERSION,
            name=QUERY_CONTROL_MIGRATION_NAME,
            sql_sha256=QUERY_CONTROL_MIGRATION_SHA256,
            applied=applied,
            runtime_login_role=self._runtime_login_role,
            auditor_login_role=self._auditor_login_role,
            backend_ids=self._backend_ids,
        )

    def _read_migration_sql(self) -> str:
        try:
            raw = self._migration_path.read_bytes()
        except OSError as exc:
            raise QueryControlMigrationError("migration SQL cannot be read") from exc
        digest = hashlib.sha256(raw).hexdigest()
        if digest != QUERY_CONTROL_MIGRATION_SHA256:
            raise QueryControlMigrationDriftError(
                "migration SQL checksum differs from the compiled contract"
            )
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise QueryControlMigrationError("migration SQL must be UTF-8") from exc

    async def _seed_backends(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
    ) -> None:
        for backend_id in self._backend_ids:
            await cursor.execute(
                """
                INSERT INTO candlescope_query_hot_quarantine (backend_id)
                VALUES (%s)
                ON CONFLICT (backend_id) DO NOTHING
                """,
                (backend_id,),
            )

    async def _grant_login_roles(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
    ) -> None:
        await _require_group_role(cursor, QUERY_RUNTIME_GROUP_ROLE)
        await _require_group_role(cursor, QUERY_AUDITOR_GROUP_ROLE)
        await _require_login_role(cursor, self._runtime_login_role)
        await _require_login_role(cursor, self._auditor_login_role)
        await cursor.execute(
            sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
                sql.Identifier(QUERY_RUNTIME_GROUP_ROLE),
                sql.Identifier(self._runtime_login_role),
            )
        )
        await cursor.execute(
            sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
                sql.Identifier(QUERY_AUDITOR_GROUP_ROLE),
                sql.Identifier(self._auditor_login_role),
            )
        )

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        connection = await psycopg.AsyncConnection.connect(
            self._dsn,
            row_factory=dict_row,
            connect_timeout=max(1, math.ceil(self._connect_timeout_ms / 1_000)),
            application_name="candlescope-query-migrator",
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
        row["version"] != QUERY_CONTROL_MIGRATION_VERSION
        or row["name"] != QUERY_CONTROL_MIGRATION_NAME
        or row["sql_sha256"] != QUERY_CONTROL_MIGRATION_SHA256
    ):
        raise QueryControlMigrationDriftError(
            "applied query-control migration metadata has drifted"
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
        raise QueryControlMigrationError(f"login role {role_name!r} does not exist")
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
        raise QueryControlMigrationError(
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
        raise QueryControlMigrationError(
            f"group role {role_name!r} is missing, login-enabled, or elevated"
        )


def _role_name(value: object, *, field: str) -> str:
    value = _required_text(value, field=field, max_length=63)
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_"
    if value[0] not in "abcdefghijklmnopqrstuvwxyz_" or any(
        character not in allowed for character in value
    ):
        raise ValueError(f"{field} must be a lower-case PostgreSQL identifier")
    if value in {QUERY_RUNTIME_GROUP_ROLE, QUERY_AUDITOR_GROUP_ROLE}:
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
