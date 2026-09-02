"""PostgreSQL-backed replay session lease, fencing, and snapshot/tenant pins."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseBusyError,
    ReplaySessionLeaseFencedError,
    ReplaySessionScopeConflictError,
    ReplaySessionSnapshotConflictError,
    normalize_replay_session_id,
    normalize_replay_worker_id,
    require_write_fence,
)

REPLAY_SESSION_LEASE_TABLE = "candlescope_replay_session_lease"
REPLAY_SESSION_LOCK_PREFIX = "replay-session:"

CREATE_REPLAY_SESSION_LEASE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {REPLAY_SESSION_LEASE_TABLE} (
    session_id TEXT PRIMARY KEY CHECK (btrim(session_id) <> ''),
    worker_id TEXT NOT NULL CHECK (btrim(worker_id) <> ''),
    fencing_epoch BIGINT NOT NULL CHECK (fencing_epoch >= 0),
    lease_token UUID NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    organization_id TEXT NOT NULL CHECK (btrim(organization_id) <> ''),
    workspace_id TEXT NOT NULL CHECK (btrim(workspace_id) <> ''),
    data_epoch TEXT NOT NULL CHECK (btrim(data_epoch) <> ''),
    snapshot_version BIGINT NOT NULL CHECK (snapshot_version > 0),
    manifest_uri TEXT NOT NULL CHECK (btrim(manifest_uri) <> ''),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
"""

_SELECT_COLUMNS = """
session_id, worker_id, fencing_epoch, lease_token, lease_expires_at,
organization_id, workspace_id, data_epoch, snapshot_version,
manifest_uri, manifest_sha256
"""

_SELECT_FOR_UPDATE_SQL = f"""
SELECT {_SELECT_COLUMNS}
FROM {REPLAY_SESSION_LEASE_TABLE}
WHERE session_id = %s
FOR UPDATE
"""

_RETURNING_SQL = f"RETURNING {_SELECT_COLUMNS}"


class PostgresReplaySessionLeaseStore:
    """Use one short PostgreSQL transaction for every session ownership transition."""

    def __init__(self, dsn: str) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be a non-blank PostgreSQL connection string")
        self._dsn = dsn.strip()

    def __repr__(self) -> str:
        return "PostgresReplaySessionLeaseStore(dsn=<redacted>)"

    async def initialize_schema(self) -> None:
        """Create the idempotent Phase 1AB replay session lease table."""

        async with await self._connect() as connection:
            await connection.execute(CREATE_REPLAY_SESSION_LEASE_TABLE_SQL)

    async def acquire(
        self,
        *,
        session_id: str,
        worker_id: str,
        snapshot: MarketDataSnapshotRef,
        organization_id: str,
        workspace_id: str,
        lease_ttl_ms: int,
    ) -> ReplaySessionLease:
        if not isinstance(snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        session_id = normalize_replay_session_id(session_id)
        worker_id = normalize_replay_worker_id(worker_id)
        organization_id = normalize_organization_id(organization_id)
        workspace_id = normalize_workspace_id(workspace_id)
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        token = uuid.uuid4()
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_session(cursor, session_id)
            now = await _database_now(cursor)
            await cursor.execute(_SELECT_FOR_UPDATE_SQL, (session_id,))
            row = await cursor.fetchone()
            expires_at = now + timedelta(milliseconds=lease_ttl_ms)
            if row is None:
                await cursor.execute(
                    f"""
                        INSERT INTO {REPLAY_SESSION_LEASE_TABLE} (
                            session_id, worker_id, fencing_epoch, lease_token,
                            lease_expires_at, organization_id, workspace_id,
                            data_epoch, snapshot_version, manifest_uri,
                            manifest_sha256
                        ) VALUES (%s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s)
                        {_RETURNING_SQL}
                        """,
                    (
                        session_id,
                        worker_id,
                        token,
                        expires_at,
                        organization_id,
                        workspace_id,
                        snapshot.data_epoch,
                        snapshot.snapshot_version,
                        snapshot.manifest_uri,
                        snapshot.manifest_sha256,
                    ),
                )
            else:
                if row["lease_expires_at"] > now:
                    raise ReplaySessionLeaseBusyError(
                        f"session {session_id!r} is leased by {row['worker_id']!r}"
                    )
                current = _row_to_lease(row)
                if current.snapshot != snapshot:
                    raise ReplaySessionSnapshotConflictError(
                        "replay session snapshot pin does not match the fenced lease"
                    )
                if (current.organization_id, current.workspace_id) != (
                    organization_id,
                    workspace_id,
                ):
                    raise ReplaySessionScopeConflictError(
                        "replay session organization/workspace pin does not match "
                        "the fenced lease"
                    )
                await cursor.execute(
                    f"""
                        UPDATE {REPLAY_SESSION_LEASE_TABLE}
                        SET worker_id = %s,
                            fencing_epoch = fencing_epoch + 1,
                            lease_token = %s,
                            lease_expires_at = %s,
                            updated_at = clock_timestamp()
                        WHERE session_id = %s
                        {_RETURNING_SQL}
                        """,
                    (worker_id, token, expires_at, session_id),
                )
            acquired = await cursor.fetchone()
            if acquired is None:  # pragma: no cover - database invariant
                raise RuntimeError(
                    "PostgreSQL did not return the acquired replay lease"
                )
            return _row_to_lease(acquired)

    async def renew(
        self,
        lease: ReplaySessionLease,
        *,
        lease_ttl_ms: int,
    ) -> ReplaySessionLease:
        _require_lease(lease)
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_session(cursor, lease.session_id)
            _, now = await self._active_row(cursor, lease)
            expires_at = now + timedelta(milliseconds=lease_ttl_ms)
            await cursor.execute(
                f"""
                    UPDATE {REPLAY_SESSION_LEASE_TABLE}
                    SET lease_expires_at = %s,
                        updated_at = clock_timestamp()
                    WHERE session_id = %s
                    {_RETURNING_SQL}
                    """,
                (expires_at, lease.session_id),
            )
            renewed = await cursor.fetchone()
            if renewed is None:  # pragma: no cover - locked row cannot vanish
                raise RuntimeError("PostgreSQL did not return the renewed replay lease")
            return _row_to_lease(renewed)

    async def release(self, lease: ReplaySessionLease) -> None:
        _require_lease(lease)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_session(cursor, lease.session_id)
            await cursor.execute(_SELECT_FOR_UPDATE_SQL, (lease.session_id,))
            row = await cursor.fetchone()
            if row is None:
                raise ReplaySessionLeaseFencedError(
                    "replay session lease is stale or no longer owned"
                )
            require_write_fence(_row_to_lease(row), lease)
            await cursor.execute(
                f"""
                    UPDATE {REPLAY_SESSION_LEASE_TABLE}
                    SET lease_expires_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE session_id = %s
                    """,
                (lease.session_id,),
            )

    async def require_active(self, lease: ReplaySessionLease) -> ReplaySessionLease:
        _require_lease(lease)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_session(cursor, lease.session_id)
            row, _now = await self._active_row(cursor, lease)
            return _row_to_lease(row)

    async def inspect(self, session_id: str) -> ReplaySessionLease | None:
        session_id = normalize_replay_session_id(session_id)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                    SELECT {_SELECT_COLUMNS}
                    FROM {REPLAY_SESSION_LEASE_TABLE}
                    WHERE session_id = %s
                    """,
                (session_id,),
            )
            row = await cursor.fetchone()
            return None if row is None else _row_to_lease(row)

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        return await psycopg.AsyncConnection.connect(
            self._dsn,
            row_factory=dict_row,
        )

    async def _active_row(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
        lease: ReplaySessionLease,
    ) -> tuple[dict[str, Any], datetime]:
        now = await _database_now(cursor)
        await cursor.execute(_SELECT_FOR_UPDATE_SQL, (lease.session_id,))
        row = await cursor.fetchone()
        if row is None:
            raise ReplaySessionLeaseFencedError(
                "replay session lease is stale or no longer owned"
            )
        require_write_fence(_row_to_lease(row), lease)
        if row["lease_expires_at"] <= now:
            raise ReplaySessionLeaseFencedError("replay session lease has expired")
        return row, now


async def _lock_session(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
    session_id: str,
) -> None:
    await cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s))",
        (f"{REPLAY_SESSION_LOCK_PREFIX}{session_id}",),
    )


async def _database_now(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
) -> datetime:
    await cursor.execute("SELECT clock_timestamp() AS now")
    row = await cursor.fetchone()
    if row is None:  # pragma: no cover - PostgreSQL always returns one row
        raise RuntimeError("PostgreSQL did not return its clock")
    return row["now"]


def _row_to_lease(row: dict[str, Any]) -> ReplaySessionLease:
    expires_at: datetime = row["lease_expires_at"]
    return ReplaySessionLease(
        session_id=row["session_id"],
        worker_id=row["worker_id"],
        fencing_epoch=int(row["fencing_epoch"]),
        lease_token=str(row["lease_token"]),
        lease_expires_at_ms=int(expires_at.timestamp() * 1000),
        snapshot=MarketDataSnapshotRef(
            data_epoch=row["data_epoch"],
            snapshot_version=int(row["snapshot_version"]),
            manifest_uri=row["manifest_uri"],
            manifest_sha256=row["manifest_sha256"],
        ),
        organization_id=row["organization_id"],
        workspace_id=row["workspace_id"],
    )


def _require_lease(lease: ReplaySessionLease) -> None:
    if not isinstance(lease, ReplaySessionLease):
        raise TypeError("lease must be a ReplaySessionLease")


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value
