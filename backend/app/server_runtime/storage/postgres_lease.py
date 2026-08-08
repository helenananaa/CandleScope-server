"""PostgreSQL-backed stream lease, fencing, pending intent, and checkpoint."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.server_contracts import MarketEventEnvelopeV1
from app.server_runtime.leases import (
    StreamCheckpointError,
    StreamLease,
    StreamLeaseBusyError,
    StreamLeaseFencedError,
    require_same_envelope,
    require_stageable_envelope,
)

STREAM_LEASE_TABLE = "candlescope_market_stream_lease"

CREATE_STREAM_LEASE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {STREAM_LEASE_TABLE} (
    partition_key TEXT PRIMARY KEY CHECK (btrim(partition_key) <> ''),
    owner_id TEXT NOT NULL CHECK (btrim(owner_id) <> ''),
    producer_epoch BIGINT NOT NULL CHECK (producer_epoch >= 0),
    lease_token UUID NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    last_sequence BIGINT NULL CHECK (last_sequence >= 0),
    last_event_id UUID NULL,
    last_partition_offset BIGINT NULL CHECK (last_partition_offset >= 0),
    pending_envelope JSONB NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK ((last_sequence IS NULL) = (last_event_id IS NULL)),
    CHECK (last_sequence IS NOT NULL OR last_partition_offset IS NULL)
)
"""

_SELECT_FOR_UPDATE_SQL = f"""
SELECT partition_key, owner_id, producer_epoch, lease_token,
       lease_expires_at, last_sequence, last_event_id,
       last_partition_offset, pending_envelope
FROM {STREAM_LEASE_TABLE}
WHERE partition_key = %s
FOR UPDATE
"""


class PostgresStreamLeaseStore:
    """Use one short PostgreSQL transaction for every ownership transition."""

    def __init__(self, dsn: str) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be a non-blank PostgreSQL connection string")
        self._dsn = dsn.strip()

    async def initialize_schema(self) -> None:
        """Create the idempotent Phase 1B control-plane table."""

        async with await self._connect() as connection:
            await connection.execute(CREATE_STREAM_LEASE_TABLE_SQL)

    async def acquire(
        self,
        *,
        partition_key: str,
        owner_id: str,
        lease_ttl_ms: int,
    ) -> StreamLease:
        partition_key = _required_text(partition_key, field="partition_key")
        owner_id = _required_text(owner_id, field="owner_id")
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        token = uuid.uuid4()
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_partition(cursor, partition_key)
            now = await _database_now(cursor)
            await cursor.execute(_SELECT_FOR_UPDATE_SQL, (partition_key,))
            row = await cursor.fetchone()
            expires_at = now + timedelta(milliseconds=lease_ttl_ms)
            if row is None:
                await cursor.execute(
                    f"""
                        INSERT INTO {STREAM_LEASE_TABLE} (
                            partition_key, owner_id, producer_epoch,
                            lease_token, lease_expires_at
                        ) VALUES (%s, %s, 0, %s, %s)
                        RETURNING partition_key, owner_id, producer_epoch,
                                  lease_token, lease_expires_at, last_sequence,
                                  last_event_id, last_partition_offset,
                                  pending_envelope
                        """,
                    (partition_key, owner_id, token, expires_at),
                )
            else:
                if row["lease_expires_at"] > now:
                    raise StreamLeaseBusyError(
                        f"stream {partition_key!r} is leased by {row['owner_id']!r}"
                    )
                await cursor.execute(
                    f"""
                        UPDATE {STREAM_LEASE_TABLE}
                        SET owner_id = %s,
                            producer_epoch = producer_epoch + 1,
                            lease_token = %s,
                            lease_expires_at = %s,
                            updated_at = clock_timestamp()
                        WHERE partition_key = %s
                        RETURNING partition_key, owner_id, producer_epoch,
                                  lease_token, lease_expires_at, last_sequence,
                                  last_event_id, last_partition_offset,
                                  pending_envelope
                        """,
                    (owner_id, token, expires_at, partition_key),
                )
            acquired = await cursor.fetchone()
            if acquired is None:  # pragma: no cover - database invariant
                raise RuntimeError("PostgreSQL did not return the acquired lease")
            return _row_to_lease(acquired)

    async def renew(
        self,
        lease: StreamLease,
        *,
        lease_ttl_ms: int,
    ) -> StreamLease:
        _require_lease(lease)
        lease_ttl_ms = _positive_int(lease_ttl_ms, field="lease_ttl_ms")
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_partition(cursor, lease.partition_key)
            _, now = await self._active_row(cursor, lease)
            expires_at = now + timedelta(milliseconds=lease_ttl_ms)
            await cursor.execute(
                f"""
                    UPDATE {STREAM_LEASE_TABLE}
                    SET lease_expires_at = %s,
                        updated_at = clock_timestamp()
                    WHERE partition_key = %s
                    RETURNING partition_key, owner_id, producer_epoch,
                              lease_token, lease_expires_at, last_sequence,
                              last_event_id, last_partition_offset,
                              pending_envelope
                    """,
                (expires_at, lease.partition_key),
            )
            renewed = await cursor.fetchone()
            if renewed is None:  # pragma: no cover - locked row cannot vanish
                raise RuntimeError("PostgreSQL did not return the renewed lease")
            return _row_to_lease(renewed)

    async def stage_pending(
        self,
        lease: StreamLease,
        envelope: MarketEventEnvelopeV1,
    ) -> StreamLease:
        _require_lease(lease)
        if not isinstance(envelope, MarketEventEnvelopeV1):
            raise TypeError("envelope must be a MarketEventEnvelopeV1")
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_partition(cursor, lease.partition_key)
            row, _ = await self._active_row(cursor, lease)
            current = _row_to_lease(row)
            require_stageable_envelope(current, envelope)
            if current.pending_envelope is not None:
                require_same_envelope(current.pending_envelope, envelope)
                return current
            await cursor.execute(
                f"""
                    UPDATE {STREAM_LEASE_TABLE}
                    SET pending_envelope = %s,
                        updated_at = clock_timestamp()
                    WHERE partition_key = %s
                    RETURNING partition_key, owner_id, producer_epoch,
                              lease_token, lease_expires_at, last_sequence,
                              last_event_id, last_partition_offset,
                              pending_envelope
                    """,
                (Jsonb(envelope.to_wire()), lease.partition_key),
            )
            staged = await cursor.fetchone()
            if staged is None:  # pragma: no cover - locked row cannot vanish
                raise RuntimeError("PostgreSQL did not return the staged lease")
            return _row_to_lease(staged)

    async def checkpoint(
        self,
        lease: StreamLease,
        envelope: MarketEventEnvelopeV1,
        *,
        partition_offset: int,
    ) -> StreamLease:
        _require_lease(lease)
        if not isinstance(envelope, MarketEventEnvelopeV1):
            raise TypeError("envelope must be a MarketEventEnvelopeV1")
        partition_offset = _non_negative_int(
            partition_offset,
            field="partition_offset",
        )
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_partition(cursor, lease.partition_key)
            row, _ = await self._active_row(cursor, lease)
            current = _row_to_lease(row)
            if (
                current.pending_envelope is None
                and current.last_sequence == envelope.sequence_end
                and current.last_event_id == envelope.event_id
                and current.last_partition_offset == partition_offset
            ):
                return current
            if current.pending_envelope is None:
                raise StreamCheckpointError(
                    "no staged event is available to checkpoint"
                )
            require_same_envelope(current.pending_envelope, envelope)
            if (
                current.last_partition_offset is not None
                and partition_offset <= current.last_partition_offset
            ):
                raise StreamCheckpointError(
                    "partition offset must advance beyond the durable checkpoint"
                )
            await cursor.execute(
                f"""
                    UPDATE {STREAM_LEASE_TABLE}
                    SET last_sequence = %s,
                        last_event_id = %s,
                        last_partition_offset = %s,
                        pending_envelope = NULL,
                        updated_at = clock_timestamp()
                    WHERE partition_key = %s
                    RETURNING partition_key, owner_id, producer_epoch,
                              lease_token, lease_expires_at, last_sequence,
                              last_event_id, last_partition_offset,
                              pending_envelope
                    """,
                (
                    envelope.sequence_end,
                    uuid.UUID(envelope.event_id),
                    partition_offset,
                    lease.partition_key,
                ),
            )
            checkpointed = await cursor.fetchone()
            if checkpointed is None:  # pragma: no cover
                raise RuntimeError("PostgreSQL did not return the checkpoint")
            return _row_to_lease(checkpointed)

    async def release(self, lease: StreamLease) -> None:
        _require_lease(lease)
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await _lock_partition(cursor, lease.partition_key)
            await cursor.execute(_SELECT_FOR_UPDATE_SQL, (lease.partition_key,))
            row = await cursor.fetchone()
            _require_fence(row, lease)
            await cursor.execute(
                f"""
                    UPDATE {STREAM_LEASE_TABLE}
                    SET lease_expires_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE partition_key = %s
                    """,
                (lease.partition_key,),
            )

    async def inspect(self, partition_key: str) -> StreamLease | None:
        """Read durable state for health checks and integration evidence."""

        partition_key = _required_text(partition_key, field="partition_key")
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                    SELECT partition_key, owner_id, producer_epoch, lease_token,
                           lease_expires_at, last_sequence, last_event_id,
                           last_partition_offset, pending_envelope
                    FROM {STREAM_LEASE_TABLE}
                    WHERE partition_key = %s
                    """,
                (partition_key,),
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
        lease: StreamLease,
    ) -> tuple[dict[str, Any], datetime]:
        now = await _database_now(cursor)
        await cursor.execute(_SELECT_FOR_UPDATE_SQL, (lease.partition_key,))
        row = await cursor.fetchone()
        _require_fence(row, lease)
        if row["lease_expires_at"] <= now:
            raise StreamLeaseFencedError("stream lease has expired")
        return row, now


async def _lock_partition(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
    partition_key: str,
) -> None:
    await cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s))",
        (partition_key,),
    )


async def _database_now(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
) -> datetime:
    await cursor.execute("SELECT clock_timestamp() AS now")
    row = await cursor.fetchone()
    if row is None:  # pragma: no cover - PostgreSQL always returns one row
        raise RuntimeError("PostgreSQL did not return its clock")
    return row["now"]


def _require_fence(row: dict[str, Any] | None, lease: StreamLease) -> None:
    if row is None or (
        row["owner_id"],
        row["producer_epoch"],
        str(row["lease_token"]),
    ) != (lease.owner_id, lease.producer_epoch, lease.lease_token):
        raise StreamLeaseFencedError("stream lease is stale or no longer owned")


def _row_to_lease(row: dict[str, Any]) -> StreamLease:
    pending_wire = row["pending_envelope"]
    pending = (
        None if pending_wire is None else MarketEventEnvelopeV1.from_wire(pending_wire)
    )
    expires_at: datetime = row["lease_expires_at"]
    return StreamLease(
        partition_key=row["partition_key"],
        owner_id=row["owner_id"],
        producer_epoch=row["producer_epoch"],
        lease_token=str(row["lease_token"]),
        lease_expires_at_ms=int(expires_at.timestamp() * 1000),
        last_sequence=row["last_sequence"],
        last_event_id=(
            None if row["last_event_id"] is None else str(row["last_event_id"])
        ),
        last_partition_offset=row["last_partition_offset"],
        pending_envelope=pending,
    )


def _require_lease(lease: StreamLease) -> None:
    if not isinstance(lease, StreamLease):
        raise TypeError("lease must be a StreamLease")


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _positive_int(value: object, *, field: str) -> int:
    value = _non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be positive")
    return value
