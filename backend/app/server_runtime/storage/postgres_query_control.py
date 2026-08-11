"""PostgreSQL query audit chain and shared hot-projection quarantine."""

from __future__ import annotations

import hashlib
import math
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
import rfc8785
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.server_runtime.query_control import (
    DEFAULT_HOT_BACKEND_ID,
    ClearHotProjectionQuarantineCommand,
    HotProjectionControlState,
    HotProjectionGenerationConflictError,
    HotProjectionNotQuarantinedError,
    QueryControlError,
    QueryControlUnavailableError,
)
from app.server_runtime.query_security import QueryAuditEvent

QUERY_AUDIT_EVENT_TABLE = "candlescope_query_audit_event"
QUERY_AUDIT_HEAD_TABLE = "candlescope_query_audit_head"
HOT_PROJECTION_QUARANTINE_TABLE = "candlescope_query_hot_quarantine"
ZERO_AUDIT_HASH = "0" * 64

CREATE_QUERY_AUDIT_HEAD_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {QUERY_AUDIT_HEAD_TABLE} (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_audit_sequence BIGINT NOT NULL CHECK (last_audit_sequence >= 0),
    last_event_hash TEXT NOT NULL CHECK (last_event_hash ~ '^[0-9a-f]{{64}}$'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
"""

CREATE_QUERY_AUDIT_EVENT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {QUERY_AUDIT_EVENT_TABLE} (
    audit_sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    schema_version TEXT NOT NULL,
    event_json JSONB NOT NULL,
    previous_hash TEXT NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{{64}}$'),
    event_hash TEXT NOT NULL UNIQUE CHECK (event_hash ~ '^[0-9a-f]{{64}}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
"""

CREATE_HOT_PROJECTION_QUARANTINE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {HOT_PROJECTION_QUARANTINE_TABLE} (
    backend_id TEXT PRIMARY KEY CHECK (btrim(backend_id) <> ''),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT NULL,
    latched_by TEXT NULL,
    latched_at TIMESTAMPTZ NULL,
    cleared_by TEXT NULL,
    clear_reason_code TEXT NULL,
    cleared_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (generation = 0 AND reason IS NULL AND latched_by IS NULL
         AND latched_at IS NULL AND cleared_by IS NULL
         AND clear_reason_code IS NULL AND cleared_at IS NULL)
        OR generation > 0
    ),
    CHECK (
        NOT active
        OR (reason IS NOT NULL AND latched_by IS NOT NULL AND latched_at IS NOT NULL)
    )
)
"""


@dataclass(frozen=True, slots=True)
class QueryAuditChainVerification:
    record_count: int
    head_audit_sequence: int
    head_event_hash: str


class PostgresQueryControlStore:
    """Serialize durable audit and quarantine transitions in PostgreSQL."""

    def __init__(
        self,
        dsn: str,
        *,
        instance_id: str,
        backend_id: str = DEFAULT_HOT_BACKEND_ID,
        connect_timeout_ms: int = 5_000,
        request_timeout_ms: int = 5_000,
    ) -> None:
        self._dsn = _required_text(dsn, field="dsn", max_length=4_096)
        self._instance_id = _required_text(
            instance_id,
            field="instance_id",
            max_length=128,
        )
        self._backend_id = _required_text(
            backend_id,
            field="backend_id",
            max_length=128,
        )
        self._connect_timeout_ms = _positive_int(
            connect_timeout_ms,
            field="connect_timeout_ms",
        )
        self._request_timeout_ms = _positive_int(
            request_timeout_ms,
            field="request_timeout_ms",
        )
        self._started = False

    async def start(self) -> None:
        if self._started:
            raise QueryControlError("query control store is already started")
        try:
            async with await self._connect() as connection:
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    ("candlescope-query-control-schema-v1",),
                )
                await connection.execute(CREATE_QUERY_AUDIT_HEAD_TABLE_SQL)
                await connection.execute(CREATE_QUERY_AUDIT_EVENT_TABLE_SQL)
                await connection.execute(CREATE_HOT_PROJECTION_QUARANTINE_TABLE_SQL)
                await connection.execute(
                    f"""
                    INSERT INTO {QUERY_AUDIT_HEAD_TABLE} (
                        singleton, last_audit_sequence, last_event_hash
                    ) VALUES (TRUE, 0, %s)
                    ON CONFLICT (singleton) DO NOTHING
                    """,
                    (ZERO_AUDIT_HASH,),
                )
                await connection.execute(
                    f"""
                    INSERT INTO {HOT_PROJECTION_QUARANTINE_TABLE} (backend_id)
                    VALUES (%s)
                    ON CONFLICT (backend_id) DO NOTHING
                    """,
                    (self._backend_id,),
                )
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL query control schema is unavailable"
            ) from exc
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def emit(self, event: QueryAuditEvent) -> None:
        self._require_started()
        if not isinstance(event, QueryAuditEvent):
            raise TypeError("event must be a QueryAuditEvent")
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await self._append_audit(cursor, event)
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL query audit is unavailable"
            ) from exc

    async def status(self) -> HotProjectionControlState:
        self._require_started()
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await cursor.execute(
                    f"""
                    SELECT backend_id, generation, active, reason, latched_by,
                           latched_at, cleared_by, clear_reason_code, cleared_at
                    FROM {HOT_PROJECTION_QUARANTINE_TABLE}
                    WHERE backend_id = %s
                    """,
                    (self._backend_id,),
                )
                row = await cursor.fetchone()
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL quarantine state is unavailable"
            ) from exc
        if row is None:  # pragma: no cover - initialized schema invariant
            raise QueryControlUnavailableError("quarantine state row is missing")
        try:
            return _state_from_row(row)
        except (TypeError, ValueError) as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL quarantine state is invalid"
            ) from exc

    async def latch(self, reason: str) -> HotProjectionControlState:
        self._require_started()
        reason = _required_text(reason, field="reason", max_length=256)
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                row = await self._locked_state(cursor)
                current = _state_from_row(row)
                if current.active:
                    return current
                await cursor.execute(
                    f"""
                    UPDATE {HOT_PROJECTION_QUARANTINE_TABLE}
                    SET generation = generation + 1,
                        active = TRUE,
                        reason = %s,
                        latched_by = %s,
                        latched_at = clock_timestamp(),
                        cleared_by = NULL,
                        clear_reason_code = NULL,
                        cleared_at = NULL,
                        updated_at = clock_timestamp()
                    WHERE backend_id = %s
                    RETURNING backend_id, generation, active, reason, latched_by,
                              latched_at, cleared_by, clear_reason_code, cleared_at
                    """,
                    (reason, self._instance_id, self._backend_id),
                )
                latched_row = await cursor.fetchone()
                if latched_row is None:  # pragma: no cover - locked row invariant
                    raise QueryControlUnavailableError(
                        "PostgreSQL did not return the latched quarantine"
                    )
                state = _state_from_row(latched_row)
                await self._append_audit(
                    cursor,
                    QueryAuditEvent(
                        request_id=f"quarantine:{uuid.uuid4()}",
                        principal=self._instance_id,
                        action="latch_hot_projection_quarantine",
                        outcome="quarantine_latched",
                        status_code=503,
                        timestamp_ms=time.time_ns() // 1_000_000,
                        latency_ms=0,
                        backend=self._backend_id,
                        control_generation=state.generation,
                        reason_code="hot_cold_parity_mismatch",
                    ),
                )
                return state
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL could not latch quarantine"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL returned invalid quarantine state"
            ) from exc

    async def clear(
        self,
        command: ClearHotProjectionQuarantineCommand,
    ) -> HotProjectionControlState:
        self._require_started()
        if not isinstance(command, ClearHotProjectionQuarantineCommand):
            raise TypeError("command must be a ClearHotProjectionQuarantineCommand")
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                row = await self._locked_state(cursor)
                current = _state_from_row(row)
                if not current.active:
                    raise HotProjectionNotQuarantinedError(
                        "the hot projection is not quarantined"
                    )
                if current.generation != command.expected_generation:
                    raise HotProjectionGenerationConflictError(
                        "the quarantine generation changed before clear"
                    )
                await cursor.execute(
                    f"""
                    UPDATE {HOT_PROJECTION_QUARANTINE_TABLE}
                    SET active = FALSE,
                        cleared_by = %s,
                        clear_reason_code = %s,
                        cleared_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    WHERE backend_id = %s
                    RETURNING backend_id, generation, active, reason, latched_by,
                              latched_at, cleared_by, clear_reason_code, cleared_at
                    """,
                    (
                        command.principal,
                        command.reason_code,
                        self._backend_id,
                    ),
                )
                cleared_row = await cursor.fetchone()
                if cleared_row is None:  # pragma: no cover - locked row invariant
                    raise QueryControlUnavailableError(
                        "PostgreSQL did not return the cleared quarantine"
                    )
                state = _state_from_row(cleared_row)
                await self._append_audit(
                    cursor,
                    QueryAuditEvent(
                        request_id=command.request_id,
                        principal=command.principal,
                        action="clear_hot_projection_quarantine",
                        outcome="quarantine_cleared",
                        status_code=200,
                        timestamp_ms=command.timestamp_ms,
                        latency_ms=0,
                        backend=self._backend_id,
                        control_generation=state.generation,
                        reason_code=command.reason_code,
                    ),
                )
                return state
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL could not clear quarantine"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL returned invalid quarantine state"
            ) from exc

    async def verify_audit_chain(
        self,
        *,
        max_records: int = 10_000,
    ) -> QueryAuditChainVerification:
        """Verify the bounded stored chain against its serialized event bodies."""

        self._require_started()
        max_records = _positive_int(max_records, field="max_records")
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await cursor.execute(
                    f"""
                    SELECT last_audit_sequence, last_event_hash
                    FROM {QUERY_AUDIT_HEAD_TABLE}
                    WHERE singleton = TRUE
                    FOR SHARE
                    """
                )
                head = await cursor.fetchone()
                await cursor.execute(
                    f"""
                    SELECT audit_sequence, event_json, previous_hash, event_hash
                    FROM {QUERY_AUDIT_EVENT_TABLE}
                    ORDER BY audit_sequence ASC
                    LIMIT %s
                    """,
                    (max_records + 1,),
                )
                records = await cursor.fetchall()
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL audit chain could not be read"
            ) from exc
        if len(records) > max_records:
            raise QueryControlError("audit chain exceeds the verification bound")
        if head is None:  # pragma: no cover - initialized schema invariant
            raise QueryControlError("audit chain head is missing")
        previous_hash = ZERO_AUDIT_HASH
        last_sequence = 0
        for record in records:
            if record["previous_hash"] != previous_hash:
                raise QueryControlError("audit chain previous hash mismatch")
            try:
                expected = _event_hash(previous_hash, record["event_json"])
            except (TypeError, ValueError) as exc:
                raise QueryControlError(
                    "audit event body cannot be canonicalized"
                ) from exc
            if record["event_hash"] != expected:
                raise QueryControlError("audit chain event hash mismatch")
            previous_hash = expected
            last_sequence = record["audit_sequence"]
        if (
            head["last_audit_sequence"] != last_sequence
            or head["last_event_hash"] != previous_hash
        ):
            raise QueryControlError("audit chain head does not match the event tail")
        return QueryAuditChainVerification(
            record_count=len(records),
            head_audit_sequence=last_sequence,
            head_event_hash=previous_hash,
        )

    async def _append_audit(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
        event: QueryAuditEvent,
    ) -> None:
        await cursor.execute(
            f"""
            SELECT last_audit_sequence, last_event_hash
            FROM {QUERY_AUDIT_HEAD_TABLE}
            WHERE singleton = TRUE
            FOR UPDATE
            """
        )
        head = await cursor.fetchone()
        if head is None:  # pragma: no cover - initialized schema invariant
            raise QueryControlUnavailableError("audit chain head is missing")
        wire = event.to_wire()
        previous_hash = head["last_event_hash"]
        event_hash = _event_hash(previous_hash, wire)
        await cursor.execute(
            f"""
            INSERT INTO {QUERY_AUDIT_EVENT_TABLE} (
                event_id, schema_version, event_json, previous_hash, event_hash
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING audit_sequence
            """,
            (
                uuid.UUID(event.event_id),
                event.schema_version,
                Jsonb(wire),
                previous_hash,
                event_hash,
            ),
        )
        inserted = await cursor.fetchone()
        if inserted is None:  # pragma: no cover - PostgreSQL insert invariant
            raise QueryControlUnavailableError("audit event was not inserted")
        await cursor.execute(
            f"""
            UPDATE {QUERY_AUDIT_HEAD_TABLE}
            SET last_audit_sequence = %s,
                last_event_hash = %s,
                updated_at = clock_timestamp()
            WHERE singleton = TRUE
            """,
            (inserted["audit_sequence"], event_hash),
        )

    async def _locked_state(
        self,
        cursor: psycopg.AsyncCursor[dict[str, Any]],
    ) -> dict[str, Any]:
        await cursor.execute(
            f"""
            SELECT backend_id, generation, active, reason, latched_by,
                   latched_at, cleared_by, clear_reason_code, cleared_at
            FROM {HOT_PROJECTION_QUARANTINE_TABLE}
            WHERE backend_id = %s
            FOR UPDATE
            """,
            (self._backend_id,),
        )
        row = await cursor.fetchone()
        if row is None:  # pragma: no cover - initialized schema invariant
            raise QueryControlUnavailableError("quarantine state row is missing")
        return row

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        connection = await psycopg.AsyncConnection.connect(
            self._dsn,
            row_factory=dict_row,
            connect_timeout=max(1, math.ceil(self._connect_timeout_ms / 1_000)),
            application_name=f"candlescope-query-control:{self._instance_id}",
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
            await connection.execute(
                "SELECT set_config('idle_in_transaction_session_timeout', %s, false)",
                (timeout,),
            )
        except BaseException:
            await connection.close()
            raise
        return connection

    def _require_started(self) -> None:
        if not self._started:
            raise QueryControlUnavailableError("query control store is not started")


def _state_from_row(row: dict[str, Any]) -> HotProjectionControlState:
    return HotProjectionControlState(
        backend_id=row["backend_id"],
        generation=row["generation"],
        active=row["active"],
        reason=row["reason"],
        latched_by=row["latched_by"],
        latched_at_ms=_timestamp_ms(row["latched_at"]),
        cleared_by=row["cleared_by"],
        clear_reason_code=row["clear_reason_code"],
        cleared_at_ms=_timestamp_ms(row["cleared_at"]),
    )


def _timestamp_ms(value: datetime | None) -> int | None:
    return None if value is None else int(value.timestamp() * 1_000)


def _event_hash(previous_hash: str, wire: dict[str, object]) -> str:
    material = bytes.fromhex(previous_hash) + rfc8785.dumps(wire)
    return hashlib.sha256(material).hexdigest()


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
