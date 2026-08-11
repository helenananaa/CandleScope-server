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
from psycopg import IsolationLevel
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
from app.server_runtime.query_migrations import (
    QUERY_AUDITOR_GROUP_ROLE,
    QUERY_CONTROL_MIGRATION_NAME,
    QUERY_CONTROL_MIGRATION_SHA256,
    QUERY_CONTROL_MIGRATION_TABLE,
    QUERY_CONTROL_MIGRATION_VERSION,
    QUERY_RUNTIME_GROUP_ROLE,
)
from app.server_runtime.query_security import QueryAuditEvent

QUERY_AUDIT_EVENT_TABLE = "candlescope_query_audit_event"
QUERY_AUDIT_HEAD_TABLE = "candlescope_query_audit_head"
HOT_PROJECTION_QUARANTINE_TABLE = "candlescope_query_hot_quarantine"
ZERO_AUDIT_HASH = "0" * 64


@dataclass(frozen=True, slots=True)
class QueryAuditChainVerification:
    record_count: int
    head_audit_sequence: int
    head_event_hash: str
    head_updated_at_ms: int
    migration_version: int
    migration_sha256: str


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
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await _validate_current_migration(cursor)
                await _validate_runtime_privileges(cursor)
                await cursor.execute(
                    f"""
                    SELECT 1 AS present
                    FROM {HOT_PROJECTION_QUARANTINE_TABLE}
                    WHERE backend_id = %s
                    """,
                    (self._backend_id,),
                )
                if await cursor.fetchone() is None:
                    raise QueryControlUnavailableError(
                        "configured hot backend was not seeded by migration"
                    )
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL query control validation failed"
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
        return await _connect(
            self._dsn,
            application_name=f"candlescope-query-control:{self._instance_id}",
            connect_timeout_ms=self._connect_timeout_ms,
            request_timeout_ms=self._request_timeout_ms,
        )

    def _require_started(self) -> None:
        if not self._started:
            raise QueryControlUnavailableError("query control store is not started")


class PostgresQueryAuditVerifier:
    """Read and verify audit state through the dedicated read-only role."""

    def __init__(
        self,
        dsn: str,
        *,
        verifier_id: str,
        connect_timeout_ms: int = 5_000,
        request_timeout_ms: int = 10_000,
    ) -> None:
        self._dsn = _required_text(dsn, field="dsn", max_length=4_096)
        self._verifier_id = _required_text(
            verifier_id,
            field="verifier_id",
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
            raise QueryControlError("query audit verifier is already started")
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                await _validate_current_migration(cursor)
                await _validate_auditor_privileges(cursor)
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL query audit verifier validation failed"
            ) from exc
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def verify_audit_chain(
        self,
        *,
        max_records: int = 10_000,
    ) -> QueryAuditChainVerification:
        self._require_started()
        max_records = _positive_int(max_records, field="max_records")
        try:
            async with (
                await self._connect() as connection,
                connection.cursor() as cursor,
            ):
                migration = await _validate_current_migration(cursor)
                await cursor.execute(
                    f"""
                    SELECT last_audit_sequence, last_event_hash, updated_at
                    FROM {QUERY_AUDIT_HEAD_TABLE}
                    WHERE singleton = TRUE
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
        if head is None:  # pragma: no cover - migration invariant
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
            head_updated_at_ms=_required_timestamp_ms(head["updated_at"]),
            migration_version=migration["version"],
            migration_sha256=migration["sql_sha256"],
        )

    async def quarantine_status(
        self,
        backend_id: str = DEFAULT_HOT_BACKEND_ID,
    ) -> HotProjectionControlState:
        self._require_started()
        backend_id = _required_text(backend_id, field="backend_id", max_length=128)
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
                    (backend_id,),
                )
                row = await cursor.fetchone()
        except psycopg.Error as exc:
            raise QueryControlUnavailableError(
                "PostgreSQL quarantine audit state could not be read"
            ) from exc
        if row is None:
            raise QueryControlError("quarantine audit state is missing")
        try:
            return _state_from_row(row)
        except (TypeError, ValueError) as exc:
            raise QueryControlError("quarantine audit state is invalid") from exc

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        return await _connect(
            self._dsn,
            application_name=f"candlescope-query-auditor:{self._verifier_id}",
            connect_timeout_ms=self._connect_timeout_ms,
            request_timeout_ms=self._request_timeout_ms,
            read_only=True,
            repeatable_read=True,
        )

    def _require_started(self) -> None:
        if not self._started:
            raise QueryControlUnavailableError("query audit verifier is not started")


async def _validate_current_migration(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
) -> dict[str, Any]:
    await cursor.execute(
        f"""
        SELECT version, name, sql_sha256
        FROM {QUERY_CONTROL_MIGRATION_TABLE}
        ORDER BY version DESC
        LIMIT 1
        """
    )
    migration = await cursor.fetchone()
    if migration is None or (
        migration["version"] != QUERY_CONTROL_MIGRATION_VERSION
        or migration["name"] != QUERY_CONTROL_MIGRATION_NAME
        or migration["sql_sha256"] != QUERY_CONTROL_MIGRATION_SHA256
    ):
        raise QueryControlUnavailableError(
            "query-control schema migration is missing or has drifted"
        )
    return migration


async def _validate_runtime_privileges(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
) -> None:
    privileges = await _read_privileges(cursor)
    _require_privileges(
        privileges,
        {
            "member_runtime": True,
            "member_auditor": False,
            "schema_usage": True,
            "schema_create": False,
            "migration_select": True,
            "audit_select": False,
            "audit_sequence_select": True,
            "audit_body_select": False,
            "audit_insert": False,
            "audit_event_id_insert": True,
            "audit_schema_version_insert": True,
            "audit_body_insert": True,
            "audit_previous_hash_insert": True,
            "audit_event_hash_insert": True,
            "audit_sequence_insert": False,
            "audit_recorded_at_insert": False,
            "audit_update": False,
            "audit_delete": False,
            "audit_truncate": False,
            "audit_sequence_usage": True,
            "head_select": True,
            "head_insert": False,
            "head_update": True,
            "head_delete": False,
            "head_truncate": False,
            "quarantine_select": True,
            "quarantine_insert": False,
            "quarantine_update": True,
            "quarantine_delete": False,
            "quarantine_truncate": False,
        },
        role_label="runtime",
    )


async def _validate_auditor_privileges(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
) -> None:
    privileges = await _read_privileges(cursor)
    _require_privileges(
        privileges,
        {
            "member_runtime": False,
            "member_auditor": True,
            "schema_usage": True,
            "schema_create": False,
            "migration_select": True,
            "audit_select": True,
            "audit_sequence_select": True,
            "audit_body_select": True,
            "audit_insert": False,
            "audit_event_id_insert": False,
            "audit_schema_version_insert": False,
            "audit_body_insert": False,
            "audit_previous_hash_insert": False,
            "audit_event_hash_insert": False,
            "audit_sequence_insert": False,
            "audit_recorded_at_insert": False,
            "audit_update": False,
            "audit_delete": False,
            "audit_truncate": False,
            "audit_sequence_usage": False,
            "head_select": True,
            "head_insert": False,
            "head_update": False,
            "head_delete": False,
            "head_truncate": False,
            "quarantine_select": True,
            "quarantine_insert": False,
            "quarantine_update": False,
            "quarantine_delete": False,
            "quarantine_truncate": False,
        },
        role_label="auditor",
    )


async def _read_privileges(
    cursor: psycopg.AsyncCursor[dict[str, Any]],
) -> dict[str, Any]:
    await cursor.execute(
        """
        SELECT r.rolsuper, r.rolcreatedb, r.rolcreaterole,
               r.rolreplication, r.rolbypassrls,
               pg_has_role(current_user, %s, 'member') AS member_runtime,
               pg_has_role(current_user, %s, 'member') AS member_auditor,
               has_schema_privilege(current_user, 'public', 'USAGE')
                   AS schema_usage,
               has_schema_privilege(current_user, 'public', 'CREATE')
                   AS schema_create,
               has_table_privilege(current_user, %s, 'SELECT')
                   AS migration_select,
               has_table_privilege(current_user, %s, 'SELECT') AS audit_select,
               has_column_privilege(current_user, %s, 'audit_sequence', 'SELECT')
                   AS audit_sequence_select,
               has_column_privilege(current_user, %s, 'event_json', 'SELECT')
                   AS audit_body_select,
               has_table_privilege(current_user, %s, 'INSERT') AS audit_insert,
               has_column_privilege(current_user, %s, 'event_id', 'INSERT')
                   AS audit_event_id_insert,
               has_column_privilege(current_user, %s, 'schema_version', 'INSERT')
                   AS audit_schema_version_insert,
               has_column_privilege(current_user, %s, 'event_json', 'INSERT')
                   AS audit_body_insert,
               has_column_privilege(current_user, %s, 'previous_hash', 'INSERT')
                   AS audit_previous_hash_insert,
               has_column_privilege(current_user, %s, 'event_hash', 'INSERT')
                   AS audit_event_hash_insert,
               has_column_privilege(current_user, %s, 'audit_sequence', 'INSERT')
                   AS audit_sequence_insert,
               has_column_privilege(current_user, %s, 'recorded_at', 'INSERT')
                   AS audit_recorded_at_insert,
               has_table_privilege(current_user, %s, 'UPDATE') AS audit_update,
               has_table_privilege(current_user, %s, 'DELETE') AS audit_delete,
               has_table_privilege(current_user, %s, 'TRUNCATE') AS audit_truncate,
               has_sequence_privilege(
                   current_user,
                   'candlescope_query_audit_event_audit_sequence_seq',
                   'USAGE'
               ) AS audit_sequence_usage,
               has_table_privilege(current_user, %s, 'SELECT') AS head_select,
               has_table_privilege(current_user, %s, 'INSERT') AS head_insert,
               has_table_privilege(current_user, %s, 'UPDATE') AS head_update,
               has_table_privilege(current_user, %s, 'DELETE') AS head_delete,
               has_table_privilege(current_user, %s, 'TRUNCATE') AS head_truncate,
               has_table_privilege(current_user, %s, 'SELECT')
                   AS quarantine_select,
               has_table_privilege(current_user, %s, 'INSERT')
                   AS quarantine_insert,
               has_table_privilege(current_user, %s, 'UPDATE')
                   AS quarantine_update,
               has_table_privilege(current_user, %s, 'DELETE')
                   AS quarantine_delete,
               has_table_privilege(current_user, %s, 'TRUNCATE')
                   AS quarantine_truncate
        FROM pg_roles AS r
        WHERE r.rolname = current_user
        """,
        (
            QUERY_RUNTIME_GROUP_ROLE,
            QUERY_AUDITOR_GROUP_ROLE,
            QUERY_CONTROL_MIGRATION_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
        ),
    )
    privileges = await cursor.fetchone()
    if privileges is None:  # pragma: no cover - current_user always exists
        raise QueryControlUnavailableError("current PostgreSQL role is missing")
    if any(
        privileges[field]
        for field in (
            "rolsuper",
            "rolcreatedb",
            "rolcreaterole",
            "rolreplication",
            "rolbypassrls",
        )
    ):
        raise QueryControlUnavailableError("query-control PostgreSQL role is elevated")
    return privileges


def _require_privileges(
    actual: dict[str, Any],
    expected: dict[str, bool],
    *,
    role_label: str,
) -> None:
    drift = sorted(
        name
        for name, expected_value in expected.items()
        if actual.get(name) is not expected_value
    )
    if drift:
        raise QueryControlUnavailableError(
            f"{role_label} PostgreSQL privilege contract drifted: {', '.join(drift)}"
        )


async def _connect(
    dsn: str,
    *,
    application_name: str,
    connect_timeout_ms: int,
    request_timeout_ms: int,
    read_only: bool = False,
    repeatable_read: bool = False,
) -> psycopg.AsyncConnection[dict[str, Any]]:
    connection = await psycopg.AsyncConnection.connect(
        dsn,
        row_factory=dict_row,
        connect_timeout=max(1, math.ceil(connect_timeout_ms / 1_000)),
        application_name=application_name,
    )
    try:
        if read_only:
            await connection.set_read_only(True)
        if repeatable_read:
            await connection.set_isolation_level(IsolationLevel.REPEATABLE_READ)
        timeout = str(request_timeout_ms)
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


def _required_timestamp_ms(value: object) -> int:
    if not isinstance(value, datetime):
        raise QueryControlError("audit chain head timestamp is invalid")
    return int(value.timestamp() * 1_000)


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
