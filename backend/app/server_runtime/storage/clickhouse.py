"""ClickHouse fact projection and integrity-conflict quarantine for Phase 1D."""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import aiohttp
import rfc8785

from app.server_runtime.projection import (
    KafkaMarketEventRecord,
    ProjectionBatchResult,
    ProjectionIntegrityError,
    require_contiguous_records,
)

MARKET_EVENT_FACT_TABLE = "market_event_fact_v1"
MARKET_EVENT_CONFLICT_TABLE = "market_event_conflict_v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FACT_COLUMNS = (
    ("event_id", "UUID"),
    ("partition_key", "LowCardinality(String)"),
    ("exchange", "LowCardinality(String)"),
    ("market_type", "LowCardinality(String)"),
    ("symbol", "LowCardinality(String)"),
    ("channel", "LowCardinality(String)"),
    ("delivery_class", "LowCardinality(String)"),
    ("source", "LowCardinality(String)"),
    ("source_event_id", "Nullable(String)"),
    ("sequence_start", "Nullable(UInt64)"),
    ("sequence_end", "Nullable(UInt64)"),
    ("previous_sequence", "Nullable(UInt64)"),
    ("producer_id", "String"),
    ("producer_epoch", "UInt64"),
    ("event_time_ms", "UInt64"),
    ("received_at_ms", "UInt64"),
    ("published_at_ms", "UInt64"),
    ("payload_schema", "LowCardinality(String)"),
    ("payload_sha256", "FixedString(64)"),
    ("payload_json", "String"),
    ("envelope_sha256", "FixedString(64)"),
    ("envelope_json", "String"),
    ("first_kafka_topic", "LowCardinality(String)"),
    ("first_kafka_partition", "UInt16"),
    ("first_kafka_offset", "UInt64"),
    ("ingested_at_ms", "UInt64"),
)
_CONFLICT_COLUMNS = (
    ("event_id", "UUID"),
    ("partition_key", "LowCardinality(String)"),
    ("existing_envelope_sha256", "FixedString(64)"),
    ("incoming_envelope_sha256", "FixedString(64)"),
    ("existing_envelope_json", "String"),
    ("incoming_envelope_json", "String"),
    ("incoming_payload_sha256", "FixedString(64)"),
    ("kafka_topic", "LowCardinality(String)"),
    ("kafka_partition", "UInt16"),
    ("kafka_offset", "UInt64"),
    ("detected_at_ms", "UInt64"),
)


class ClickHouseProjectionError(RuntimeError):
    """Base class for fail-closed ClickHouse projection errors."""


class ClickHouseHttpError(ClickHouseProjectionError):
    """ClickHouse rejected a request or returned an invalid response."""


class ClickHouseSchemaError(ClickHouseProjectionError):
    """The required Phase 1D database schema is missing or drifted."""


@dataclass(frozen=True, slots=True)
class _StoredIdentity:
    envelope_sha256: str
    envelope_json: str
    first_kafka_offset: int


class ClickHouseMarketEventProjector:
    """Project validated records with replay-safe identity classification."""

    def __init__(
        self,
        *,
        url: str,
        database: str,
        user: str,
        password: str,
        request_timeout_ms: int = 10_000,
        session_factory: Any = aiohttp.ClientSession,
    ) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("url must be an absolute HTTP(S) ClickHouse endpoint")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("url cannot contain credentials, query, or fragment")
        self._url = url.rstrip("/")
        self._database = _identifier(database, field="database")
        self._user = _required_text(user, field="user")
        self._password = _required_text(password, field="password")
        self._request_timeout_ms = _positive_int(
            request_timeout_ms,
            field="request_timeout_ms",
        )
        self._session_factory = session_factory
        self._session: aiohttp.ClientSession | None = None

    @property
    def started(self) -> bool:
        return self._session is not None

    async def initialize_schema(self) -> None:
        """Create the idempotent Phase 1D fact and conflict tables."""

        temporary = self._session is None
        session = self._session or self._new_session()
        try:
            await self._execute(
                session,
                f"CREATE DATABASE IF NOT EXISTS `{self._database}`",
                database=None,
            )
            for statement in self._schema_statements():
                await self._execute(session, statement)
        finally:
            if temporary:
                await session.close()

    async def start(self) -> None:
        if self._session is not None:
            raise ClickHouseProjectionError("projector is already started")
        session = self._new_session()
        try:
            response = await self._execute(
                session,
                """
                SELECT count() AS table_count
                FROM system.tables
                WHERE database = {database:String}
                  AND name IN {tables:Array(String)}
                FORMAT JSONEachRow
                """,
                database=None,
                parameters={
                    "database": self._database,
                    "tables": [MARKET_EVENT_FACT_TABLE, MARKET_EVENT_CONFLICT_TABLE],
                },
            )
            rows = _json_each_row(response)
            if len(rows) != 1 or int(rows[0].get("table_count", -1)) != 2:
                raise ClickHouseSchemaError(
                    "Phase 1D tables are missing; run init-schema first"
                )
            await self._verify_schema(session)
        except BaseException:
            await session.close()
            raise
        self._session = session

    async def stop(self) -> None:
        session = self._session
        self._session = None
        if session is not None:
            await session.close()

    async def apply_batch(
        self,
        records: Sequence[KafkaMarketEventRecord],
    ) -> ProjectionBatchResult:
        batch = require_contiguous_records(records)
        session = self._require_session()
        identities: dict[tuple[str, str], _StoredIdentity | None] = {}
        facts: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        duplicate_count = 0

        loaded = await self._load_identities(
            session,
            partition_key=batch[0].envelope.partition_key,
            event_ids=tuple(
                dict.fromkeys(record.envelope.event_id for record in batch)
            ),
        )

        for record in batch:
            envelope = record.envelope
            identity_key = (envelope.partition_key, envelope.event_id)
            if identity_key not in identities:
                identities[identity_key] = loaded.get(envelope.event_id)
            existing = identities[identity_key]
            incoming_json = record.envelope_bytes.decode("utf-8")
            if existing is None:
                facts.append(_fact_row(record))
                identities[identity_key] = _StoredIdentity(
                    envelope_sha256=record.envelope_sha256,
                    envelope_json=incoming_json,
                    first_kafka_offset=record.offset,
                )
            elif (
                existing.envelope_sha256 == record.envelope_sha256
                and existing.envelope_json == incoming_json
            ):
                duplicate_count += 1
            else:
                conflicts.append(_conflict_row(record, existing))

        if facts:
            await self._insert_rows(
                session,
                table=MARKET_EVENT_FACT_TABLE,
                rows=facts,
                deduplication_token=_batch_token("facts", batch),
            )
        if conflicts:
            await self._insert_rows(
                session,
                table=MARKET_EVENT_CONFLICT_TABLE,
                rows=conflicts,
                deduplication_token=_batch_token("conflicts", batch),
            )
        return ProjectionBatchResult(
            inserted_count=len(facts),
            duplicate_count=duplicate_count,
            conflict_count=len(conflicts),
            first_offset=batch[0].offset,
            last_offset=batch[-1].offset,
        )

    async def _load_identities(
        self,
        session: aiohttp.ClientSession,
        *,
        partition_key: str,
        event_ids: Sequence[str],
    ) -> dict[str, _StoredIdentity]:
        response = await self._execute(
            session,
            f"""
            SELECT toString(event_id) AS event_id,
                   envelope_sha256, envelope_json,
                   min(first_kafka_offset) AS first_kafka_offset
            FROM `{self._database}`.`{MARKET_EVENT_FACT_TABLE}`
            WHERE partition_key = {{partition_key:String}}
              AND event_id IN {{event_ids:Array(UUID)}}
            GROUP BY event_id, envelope_sha256, envelope_json
            FORMAT JSONEachRow
            """,
            parameters={"partition_key": partition_key, "event_ids": list(event_ids)},
        )
        rows = _json_each_row(response)
        identities: dict[str, _StoredIdentity] = {}
        for row in rows:
            event_id = str(row["event_id"])
            if event_id in identities:
                raise ProjectionIntegrityError(
                    f"event identity {event_id} already has divergent stored envelopes"
                )
            identities[event_id] = _StoredIdentity(
                envelope_sha256=str(row["envelope_sha256"]),
                envelope_json=str(row["envelope_json"]),
                first_kafka_offset=int(row["first_kafka_offset"]),
            )
        return identities

    async def _verify_schema(self, session: aiohttp.ClientSession) -> None:
        columns_response = await self._execute(
            session,
            """
            SELECT table, name, type
            FROM system.columns
            WHERE database = {database:String}
              AND table IN {tables:Array(String)}
            ORDER BY table, position
            FORMAT JSONEachRow
            """,
            database=None,
            parameters={
                "database": self._database,
                "tables": [MARKET_EVENT_FACT_TABLE, MARKET_EVENT_CONFLICT_TABLE],
            },
        )
        actual_columns: dict[str, list[tuple[str, str]]] = {}
        for row in _json_each_row(columns_response):
            actual_columns.setdefault(str(row["table"]), []).append(
                (str(row["name"]), str(row["type"]))
            )
        expected_columns = {
            MARKET_EVENT_FACT_TABLE: list(_FACT_COLUMNS),
            MARKET_EVENT_CONFLICT_TABLE: list(_CONFLICT_COLUMNS),
        }
        if actual_columns != expected_columns:
            raise ClickHouseSchemaError("Phase 1D ClickHouse columns have drifted")

        table_response = await self._execute(
            session,
            """
            SELECT name, engine, sorting_key, partition_key
            FROM system.tables
            WHERE database = {database:String}
              AND name IN {tables:Array(String)}
            FORMAT JSONEachRow
            """,
            database=None,
            parameters={
                "database": self._database,
                "tables": [MARKET_EVENT_FACT_TABLE, MARKET_EVENT_CONFLICT_TABLE],
            },
        )
        actual_tables = {
            str(row["name"]): (
                str(row["engine"]),
                str(row["sorting_key"]),
                str(row["partition_key"]),
            )
            for row in _json_each_row(table_response)
        }
        expected_tables = {
            MARKET_EVENT_FACT_TABLE: (
                "ReplacingMergeTree",
                "partition_key, event_time_ms, event_id",
                "toYYYYMM(fromUnixTimestamp64Milli(event_time_ms))",
            ),
            MARKET_EVENT_CONFLICT_TABLE: (
                "ReplacingMergeTree",
                "partition_key, event_id, kafka_topic, kafka_partition, kafka_offset",
                "toYYYYMM(fromUnixTimestamp64Milli(detected_at_ms))",
            ),
        }
        if actual_tables != expected_tables:
            raise ClickHouseSchemaError("Phase 1D ClickHouse table keys have drifted")

    async def _insert_rows(
        self,
        session: aiohttp.ClientSession,
        *,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        deduplication_token: str,
    ) -> None:
        columns = tuple(rows[0])
        if any(tuple(row) != columns for row in rows):
            raise ProjectionIntegrityError(
                "ClickHouse batch rows have different columns"
            )
        column_sql = ", ".join(f"`{column}`" for column in columns)
        body = b"\n".join(
            json.dumps(
                dict(row),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            for row in rows
        )
        await self._execute(
            session,
            f"INSERT INTO `{self._database}`.`{table}` ({column_sql}) FORMAT JSONEachRow",
            body=body,
            settings={"insert_deduplication_token": deduplication_token},
        )

    async def _execute(
        self,
        session: aiohttp.ClientSession,
        query: str,
        *,
        body: bytes = b"",
        database: str | None = "default",
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> str:
        request_parameters: dict[str, str] = {"query": query.strip()}
        selected_database = self._database if database == "default" else database
        if selected_database is not None:
            request_parameters["database"] = selected_database
        for name, value in (parameters or {}).items():
            request_parameters[f"param_{name}"] = _query_parameter(value)
        request_parameters.update(settings or {})
        try:
            async with session.post(
                self._url,
                params=request_parameters,
                data=body,
                headers={
                    "X-ClickHouse-User": self._user,
                    "X-ClickHouse-Key": self._password,
                },
            ) as response:
                text = await response.text()
                if response.status != 200:
                    raise ClickHouseHttpError(
                        f"ClickHouse HTTP {response.status}: {text[:500].strip()}"
                    )
                return text
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise ClickHouseHttpError("ClickHouse HTTP request failed") from exc

    def _schema_statements(self) -> tuple[str, str]:
        database = self._database
        fact = f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`{MARKET_EVENT_FACT_TABLE}` (
            event_id UUID,
            partition_key LowCardinality(String),
            exchange LowCardinality(String),
            market_type LowCardinality(String),
            symbol LowCardinality(String),
            channel LowCardinality(String),
            delivery_class LowCardinality(String),
            source LowCardinality(String),
            source_event_id Nullable(String),
            sequence_start Nullable(UInt64),
            sequence_end Nullable(UInt64),
            previous_sequence Nullable(UInt64),
            producer_id String,
            producer_epoch UInt64,
            event_time_ms UInt64,
            received_at_ms UInt64,
            published_at_ms UInt64,
            payload_schema LowCardinality(String),
            payload_sha256 FixedString(64),
            payload_json String,
            envelope_sha256 FixedString(64),
            envelope_json String,
            first_kafka_topic LowCardinality(String),
            first_kafka_partition UInt16,
            first_kafka_offset UInt64,
            ingested_at_ms UInt64
        )
        ENGINE = ReplacingMergeTree(ingested_at_ms)
        PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(event_time_ms))
        ORDER BY (partition_key, event_time_ms, event_id)
        SETTINGS non_replicated_deduplication_window = 100000
        """
        conflict = f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`{MARKET_EVENT_CONFLICT_TABLE}` (
            event_id UUID,
            partition_key LowCardinality(String),
            existing_envelope_sha256 FixedString(64),
            incoming_envelope_sha256 FixedString(64),
            existing_envelope_json String,
            incoming_envelope_json String,
            incoming_payload_sha256 FixedString(64),
            kafka_topic LowCardinality(String),
            kafka_partition UInt16,
            kafka_offset UInt64,
            detected_at_ms UInt64
        )
        ENGINE = ReplacingMergeTree(detected_at_ms)
        PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(detected_at_ms))
        ORDER BY (partition_key, event_id, kafka_topic, kafka_partition, kafka_offset)
        SETTINGS non_replicated_deduplication_window = 100000
        """
        return fact, conflict

    def _new_session(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=self._request_timeout_ms / 1_000)
        return self._session_factory(timeout=timeout)

    def _require_session(self) -> aiohttp.ClientSession:
        session = self._session
        if session is None:
            raise ClickHouseProjectionError("projector is not started")
        return session


def _fact_row(record: KafkaMarketEventRecord) -> dict[str, Any]:
    envelope = record.envelope
    stream = envelope.stream
    payload_json = rfc8785.dumps(envelope.to_wire()["payload"]).decode("utf-8")
    return {
        "event_id": envelope.event_id,
        "partition_key": envelope.partition_key,
        "exchange": stream.exchange,
        "market_type": stream.market_type,
        "symbol": stream.symbol,
        "channel": stream.channel,
        "delivery_class": envelope.delivery_class.value,
        "source": envelope.source,
        "source_event_id": envelope.source_event_id,
        "sequence_start": envelope.sequence_start,
        "sequence_end": envelope.sequence_end,
        "previous_sequence": envelope.previous_sequence,
        "producer_id": envelope.producer_id,
        "producer_epoch": envelope.producer_epoch,
        "event_time_ms": envelope.event_time_ms,
        "received_at_ms": envelope.received_at_ms,
        "published_at_ms": envelope.published_at_ms,
        "payload_schema": envelope.payload_schema,
        "payload_sha256": envelope.payload_sha256,
        "payload_json": payload_json,
        "envelope_sha256": record.envelope_sha256,
        "envelope_json": record.envelope_bytes.decode("utf-8"),
        "first_kafka_topic": record.topic,
        "first_kafka_partition": record.partition,
        "first_kafka_offset": record.offset,
        "ingested_at_ms": _clock_ms(),
    }


def _conflict_row(
    record: KafkaMarketEventRecord,
    existing: _StoredIdentity,
) -> dict[str, Any]:
    return {
        "event_id": record.envelope.event_id,
        "partition_key": record.envelope.partition_key,
        "existing_envelope_sha256": existing.envelope_sha256,
        "incoming_envelope_sha256": record.envelope_sha256,
        "existing_envelope_json": existing.envelope_json,
        "incoming_envelope_json": record.envelope_bytes.decode("utf-8"),
        "incoming_payload_sha256": record.envelope.payload_sha256,
        "kafka_topic": record.topic,
        "kafka_partition": record.partition,
        "kafka_offset": record.offset,
        "detected_at_ms": _clock_ms(),
    }


def _batch_token(
    kind: str,
    records: Sequence[KafkaMarketEventRecord],
) -> str:
    digest = hashlib.sha256()
    digest.update(kind.encode("ascii"))
    for record in records:
        digest.update(f"\n{record.topic}:{record.partition}:{record.offset}:".encode())
        digest.update(record.envelope_sha256.encode("ascii"))
    return f"candlescope-phase1d-{kind}-{digest.hexdigest()}"


def _query_parameter(value: Any) -> str:
    if isinstance(value, list):
        return "[" + ",".join(_clickhouse_quote(str(item)) for item in value) + "]"
    return str(value)


def _clickhouse_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _json_each_row(value: str) -> list[dict[str, Any]]:
    try:
        return [json.loads(line) for line in value.splitlines() if line.strip()]
    except json.JSONDecodeError as exc:
        raise ClickHouseHttpError("ClickHouse returned invalid JSONEachRow") from exc


def _identifier(value: object, *, field: str) -> str:
    value = _required_text(value, field=field)
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must be a simple SQL identifier")
    return value


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _clock_ms() -> int:
    return time.time_ns() // 1_000_000
