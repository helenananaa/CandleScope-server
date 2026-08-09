"""Snapshot-bounded ClickHouse market-event queries for Phase 1F."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventCursor,
    MarketEventEnvelopeV1,
    MarketEventPage,
)
from app.server_runtime.query_pagination import (
    SnapshotQueryIntegrityError,
    SnapshotQueryRow,
    canonical_fact_rows,
    paginate_snapshot_rows,
)

from .clickhouse import MARKET_EVENT_FACT_TABLE

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ClickHouseQueryError(RuntimeError):
    """Base class for fail-closed hot snapshot query errors."""


class ClickHouseQueryHttpError(ClickHouseQueryError):
    """ClickHouse rejected a query or returned an invalid response."""


class ClickHouseQueryIntegrityError(ClickHouseQueryError):
    """A hot row cannot be reconciled with the frozen event contract."""


class ClickHouseSnapshotMarketEventQuery:
    """Read canonical facts whose first Kafka offset belongs to a snapshot."""

    def __init__(
        self,
        *,
        url: str,
        database: str,
        user: str,
        password: str,
        request_timeout_ms: int = 10_000,
        max_scan_rows: int = 100_000,
        max_page_rows: int = 1_000,
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
        self._max_scan_rows = _positive_int(max_scan_rows, field="max_scan_rows")
        self._max_page_rows = _positive_int(max_page_rows, field="max_page_rows")
        self._session_factory = session_factory
        self._session: aiohttp.ClientSession | None = None

    @property
    def started(self) -> bool:
        return self._session is not None

    async def start(self) -> None:
        if self._session is not None:
            raise ClickHouseQueryError("hot query adapter is already started")
        session = self._new_session()
        try:
            response = await self._execute(
                session,
                """
                SELECT count() AS table_count
                FROM system.tables
                WHERE database = {database:String}
                  AND name = {table:String}
                FORMAT JSONEachRow
                """,
                database=None,
                parameters={
                    "database": self._database,
                    "table": MARKET_EVENT_FACT_TABLE,
                },
            )
            rows = _json_each_row(response)
            if len(rows) != 1 or int(rows[0].get("table_count", -1)) != 1:
                raise ClickHouseQueryIntegrityError(
                    "Phase 1D market-event fact table is missing"
                )
            columns_response = await self._execute(
                session,
                """
                SELECT name, type
                FROM system.columns
                WHERE database = {database:String}
                  AND table = {table:String}
                  AND name IN (
                    'envelope_sha256',
                    'envelope_json',
                    'first_kafka_partition',
                    'first_kafka_offset'
                  )
                ORDER BY position
                FORMAT JSONEachRow
                """,
                database=None,
                parameters={
                    "database": self._database,
                    "table": MARKET_EVENT_FACT_TABLE,
                },
            )
            columns = [
                (str(row.get("name")), str(row.get("type")))
                for row in _json_each_row(columns_response)
            ]
            if columns != [
                ("envelope_sha256", "FixedString(64)"),
                ("envelope_json", "String"),
                ("first_kafka_partition", "UInt16"),
                ("first_kafka_offset", "UInt64"),
            ]:
                raise ClickHouseQueryIntegrityError(
                    "Phase 1D query columns are missing or have drifted"
                )
        except BaseException:
            await session.close()
            raise
        self._session = session

    async def stop(self) -> None:
        session = self._session
        self._session = None
        if session is not None:
            await session.close()

    async def query(
        self,
        *,
        snapshot: MarketDataSnapshotRef,
        stream: MarketStreamKey,
        start_event_time_ms: int,
        end_event_time_ms: int,
        limit: int,
        cursor: MarketEventCursor | None = None,
    ) -> MarketEventPage:
        session = self._require_session()
        if not isinstance(snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        if not isinstance(stream, MarketStreamKey):
            raise TypeError("stream must be a MarketStreamKey")
        response = await self._execute(
            session,
            f"""
            SELECT
                envelope_sha256,
                envelope_json,
                first_kafka_partition,
                first_kafka_offset
            FROM `{self._database}`.`{MARKET_EVENT_FACT_TABLE}` FINAL
            WHERE partition_key = {{partition_key:String}}
              AND event_time_ms >= {{start_event_time_ms:UInt64}}
              AND event_time_ms <= {{end_event_time_ms:UInt64}}
              AND first_kafka_offset < {{snapshot_version:UInt64}}
            ORDER BY event_time_ms, first_kafka_partition, first_kafka_offset
            LIMIT {{scan_limit:UInt64}}
            FORMAT JSONEachRow
            """,
            parameters={
                "partition_key": stream.topic,
                "start_event_time_ms": start_event_time_ms,
                "end_event_time_ms": end_event_time_ms,
                "snapshot_version": snapshot.snapshot_version,
                "scan_limit": self._max_scan_rows + 1,
            },
        )
        raw_rows = _json_each_row(response)
        if len(raw_rows) > self._max_scan_rows:
            raise ClickHouseQueryIntegrityError(
                "hot query exceeded the configured scan-row limit"
            )
        rows: list[SnapshotQueryRow] = []
        try:
            for raw in raw_rows:
                envelope_json = raw["envelope_json"]
                if not isinstance(envelope_json, str):
                    raise TypeError("envelope_json must be a string")
                envelope_sha256 = raw["envelope_sha256"]
                if not isinstance(envelope_sha256, str):
                    raise TypeError("envelope_sha256 must be a string")
                envelope_bytes = envelope_json.encode("utf-8")
                envelope = MarketEventEnvelopeV1.from_wire(json.loads(envelope_json))
                rows.append(
                    SnapshotQueryRow(
                        envelope=envelope,
                        envelope_sha256=envelope_sha256,
                        envelope_bytes=envelope_bytes,
                        kafka_partition=_clickhouse_uint(
                            raw["first_kafka_partition"],
                            field="first_kafka_partition",
                        ),
                        kafka_offset=_clickhouse_uint(
                            raw["first_kafka_offset"],
                            field="first_kafka_offset",
                        ),
                    )
                )
            facts = canonical_fact_rows(rows)
            if facts.exact_duplicate_count or facts.integrity_conflict_count:
                raise SnapshotQueryIntegrityError(
                    "ClickHouse returned duplicate logical fact identities"
                )
            return paginate_snapshot_rows(
                facts.rows,
                snapshot=snapshot,
                stream=stream,
                start_event_time_ms=start_event_time_ms,
                end_event_time_ms=end_event_time_ms,
                limit=limit,
                max_page_rows=self._max_page_rows,
                cursor=cursor,
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            SnapshotQueryIntegrityError,
        ) as exc:
            raise ClickHouseQueryIntegrityError(
                "ClickHouse returned an invalid canonical market-event row"
            ) from exc

    async def _execute(
        self,
        session: aiohttp.ClientSession,
        query: str,
        *,
        database: str | None = "default",
        parameters: Mapping[str, object] | None = None,
    ) -> str:
        request_parameters = {"query": query.strip()}
        selected_database = self._database if database == "default" else database
        if selected_database is not None:
            request_parameters["database"] = selected_database
        for name, value in (parameters or {}).items():
            request_parameters[f"param_{name}"] = str(value)
        try:
            async with session.post(
                self._url,
                params=request_parameters,
                headers={
                    "X-ClickHouse-User": self._user,
                    "X-ClickHouse-Key": self._password,
                },
            ) as response:
                text = await response.text()
                if response.status != 200:
                    raise ClickHouseQueryHttpError(
                        f"ClickHouse HTTP {response.status}: {text[:500].strip()}"
                    )
                return text
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise ClickHouseQueryHttpError(
                "ClickHouse hot query request failed"
            ) from exc

    def _new_session(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=self._request_timeout_ms / 1_000)
        return self._session_factory(timeout=timeout)

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise ClickHouseQueryError("hot query adapter is not started")
        return self._session


def _json_each_row(value: str) -> list[dict[str, Any]]:
    try:
        rows = [json.loads(line) for line in value.splitlines() if line.strip()]
    except json.JSONDecodeError as exc:
        raise ClickHouseQueryHttpError(
            "ClickHouse returned invalid JSONEachRow"
        ) from exc
    if any(not isinstance(row, dict) for row in rows):
        raise ClickHouseQueryHttpError("ClickHouse returned a non-object row")
    return rows


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


def _clickhouse_uint(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an unsigned integer")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field} must be non-negative")
        return value
    if isinstance(value, str) and value and value.isascii() and value.isdecimal():
        return int(value)
    raise TypeError(f"{field} must be an unsigned integer")
