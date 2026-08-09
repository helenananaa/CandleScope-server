"""Proof-oriented hot/cold snapshot query routing for Phase 1F."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventCursor,
    MarketEventPage,
)
from app.server_runtime.query_cursor import ProjectionCursorReader


class QueryPreference(str, Enum):
    AUTO = "auto"
    HOT = "hot"
    COLD = "cold"


class SnapshotQueryRouteError(RuntimeError):
    """Base class for fail-closed snapshot routing failures."""


class HotProjectionUnavailableError(SnapshotQueryRouteError):
    """The requested snapshot is newer than the hot projection cursor."""


class HotProjectionParityError(SnapshotQueryRouteError):
    """Hot and cold returned different facts for one immutable snapshot."""


class SnapshotMarketEventQuery(Protocol):
    async def query(
        self,
        *,
        snapshot: MarketDataSnapshotRef,
        stream: MarketStreamKey,
        start_event_time_ms: int,
        end_event_time_ms: int,
        limit: int,
        cursor: MarketEventCursor | None = None,
    ) -> MarketEventPage: ...


class LifecycleSnapshotMarketEventQuery(SnapshotMarketEventQuery, Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SnapshotQueryRouteResult:
    backend: str
    page: MarketEventPage
    hot_committed_next_offset: int | None
    parity_verified: bool


class SnapshotQueryRouter:
    """Use hot only after cursor coverage and exact cold-page parity proof."""

    def __init__(
        self,
        *,
        cold_query: LifecycleSnapshotMarketEventQuery,
        hot_query: LifecycleSnapshotMarketEventQuery,
        projection_cursor: ProjectionCursorReader,
    ) -> None:
        self._cold_query = cold_query
        self._hot_query = hot_query
        self._projection_cursor = projection_cursor
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        if self._started:
            raise SnapshotQueryRouteError("snapshot query router is already started")
        await self._cold_query.start()
        try:
            await self._hot_query.start()
        except BaseException:
            await self._cold_query.stop()
            raise
        try:
            await self._projection_cursor.start()
        except BaseException:
            try:
                await self._hot_query.stop()
            finally:
                await self._cold_query.stop()
            raise
        self._started = True

    async def stop(self) -> None:
        self._started = False
        try:
            try:
                await self._projection_cursor.stop()
            finally:
                await self._hot_query.stop()
        finally:
            await self._cold_query.stop()

    async def query(
        self,
        *,
        snapshot: MarketDataSnapshotRef,
        stream: MarketStreamKey,
        start_event_time_ms: int,
        end_event_time_ms: int,
        limit: int,
        cursor: MarketEventCursor | None = None,
        preference: QueryPreference | str = QueryPreference.AUTO,
    ) -> SnapshotQueryRouteResult:
        if not self._started:
            raise SnapshotQueryRouteError("snapshot query router is not started")
        try:
            preference = QueryPreference(preference)
        except ValueError as exc:
            raise ValueError("preference must be auto, hot, or cold") from exc
        arguments = {
            "snapshot": snapshot,
            "stream": stream,
            "start_event_time_ms": start_event_time_ms,
            "end_event_time_ms": end_event_time_ms,
            "limit": limit,
            "cursor": cursor,
        }
        cold_page = await self._cold_query.query(**arguments)
        if preference is QueryPreference.COLD:
            return SnapshotQueryRouteResult(
                backend="cold",
                page=cold_page,
                hot_committed_next_offset=None,
                parity_verified=False,
            )
        committed = await self._projection_cursor.committed_next_offset()
        if committed is None or committed < snapshot.snapshot_version:
            if preference is QueryPreference.HOT:
                raise HotProjectionUnavailableError(
                    "ClickHouse writer cursor has not covered the requested snapshot"
                )
            return SnapshotQueryRouteResult(
                backend="cold",
                page=cold_page,
                hot_committed_next_offset=committed,
                parity_verified=False,
            )
        hot_page = await self._hot_query.query(**arguments)
        if hot_page != cold_page:
            raise HotProjectionParityError(
                "ClickHouse and Parquet pages differ for the requested snapshot"
            )
        return SnapshotQueryRouteResult(
            backend="hot",
            page=hot_page,
            hot_committed_next_offset=committed,
            parity_verified=True,
        )
