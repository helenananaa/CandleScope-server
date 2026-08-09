"""Standalone strict HTTP boundary for Phase 1F snapshot queries."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import MarketDataSnapshotRef, MarketEventCursor
from app.server_runtime.object_store import ObjectStoreError
from app.server_runtime.query_cursor import ProjectionCursorError
from app.server_runtime.query_router import (
    HotProjectionParityError,
    HotProjectionUnavailableError,
    QueryPreference,
    SnapshotQueryRouter,
    SnapshotQueryRouteResult,
)
from app.server_runtime.storage.clickhouse_query import ClickHouseQueryError
from app.server_runtime.storage.parquet_archive import (
    ParquetArchiveCursorError,
    ParquetArchiveError,
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SnapshotRefRequest(_StrictModel):
    data_epoch: str
    snapshot_version: Annotated[int, Field(gt=0)]
    manifest_uri: str
    manifest_sha256: str


class StreamRequest(_StrictModel):
    exchange: str
    market_type: str
    symbol: str
    channel: str
    params: dict[str, str] = Field(default_factory=dict)


class CursorRequest(_StrictModel):
    value: str
    manifest_sha256: str


class SnapshotQueryRequest(_StrictModel):
    snapshot: SnapshotRefRequest
    stream: StreamRequest
    start_event_time_ms: Annotated[int, Field(ge=0)]
    end_event_time_ms: Annotated[int, Field(ge=0)]
    limit: Annotated[int, Field(gt=0)]
    cursor: CursorRequest | None = None
    preference: Literal["auto", "hot", "cold"] = "auto"


@dataclass(slots=True)
class QueryApiMetrics:
    requests_total: int = 0
    requests_in_flight: int = 0
    hot_responses_total: int = 0
    cold_responses_total: int = 0
    failures_total: int = 0
    parity_failures_total: int = 0
    overload_rejections_total: int = 0

    def to_wire(self) -> dict[str, int]:
        return {
            "requests_total": self.requests_total,
            "requests_in_flight": self.requests_in_flight,
            "hot_responses_total": self.hot_responses_total,
            "cold_responses_total": self.cold_responses_total,
            "failures_total": self.failures_total,
            "parity_failures_total": self.parity_failures_total,
            "overload_rejections_total": self.overload_rejections_total,
        }


def create_snapshot_query_app(
    *,
    router: SnapshotQueryRouter,
    max_concurrent_queries: int = 16,
    query_queue_timeout_ms: int = 1_000,
) -> FastAPI:
    if not isinstance(router, SnapshotQueryRouter):
        raise TypeError("router must be a SnapshotQueryRouter")
    max_concurrent_queries = _positive_int(
        max_concurrent_queries,
        field="max_concurrent_queries",
    )
    query_queue_timeout_ms = _positive_int(
        query_queue_timeout_ms,
        field="query_queue_timeout_ms",
    )
    metrics = QueryApiMetrics()
    semaphore = asyncio.Semaphore(max_concurrent_queries)
    state = {"ready": False}

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await router.start()
        state["ready"] = True
        try:
            yield
        finally:
            state["ready"] = False
            await router.stop()

    app = FastAPI(
        title="CandleScope Snapshot Query Service",
        version="1f",
        lifespan=lifespan,
    )

    @app.get("/health/live")
    async def live() -> dict[str, object]:
        return {"status": "live", "phase": "1f"}

    @app.get("/health/ready")
    async def ready() -> dict[str, object]:
        if not state["ready"]:
            raise HTTPException(status_code=503, detail={"code": "NOT_READY"})
        return {"status": "ready", "phase": "1f"}

    @app.get("/metrics")
    async def service_metrics() -> dict[str, int]:
        return metrics.to_wire()

    @app.post("/api/v1/server/market-events/query")
    async def query(request: SnapshotQueryRequest) -> dict[str, object]:
        metrics.requests_total += 1
        try:
            await asyncio.wait_for(
                semaphore.acquire(),
                timeout=query_queue_timeout_ms / 1_000,
            )
        except TimeoutError as exc:
            metrics.failures_total += 1
            metrics.overload_rejections_total += 1
            raise HTTPException(
                status_code=429,
                detail={"code": "QUERY_CAPACITY_EXHAUSTED"},
            ) from exc
        metrics.requests_in_flight += 1
        try:
            arguments = _domain_request(request)
            result = await router.query(**arguments)
            if result.backend == "hot":
                metrics.hot_responses_total += 1
            else:
                metrics.cold_responses_total += 1
            return _route_result_wire(result)
        except HotProjectionUnavailableError as exc:
            metrics.failures_total += 1
            raise HTTPException(
                status_code=409,
                detail={"code": "HOT_PROJECTION_BEHIND", "message": str(exc)},
            ) from exc
        except HotProjectionParityError as exc:
            metrics.failures_total += 1
            metrics.parity_failures_total += 1
            raise HTTPException(
                status_code=503,
                detail={"code": "HOT_COLD_PARITY_FAILED", "message": str(exc)},
            ) from exc
        except ParquetArchiveCursorError as exc:
            metrics.failures_total += 1
            raise HTTPException(
                status_code=409,
                detail={"code": "CURSOR_SNAPSHOT_CONFLICT", "message": str(exc)},
            ) from exc
        except (TypeError, ValueError) as exc:
            metrics.failures_total += 1
            raise HTTPException(
                status_code=422,
                detail={"code": "INVALID_QUERY", "message": str(exc)},
            ) from exc
        except (
            ClickHouseQueryError,
            ObjectStoreError,
            ParquetArchiveError,
            ProjectionCursorError,
        ) as exc:
            metrics.failures_total += 1
            raise HTTPException(
                status_code=503,
                detail={"code": "QUERY_BACKEND_FAILED", "message": str(exc)},
            ) from exc
        finally:
            metrics.requests_in_flight -= 1
            semaphore.release()

    return app


def _domain_request(request: SnapshotQueryRequest) -> dict[str, object]:
    snapshot = MarketDataSnapshotRef(
        data_epoch=request.snapshot.data_epoch,
        snapshot_version=request.snapshot.snapshot_version,
        manifest_uri=request.snapshot.manifest_uri,
        manifest_sha256=request.snapshot.manifest_sha256,
    )
    stream = MarketStreamKey.build(
        request.stream.exchange,
        request.stream.market_type,
        request.stream.symbol,
        request.stream.channel,
        request.stream.params,
    )
    cursor = (
        None
        if request.cursor is None
        else MarketEventCursor(
            value=request.cursor.value,
            manifest_sha256=request.cursor.manifest_sha256,
        )
    )
    return {
        "snapshot": snapshot,
        "stream": stream,
        "start_event_time_ms": request.start_event_time_ms,
        "end_event_time_ms": request.end_event_time_ms,
        "limit": request.limit,
        "cursor": cursor,
        "preference": QueryPreference(request.preference),
    }


def _route_result_wire(result: SnapshotQueryRouteResult) -> dict[str, object]:
    page = result.page
    snapshot = page.snapshot
    covered = page.covered_range
    return {
        "backend": result.backend,
        "hot_committed_next_offset": result.hot_committed_next_offset,
        "parity_verified": result.parity_verified,
        "page": {
            "snapshot": {
                "data_epoch": snapshot.data_epoch,
                "snapshot_version": snapshot.snapshot_version,
                "manifest_uri": snapshot.manifest_uri,
                "manifest_sha256": snapshot.manifest_sha256,
            },
            "events": [event.to_wire() for event in page.events],
            "covered_range": {
                "partition_key": covered.partition_key,
                "start_event_time_ms": covered.start_event_time_ms,
                "end_event_time_ms": covered.end_event_time_ms,
                "event_count": covered.event_count,
                "sequence_start": covered.sequence_start,
                "sequence_end": covered.sequence_end,
            },
            "next_cursor": (
                None
                if page.next_cursor is None
                else {
                    "value": page.next_cursor.value,
                    "manifest_sha256": page.next_cursor.manifest_sha256,
                }
            ),
        },
    }


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value
