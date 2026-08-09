"""Authenticated and audited HTTP boundary for Phase 1G snapshot queries."""

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import MarketDataSnapshotRef, MarketEventCursor
from app.server_runtime.object_store import ObjectStoreError
from app.server_runtime.query_cursor import ProjectionCursorError
from app.server_runtime.query_router import (
    HotProjectionParityError,
    HotProjectionQuarantinedError,
    HotProjectionUnavailableError,
    QueryPreference,
    SnapshotQueryRouter,
    SnapshotQueryRouteResult,
)
from app.server_runtime.query_security import (
    BearerTokenAuthenticator,
    QueryAuditEvent,
    QueryAuditSink,
    QueryAuthenticationError,
)
from app.server_runtime.storage.clickhouse_query import ClickHouseQueryError
from app.server_runtime.storage.parquet_archive import (
    ParquetArchiveCursorError,
    ParquetArchiveError,
)

logger = logging.getLogger(__name__)
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class SnapshotRefRequest(_StrictModel):
    data_epoch: Annotated[str, Field(min_length=1, max_length=128)]
    snapshot_version: Annotated[int, Field(gt=0, le=18_446_744_073_709_551_615)]
    manifest_uri: Annotated[str, Field(min_length=1, max_length=2_048)]
    manifest_sha256: Annotated[str, Field(pattern=r"^[0-9A-Fa-f]{64}$")]


class StreamRequest(_StrictModel):
    exchange: Annotated[str, Field(min_length=1, max_length=128)]
    market_type: Annotated[str, Field(min_length=1, max_length=128)]
    symbol: Annotated[str, Field(min_length=1, max_length=128)]
    channel: Annotated[str, Field(min_length=1, max_length=128)]
    params: Annotated[dict[str, str], Field(max_length=32)] = Field(
        default_factory=dict
    )

    @field_validator("params")
    @classmethod
    def bounded_params(cls, value: dict[str, str]) -> dict[str, str]:
        if any(
            not 1 <= len(name) <= 128 or not 1 <= len(item) <= 256
            for name, item in value.items()
        ):
            raise ValueError("stream param names/values exceed their bounds")
        return value


class CursorRequest(_StrictModel):
    value: Annotated[str, Field(min_length=1, max_length=2_048)]
    manifest_sha256: Annotated[str, Field(pattern=r"^[0-9A-Fa-f]{64}$")]


class SnapshotQueryRequest(_StrictModel):
    snapshot: SnapshotRefRequest
    stream: StreamRequest
    start_event_time_ms: Annotated[
        int,
        Field(ge=0, le=18_446_744_073_709_551_615),
    ]
    end_event_time_ms: Annotated[
        int,
        Field(ge=0, le=18_446_744_073_709_551_615),
    ]
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
    unauthorized_total: int = 0
    audit_failures_total: int = 0

    def to_wire(self) -> dict[str, int]:
        return {
            "requests_total": self.requests_total,
            "requests_in_flight": self.requests_in_flight,
            "hot_responses_total": self.hot_responses_total,
            "cold_responses_total": self.cold_responses_total,
            "failures_total": self.failures_total,
            "parity_failures_total": self.parity_failures_total,
            "overload_rejections_total": self.overload_rejections_total,
            "unauthorized_total": self.unauthorized_total,
            "audit_failures_total": self.audit_failures_total,
        }


def create_snapshot_query_app(
    *,
    router: SnapshotQueryRouter,
    authenticator: BearerTokenAuthenticator,
    audit_sink: QueryAuditSink,
    max_concurrent_queries: int = 16,
    query_queue_timeout_ms: int = 1_000,
) -> FastAPI:
    if not isinstance(router, SnapshotQueryRouter):
        raise TypeError("router must be a SnapshotQueryRouter")
    if not isinstance(authenticator, BearerTokenAuthenticator):
        raise TypeError("authenticator must be a BearerTokenAuthenticator")
    if not isinstance(audit_sink, QueryAuditSink):
        raise TypeError("audit_sink must implement QueryAuditSink")
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
        version="1g",
        lifespan=lifespan,
    )
    bearer = HTTPBearer(auto_error=False)
    bearer_dependency = Depends(bearer)

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        try:
            request_id = _request_id(request.headers.get("X-Request-ID"))
        except ValueError as exc:
            request_id = str(uuid.uuid4())
            try:
                await audit_sink.emit(
                    QueryAuditEvent(
                        request_id=request_id,
                        principal=None,
                        action="validate_query_request_id",
                        outcome="denied",
                        status_code=400,
                        timestamp_ms=time.time_ns() // 1_000_000,
                        latency_ms=0,
                    )
                )
            except (RuntimeError, TypeError, ValueError):
                metrics.audit_failures_total += 1
                response = JSONResponse(
                    status_code=503,
                    content={"detail": {"code": "QUERY_AUDIT_UNAVAILABLE"}},
                )
                response.headers["X-Request-ID"] = request_id
                return response
            response = JSONResponse(
                status_code=400,
                content={
                    "detail": {
                        "code": "INVALID_REQUEST_ID",
                        "message": str(exc),
                    }
                },
            )
            response.headers["X-Request-ID"] = request_id
            return response
        request.state.query_request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    async def authorize(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = bearer_dependency,
    ) -> str:
        try:
            credential = None if credentials is None else credentials.credentials
            principal = authenticator.authenticate(credential)
            request.state.query_principal = principal
            return principal
        except QueryAuthenticationError as exc:
            metrics.unauthorized_total += 1
            try:
                await audit_sink.emit(
                    QueryAuditEvent(
                        request_id=request.state.query_request_id,
                        principal=None,
                        action="authorize_snapshot_query_service",
                        outcome="denied",
                        status_code=401,
                        timestamp_ms=time.time_ns() // 1_000_000,
                        latency_ms=0,
                    )
                )
            except Exception as audit_exc:
                metrics.audit_failures_total += 1
                raise HTTPException(
                    status_code=503,
                    detail={"code": "QUERY_AUDIT_UNAVAILABLE"},
                ) from audit_exc
            raise HTTPException(
                status_code=401,
                detail={"code": "QUERY_AUTHENTICATION_REQUIRED"},
                headers={"WWW-Authenticate": "Bearer"},
            ) from exc

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request,
        exc: RequestValidationError,
    ):
        request_id = getattr(request.state, "query_request_id", str(uuid.uuid4()))
        principal = getattr(request.state, "query_principal", None)
        try:
            await audit_sink.emit(
                QueryAuditEvent(
                    request_id=request_id,
                    principal=principal,
                    action="validate_snapshot_query_request",
                    outcome="invalid_request",
                    status_code=422,
                    timestamp_ms=time.time_ns() // 1_000_000,
                    latency_ms=0,
                )
            )
        except (RuntimeError, TypeError, ValueError):
            metrics.audit_failures_total += 1
            return JSONResponse(
                status_code=503,
                content={"detail": {"code": "QUERY_AUDIT_UNAVAILABLE"}},
            )
        return await request_validation_exception_handler(request, exc)

    @app.get("/health/live")
    async def live() -> dict[str, object]:
        return {"status": "live", "phase": "1g"}

    @app.get("/health/ready")
    async def ready() -> dict[str, object]:
        if not state["ready"]:
            raise HTTPException(status_code=503, detail={"code": "NOT_READY"})
        parity = router.parity_status()
        return {
            "status": "ready",
            "phase": "1g",
            "hot_status": ("quarantined" if parity.hot_quarantined else "available"),
        }

    @app.get("/metrics")
    async def service_metrics(
        _principal: str = Depends(authorize),
    ) -> dict[str, object]:
        return {**metrics.to_wire(), "parity": router.parity_status().to_wire()}

    @app.post("/api/v1/server/market-events/query")
    async def query(
        query_request: SnapshotQueryRequest,
        http_request: Request,
        response: Response,
        principal: str = Depends(authorize),
    ) -> dict[str, object]:
        metrics.requests_total += 1
        started_ns = time.monotonic_ns()
        request_id = http_request.state.query_request_id
        acquired = False
        failure: HTTPException | None = None
        wire: dict[str, object] | None = None
        status_code = 500
        outcome = "internal_error"
        backend: str | None = None
        partition_key: str | None = None
        try:
            await asyncio.wait_for(
                semaphore.acquire(),
                timeout=query_queue_timeout_ms / 1_000,
            )
            acquired = True
            metrics.requests_in_flight += 1
            arguments = _domain_request(query_request)
            domain_stream = arguments.get("stream")
            if not isinstance(domain_stream, MarketStreamKey):
                raise TypeError("domain query stream is invalid")
            partition_key = domain_stream.topic
            result = await router.query(**arguments)
            backend = result.backend
            if result.backend == "hot":
                metrics.hot_responses_total += 1
            else:
                metrics.cold_responses_total += 1
            wire = _route_result_wire(result)
            status_code = 200
            outcome = "success"
        except TimeoutError:
            metrics.failures_total += 1
            metrics.overload_rejections_total += 1
            status_code = 429
            outcome = "capacity_exhausted"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "QUERY_CAPACITY_EXHAUSTED"},
            )
        except HotProjectionUnavailableError as exc:
            metrics.failures_total += 1
            status_code = 409
            outcome = "hot_projection_behind"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "HOT_PROJECTION_BEHIND", "message": str(exc)},
            )
        except HotProjectionParityError as exc:
            metrics.failures_total += 1
            metrics.parity_failures_total += 1
            status_code = 503
            outcome = "hot_cold_parity_failed"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "HOT_COLD_PARITY_FAILED", "message": str(exc)},
            )
        except HotProjectionQuarantinedError as exc:
            metrics.failures_total += 1
            status_code = 503
            outcome = "hot_projection_quarantined"
            failure = HTTPException(
                status_code=status_code,
                detail={
                    "code": "HOT_PROJECTION_QUARANTINED",
                    "message": str(exc),
                },
            )
        except ParquetArchiveCursorError as exc:
            metrics.failures_total += 1
            status_code = 409
            outcome = "cursor_snapshot_conflict"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "CURSOR_SNAPSHOT_CONFLICT", "message": str(exc)},
            )
        except (TypeError, ValueError) as exc:
            metrics.failures_total += 1
            status_code = 422
            outcome = "invalid_query"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "INVALID_QUERY", "message": str(exc)},
            )
        except (
            ClickHouseQueryError,
            ObjectStoreError,
            ParquetArchiveError,
            ProjectionCursorError,
        ) as exc:
            metrics.failures_total += 1
            status_code = 503
            outcome = "query_backend_failed"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "QUERY_BACKEND_FAILED", "message": str(exc)},
            )
        except Exception:
            metrics.failures_total += 1
            logger.exception("unhandled snapshot query failure")
            status_code = 500
            outcome = "internal_error"
            failure = HTTPException(
                status_code=status_code,
                detail={"code": "INTERNAL_QUERY_ERROR"},
            )
        finally:
            if acquired:
                metrics.requests_in_flight -= 1
                semaphore.release()

        try:
            await audit_sink.emit(
                QueryAuditEvent(
                    request_id=request_id,
                    principal=principal,
                    action="snapshot_market_event_query",
                    outcome=outcome,
                    status_code=status_code,
                    timestamp_ms=time.time_ns() // 1_000_000,
                    latency_ms=(time.monotonic_ns() - started_ns) // 1_000_000,
                    snapshot_version=query_request.snapshot.snapshot_version,
                    manifest_sha256=_audit_sha256(
                        query_request.snapshot.manifest_sha256
                    ),
                    partition_key=partition_key,
                    preference=query_request.preference,
                    backend=backend,
                )
            )
        except Exception as exc:
            metrics.audit_failures_total += 1
            raise HTTPException(
                status_code=503,
                detail={"code": "QUERY_AUDIT_UNAVAILABLE"},
            ) from exc
        if failure is not None:
            raise failure
        if wire is None:  # pragma: no cover - result/failure invariant
            raise RuntimeError("query completed without a response or failure")
        response.headers["X-Request-ID"] = request_id
        return wire

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
        "hot_quarantined": result.hot_quarantined,
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


def _request_id(value: str | None) -> str:
    if value is None:
        return str(uuid.uuid4())
    if value != value.strip() or not _REQUEST_ID.fullmatch(value):
        raise ValueError("X-Request-ID must use 1-128 safe ASCII identifier characters")
    return value


def _audit_sha256(value: str) -> str | None:
    normalized = value.lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        return None
    return normalized
