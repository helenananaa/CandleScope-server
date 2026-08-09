"""Always-on Kafka-to-ClickHouse projection lifecycle for Phase 1D."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from app.server_runtime.consumers import KafkaMarketEventConsumer
from app.server_runtime.projection import MarketEventProjector, ProjectionBatchResult
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)
from app.server_runtime.writer_settings import ClickHouseWriterSettings

logger = logging.getLogger("server_runtime.clickhouse_writer")

WriterHealthObserver = Callable[[ClickHouseWriterHealth], Awaitable[None]]
AfterProjectHook = Callable[[ProjectionBatchResult], Awaitable[None]]


class ClickHouseWriterServiceError(RuntimeError):
    """The writer lifecycle failed before a safe Kafka commit."""


class ClickHouseWriterService:
    """Project a contiguous batch, then durably commit its next Kafka offset."""

    def __init__(
        self,
        *,
        settings: ClickHouseWriterSettings,
        consumer: KafkaMarketEventConsumer,
        projector: MarketEventProjector,
        on_health: WriterHealthObserver | None = None,
        after_project_before_commit: AfterProjectHook | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._settings = settings
        self._consumer = consumer
        self._projector = projector
        self._on_health = on_health
        self._after_project_before_commit = after_project_before_commit
        self._clock_ms = clock_ms or _system_clock_ms
        now = self._clock_ms()
        self._health = ClickHouseWriterHealth(
            state=ClickHouseWriterState.STOPPED,
            ready=False,
            reason="not started",
            owner_id=settings.owner_id,
            kafka_group_id=settings.kafka_group_id,
            committed_next_offset=None,
            batches_committed=0,
            inserted_events=0,
            duplicate_events=0,
            conflict_events=0,
            started_at_ms=now,
            updated_at_ms=now,
            terminal_error=None,
        )
        self._local_stop = asyncio.Event()
        self._running = False
        self._consumer_started = False
        self._projector_started = False
        self._health_lock = asyncio.Lock()

    @property
    def health(self) -> ClickHouseWriterHealth:
        return self._health

    def request_stop(self) -> None:
        self._local_stop.set()

    async def run(
        self, stop_event: asyncio.Event | None = None
    ) -> ClickHouseWriterHealth:
        if self._running:
            raise ClickHouseWriterServiceError("writer service is already running")
        self._running = True
        requested_stop = stop_event or self._local_stop
        primary_error: BaseException | None = None
        try:
            await self._transition(
                ClickHouseWriterState.STARTING, "starting ClickHouse"
            )
            await self._projector.start()
            self._projector_started = True
            await self._transition(
                ClickHouseWriterState.STARTING, "joining Kafka group"
            )
            await self._consumer.start()
            self._consumer_started = True
            await self._transition(
                ClickHouseWriterState.RUNNING,
                "waiting for market-event batches",
            )
            while not requested_stop.is_set() and not self._local_stop.is_set():
                batch = await self._consumer.poll(
                    timeout_ms=self._settings.poll_timeout_ms,
                    max_records=self._settings.batch_size,
                )
                if not batch:
                    continue
                result = await self._projector.apply_batch(batch)
                if self._after_project_before_commit is not None:
                    await self._after_project_before_commit(result)
                await self._consumer.commit_through(batch[-1])
                state = (
                    ClickHouseWriterState.DEGRADED
                    if self._health.conflict_events + result.conflict_count > 0
                    else ClickHouseWriterState.RUNNING
                )
                reason = (
                    "integrity conflicts quarantined"
                    if state is ClickHouseWriterState.DEGRADED
                    else "batch projected and Kafka offset committed"
                )
                await self._transition(
                    state,
                    reason,
                    result=result,
                    committed_next_offset=batch[-1].offset + 1,
                )
        except asyncio.CancelledError as exc:
            primary_error = exc
        except Exception as exc:  # noqa: BLE001 - terminal service boundary
            primary_error = exc
            await self._transition(
                ClickHouseWriterState.DEGRADED,
                f"terminal {type(exc).__name__}",
                terminal_error=f"{type(exc).__name__}: {exc}",
            )
        finally:
            cleanup_error = await self._teardown(primary_error)
            self._running = False
            if primary_error is None:
                primary_error = cleanup_error
        if primary_error is not None:
            raise primary_error
        return self._health

    async def _teardown(
        self,
        primary_error: BaseException | None,
    ) -> BaseException | None:
        await self._transition(ClickHouseWriterState.STOPPING, "ordered shutdown")
        errors: list[BaseException] = []
        timeout = self._settings.shutdown_timeout_ms / 1_000
        if self._consumer_started:
            error = await _cleanup_step(self._consumer.stop(), timeout, "consumer stop")
            self._consumer_started = False
            if error is not None:
                errors.append(error)
        if self._projector_started:
            error = await _cleanup_step(
                self._projector.stop(), timeout, "projector stop"
            )
            self._projector_started = False
            if error is not None:
                errors.append(error)
        terminal = primary_error or (errors[0] if errors else None)
        await self._transition(
            ClickHouseWriterState.STOPPED,
            "stopped cleanly" if terminal is None else "stopped after failure",
            terminal_error=(
                None if terminal is None else f"{type(terminal).__name__}: {terminal}"
            ),
        )
        if errors:
            return ClickHouseWriterServiceError(
                "ordered shutdown failed: " + "; ".join(str(error) for error in errors)
            )
        return None

    async def _transition(
        self,
        state: ClickHouseWriterState,
        reason: str,
        *,
        result: ProjectionBatchResult | None = None,
        committed_next_offset: int | None = None,
        terminal_error: str | None = None,
    ) -> None:
        async with self._health_lock:
            current = self._health
            snapshot = ClickHouseWriterHealth(
                state=state,
                ready=state is ClickHouseWriterState.RUNNING,
                reason=reason,
                owner_id=self._settings.owner_id,
                kafka_group_id=self._settings.kafka_group_id,
                committed_next_offset=(
                    committed_next_offset
                    if committed_next_offset is not None
                    else current.committed_next_offset
                ),
                batches_committed=current.batches_committed + (result is not None),
                inserted_events=current.inserted_events
                + (result.inserted_count if result is not None else 0),
                duplicate_events=current.duplicate_events
                + (result.duplicate_count if result is not None else 0),
                conflict_events=current.conflict_events
                + (result.conflict_count if result is not None else 0),
                started_at_ms=current.started_at_ms,
                updated_at_ms=self._clock_ms(),
                terminal_error=(
                    terminal_error
                    if terminal_error is not None
                    else current.terminal_error
                ),
            )
            self._health = snapshot
        if self._on_health is not None:
            try:
                await self._on_health(snapshot)
            except Exception:
                logger.exception("ClickHouse writer health observer failed")


async def _cleanup_step(
    operation: Awaitable[None],
    timeout: float,
    name: str,
) -> BaseException | None:
    try:
        await asyncio.wait_for(operation, timeout=timeout)
    except Exception as exc:  # noqa: BLE001 - aggregate ordered cleanup failures
        return ClickHouseWriterServiceError(f"{name} failed: {exc}")
    return None


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000
