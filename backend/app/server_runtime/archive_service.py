"""Always-on Kafka-to-immutable-Parquet archive lifecycle for Phase 1E."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.archive_settings import ArchiveWriterSettings
from app.server_runtime.consumers import KafkaMarketEventConsumer
from app.server_runtime.storage.parquet_archive import (
    ArchiveAppendResult,
    ImmutableParquetMarketEventArchive,
)

logger = logging.getLogger("server_runtime.parquet_archiver")

ArchiveHealthObserver = Callable[[ArchiveWriterHealth], Awaitable[None]]
AfterArchiveHook = Callable[[ArchiveAppendResult], Awaitable[None]]


class ArchiveWriterServiceError(RuntimeError):
    """The archive lifecycle failed before a safe Kafka commit."""


class ArchiveWriterService:
    """Archive one exact segment, publish its manifest, then commit Kafka."""

    def __init__(
        self,
        *,
        settings: ArchiveWriterSettings,
        consumer: KafkaMarketEventConsumer,
        archive: ImmutableParquetMarketEventArchive,
        on_health: ArchiveHealthObserver | None = None,
        after_archive_before_commit: AfterArchiveHook | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._settings = settings
        self._consumer = consumer
        self._archive = archive
        self._on_health = on_health
        self._after_archive_before_commit = after_archive_before_commit
        self._clock_ms = clock_ms or _clock_ms
        now = self._clock_ms()
        self._health = ArchiveWriterHealth(
            state=ArchiveWriterState.STOPPED,
            ready=False,
            reason="not started",
            owner_id=settings.owner_id,
            kafka_group_id=settings.kafka_group_id,
            data_epoch=settings.data_epoch,
            committed_next_offset=None,
            segments_committed=0,
            events_archived=0,
            current_snapshot=None,
            started_at_ms=now,
            updated_at_ms=now,
            terminal_error=None,
        )
        self._local_stop = asyncio.Event()
        self._running = False
        self._consumer_started = False
        self._health_lock = asyncio.Lock()

    @property
    def health(self) -> ArchiveWriterHealth:
        return self._health

    def request_stop(self) -> None:
        self._local_stop.set()

    async def run(
        self,
        stop_event: asyncio.Event | None = None,
    ) -> ArchiveWriterHealth:
        if self._running:
            raise ArchiveWriterServiceError("archive writer is already running")
        self._running = True
        requested_stop = stop_event or self._local_stop
        primary_error: BaseException | None = None
        try:
            await self._transition(ArchiveWriterState.STARTING, "checking object store")
            await self._archive.initialize()
            await self._transition(ArchiveWriterState.STARTING, "joining Kafka group")
            await self._consumer.start()
            self._consumer_started = True
            await self._transition(
                ArchiveWriterState.RUNNING,
                "waiting for complete deterministic archive segments",
            )
            while not requested_stop.is_set() and not self._local_stop.is_set():
                batch = await self._consumer.poll(
                    timeout_ms=self._settings.poll_timeout_ms,
                    max_records=self._settings.segment_event_count,
                )
                if not batch:
                    continue
                result = await self._archive.append_records(
                    batch,
                    data_epoch=self._settings.data_epoch,
                )
                if self._after_archive_before_commit is not None:
                    await self._after_archive_before_commit(result)
                await self._consumer.commit_through(batch[-1])
                await self._transition(
                    ArchiveWriterState.RUNNING,
                    "immutable manifest published and Kafka offset committed",
                    result=result,
                )
        except asyncio.CancelledError as exc:
            primary_error = exc
        except Exception as exc:  # noqa: BLE001 - terminal service boundary
            primary_error = exc
            await self._transition(
                ArchiveWriterState.DEGRADED,
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
        await self._transition(ArchiveWriterState.STOPPING, "ordered shutdown")
        cleanup_error: BaseException | None = None
        if self._consumer_started:
            try:
                await asyncio.wait_for(
                    self._consumer.stop(),
                    timeout=self._settings.shutdown_timeout_ms / 1_000,
                )
            except Exception as exc:  # noqa: BLE001 - cleanup boundary
                cleanup_error = ArchiveWriterServiceError(
                    f"consumer stop failed: {exc}"
                )
            self._consumer_started = False
        terminal = primary_error or cleanup_error
        await self._transition(
            ArchiveWriterState.STOPPED,
            "stopped cleanly" if terminal is None else "stopped after failure",
            terminal_error=(
                None if terminal is None else f"{type(terminal).__name__}: {terminal}"
            ),
        )
        return cleanup_error

    async def _transition(
        self,
        state: ArchiveWriterState,
        reason: str,
        *,
        result: ArchiveAppendResult | None = None,
        terminal_error: str | None = None,
    ) -> None:
        async with self._health_lock:
            current = self._health
            self._health = ArchiveWriterHealth(
                state=state,
                ready=state is ArchiveWriterState.RUNNING,
                reason=reason,
                owner_id=current.owner_id,
                kafka_group_id=current.kafka_group_id,
                data_epoch=current.data_epoch,
                committed_next_offset=(
                    result.last_offset + 1
                    if result is not None
                    else current.committed_next_offset
                ),
                segments_committed=current.segments_committed + (result is not None),
                events_archived=current.events_archived
                + (result.commit.accepted_count if result is not None else 0),
                current_snapshot=(
                    result.commit.snapshot
                    if result is not None
                    else current.current_snapshot
                ),
                started_at_ms=current.started_at_ms,
                updated_at_ms=self._clock_ms(),
                terminal_error=(
                    terminal_error
                    if terminal_error is not None
                    else current.terminal_error
                ),
            )
            snapshot = self._health
        if self._on_health is not None:
            try:
                await self._on_health(snapshot)
            except Exception:
                logger.exception("archive writer health observer failed")


def _clock_ms() -> int:
    return time.time_ns() // 1_000_000
