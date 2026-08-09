"""Always-on, single-leader Phase 1C aggregate-trade collector service."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from app.data_engine.ingestion.models import GapMarker, MarketEvent, SessionHealth
from app.server_contracts import MarketEventEnvelopeV1, PublishReceipt
from app.server_runtime.collector import LeasedAggTradeCollector
from app.server_runtime.health import CollectorHealth, CollectorState
from app.server_runtime.leases import (
    StreamLeaseBusyError,
    StreamLeaseError,
    StreamLeaseStore,
)
from app.server_runtime.settings import ServerCollectorSettings
from app.server_runtime.sources import AggTradeEventSource

logger = logging.getLogger("server_runtime.collector_service")

HealthObserver = Callable[[CollectorHealth], Awaitable[None]]


class ManagedMarketEventPublisher(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(
        self,
        events: Sequence[MarketEventEnvelopeV1],
    ) -> PublishReceipt: ...


class CollectorServiceError(RuntimeError):
    """Base error for a terminal Phase 1C service failure."""


class CollectorGapError(CollectorServiceError):
    """An unresolved delivery-layer gap makes the stream unsafe to publish."""


class CollectorShutdownError(CollectorServiceError):
    """One or more ordered shutdown steps did not complete."""


class AggTradeCollectorService:
    """Acquire leadership, run ingestion, heartbeat, and stop in strict order."""

    def __init__(
        self,
        *,
        settings: ServerCollectorSettings,
        lease_store: StreamLeaseStore,
        publisher: ManagedMarketEventPublisher,
        source: AggTradeEventSource,
        on_health: HealthObserver | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._settings = settings
        self._lease_store = lease_store
        self._publisher = publisher
        self._source = source
        self._on_health = on_health
        self._clock_ms = clock_ms or _system_clock_ms
        started_at_ms = self._clock_ms()
        self._health = CollectorHealth(
            state=CollectorState.STOPPED,
            ready=False,
            reason="not started",
            owner_id=settings.owner_id,
            source_health=None,
            producer_epoch=None,
            lease_expires_at_ms=None,
            last_sequence=None,
            last_partition_offset=None,
            pending_event_id=None,
            events_published=0,
            heartbeat_successes=0,
            heartbeat_failures=0,
            started_at_ms=started_at_ms,
            updated_at_ms=started_at_ms,
            terminal_error=None,
        )
        self._collector: LeasedAggTradeCollector | None = None
        self._source_health: SessionHealth | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._local_stop = asyncio.Event()
        self._fatal_event = asyncio.Event()
        self._fatal_error: BaseException | None = None
        self._publisher_started = False
        self._source_started = False
        self._running = False
        self._shutdown_started = False
        self._health_lock = asyncio.Lock()

    @property
    def health(self) -> CollectorHealth:
        return self._health

    def request_stop(self) -> None:
        self._local_stop.set()

    async def run(self, stop_event: asyncio.Event | None = None) -> CollectorHealth:
        if self._running:
            raise CollectorServiceError("collector service is already running")
        self._running = True
        external_stop = stop_event or self._local_stop
        primary_error: BaseException | None = None
        try:
            await self._transition(CollectorState.STARTING, "starting publisher")
            await self._publisher.start()
            self._publisher_started = True
            acquired = await self._acquire_leadership(external_stop)
            if not acquired:
                await self._finish_without_leadership()
            else:
                collector = self._require_collector()
                recovery_reason = (
                    "replaying durable pending event"
                    if collector.lease.pending_envelope is not None
                    else "starting exchange ingestion"
                )
                await self._transition(CollectorState.RECOVERING, recovery_reason)
                self._heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(),
                    name="candlescope-phase1c-heartbeat",
                )
                await self._source.start(
                    self._on_event,
                    on_gap=self._on_gap,
                    on_health=self._on_source_health,
                )
                self._source_started = True
                await self._wait_for_stop_or_failure(external_stop)
                if self._fatal_error is not None:
                    primary_error = self._fatal_error
        except asyncio.CancelledError as exc:
            primary_error = exc
        except Exception as exc:  # noqa: BLE001 - terminal service boundary
            primary_error = exc
            if self._fatal_error is None:
                await self._record_failure(exc)
        finally:
            teardown_error = await self._teardown(primary_error)
            self._running = False
            if primary_error is None:
                primary_error = teardown_error

        if primary_error is not None:
            raise primary_error
        return self._health

    async def _acquire_leadership(self, stop_event: asyncio.Event) -> bool:
        while not stop_event.is_set() and not self._local_stop.is_set():
            try:
                self._collector = await LeasedAggTradeCollector.acquire(
                    lease_store=self._lease_store,
                    publisher=self._publisher,
                    owner_id=self._settings.owner_id,
                    lease_ttl_ms=self._settings.lease_ttl_ms,
                    clock_ms=self._clock_ms,
                )
                return True
            except StreamLeaseBusyError:
                await self._transition(
                    CollectorState.STANDBY,
                    "another collector owns the stream lease",
                )
                if await _event_or_timeout(
                    (stop_event, self._local_stop),
                    self._settings.leadership_retry_ms / 1_000,
                ):
                    return False
        return False

    async def _heartbeat_loop(self) -> None:
        interval = self._settings.heartbeat_interval_ms / 1_000
        while True:
            await asyncio.sleep(interval)
            collector = self._require_collector()
            try:
                # A database call that never returns must not leave a process
                # reporting ready past its lease deadline. With the enforced
                # 3:1 TTL ratio this fails closed before the last lease expires.
                await asyncio.wait_for(collector.renew(), timeout=interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - heartbeat must fail closed
                await self._record_failure(
                    exc,
                    ownership_ambiguous=True,
                    heartbeat_failure=True,
                )
                return
            await self._refresh_health(heartbeat_success_delta=1)

    async def _on_event(self, event: MarketEvent) -> None:
        try:
            collector = self._require_collector()
            await collector.handle(event)
            state = (
                CollectorState.LEADER
                if self._source_health is SessionHealth.CONNECTED
                else CollectorState.RECOVERING
            )
            await self._transition(
                state,
                "publishing live aggregate trades",
                events_published_delta=1,
            )
        except asyncio.CancelledError:
            if not self._shutdown_started:
                await self._record_failure(
                    CollectorServiceError("event delivery was cancelled unexpectedly")
                )
            raise
        except BaseException as exc:
            collector = self._collector
            await self._record_failure(
                exc,
                ownership_ambiguous=collector is not None and collector.failed,
            )
            raise

    async def _on_gap(self, gap: GapMarker) -> None:
        error = CollectorGapError(
            "unresolved aggregate-trade gap "
            f"{gap.gap_start}->{gap.gap_end} ({gap.expected_count} missing)"
        )
        await self._record_failure(error)
        raise error

    async def _on_source_health(self, health: SessionHealth, reason: str) -> None:
        self._source_health = health
        collector = self._collector
        has_pending = (
            collector is not None and collector.lease.pending_envelope is not None
        )
        if health is SessionHealth.CONNECTED and not has_pending:
            state = CollectorState.LEADER
        elif health in {SessionHealth.CONNECTING, SessionHealth.RECONNECTING}:
            state = CollectorState.RECOVERING
        else:
            state = CollectorState.DEGRADED
        await self._transition(state, f"source {health.value}: {reason}")

    async def _record_failure(
        self,
        error: BaseException,
        *,
        ownership_ambiguous: bool = False,
        heartbeat_failure: bool = False,
    ) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
            self._fatal_event.set()
        state = (
            CollectorState.FENCED
            if ownership_ambiguous or isinstance(error, StreamLeaseError)
            else CollectorState.DEGRADED
        )
        await self._transition(
            state,
            f"terminal {type(error).__name__}",
            heartbeat_failure_delta=(1 if heartbeat_failure else 0),
            terminal_error=f"{type(error).__name__}: {error}",
        )

    async def _wait_for_stop_or_failure(self, stop_event: asyncio.Event) -> None:
        await _wait_for_any((stop_event, self._local_stop, self._fatal_event))

    async def _finish_without_leadership(self) -> CollectorHealth:
        await self._transition(CollectorState.STOPPING, "stop requested in standby")
        return self._health

    async def _teardown(
        self,
        primary_error: BaseException | None,
    ) -> BaseException | None:
        self._shutdown_started = True
        await self._transition(CollectorState.STOPPING, "ordered shutdown")
        errors: list[BaseException] = []
        timeout = self._settings.shutdown_timeout_ms / 1_000

        if self._source_started:
            error = await _cleanup_step(self._source.stop(), timeout, "source stop")
            self._source_started = False
            if error is not None:
                errors.append(error)

        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat is not None:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 - retain task shutdown failure
                errors.append(exc)

        collector = self._collector
        if collector is not None and not collector.failed:
            error = await _cleanup_step(collector.release(), timeout, "lease release")
            if error is not None:
                errors.append(error)

        if self._publisher_started:
            error = await _cleanup_step(
                self._publisher.stop(),
                timeout,
                "publisher stop",
            )
            self._publisher_started = False
            if error is not None:
                errors.append(error)

        terminal = primary_error or (errors[0] if errors else None)
        await self._transition(
            CollectorState.STOPPED,
            "stopped cleanly" if terminal is None else "stopped after failure",
            terminal_error=(
                None if terminal is None else f"{type(terminal).__name__}: {terminal}"
            ),
        )
        if errors:
            details = "; ".join(str(error) for error in errors)
            return CollectorShutdownError(f"ordered shutdown failed: {details}")
        return None

    async def _transition(
        self,
        state: CollectorState | None,
        reason: str,
        *,
        events_published_delta: int = 0,
        heartbeat_success_delta: int = 0,
        heartbeat_failure_delta: int = 0,
        terminal_error: str | None = None,
    ) -> None:
        async with self._health_lock:
            lease = self._collector.lease if self._collector is not None else None
            source_health = (
                self._source_health.value if self._source_health is not None else None
            )
            current = self._health
            state = current.state if state is None else state
            snapshot = CollectorHealth(
                state=state,
                ready=(
                    state is CollectorState.LEADER
                    and self._source_health is SessionHealth.CONNECTED
                    and lease is not None
                    and lease.lease_expires_at_ms > self._clock_ms()
                ),
                reason=reason,
                owner_id=self._settings.owner_id,
                source_health=source_health,
                producer_epoch=lease.producer_epoch if lease is not None else None,
                lease_expires_at_ms=(
                    lease.lease_expires_at_ms if lease is not None else None
                ),
                last_sequence=lease.last_sequence if lease is not None else None,
                last_partition_offset=(
                    lease.last_partition_offset if lease is not None else None
                ),
                pending_event_id=(
                    lease.pending_envelope.event_id
                    if lease is not None and lease.pending_envelope is not None
                    else None
                ),
                events_published=current.events_published + events_published_delta,
                heartbeat_successes=(
                    current.heartbeat_successes + heartbeat_success_delta
                ),
                heartbeat_failures=(
                    current.heartbeat_failures + heartbeat_failure_delta
                ),
                started_at_ms=current.started_at_ms,
                updated_at_ms=self._clock_ms(),
                terminal_error=(
                    terminal_error
                    if terminal_error is not None
                    else current.terminal_error
                ),
            )
            self._health = snapshot
        observer = self._on_health
        if observer is not None:
            try:
                await observer(snapshot)
            except Exception:
                logger.exception("collector health observer failed")

    async def _refresh_health(self, *, heartbeat_success_delta: int) -> None:
        await self._transition(
            None,
            "lease heartbeat renewed",
            heartbeat_success_delta=heartbeat_success_delta,
        )

    def _require_collector(self) -> LeasedAggTradeCollector:
        if self._collector is None:
            raise CollectorServiceError("collector leadership is not active")
        return self._collector


async def _wait_for_any(events: tuple[asyncio.Event, ...]) -> None:
    tasks = [asyncio.create_task(event.wait()) for event in events]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _event_or_timeout(
    events: tuple[asyncio.Event, ...],
    timeout: float,
) -> bool:
    try:
        await asyncio.wait_for(_wait_for_any(events), timeout=timeout)
        return True
    except TimeoutError:
        return False


async def _cleanup_step(
    operation: Awaitable[None],
    timeout: float,
    name: str,
) -> BaseException | None:
    try:
        await asyncio.wait_for(operation, timeout=timeout)
    except Exception as exc:  # noqa: BLE001 - aggregate ordered cleanup failures
        return CollectorShutdownError(f"{name} failed: {exc}")
    return None


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000
