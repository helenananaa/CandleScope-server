"""Proof-oriented hot/cold routing with shared Phase 1H quarantine state."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
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
from app.server_runtime.query_control import (
    ClearHotProjectionQuarantineCommand,
    HotProjectionControlState,
    HotProjectionControlStore,
    InProcessHotProjectionControlStore,
)

logger = logging.getLogger(__name__)


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


class HotProjectionQuarantinedError(SnapshotQueryRouteError):
    """Background or foreground parity proof quarantined the hot backend."""


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
    hot_quarantined: bool


@dataclass(frozen=True, slots=True)
class SnapshotParityProbe:
    snapshot: MarketDataSnapshotRef
    stream: MarketStreamKey
    start_event_time_ms: int
    end_event_time_ms: int
    limit: int
    cursor: MarketEventCursor | None


@dataclass(frozen=True, slots=True)
class SnapshotParityStatus:
    registered_probes: int
    samples_total: int
    samples_passed: int
    samples_skipped_lag: int
    samples_failed: int
    hot_quarantined: bool
    quarantine_reason: str | None
    quarantine_generation: int
    quarantine_latched_by: str | None
    last_sample_at_ms: int | None

    def to_wire(self) -> dict[str, object]:
        return {
            "registered_probes": self.registered_probes,
            "samples_total": self.samples_total,
            "samples_passed": self.samples_passed,
            "samples_skipped_lag": self.samples_skipped_lag,
            "samples_failed": self.samples_failed,
            "hot_quarantined": self.hot_quarantined,
            "quarantine_reason": self.quarantine_reason,
            "quarantine_generation": self.quarantine_generation,
            "quarantine_latched_by": self.quarantine_latched_by,
            "last_sample_at_ms": self.last_sample_at_ms,
        }


class SnapshotQueryRouter:
    """Use hot only after cursor coverage and exact cold-page parity proof."""

    def __init__(
        self,
        *,
        cold_query: LifecycleSnapshotMarketEventQuery,
        hot_query: LifecycleSnapshotMarketEventQuery,
        projection_cursor: ProjectionCursorReader,
        quarantine_store: HotProjectionControlStore | None = None,
        parity_sample_interval_ms: int = 30_000,
        parity_probe_capacity: int = 128,
    ) -> None:
        self._cold_query = cold_query
        self._hot_query = hot_query
        self._projection_cursor = projection_cursor
        self._quarantine_store = (
            InProcessHotProjectionControlStore()
            if quarantine_store is None
            else quarantine_store
        )
        if not isinstance(self._quarantine_store, HotProjectionControlStore):
            raise TypeError("quarantine_store must implement HotProjectionControlStore")
        self._parity_sample_interval_ms = _positive_int(
            parity_sample_interval_ms,
            field="parity_sample_interval_ms",
        )
        self._parity_probe_capacity = _positive_int(
            parity_probe_capacity,
            field="parity_probe_capacity",
        )
        self._started = False
        self._parity_task: asyncio.Task[None] | None = None
        self._probes: OrderedDict[tuple[object, ...], SnapshotParityProbe] = (
            OrderedDict()
        )
        self._samples_total = 0
        self._samples_passed = 0
        self._samples_skipped_lag = 0
        self._samples_failed = 0
        self._quarantine_state = HotProjectionControlState.initial(
            "clickhouse-market-events-v1"
        )
        self._last_sample_at_ms: int | None = None

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        if self._started:
            raise SnapshotQueryRouteError("snapshot query router is already started")
        control_started = False
        cold_started = False
        hot_started = False
        cursor_started = False
        try:
            await self._quarantine_store.start()
            control_started = True
            await self._cold_query.start()
            cold_started = True
            await self._hot_query.start()
            hot_started = True
            await self._projection_cursor.start()
            cursor_started = True
            await self._refresh_quarantine()
        except BaseException:
            try:
                if cursor_started:
                    await self._projection_cursor.stop()
            finally:
                try:
                    if hot_started:
                        await self._hot_query.stop()
                finally:
                    try:
                        if cold_started:
                            await self._cold_query.stop()
                    finally:
                        if control_started:
                            await self._quarantine_store.stop()
            raise
        self._started = True
        self._parity_task = asyncio.create_task(
            self._parity_loop(),
            name="candlescope-phase1h-parity-sampler",
        )

    async def stop(self) -> None:
        self._started = False
        parity_task = self._parity_task
        self._parity_task = None
        if parity_task is not None:
            parity_task.cancel()
            try:
                await parity_task
            except asyncio.CancelledError:
                pass
        try:
            try:
                await self._projection_cursor.stop()
            finally:
                await self._hot_query.stop()
        finally:
            try:
                await self._cold_query.stop()
            finally:
                await self._quarantine_store.stop()

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
                hot_quarantined=self._quarantine_state.active,
            )
        await self._refresh_quarantine()
        if self._quarantine_state.active:
            if preference is QueryPreference.HOT:
                raise HotProjectionQuarantinedError(
                    "ClickHouse hot queries are quarantined after a parity failure"
                )
            return SnapshotQueryRouteResult(
                backend="cold",
                page=cold_page,
                hot_committed_next_offset=None,
                parity_verified=False,
                hot_quarantined=True,
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
                hot_quarantined=False,
            )
        hot_page = await self._hot_query.query(**arguments)
        if hot_page != cold_page:
            await self._quarantine("foreground hot/cold page mismatch")
            raise HotProjectionParityError(
                "ClickHouse and Parquet pages differ for the requested snapshot"
            )
        await self._refresh_quarantine()
        if self._quarantine_state.active:
            if preference is QueryPreference.HOT:
                raise HotProjectionQuarantinedError(
                    "ClickHouse hot queries were quarantined during parity proof"
                )
            return SnapshotQueryRouteResult(
                backend="cold",
                page=cold_page,
                hot_committed_next_offset=committed,
                parity_verified=False,
                hot_quarantined=True,
            )
        self._register_probe(
            SnapshotParityProbe(
                snapshot=snapshot,
                stream=stream,
                start_event_time_ms=start_event_time_ms,
                end_event_time_ms=end_event_time_ms,
                limit=limit,
                cursor=cursor,
            )
        )
        return SnapshotQueryRouteResult(
            backend="hot",
            page=hot_page,
            hot_committed_next_offset=committed,
            parity_verified=True,
            hot_quarantined=False,
        )

    def parity_status(self) -> SnapshotParityStatus:
        return SnapshotParityStatus(
            registered_probes=len(self._probes),
            samples_total=self._samples_total,
            samples_passed=self._samples_passed,
            samples_skipped_lag=self._samples_skipped_lag,
            samples_failed=self._samples_failed,
            hot_quarantined=self._quarantine_state.active,
            quarantine_reason=self._quarantine_state.reason,
            quarantine_generation=self._quarantine_state.generation,
            quarantine_latched_by=self._quarantine_state.latched_by,
            last_sample_at_ms=self._last_sample_at_ms,
        )

    async def hot_control_status(
        self,
        *,
        refresh: bool = True,
    ) -> HotProjectionControlState:
        if not self._started:
            raise SnapshotQueryRouteError("snapshot query router is not started")
        if refresh:
            await self._refresh_quarantine()
        return self._quarantine_state

    async def clear_hot_quarantine(
        self,
        command: ClearHotProjectionQuarantineCommand,
    ) -> HotProjectionControlState:
        if not self._started:
            raise SnapshotQueryRouteError("snapshot query router is not started")
        state = await self._quarantine_store.clear(command)
        self._apply_quarantine_state(state)
        logger.warning(
            "ClickHouse snapshot query quarantine generation %s cleared by %s",
            state.generation,
            command.principal,
        )
        return state

    def _register_probe(self, probe: SnapshotParityProbe) -> None:
        key = (
            probe.snapshot,
            probe.stream,
            probe.start_event_time_ms,
            probe.end_event_time_ms,
            probe.limit,
            probe.cursor,
        )
        self._probes.pop(key, None)
        self._probes[key] = probe
        while len(self._probes) > self._parity_probe_capacity:
            self._probes.popitem(last=False)

    async def _parity_loop(self) -> None:
        while True:
            await asyncio.sleep(self._parity_sample_interval_ms / 1_000)
            try:
                await self._refresh_quarantine()
            except asyncio.CancelledError:
                raise
            except (RuntimeError, TypeError, ValueError) as exc:
                logger.warning("shared snapshot quarantine refresh failed: %s", exc)
                continue
            if self._quarantine_state.active:
                continue
            probe = self._next_probe()
            if probe is None:
                continue
            await self._sample_probe(probe)

    def _next_probe(self) -> SnapshotParityProbe | None:
        if not self._probes:
            return None
        key, probe = self._probes.popitem(last=False)
        self._probes[key] = probe
        return probe

    async def _sample_probe(self, probe: SnapshotParityProbe) -> None:
        self._samples_total += 1
        try:
            committed = await self._projection_cursor.committed_next_offset()
            if committed is None or committed < probe.snapshot.snapshot_version:
                self._samples_skipped_lag += 1
                return
            arguments = {
                "snapshot": probe.snapshot,
                "stream": probe.stream,
                "start_event_time_ms": probe.start_event_time_ms,
                "end_event_time_ms": probe.end_event_time_ms,
                "limit": probe.limit,
                "cursor": probe.cursor,
            }
            cold_page = await self._cold_query.query(**arguments)
            hot_page = await self._hot_query.query(**arguments)
            if hot_page != cold_page:
                self._samples_failed += 1
                await self._quarantine("background hot/cold page mismatch")
                return
            self._samples_passed += 1
        except asyncio.CancelledError:
            raise
        except (RuntimeError, TypeError, ValueError) as exc:
            self._samples_failed += 1
            logger.warning("background snapshot parity probe failed: %s", exc)
        finally:
            self._last_sample_at_ms = time.time_ns() // 1_000_000

    async def _refresh_quarantine(self) -> None:
        self._apply_quarantine_state(await self._quarantine_store.status())

    async def _quarantine(self, reason: str) -> None:
        was_active = self._quarantine_state.active
        state = await self._quarantine_store.latch(reason)
        self._apply_quarantine_state(state)
        if not was_active and state.active:
            logger.error(
                "ClickHouse snapshot query backend quarantined at generation %s: %s",
                state.generation,
                reason,
            )

    def _apply_quarantine_state(self, state: HotProjectionControlState) -> None:
        if not isinstance(state, HotProjectionControlState):
            raise TypeError("quarantine store returned an invalid state")
        self._quarantine_state = state


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value
