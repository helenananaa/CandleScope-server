"""Phase 1C source adapter over CandleScope's existing six-layer ingestion."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from app.data_engine.ingestion.factory import ExchangeIngestionFactory
from app.data_engine.ingestion.models import (
    GapMarker,
    MarketEvent,
    SessionHealth,
    StreamDescriptor,
    StreamType,
)

EventCallback = Callable[[MarketEvent], Awaitable[None]]
GapCallback = Callable[[GapMarker], Awaitable[None]]
HealthCallback = Callable[[SessionHealth, str], Awaitable[None]]

PHASE1C_STREAM_DESCRIPTOR = StreamDescriptor(
    symbol="BTCUSDT",
    stream_type=StreamType.AGG_TRADE,
    exchange="binance",
    market_type="futures",
)


@runtime_checkable
class AggTradeEventSource(Protocol):
    async def start(
        self,
        on_event: EventCallback,
        *,
        on_gap: GapCallback,
        on_health: HealthCallback,
    ) -> None: ...

    async def stop(self) -> None: ...


class BinanceAggTradeEventSource:
    """Own exactly one production BTCUSDT futures aggTrade pipeline."""

    def __init__(self, factory: ExchangeIngestionFactory | None = None) -> None:
        self._factory = factory or ExchangeIngestionFactory()
        self._handle: Any | None = None

    @property
    def started(self) -> bool:
        return self._handle is not None

    async def start(
        self,
        on_event: EventCallback,
        *,
        on_gap: GapCallback,
        on_health: HealthCallback,
    ) -> None:
        if self._handle is not None:
            raise RuntimeError("aggTrade event source is already started")
        try:
            self._handle = await self._factory.start_market(
                PHASE1C_STREAM_DESCRIPTOR,
                on_event,
                on_gap=on_gap,
                on_health=on_health,
            )
        except BaseException:
            await self._factory.shutdown()
            raise

    async def stop(self) -> None:
        handle = self._handle
        self._handle = None
        stop_error: BaseException | None = None
        if handle is not None:
            try:
                stopped = await handle.stop()
                if not stopped:
                    stop_error = RuntimeError("ingestion handle failed to stop")
            except Exception as exc:  # noqa: BLE001 - preserve arbitrary adapter failure
                stop_error = exc
        try:
            await self._factory.shutdown()
        except Exception as exc:  # noqa: BLE001 - preserve arbitrary adapter failure
            if stop_error is None:
                stop_error = exc
        if stop_error is not None:
            raise stop_error
