from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import pytest
from app.data_engine.ingestion.models import (
    DataSource,
    GapMarker,
    MarketEvent,
    SessionHealth,
    StreamType,
)
from app.server_contracts import MarketEventEnvelopeV1, PublishReceipt
from app.server_runtime import (
    AggTradeCollectorService,
    CollectorGapError,
    CollectorState,
    ServerCollectorConfigurationError,
    ServerCollectorSettings,
)
from app.server_runtime.collector import LeasedAggTradeCollector
from app.server_runtime.publishers import PHASE1B_PARTITION_KEY
from app.server_runtime.sources import (
    PHASE1C_STREAM_DESCRIPTOR,
    BinanceAggTradeEventSource,
)
from app.server_runtime.testing import (
    InMemoryMarketEventLog,
    InMemoryStreamLeaseStore,
)


def _settings(owner_id: str) -> ServerCollectorSettings:
    return ServerCollectorSettings(
        postgres_dsn="postgresql://not-used",
        kafka_bootstrap_servers=("localhost:9092",),
        owner_id=owner_id,
        lease_ttl_ms=300,
        heartbeat_interval_ms=50,
        leadership_retry_ms=10,
        shutdown_timeout_ms=1_000,
    )


def _event(sequence: int) -> MarketEvent:
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=1_700_000_000_000 + sequence,
        received_at_ms=1_700_000_000_100 + sequence,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": sequence,
            "price": 100000.1,
            "quantity": 0.025,
            "price_text": "100000.1000",
            "quantity_text": "0.02500000",
            "first_trade_id": sequence * 10,
            "last_trade_id": sequence * 10 + 2,
            "trade_time_ms": 1_700_000_000_000 + sequence,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=sequence,
        market_type="futures",
    )


class _ManagedLog:
    def __init__(
        self,
        inner: InMemoryMarketEventLog,
        order: list[str] | None = None,
        name: str = "publisher",
    ) -> None:
        self.inner = inner
        self.order = order
        self.name = name
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        if self.order is not None:
            self.order.append(f"{self.name}.stop")
        self.started = False

    async def publish(
        self,
        events: Sequence[MarketEventEnvelopeV1],
    ) -> PublishReceipt:
        assert self.started
        return await self.inner.publish(events)


class _ManualSource:
    def __init__(self, order: list[str] | None = None, name: str = "source") -> None:
        self.order = order
        self.name = name
        self.start_calls = 0
        self.stop_calls = 0
        self.started_event = asyncio.Event()
        self._on_event: Callable[[MarketEvent], Awaitable[None]] | None = None
        self._on_gap: Callable[[GapMarker], Awaitable[None]] | None = None

    async def start(
        self,
        on_event: Callable[[MarketEvent], Awaitable[None]],
        *,
        on_gap: Callable[[GapMarker], Awaitable[None]],
        on_health: Callable[[SessionHealth, str], Awaitable[None]],
    ) -> None:
        self.start_calls += 1
        self._on_event = on_event
        self._on_gap = on_gap
        await on_health(SessionHealth.CONNECTED, "manual source ready")
        self.started_event.set()

    async def stop(self) -> None:
        self.stop_calls += 1
        if self.order is not None:
            self.order.append(f"{self.name}.stop")

    async def emit(self, event: MarketEvent) -> None:
        assert self._on_event is not None
        await self._on_event(event)

    async def emit_gap(self, gap: GapMarker) -> None:
        assert self._on_gap is not None
        await self._on_gap(gap)


def test_settings_are_strict_and_keep_postgres_dsn_out_of_repr() -> None:
    names = {
        "CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN": "postgresql://secret",
        "CANDLESCOPE_SERVER_COLLECTOR_KAFKA_BOOTSTRAP_SERVERS": (
            "kafka-a:9092,kafka-b:9092"
        ),
        "CANDLESCOPE_SERVER_COLLECTOR_OWNER_ID": "collector-a",
        "CANDLESCOPE_SERVER_COLLECTOR_LEASE_TTL_MS": "300",
        "CANDLESCOPE_SERVER_COLLECTOR_HEARTBEAT_INTERVAL_MS": "100",
    }
    settings = ServerCollectorSettings.from_env(names)
    assert settings.kafka_bootstrap_servers == ("kafka-a:9092", "kafka-b:9092")
    assert "secret" not in repr(settings)

    with pytest.raises(ServerCollectorConfigurationError, match="POSTGRES_DSN"):
        ServerCollectorSettings.from_env({})
    with pytest.raises(ServerCollectorConfigurationError, match="unsigned"):
        ServerCollectorSettings.from_env(
            {**names, "CANDLESCOPE_SERVER_COLLECTOR_LEASE_TTL_MS": " 300"}
        )
    with pytest.raises(ServerCollectorConfigurationError, match="one third"):
        ServerCollectorSettings.from_env(
            {
                **names,
                "CANDLESCOPE_SERVER_COLLECTOR_HEARTBEAT_INTERVAL_MS": "101",
            }
        )
    with pytest.raises(ServerCollectorConfigurationError, match="tuple"):
        ServerCollectorSettings(
            postgres_dsn="postgresql://secret",
            kafka_bootstrap_servers="localhost:9092",  # type: ignore[arg-type]
            owner_id="collector-a",
        )


def test_heartbeat_renews_while_kafka_publish_is_blocked() -> None:
    class BlockingPublisher:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def publish(self, events: Any) -> PublishReceipt:
            self.entered.set()
            await self.release.wait()
            return PublishReceipt(
                accepted_count=1,
                partition_offsets=((PHASE1B_PARTITION_KEY, 0),),
            )

    async def run() -> None:
        store = InMemoryStreamLeaseStore()
        publisher = BlockingPublisher()
        collector = await LeasedAggTradeCollector.acquire(
            lease_store=store,
            publisher=publisher,
            owner_id="collector-a",
            lease_ttl_ms=1_000,
        )
        publish_task = asyncio.create_task(collector.handle(_event(42)))
        await asyncio.wait_for(publisher.entered.wait(), timeout=1)
        before = collector.lease.lease_expires_at_ms
        renewed = await asyncio.wait_for(collector.renew(), timeout=0.2)
        assert renewed.lease_expires_at_ms >= before
        assert not publish_task.done()
        publisher.release.set()
        await publish_task
        assert collector.lease.last_sequence == 42

    asyncio.run(run())


def test_two_services_enforce_standby_then_take_over_after_graceful_release() -> None:
    async def run() -> None:
        store = InMemoryStreamLeaseStore()
        log = InMemoryMarketEventLog()
        source_a = _ManualSource(name="source-a")
        source_b = _ManualSource(name="source-b")
        stop_a = asyncio.Event()
        stop_b = asyncio.Event()
        service_a = AggTradeCollectorService(
            settings=_settings("collector-a"),
            lease_store=store,
            publisher=_ManagedLog(log, name="publisher-a"),
            source=source_a,
        )
        service_b = AggTradeCollectorService(
            settings=_settings("collector-b"),
            lease_store=store,
            publisher=_ManagedLog(log, name="publisher-b"),
            source=source_b,
        )
        task_a = asyncio.create_task(service_a.run(stop_a))
        await asyncio.wait_for(source_a.started_event.wait(), timeout=1)
        assert service_a.health.state is CollectorState.LEADER
        assert service_a.health.ready
        await source_a.emit(_event(42))

        task_b = asyncio.create_task(service_b.run(stop_b))
        await _wait_for_state(service_b, CollectorState.STANDBY)
        assert source_b.start_calls == 0

        stop_a.set()
        await asyncio.wait_for(task_a, timeout=1)
        await asyncio.wait_for(source_b.started_event.wait(), timeout=1)
        assert service_b.health.state is CollectorState.LEADER
        assert service_b.health.producer_epoch == 1
        await source_b.emit(_event(43))

        stop_b.set()
        await asyncio.wait_for(task_b, timeout=1)
        assert [event.sequence_end for event in log.events] == [42, 43]
        assert [event.producer_epoch for event in log.events] == [0, 1]
        durable = await store.inspect(PHASE1B_PARTITION_KEY)
        assert durable is not None
        assert durable.last_sequence == 43
        assert durable.pending_envelope is None

    asyncio.run(run())


def test_unresolved_gap_is_terminal_even_when_delivery_would_swallow_callback() -> None:
    async def run() -> None:
        source = _ManualSource()
        service = AggTradeCollectorService(
            settings=_settings("collector-a"),
            lease_store=InMemoryStreamLeaseStore(),
            publisher=_ManagedLog(InMemoryMarketEventLog()),
            source=source,
        )
        task = asyncio.create_task(service.run())
        await asyncio.wait_for(source.started_event.wait(), timeout=1)
        gap = GapMarker(
            stream_key="futures:BTCUSDT@aggTrade",
            symbol="BTCUSDT",
            stream_type=StreamType.AGG_TRADE,
            gap_start=42,
            gap_end=44,
            expected_count=1,
        )
        with pytest.raises(CollectorGapError):
            await source.emit_gap(gap)
        with pytest.raises(CollectorGapError):
            await asyncio.wait_for(task, timeout=1)
        assert service.health.state is CollectorState.STOPPED
        assert "CollectorGapError" in (service.health.terminal_error or "")

    asyncio.run(run())


def test_hung_heartbeat_fences_service_before_lease_ttl() -> None:
    class HungRenewStore(InMemoryStreamLeaseStore):
        async def renew(self, lease: Any, *, lease_ttl_ms: int) -> Any:
            del lease, lease_ttl_ms
            await asyncio.Event().wait()

    async def run() -> None:
        source = _ManualSource()
        settings = ServerCollectorSettings(
            postgres_dsn="postgresql://not-used",
            kafka_bootstrap_servers=("localhost:9092",),
            owner_id="collector-a",
            lease_ttl_ms=60,
            heartbeat_interval_ms=20,
            leadership_retry_ms=10,
            shutdown_timeout_ms=1_000,
        )
        service = AggTradeCollectorService(
            settings=settings,
            lease_store=HungRenewStore(),
            publisher=_ManagedLog(InMemoryMarketEventLog()),
            source=source,
        )
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(service.run(), timeout=1)
        assert service.health.heartbeat_failures == 1
        assert "TimeoutError" in (service.health.terminal_error or "")

    asyncio.run(run())


def test_graceful_shutdown_stops_source_before_release_and_publisher() -> None:
    class OrderedStore(InMemoryStreamLeaseStore):
        def __init__(self, order: list[str]) -> None:
            super().__init__()
            self.order = order

        async def release(self, lease: Any) -> None:
            self.order.append("lease.release")
            await super().release(lease)

    async def run() -> None:
        order: list[str] = []
        source = _ManualSource(order)
        stop = asyncio.Event()
        service = AggTradeCollectorService(
            settings=_settings("collector-a"),
            lease_store=OrderedStore(order),
            publisher=_ManagedLog(InMemoryMarketEventLog(), order),
            source=source,
        )
        task = asyncio.create_task(service.run(stop))
        await asyncio.wait_for(source.started_event.wait(), timeout=1)
        stop.set()
        health = await asyncio.wait_for(task, timeout=1)
        assert order == ["source.stop", "lease.release", "publisher.stop"]
        assert health.state is CollectorState.STOPPED
        assert health.terminal_error is None

    asyncio.run(run())


def test_production_source_uses_frozen_descriptor_and_owns_factory_shutdown() -> None:
    class Handle:
        async def stop(self) -> bool:
            return True

    class Factory:
        def __init__(self) -> None:
            self.descriptor: Any = None
            self.shutdown_calls = 0

        async def start_market(self, descriptor: Any, *_: Any, **__: Any) -> Handle:
            self.descriptor = descriptor
            return Handle()

        async def shutdown(self) -> None:
            self.shutdown_calls += 1

    async def run() -> None:
        factory = Factory()
        source = BinanceAggTradeEventSource(factory)  # type: ignore[arg-type]

        async def ignore(*_: Any) -> None:
            return None

        await source.start(ignore, on_gap=ignore, on_health=ignore)
        assert factory.descriptor is PHASE1C_STREAM_DESCRIPTOR
        assert factory.descriptor.key == "futures:BTCUSDT@aggTrade"
        await source.stop()
        assert factory.shutdown_calls == 1

    asyncio.run(run())


async def _wait_for_state(
    service: AggTradeCollectorService,
    state: CollectorState,
) -> None:
    deadline = asyncio.get_running_loop().time() + 1
    while service.health.state is not state:
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"service did not enter {state.value}")
        await asyncio.sleep(0.005)
