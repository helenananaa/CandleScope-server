"""Deterministic process worker for the Phase 1C takeover gate."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
from pathlib import Path
from typing import Any

from app.data_engine.ingestion.models import (
    DataSource,
    MarketEvent,
    SessionHealth,
    StreamType,
)
from app.server_runtime import (
    AggTradeCollectorService,
    CollectorHealth,
    ServerCollectorSettings,
)
from app.server_runtime.publishers import KafkaMarketEventPublisher
from app.server_runtime.storage import PostgresStreamLeaseStore


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


class ScriptedAggTradeSource:
    """Emit one event only after this process actually becomes leader."""

    def __init__(self, sequence: int) -> None:
        self._sequence = sequence
        self._task: asyncio.Task[None] | None = None

    async def start(
        self,
        on_event: Any,
        *,
        on_gap: Any,
        on_health: Any,
    ) -> None:
        del on_gap
        await on_health(SessionHealth.CONNECTED, "scripted process source ready")

        async def emit() -> None:
            await asyncio.sleep(0.05)
            await on_event(_event(self._sequence))

        self._task = asyncio.create_task(emit(), name="phase1c-scripted-event")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        if not task.done():
            task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class AtomicHealthFile:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._temporary = path.with_suffix(f"{path.suffix}.tmp")

    async def __call__(self, health: CollectorHealth) -> None:
        self._temporary.write_text(
            json.dumps(health.to_wire(), sort_keys=True),
            encoding="utf-8",
        )
        self._temporary.replace(self._path)


async def _run(args: argparse.Namespace) -> CollectorHealth:
    settings = ServerCollectorSettings(
        postgres_dsn=args.postgres_dsn,
        kafka_bootstrap_servers=(args.bootstrap_servers,),
        owner_id=args.owner_id,
        lease_ttl_ms=args.lease_ttl_ms,
        heartbeat_interval_ms=args.heartbeat_interval_ms,
        leadership_retry_ms=args.leadership_retry_ms,
        shutdown_timeout_ms=2_000,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for requested_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(requested_signal, stop_event.set)
    service = AggTradeCollectorService(
        settings=settings,
        lease_store=PostgresStreamLeaseStore(settings.postgres_dsn),
        publisher=KafkaMarketEventPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.owner_id}-phase1c-gate",
        ),
        source=ScriptedAggTradeSource(args.sequence),
        on_health=AtomicHealthFile(args.health_file),
    )
    return await service.run(stop_event)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres-dsn", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--sequence", required=True, type=int)
    parser.add_argument("--health-file", required=True, type=Path)
    parser.add_argument("--lease-ttl-ms", default=1_200, type=int)
    parser.add_argument("--heartbeat-interval-ms", default=300, type=int)
    parser.add_argument("--leadership-retry-ms", default=100, type=int)
    health = asyncio.run(_run(parser.parse_args()))
    print(json.dumps(health.to_wire(), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
