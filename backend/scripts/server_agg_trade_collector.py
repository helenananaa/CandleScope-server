"""Standalone Phase 1C BTCUSDT futures aggTrade collector entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal

from app.server_runtime import AggTradeCollectorService, ServerCollectorSettings
from app.server_runtime.health import CollectorHealth
from app.server_runtime.publishers import KafkaMarketEventPublisher
from app.server_runtime.sources import BinanceAggTradeEventSource
from app.server_runtime.storage import PostgresStreamLeaseStore

logger = logging.getLogger("server_agg_trade_collector")


async def _log_health(health: CollectorHealth) -> None:
    logger.info("collector_health=%s", json.dumps(health.to_wire(), sort_keys=True))


async def _run_collector(settings: ServerCollectorSettings) -> None:
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    service = AggTradeCollectorService(
        settings=settings,
        lease_store=PostgresStreamLeaseStore(settings.postgres_dsn),
        publisher=KafkaMarketEventPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.owner_id}-phase1c",
        ),
        source=BinanceAggTradeEventSource(),
        on_health=_log_health,
    )
    await service.run(stop_event)


async def _init_schema(dsn: str) -> None:
    await PostgresStreamLeaseStore(dsn).initialize_schema()
    logger.info("Phase 1C lease schema is ready")


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for requested_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(requested_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(
                requested_signal,
                lambda *_args: loop.call_soon_threadsafe(stop_event.set),
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CandleScope Phase 1C aggregate-trade collector",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run", help="run the always-on collector from env")
    init_parser = subparsers.add_parser(
        "init-schema",
        help="idempotently create the PostgreSQL lease table",
    )
    init_parser.add_argument(
        "--postgres-dsn",
        default=os.environ.get("CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN"),
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=os.environ.get("CANDLESCOPE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.command == "init-schema":
        if not args.postgres_dsn or not args.postgres_dsn.strip():
            parser.error(
                "init-schema requires --postgres-dsn or "
                "CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN"
            )
        asyncio.run(_init_schema(args.postgres_dsn))
    else:
        asyncio.run(_run_collector(ServerCollectorSettings.from_env()))


if __name__ == "__main__":
    main()
