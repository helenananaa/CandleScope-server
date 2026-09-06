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
from app.server_runtime.health_http import (
    HealthObserver,
    add_health_bind_argument,
    run_with_health_bind,
)
from app.server_runtime.publishers import KafkaMarketEventPublisher
from app.server_runtime.soak_faults import hold_after_aligned_publish
from app.server_runtime.sources import BinanceAggTradeEventSource
from app.server_runtime.storage import PostgresStreamLeaseStore

logger = logging.getLogger("server_agg_trade_collector")


async def _log_health(health: CollectorHealth) -> None:
    logger.info("collector_health=%s", json.dumps(health.to_wire(), sort_keys=True))


async def _run_collector(
    settings: ServerCollectorSettings,
    *,
    health_bind: str | None = None,
) -> None:
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    async def after_event(health: CollectorHealth) -> None:
        hook_dir = os.environ.get("CANDLESCOPE_PHASE1AI_HOOK_DIR")
        if not hook_dir or not hook_dir.strip():
            return
        offset = health.last_partition_offset
        if offset is None:
            return
        raw_segment = os.environ.get(
            "CANDLESCOPE_SERVER_ARCHIVE_WRITER_SEGMENT_EVENT_COUNT",
            "",
        )
        try:
            segment = int(raw_segment)
        except ValueError:
            return
        await hold_after_aligned_publish(
            hook_dir.strip(),
            last_offset=offset,
            segment_event_count=segment,
        )

    async def run(on_health: HealthObserver) -> None:
        service = AggTradeCollectorService(
            settings=settings,
            lease_store=PostgresStreamLeaseStore(settings.postgres_dsn),
            publisher=KafkaMarketEventPublisher(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                client_id=f"{settings.owner_id}-phase1c",
            ),
            source=BinanceAggTradeEventSource(),
            on_health=on_health,
            after_event=after_event,
        )
        await service.run(stop_event)

    await run_with_health_bind(health_bind, _log_health, run)


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
    run_parser = subparsers.add_parser(
        "run", help="run the always-on collector from env"
    )
    add_health_bind_argument(
        run_parser,
        env_name="CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND",
    )
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
        asyncio.run(
            _run_collector(
                ServerCollectorSettings.from_env(),
                health_bind=getattr(args, "health_bind", None),
            )
        )


if __name__ == "__main__":
    main()
