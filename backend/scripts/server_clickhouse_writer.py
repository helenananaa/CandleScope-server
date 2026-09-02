"""Standalone Phase 1D Kafka-to-ClickHouse writer entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal

from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.health_http import (
    HealthObserver,
    add_health_bind_argument,
    run_with_health_bind,
)
from app.server_runtime.projector_service import ClickHouseWriterService
from app.server_runtime.storage import ClickHouseMarketEventProjector
from app.server_runtime.writer_health import ClickHouseWriterHealth
from app.server_runtime.writer_settings import ClickHouseWriterSettings

logger = logging.getLogger("server_clickhouse_writer")


async def _log_health(health: ClickHouseWriterHealth) -> None:
    logger.info(
        "clickhouse_writer_health=%s", json.dumps(health.to_wire(), sort_keys=True)
    )


def _projector(settings: ClickHouseWriterSettings) -> ClickHouseMarketEventProjector:
    return ClickHouseMarketEventProjector(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        request_timeout_ms=settings.clickhouse_request_timeout_ms,
    )


async def _run_writer(
    settings: ClickHouseWriterSettings,
    *,
    health_bind: str | None = None,
) -> None:
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    async def run(on_health: HealthObserver) -> None:
        consumer = KafkaMarketEventBatchConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            client_id=f"{settings.owner_id}-phase1d",
            connection_options={
                "session_timeout_ms": settings.kafka_session_timeout_ms,
                "heartbeat_interval_ms": settings.kafka_heartbeat_interval_ms,
            },
        )
        service = ClickHouseWriterService(
            settings=settings,
            consumer=consumer,
            projector=_projector(settings),
            on_health=on_health,
        )
        await service.run(stop_event)

    await run_with_health_bind(health_bind, _log_health, run)


async def _init_schema(args: argparse.Namespace) -> None:
    projector = ClickHouseMarketEventProjector(
        url=args.clickhouse_url,
        database=args.clickhouse_database,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
    )
    await projector.initialize_schema()
    logger.info("Phase 1D ClickHouse schema is ready")


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
        description="CandleScope Phase 1D Kafka-to-ClickHouse writer",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser(
        "run", help="run the writer from strict environment settings"
    )
    add_health_bind_argument(
        run_parser,
        env_name="CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_HEALTH_BIND",
    )
    init_parser = subparsers.add_parser(
        "init-schema",
        help="idempotently create the Phase 1D ClickHouse tables",
    )
    prefix = "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_"
    init_parser.add_argument(
        "--clickhouse-url",
        default=os.environ.get(f"{prefix}CLICKHOUSE_URL"),
    )
    init_parser.add_argument(
        "--clickhouse-user",
        default=os.environ.get(f"{prefix}CLICKHOUSE_USER"),
    )
    init_parser.add_argument(
        "--clickhouse-password",
        default=os.environ.get(f"{prefix}CLICKHOUSE_PASSWORD"),
    )
    init_parser.add_argument(
        "--clickhouse-database",
        default=os.environ.get(f"{prefix}CLICKHOUSE_DATABASE", "candlescope"),
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=os.environ.get("CANDLESCOPE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.command == "init-schema":
        missing = [
            name
            for name in (
                "clickhouse_url",
                "clickhouse_user",
                "clickhouse_password",
            )
            if not getattr(args, name)
        ]
        if missing:
            parser.error("init-schema is missing: " + ", ".join(missing))
        asyncio.run(_init_schema(args))
    else:
        asyncio.run(
            _run_writer(
                ClickHouseWriterSettings.from_env(),
                health_bind=getattr(args, "health_bind", None),
            )
        )


if __name__ == "__main__":
    main()
