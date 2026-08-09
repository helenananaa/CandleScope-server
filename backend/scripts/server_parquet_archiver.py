"""Standalone Phase 1E Kafka-to-immutable-Parquet archiver entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal

from app.server_runtime.archive_health import ArchiveWriterHealth
from app.server_runtime.archive_service import ArchiveWriterService
from app.server_runtime.archive_settings import ArchiveWriterSettings
from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

logger = logging.getLogger("server_parquet_archiver")


async def _log_health(health: ArchiveWriterHealth) -> None:
    logger.info(
        "archive_writer_health=%s", json.dumps(health.to_wire(), sort_keys=True)
    )


def _store(settings: ArchiveWriterSettings) -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=settings.s3_endpoint_url,
        region=settings.s3_region,
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        access_key_id=settings.s3_access_key_id,
        secret_access_key=settings.s3_secret_access_key,
        request_timeout_ms=settings.s3_request_timeout_ms,
    )


async def _run_writer(settings: ArchiveWriterSettings) -> None:
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    archive = ImmutableParquetMarketEventArchive(
        object_store=_store(settings),
        segment_event_count=settings.segment_event_count,
    )
    consumer = KafkaMarketEventBatchConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        client_id=f"{settings.owner_id}-phase1e",
        exact_batch_size=settings.segment_event_count,
        connection_options={
            "session_timeout_ms": settings.kafka_session_timeout_ms,
            "heartbeat_interval_ms": settings.kafka_heartbeat_interval_ms,
        },
    )
    service = ArchiveWriterService(
        settings=settings,
        consumer=consumer,
        archive=archive,
        on_health=_log_health,
    )
    await service.run(stop_event)


async def _init_bucket(settings: ArchiveWriterSettings) -> None:
    await _store(settings).ensure_bucket()
    logger.info("Phase 1E S3-compatible archive bucket is ready")


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
        description="CandleScope Phase 1E immutable Parquet archive writer",
    )
    parser.add_argument(
        "command",
        choices=("init-bucket", "run"),
        help="initialize the configured bucket or run the archive writer",
    )
    logging.basicConfig(
        level=os.environ.get("CANDLESCOPE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = ArchiveWriterSettings.from_env()
    command = parser.parse_args().command
    if command == "init-bucket":
        asyncio.run(_init_bucket(settings))
    else:
        asyncio.run(_run_writer(settings))


if __name__ == "__main__":
    main()
