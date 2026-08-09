"""Process worker for the ClickHouse-success/Kafka-commit fault gate."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path

from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.projection import ProjectionBatchResult
from app.server_runtime.projector_service import ClickHouseWriterService
from app.server_runtime.storage import ClickHouseMarketEventProjector
from app.server_runtime.writer_health import ClickHouseWriterHealth
from app.server_runtime.writer_settings import ClickHouseWriterSettings


class AtomicHealthFile:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._temporary = path.with_suffix(f"{path.suffix}.tmp")

    async def __call__(self, health: ClickHouseWriterHealth) -> None:
        self._temporary.write_text(
            json.dumps(health.to_wire(), sort_keys=True),
            encoding="utf-8",
        )
        self._temporary.replace(self._path)


async def _run(args: argparse.Namespace) -> ClickHouseWriterHealth:
    settings = ClickHouseWriterSettings(
        kafka_bootstrap_servers=(args.bootstrap_servers,),
        owner_id=args.owner_id,
        clickhouse_url=args.clickhouse_url,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        clickhouse_database=args.clickhouse_database,
        kafka_group_id=args.group_id,
        batch_size=500,
        poll_timeout_ms=100,
        clickhouse_request_timeout_ms=5_000,
        kafka_session_timeout_ms=6_000,
        kafka_heartbeat_interval_ms=2_000,
        shutdown_timeout_ms=2_000,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for requested_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(requested_signal, stop_event.set)

    async def fault_hook(result: ProjectionBatchResult) -> None:
        if not args.crash_after_project:
            return
        last_offset = result.last_offset
        if last_offset < args.crash_after_offset:
            return
        args.crash_marker.write_text(
            json.dumps(
                {
                    "first_offset": result.first_offset,
                    "last_offset": last_offset,
                    "inserted_count": result.inserted_count,
                    "duplicate_count": result.duplicate_count,
                    "conflict_count": result.conflict_count,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        os._exit(97)

    service = ClickHouseWriterService(
        settings=settings,
        consumer=KafkaMarketEventBatchConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            client_id=f"{settings.owner_id}-phase1d-gate",
            connection_options={
                "session_timeout_ms": settings.kafka_session_timeout_ms,
                "heartbeat_interval_ms": settings.kafka_heartbeat_interval_ms,
            },
        ),
        projector=ClickHouseMarketEventProjector(
            url=settings.clickhouse_url,
            database=settings.clickhouse_database,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            request_timeout_ms=settings.clickhouse_request_timeout_ms,
        ),
        on_health=AtomicHealthFile(args.health_file),
        after_project_before_commit=fault_hook,
    )
    return await service.run(stop_event)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--clickhouse-url", required=True)
    parser.add_argument("--clickhouse-user", required=True)
    parser.add_argument("--clickhouse-password", required=True)
    parser.add_argument("--clickhouse-database", default="candlescope")
    parser.add_argument("--health-file", required=True, type=Path)
    parser.add_argument("--crash-marker", required=True, type=Path)
    parser.add_argument("--crash-after-project", action="store_true")
    parser.add_argument("--crash-after-offset", default=3, type=int)
    health = asyncio.run(_run(parser.parse_args()))
    print(json.dumps(health.to_wire(), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
