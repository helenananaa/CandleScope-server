"""Process worker for the manifest-success/Kafka-commit Phase 1E fault gate."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path

from app.server_runtime.archive_health import ArchiveWriterHealth
from app.server_runtime.archive_service import ArchiveWriterService
from app.server_runtime.archive_settings import ArchiveWriterSettings
from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.storage.parquet_archive import (
    ArchiveAppendResult,
    ImmutableParquetMarketEventArchive,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore


class AtomicHealthFile:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._temporary = path.with_suffix(f"{path.suffix}.tmp")

    async def __call__(self, health: ArchiveWriterHealth) -> None:
        self._temporary.write_text(
            json.dumps(health.to_wire(), sort_keys=True),
            encoding="utf-8",
        )
        self._temporary.replace(self._path)


async def _run(args: argparse.Namespace) -> ArchiveWriterHealth:
    settings = ArchiveWriterSettings(
        kafka_bootstrap_servers=(args.bootstrap_servers,),
        owner_id=args.owner_id,
        data_epoch=args.data_epoch,
        s3_endpoint_url=args.s3_endpoint_url,
        s3_region="us-east-1",
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        s3_access_key_id=args.s3_access_key_id,
        s3_secret_access_key=args.s3_secret_access_key,
        kafka_group_id=args.group_id,
        segment_event_count=args.segment_event_count,
        poll_timeout_ms=100,
        s3_request_timeout_ms=5_000,
        kafka_session_timeout_ms=6_000,
        kafka_heartbeat_interval_ms=2_000,
        shutdown_timeout_ms=2_000,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for requested_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(requested_signal, stop_event.set)

    async def fault_hook(result: ArchiveAppendResult) -> None:
        if not args.crash_after_archive:
            return
        if result.last_offset < args.crash_after_offset:
            return
        args.crash_marker.write_text(
            json.dumps(
                {
                    "first_offset": result.first_offset,
                    "last_offset": result.last_offset,
                    "object_created": result.object_created,
                    "manifest_created": result.manifest_created,
                    "snapshot": {
                        "snapshot_version": result.commit.snapshot.snapshot_version,
                        "manifest_uri": result.commit.snapshot.manifest_uri,
                        "manifest_sha256": result.commit.snapshot.manifest_sha256,
                    },
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        os._exit(98)

    store = S3ImmutableObjectStore(
        endpoint_url=settings.s3_endpoint_url,
        region=settings.s3_region,
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        access_key_id=settings.s3_access_key_id,
        secret_access_key=settings.s3_secret_access_key,
        request_timeout_ms=settings.s3_request_timeout_ms,
    )
    archive = ImmutableParquetMarketEventArchive(
        object_store=store,
        segment_event_count=settings.segment_event_count,
    )
    service = ArchiveWriterService(
        settings=settings,
        consumer=KafkaMarketEventBatchConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            client_id=f"{settings.owner_id}-phase1e-gate",
            exact_batch_size=settings.segment_event_count,
            connection_options={
                "session_timeout_ms": settings.kafka_session_timeout_ms,
                "heartbeat_interval_ms": settings.kafka_heartbeat_interval_ms,
            },
        ),
        archive=archive,
        on_health=AtomicHealthFile(args.health_file),
        after_archive_before_commit=fault_hook,
    )
    return await service.run(stop_event)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--data-epoch", required=True)
    parser.add_argument("--segment-event-count", default=4, type=int)
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", required=True)
    parser.add_argument("--s3-access-key-id", required=True)
    parser.add_argument("--s3-secret-access-key", required=True)
    parser.add_argument("--health-file", required=True, type=Path)
    parser.add_argument("--crash-marker", required=True, type=Path)
    parser.add_argument("--crash-after-archive", action="store_true")
    parser.add_argument("--crash-after-offset", default=3, type=int)
    health = asyncio.run(_run(parser.parse_args()))
    print(json.dumps(health.to_wire(), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
