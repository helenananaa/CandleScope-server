from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Any

import boto3
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import TopicPartition
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import MARKET_EVENTS_TOPIC, KafkaMarketEventPublisher
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
    ParquetMarketEventQuery,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore
from botocore.client import Config

BOOTSTRAP_SERVERS = os.environ.get(
    "CANDLESCOPE_PHASE1E_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)
S3_ENDPOINT_URL = os.environ.get(
    "CANDLESCOPE_PHASE1E_S3_ENDPOINT_URL",
    "http://localhost:19000",
)
S3_BUCKET = "candlescope-archive"
S3_PREFIX = "market-data"
S3_ACCESS_KEY_ID = "candlescope"
S3_SECRET_ACCESS_KEY = "phase1e-local-secret"
DATA_EPOCH = "phase1e-fault-epoch"
SEGMENT_EVENT_COUNT = 4
WORKER = (
    Path(__file__).resolve().parents[2] / "scripts" / "server_phase1e_process_worker.py"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1E_INTEGRATION") != "1",
    reason="requires the explicit Phase 1E Redpanda/MinIO stack",
)


def test_manifest_success_before_kafka_commit_is_replay_safe(tmp_path: Path) -> None:
    asyncio.run(_run_fault_gate(tmp_path))


async def _run_fault_gate(tmp_path: Path) -> None:
    _require_explicit_local_reset()
    await _reset_topic()
    await _reset_bucket()
    await _publish_gate_records()
    group_id = f"phase1e-fault-{uuid.uuid4()}"

    first_health = tmp_path / "first-health.json"
    crash_marker = tmp_path / "crash-marker.json"
    first = await _start_worker(
        owner_id="archiver-process-a",
        group_id=group_id,
        health_file=first_health,
        crash_marker=crash_marker,
        crash_after_archive=True,
    )
    second: asyncio.subprocess.Process | None = None
    try:
        stdout, stderr = await asyncio.wait_for(first.communicate(), timeout=15)
        assert first.returncode == 98, (stdout.decode(), stderr.decode())
        marker = json.loads(crash_marker.read_text(encoding="utf-8"))
        assert marker["last_offset"] == 3
        assert marker["object_created"] is True
        assert marker["manifest_created"] is True
        assert marker["snapshot"]["snapshot_version"] == 4
        committed_before_restart = await _committed_offset(group_id)
        assert committed_before_restart is None or committed_before_restart < 4
        assert len(await _object_keys()) == 2

        second_health = tmp_path / "second-health.json"
        second = await _start_worker(
            owner_id="archiver-process-b",
            group_id=group_id,
            health_file=second_health,
            crash_marker=tmp_path / "unused-marker.json",
            crash_after_archive=False,
        )
        recovered = await _wait_for_health(
            second_health,
            lambda value: value.get("committed_next_offset") == 8,
            timeout=20,
        )
        assert recovered["segments_committed"] == 2
        assert recovered["events_archived"] == 8
        assert recovered["current_snapshot"]["snapshot_version"] == 8

        second.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(second.communicate(), timeout=5)
        assert second.returncode == 0, stderr.decode()
        final_health = json.loads(stdout.decode().strip().splitlines()[-1])
        assert final_health["state"] == "stopped"
        assert final_health["terminal_error"] is None
        assert await _committed_offset(group_id) == 8

        keys = await _object_keys()
        assert keys == [
            (
                f"{S3_PREFIX}/epochs/{DATA_EPOCH}/manifests/"
                "snapshot-00000000000000000004.json"
            ),
            (
                f"{S3_PREFIX}/epochs/{DATA_EPOCH}/manifests/"
                "snapshot-00000000000000000008.json"
            ),
            (
                f"{S3_PREFIX}/epochs/{DATA_EPOCH}/segments/partition-00000/"
                "offset-00000000000000000000-00000000000000000003.parquet"
            ),
            (
                f"{S3_PREFIX}/epochs/{DATA_EPOCH}/segments/partition-00000/"
                "offset-00000000000000000004-00000000000000000007.parquet"
            ),
        ]

        snapshot_wire = recovered["current_snapshot"]
        snapshot = MarketDataSnapshotRef(
            data_epoch=snapshot_wire["data_epoch"],
            snapshot_version=snapshot_wire["snapshot_version"],
            manifest_uri=snapshot_wire["manifest_uri"],
            manifest_sha256=snapshot_wire["manifest_sha256"],
        )
        store = _store()
        archive = ImmutableParquetMarketEventArchive(
            object_store=store,
            segment_event_count=SEGMENT_EVENT_COUNT,
        )
        chain = await archive.load_chain(snapshot)
        assert [manifest.snapshot_version for manifest in chain] == [4, 8]
        assert chain[0].parent_snapshot is None
        assert chain[1].parent_snapshot is not None
        assert (
            chain[1].parent_snapshot.manifest_sha256
            == hashlib.sha256(chain[0].canonical_bytes()).hexdigest()
        )

        query = ParquetMarketEventQuery(archive=archive, max_page_rows=4)
        stream = _envelope(42).stream
        page_one = await query.query(
            snapshot=snapshot,
            stream=stream,
            start_event_time_ms=1_700_000_000_000,
            end_event_time_ms=1_700_000_001_000,
            limit=3,
        )
        assert [event.sequence_end for event in page_one.events] == [42, 43, 44]
        assert page_one.next_cursor is not None
        page_two = await query.query(
            snapshot=snapshot,
            stream=stream,
            start_event_time_ms=1_700_000_000_000,
            end_event_time_ms=1_700_000_001_000,
            limit=4,
            cursor=page_one.next_cursor,
        )
        assert [event.sequence_end for event in page_two.events] == [45, 46, 47, 48]
        assert page_two.next_cursor is not None
        page_three = await query.query(
            snapshot=snapshot,
            stream=stream,
            start_event_time_ms=1_700_000_000_000,
            end_event_time_ms=1_700_000_001_000,
            limit=4,
            cursor=page_two.next_cursor,
        )
        assert [event.sequence_end for event in page_three.events] == [49]
        assert page_three.next_cursor is None
    finally:
        await _terminate_process(first)
        if second is not None:
            await _terminate_process(second)


def _market_event(sequence: int) -> MarketEvent:
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


def _envelope(sequence: int) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        _market_event(sequence),
        previous_sequence=None if sequence == 42 else sequence - 1,
        published_at_ms=1_700_000_001_000 + sequence,
    )


async def _publish_gate_records() -> None:
    publisher = KafkaMarketEventPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="phase1e-fixture-publisher",
    )
    await publisher.start()
    try:
        for sequence in range(42, 50):
            await publisher.publish((_envelope(sequence),))
    finally:
        await publisher.stop()


def _store() -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=S3_ENDPOINT_URL,
        region="us-east-1",
        bucket=S3_BUCKET,
        prefix=S3_PREFIX,
        access_key_id=S3_ACCESS_KEY_ID,
        secret_access_key=S3_SECRET_ACCESS_KEY,
    )


def _s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


async def _reset_bucket() -> None:
    store = _store()
    await store.ensure_bucket()
    client = _s3_client()

    def reset() -> None:
        response = client.list_objects_v2(Bucket=S3_BUCKET)
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects})

    await asyncio.to_thread(reset)


async def _object_keys() -> list[str]:
    client = _s3_client()
    response = await asyncio.to_thread(client.list_objects_v2, Bucket=S3_BUCKET)
    return sorted(str(item["Key"]) for item in response.get("Contents", []))


async def _committed_offset(group_id: str) -> int | None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        return await consumer.committed(TopicPartition(MARKET_EVENTS_TOPIC, 0))
    finally:
        await consumer.stop()


async def _start_worker(
    *,
    owner_id: str,
    group_id: str,
    health_file: Path,
    crash_marker: Path,
    crash_after_archive: bool,
) -> asyncio.subprocess.Process:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    command = [
        sys.executable,
        str(WORKER),
        "--bootstrap-servers",
        BOOTSTRAP_SERVERS,
        "--group-id",
        group_id,
        "--owner-id",
        owner_id,
        "--data-epoch",
        DATA_EPOCH,
        "--segment-event-count",
        str(SEGMENT_EVENT_COUNT),
        "--s3-endpoint-url",
        S3_ENDPOINT_URL,
        "--s3-bucket",
        S3_BUCKET,
        "--s3-prefix",
        S3_PREFIX,
        "--s3-access-key-id",
        S3_ACCESS_KEY_ID,
        "--s3-secret-access-key",
        S3_SECRET_ACCESS_KEY,
        "--health-file",
        str(health_file),
        "--crash-marker",
        str(crash_marker),
    ]
    if crash_after_archive:
        command.append("--crash-after-archive")
    return await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )


async def _wait_for_health(
    path: Path,
    predicate: Any,
    *,
    timeout: float,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    last: dict[str, Any] | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            last = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        if last is not None and predicate(last):
            return last
        await asyncio.sleep(0.05)
    raise TimeoutError(f"health predicate failed; last snapshot: {last}")


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.communicate(), timeout=2)
    except TimeoutError:
        process.kill()
        await process.communicate()


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1E_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1E_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its topic and object-store prefix"
        )
    if BOOTSTRAP_SERVERS not in {"localhost:19092", "127.0.0.1:19092"}:
        raise RuntimeError("Phase 1E reset only accepts local Redpanda on port 19092")
    if S3_ENDPOINT_URL not in {
        "http://localhost:19000",
        "http://127.0.0.1:19000",
    }:
        raise RuntimeError("Phase 1E reset only accepts local MinIO on port 19000")


async def _reset_topic() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        topics = await admin.list_topics()
        if MARKET_EVENTS_TOPIC in topics:
            await admin.delete_topics([MARKET_EVENTS_TOPIC])
            deadline = asyncio.get_running_loop().time() + 5
            while MARKET_EVENTS_TOPIC in await admin.list_topics():
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("Kafka topic deletion did not complete")
                await asyncio.sleep(0.1)
        try:
            await admin.create_topics(
                [NewTopic(MARKET_EVENTS_TOPIC, num_partitions=1, replication_factor=1)]
            )
        except TopicAlreadyExistsError:
            pass
    finally:
        await admin.close()
