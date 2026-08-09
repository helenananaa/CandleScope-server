from __future__ import annotations

import asyncio
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import boto3
import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.consumers import KafkaMarketEventBatchConsumer
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import MARKET_EVENTS_TOPIC, KafkaMarketEventPublisher
from app.server_runtime.storage.clickhouse import ClickHouseMarketEventProjector
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore
from botocore.client import Config

BOOTSTRAP_SERVERS = "localhost:19092"
CLICKHOUSE_URL = "http://localhost:18123"
CLICKHOUSE_USER = "candlescope"
CLICKHOUSE_PASSWORD = "phase1f-local-only"
CLICKHOUSE_DATABASE = "candlescope"
S3_ENDPOINT_URL = "http://localhost:19000"
S3_BUCKET = "candlescope-query-archive"
S3_PREFIX = "market-data"
S3_ACCESS_KEY_ID = "candlescope"
S3_SECRET_ACCESS_KEY = "phase1f-local-secret"
DATA_EPOCH = "phase1f-query-epoch"
QUERY_URL = "http://127.0.0.1:18110"
SEGMENT_EVENT_COUNT = 4
QUERY_SCRIPT = Path(__file__).resolve().parents[2] / "scripts/server_snapshot_query.py"

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1F_INTEGRATION") != "1",
    reason="requires the explicit Phase 1F Redpanda/ClickHouse/MinIO stack",
)


def test_real_http_hot_parity_and_lagged_cold_route() -> None:
    asyncio.run(_run_gate())


async def _run_gate() -> None:
    _require_explicit_local_reset()
    await _reset_topic()
    await _reset_clickhouse()
    await _reset_bucket()
    clickhouse_group = f"phase1f-clickhouse-{uuid.uuid4()}"
    archive_group = f"phase1f-archive-{uuid.uuid4()}"

    original = _envelope(42)
    conflict = _envelope(42, quantity="0.03000000")
    following = _envelope(43)
    assert original.event_id == conflict.event_id
    await _publish((original, original, conflict, following))
    await _project_clickhouse(clickhouse_group)
    snapshot_four = await _archive_segment(archive_group)

    await _publish(tuple(_envelope(sequence) for sequence in range(44, 48)))
    snapshot_eight = await _archive_segment(archive_group)

    process = await _start_query_service(clickhouse_group)
    try:
        await _wait_ready(process)
        hot = await _query(snapshot_four, original, preference="auto", limit=10)
        assert hot[0] == 200
        assert hot[1]["backend"] == "hot"
        assert hot[1]["parity_verified"] is True
        assert hot[1]["hot_committed_next_offset"] == 4
        assert [event["sequence_end"] for event in hot[1]["page"]["events"]] == [
            42,
            43,
        ]

        lagged = await _query(snapshot_eight, original, preference="auto", limit=3)
        assert lagged[0] == 200
        assert lagged[1]["backend"] == "cold"
        assert lagged[1]["parity_verified"] is False
        assert lagged[1]["hot_committed_next_offset"] == 4
        assert [event["sequence_end"] for event in lagged[1]["page"]["events"]] == [
            42,
            43,
            44,
        ]
        assert lagged[1]["page"]["next_cursor"] is not None

        next_page = await _query(
            snapshot_eight,
            original,
            preference="cold",
            limit=3,
            cursor=lagged[1]["page"]["next_cursor"],
        )
        assert next_page[0] == 200
        assert next_page[1]["backend"] == "cold"
        assert [event["sequence_end"] for event in next_page[1]["page"]["events"]] == [
            45,
            46,
            47,
        ]

        forced_hot = await _query(
            snapshot_eight,
            original,
            preference="hot",
            limit=10,
        )
        assert forced_hot[0] == 409
        assert forced_hot[1]["detail"]["code"] == "HOT_PROJECTION_BEHIND"
    finally:
        await _terminate_process(process)


def _market_event(sequence: int, *, quantity: str = "0.02500000") -> MarketEvent:
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
            "quantity": float(quantity),
            "price_text": "100000.1000",
            "quantity_text": quantity,
            "first_trade_id": sequence * 10,
            "last_trade_id": sequence * 10 + 2,
            "trade_time_ms": 1_700_000_000_000 + sequence,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=sequence,
        market_type="futures",
    )


def _envelope(sequence: int, *, quantity: str = "0.02500000") -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        _market_event(sequence, quantity=quantity),
        previous_sequence=None if sequence == 42 else sequence - 1,
        published_at_ms=1_700_000_001_000 + sequence,
    )


async def _publish(envelopes: tuple[Any, ...]) -> None:
    publisher = KafkaMarketEventPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=f"phase1f-publisher-{uuid.uuid4()}",
    )
    await publisher.start()
    try:
        for envelope in envelopes:
            await publisher.publish((envelope,))
    finally:
        await publisher.stop()


async def _poll_segment(consumer: KafkaMarketEventBatchConsumer):
    deadline = asyncio.get_running_loop().time() + 10
    while asyncio.get_running_loop().time() < deadline:
        records = await consumer.poll(timeout_ms=250, max_records=SEGMENT_EVENT_COUNT)
        if records:
            return records
    raise TimeoutError("Kafka segment did not become available")


async def _project_clickhouse(group_id: str) -> None:
    projector = ClickHouseMarketEventProjector(
        url=CLICKHOUSE_URL,
        database=CLICKHOUSE_DATABASE,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    consumer = KafkaMarketEventBatchConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        client_id="phase1f-clickhouse-writer",
        exact_batch_size=SEGMENT_EVENT_COUNT,
    )
    await projector.start()
    await consumer.start()
    try:
        records = await _poll_segment(consumer)
        result = await projector.apply_batch(records)
        assert (
            result.inserted_count,
            result.duplicate_count,
            result.conflict_count,
        ) == (2, 1, 1)
        await consumer.commit_through(records[-1])
    finally:
        await consumer.stop()
        await projector.stop()


async def _archive_segment(group_id: str):
    archive = ImmutableParquetMarketEventArchive(
        object_store=_store(),
        segment_event_count=SEGMENT_EVENT_COUNT,
    )
    consumer = KafkaMarketEventBatchConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        client_id=f"phase1f-archive-{uuid.uuid4()}",
        exact_batch_size=SEGMENT_EVENT_COUNT,
    )
    await archive.initialize()
    await consumer.start()
    try:
        records = await _poll_segment(consumer)
        result = await archive.append_records(records, data_epoch=DATA_EPOCH)
        await consumer.commit_through(records[-1])
        return result.commit.snapshot
    finally:
        await consumer.stop()


async def _query(
    snapshot: Any,
    envelope: Any,
    *,
    preference: str,
    limit: int,
    cursor: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    body = {
        "snapshot": {
            "data_epoch": snapshot.data_epoch,
            "snapshot_version": snapshot.snapshot_version,
            "manifest_uri": snapshot.manifest_uri,
            "manifest_sha256": snapshot.manifest_sha256,
        },
        "stream": envelope.stream.to_dict(),
        "start_event_time_ms": 0,
        "end_event_time_ms": 2_000_000_000_000,
        "limit": limit,
        "cursor": cursor,
        "preference": preference,
    }
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            f"{QUERY_URL}/api/v1/server/market-events/query",
            json=body,
        ) as response,
    ):
        return response.status, await response.json()


def _store() -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=S3_ENDPOINT_URL,
        region="us-east-1",
        bucket=S3_BUCKET,
        prefix=S3_PREFIX,
        access_key_id=S3_ACCESS_KEY_ID,
        secret_access_key=S3_SECRET_ACCESS_KEY,
    )


async def _reset_clickhouse() -> None:
    await _execute_clickhouse(f"DROP DATABASE IF EXISTS `{CLICKHOUSE_DATABASE}`")
    projector = ClickHouseMarketEventProjector(
        url=CLICKHOUSE_URL,
        database=CLICKHOUSE_DATABASE,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    await projector.initialize_schema()


async def _execute_clickhouse(query: str) -> None:
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            CLICKHOUSE_URL,
            params={"query": query},
            headers={
                "X-ClickHouse-User": CLICKHOUSE_USER,
                "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
            },
        ) as response,
    ):
        body = await response.text()
        assert response.status == 200, body


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
    await _store().ensure_bucket()
    client = _s3_client()

    def reset() -> None:
        response = client.list_objects_v2(Bucket=S3_BUCKET)
        objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects})

    await asyncio.to_thread(reset)


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


async def _start_query_service(group_id: str) -> asyncio.subprocess.Process:
    environment = dict(os.environ)
    environment.update(
        {
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            "CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
            "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL": CLICKHOUSE_URL,
            "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER": CLICKHOUSE_USER,
            "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
            "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
            "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_WRITER_GROUP_ID": group_id,
            "CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "CANDLESCOPE_SERVER_QUERY_S3_BUCKET": S3_BUCKET,
            "CANDLESCOPE_SERVER_QUERY_S3_PREFIX": S3_PREFIX,
            "CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID": S3_ACCESS_KEY_ID,
            "CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
            "CANDLESCOPE_SERVER_QUERY_BIND_HOST": "127.0.0.1",
            "CANDLESCOPE_SERVER_QUERY_BIND_PORT": "18110",
            "CANDLESCOPE_SERVER_QUERY_MAX_PAGE_ROWS": "10",
            "CANDLESCOPE_SERVER_QUERY_MAX_SCAN_ROWS": "100",
            "CANDLESCOPE_LOG_LEVEL": "WARNING",
        }
    )
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(QUERY_SCRIPT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )


async def _wait_ready(process: asyncio.subprocess.Process) -> None:
    deadline = asyncio.get_running_loop().time() + 15
    while asyncio.get_running_loop().time() < deadline:
        if process.returncode is not None:
            stdout, stderr = await process.communicate()
            raise RuntimeError(
                f"query service exited: {stdout.decode()} {stderr.decode()}"
            )
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(f"{QUERY_URL}/health/ready") as response,
            ):
                if response.status == 200:
                    return
        except aiohttp.ClientError:
            pass
        await asyncio.sleep(0.1)
    raise TimeoutError("query service did not become ready")


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=5)
    except TimeoutError:
        process.kill()
        stdout, stderr = await process.communicate()
    assert process.returncode == 0, (stdout.decode(), stderr.decode())


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1F_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1F_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its local topic, database, and bucket"
        )
