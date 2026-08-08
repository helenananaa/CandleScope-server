from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import TopicPartition
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_runtime import StreamLeaseBusyError, StreamLeaseFencedError
from app.server_runtime.collector import LeasedAggTradeCollector
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    PHASE1B_PARTITION_KEY,
    KafkaMarketEventPublisher,
)
from app.server_runtime.storage import (
    STREAM_LEASE_TABLE,
    PostgresStreamLeaseStore,
)
from psycopg.conninfo import conninfo_to_dict

POSTGRES_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1B_POSTGRES_DSN",
    "postgresql://candlescope:phase1b-local-only@localhost:15432/candlescope",
)
BOOTSTRAP_SERVERS = os.environ.get(
    "CANDLESCOPE_PHASE1B_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)
WORKER = (
    Path(__file__).resolve().parents[2] / "scripts" / "server_phase1b_fault_worker.py"
)
EVENT_TIME_MS = 1_700_000_000_010
RECEIVED_AT_MS = 1_700_000_000_020
PUBLISHED_AT_MS = 1_700_000_000_030

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1B_INTEGRATION") != "1",
    reason="requires the explicit Phase 1B Redpanda/PostgreSQL stack",
)


def _event(agg_trade_id: int) -> MarketEvent:
    sequence_delta = agg_trade_id - 42
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=EVENT_TIME_MS + sequence_delta,
        received_at_ms=RECEIVED_AT_MS + sequence_delta,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": agg_trade_id,
            "price": 100000.1,
            "quantity": 0.025,
            "price_text": "100000.1000",
            "quantity_text": "0.02500000",
            "first_trade_id": agg_trade_id * 10,
            "last_trade_id": agg_trade_id * 10 + 2,
            "trade_time_ms": EVENT_TIME_MS + sequence_delta - 10,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=agg_trade_id,
        market_type="futures",
    )


def test_process_exit_after_kafka_accept_recovers_exact_pending_bytes() -> None:
    asyncio.run(_run_process_fault_gate())


async def _run_process_fault_gate() -> None:
    _require_explicit_local_reset()
    await _reset_postgres()
    await _reset_topic()
    store = PostgresStreamLeaseStore(POSTGRES_DSN)
    await store.initialize_schema()

    worker_environment = dict(os.environ)
    backend_root = str(Path(__file__).resolve().parents[2])
    worker_environment["PYTHONPATH"] = backend_root
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(WORKER),
        "--postgres-dsn",
        POSTGRES_DSN,
        "--bootstrap-servers",
        BOOTSTRAP_SERVERS,
        "--owner-id",
        "collector-process-a",
        "--lease-ttl-ms",
        "1500",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=worker_environment,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode()
    child_result = json.loads(stdout.decode().strip().splitlines()[-1])
    assert child_result["partition_offset"] == 0
    assert child_result["producer_epoch"] == 0

    stale = await store.inspect(PHASE1B_PARTITION_KEY)
    assert stale is not None
    assert stale.pending_envelope is not None
    assert stale.pending_envelope.event_id == child_result["event_id"]

    publisher = KafkaMarketEventPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="collector-process-b",
    )
    await publisher.start()
    try:
        recovered = await _acquire_after_expiry(store, publisher)
        assert recovered.lease.producer_epoch == 1
        with pytest.raises(StreamLeaseFencedError):
            await store.renew(stale, lease_ttl_ms=10_000)

        retry_receipt = await recovered.handle(
            _event(42),
            published_at_ms=PUBLISHED_AT_MS + 999,
        )
        next_receipt = await recovered.handle(
            _event(43),
            published_at_ms=PUBLISHED_AT_MS + 1_000,
        )
        assert retry_receipt.partition_offsets == ((PHASE1B_PARTITION_KEY, 1),)
        assert next_receipt.partition_offsets == ((PHASE1B_PARTITION_KEY, 2),)

        records = await _consume_records(expected_count=3)
        assert [record.offset for record in records] == [0, 1, 2]
        assert all(record.key == PHASE1B_PARTITION_KEY.encode() for record in records)
        assert records[0].value == records[1].value
        first_wire = json.loads(records[0].value)
        third_wire = json.loads(records[2].value)
        assert first_wire["event_id"] == child_result["event_id"]
        assert first_wire["producer_epoch"] == 0
        assert third_wire["producer_epoch"] == 1
        assert third_wire["sequence_end"] == 43

        durable = await store.inspect(PHASE1B_PARTITION_KEY)
        assert durable is not None
        assert durable.last_sequence == 43
        assert durable.last_partition_offset == 2
        assert durable.pending_envelope is None
        await recovered.release()
    finally:
        await publisher.stop()


async def _acquire_after_expiry(
    store: PostgresStreamLeaseStore,
    publisher: KafkaMarketEventPublisher,
) -> LeasedAggTradeCollector:
    deadline = asyncio.get_running_loop().time() + 5
    while True:
        try:
            return await LeasedAggTradeCollector.acquire(
                lease_store=store,
                publisher=publisher,
                owner_id="collector-process-b",
                lease_ttl_ms=10_000,
            )
        except StreamLeaseBusyError:
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(0.1)


async def _reset_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(POSTGRES_DSN) as connection:
        await connection.execute(f"DROP TABLE IF EXISTS {STREAM_LEASE_TABLE}")


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1B_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1B_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its topic and table"
        )
    connection = conninfo_to_dict(POSTGRES_DSN)
    allowed_hosts = {"127.0.0.1", "::1", "localhost"}
    if (
        connection.get("host", "localhost") not in allowed_hosts
        or connection.get("port", "5432") != "15432"
        or connection.get("dbname") != "candlescope"
        or connection.get("user") != "candlescope"
    ):
        raise RuntimeError(
            "the destructive Phase 1B gate only accepts the local Compose "
            "PostgreSQL target candlescope@localhost:15432/candlescope"
        )
    if BOOTSTRAP_SERVERS not in {"127.0.0.1:19092", "localhost:19092"}:
        raise RuntimeError(
            "the destructive Phase 1B gate only accepts local Redpanda on 19092"
        )


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
                [
                    NewTopic(
                        MARKET_EVENTS_TOPIC,
                        num_partitions=1,
                        replication_factor=1,
                    )
                ]
            )
        except TopicAlreadyExistsError:
            pass
    finally:
        await admin.close()


async def _consume_records(*, expected_count: int) -> list[object]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"phase1b-gate-{uuid.uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        topic_partition = TopicPartition(MARKET_EVENTS_TOPIC, 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_beginning(topic_partition)
        records: list[object] = []
        deadline = asyncio.get_running_loop().time() + 5
        while len(records) < expected_count:
            batches = await consumer.getmany(
                topic_partition,
                timeout_ms=500,
                max_records=expected_count,
            )
            records.extend(batches.get(topic_partition, ()))
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"received {len(records)} of {expected_count} Kafka records"
                )
        return records
    finally:
        await consumer.stop()
