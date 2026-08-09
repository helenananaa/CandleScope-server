from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import TopicPartition
from app.server_runtime.publishers import MARKET_EVENTS_TOPIC, PHASE1B_PARTITION_KEY
from app.server_runtime.storage import STREAM_LEASE_TABLE, PostgresStreamLeaseStore
from psycopg.conninfo import conninfo_to_dict

POSTGRES_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1C_POSTGRES_DSN",
    "postgresql://candlescope:phase1b-local-only@localhost:15432/candlescope",
)
BOOTSTRAP_SERVERS = os.environ.get(
    "CANDLESCOPE_PHASE1C_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)
WORKER = (
    Path(__file__).resolve().parents[2] / "scripts" / "server_phase1c_process_worker.py"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1C_INTEGRATION") != "1",
    reason="requires the explicit Phase 1C Redpanda/PostgreSQL stack",
)


def test_two_processes_keep_one_leader_and_take_over_after_sigkill(
    tmp_path: Path,
) -> None:
    asyncio.run(_run_takeover_gate(tmp_path))


async def _run_takeover_gate(tmp_path: Path) -> None:
    _require_explicit_local_reset()
    await _reset_postgres()
    await _reset_topic()
    store = PostgresStreamLeaseStore(POSTGRES_DSN)
    await store.initialize_schema()

    leader_health = tmp_path / "leader.json"
    standby_health = tmp_path / "standby.json"
    leader = await _start_worker("collector-process-a", 42, leader_health)
    standby: asyncio.subprocess.Process | None = None
    try:
        first = await _wait_for_health(
            leader_health,
            lambda value: value.get("last_sequence") == 42,
        )
        assert first["state"] == "leader"
        assert first["producer_epoch"] == 0
        assert first["ready"] is True

        standby = await _start_worker("collector-process-b", 43, standby_health)
        waiting = await _wait_for_health(
            standby_health,
            lambda value: value.get("state") == "standby",
        )
        assert waiting["producer_epoch"] is None
        assert waiting["events_published"] == 0

        leader.kill()
        await asyncio.wait_for(leader.wait(), timeout=3)
        assert leader.returncode == -signal.SIGKILL

        taken_over = await _wait_for_health(
            standby_health,
            lambda value: value.get("last_sequence") == 43,
            timeout=5,
        )
        assert taken_over["state"] == "leader"
        assert taken_over["producer_epoch"] == 1
        assert taken_over["events_published"] == 1

        standby.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(standby.communicate(), timeout=5)
        assert standby.returncode == 0, stderr.decode()
        final = json.loads(stdout.decode().strip().splitlines()[-1])
        assert final["state"] == "stopped"
        assert final["terminal_error"] is None

        records = await _consume_records(expected_count=2)
        assert [record.offset for record in records] == [0, 1]
        assert all(record.key == PHASE1B_PARTITION_KEY.encode() for record in records)
        wires = [json.loads(record.value) for record in records]
        assert [wire["sequence_end"] for wire in wires] == [42, 43]
        assert [wire["producer_epoch"] for wire in wires] == [0, 1]

        durable = await store.inspect(PHASE1B_PARTITION_KEY)
        assert durable is not None
        assert durable.producer_epoch == 1
        assert durable.last_sequence == 43
        assert durable.last_partition_offset == 1
        assert durable.pending_envelope is None
    finally:
        await _terminate_process(leader)
        if standby is not None:
            await _terminate_process(standby)


async def _start_worker(
    owner_id: str,
    sequence: int,
    health_file: Path,
) -> asyncio.subprocess.Process:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(WORKER),
        "--postgres-dsn",
        POSTGRES_DSN,
        "--bootstrap-servers",
        BOOTSTRAP_SERVERS,
        "--owner-id",
        owner_id,
        "--sequence",
        str(sequence),
        "--health-file",
        str(health_file),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )


async def _wait_for_health(
    path: Path,
    predicate: Any,
    *,
    timeout: float = 3,
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
    if os.environ.get("CANDLESCOPE_PHASE1C_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1C_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its topic and table"
        )
    connection = conninfo_to_dict(POSTGRES_DSN)
    if (
        connection.get("host", "localhost") not in {"127.0.0.1", "::1", "localhost"}
        or connection.get("port", "5432") != "15432"
        or connection.get("dbname") != "candlescope"
        or connection.get("user") != "candlescope"
    ):
        raise RuntimeError(
            "the destructive Phase 1C gate only accepts the local Compose "
            "PostgreSQL target candlescope@localhost:15432/candlescope"
        )
    if BOOTSTRAP_SERVERS not in {"127.0.0.1:19092", "localhost:19092"}:
        raise RuntimeError(
            "the destructive Phase 1C gate only accepts local Redpanda on 19092"
        )


async def _reset_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(POSTGRES_DSN) as connection:
        await connection.execute(f"DROP TABLE IF EXISTS {STREAM_LEASE_TABLE}")


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


async def _consume_records(*, expected_count: int) -> list[Any]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"phase1c-gate-{uuid.uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        topic_partition = TopicPartition(MARKET_EVENTS_TOPIC, 0)
        consumer.assign([topic_partition])
        await consumer.seek_to_beginning(topic_partition)
        records: list[Any] = []
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
