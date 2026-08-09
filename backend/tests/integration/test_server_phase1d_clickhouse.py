from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import TopicPartition
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    KafkaMarketEventPublisher,
)
from app.server_runtime.storage import (
    MARKET_EVENT_CONFLICT_TABLE,
    MARKET_EVENT_FACT_TABLE,
    ClickHouseMarketEventProjector,
)

BOOTSTRAP_SERVERS = os.environ.get(
    "CANDLESCOPE_PHASE1D_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)
CLICKHOUSE_URL = os.environ.get(
    "CANDLESCOPE_PHASE1D_CLICKHOUSE_URL",
    "http://localhost:18123",
)
CLICKHOUSE_USER = "candlescope"
CLICKHOUSE_PASSWORD = "phase1d-local-only"
CLICKHOUSE_DATABASE = "candlescope"
WORKER = (
    Path(__file__).resolve().parents[2] / "scripts" / "server_phase1d_process_worker.py"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1D_INTEGRATION") != "1",
    reason="requires the explicit Phase 1D Redpanda/ClickHouse stack",
)


def test_clickhouse_success_before_kafka_commit_is_replay_safe(tmp_path: Path) -> None:
    asyncio.run(_run_fault_gate(tmp_path))


async def _run_fault_gate(tmp_path: Path) -> None:
    _require_explicit_local_reset()
    await _reset_topic()
    await _reset_clickhouse()
    await _publish_gate_records()
    group_id = f"phase1d-fault-{uuid.uuid4()}"

    first_health = tmp_path / "first-health.json"
    crash_marker = tmp_path / "crash-marker.json"
    first = await _start_worker(
        owner_id="writer-process-a",
        group_id=group_id,
        health_file=first_health,
        crash_marker=crash_marker,
        crash_after_project=True,
    )
    second: asyncio.subprocess.Process | None = None
    try:
        stdout, stderr = await asyncio.wait_for(first.communicate(), timeout=10)
        assert first.returncode == 97, (stdout.decode(), stderr.decode())
        marker = json.loads(crash_marker.read_text(encoding="utf-8"))
        assert marker["last_offset"] == 3

        before_fact = await _query_rows(
            f"SELECT count() AS count FROM `{CLICKHOUSE_DATABASE}`."
            f"`{MARKET_EVENT_FACT_TABLE}` FINAL FORMAT JSONEachRow"
        )
        before_conflict = await _query_rows(
            f"SELECT count() AS count FROM `{CLICKHOUSE_DATABASE}`."
            f"`{MARKET_EVENT_CONFLICT_TABLE}` FINAL FORMAT JSONEachRow"
        )
        assert int(before_fact[0]["count"]) == 2
        assert int(before_conflict[0]["count"]) == 1
        committed_before_restart = await _committed_offset(group_id)
        assert committed_before_restart is None or committed_before_restart < 4

        second_health = tmp_path / "second-health.json"
        second = await _start_worker(
            owner_id="writer-process-b",
            group_id=group_id,
            health_file=second_health,
            crash_marker=tmp_path / "unused-marker.json",
            crash_after_project=False,
        )
        recovered = await _wait_for_health(
            second_health,
            lambda value: value.get("committed_next_offset") == 4,
            timeout=15,
        )
        assert recovered["batches_committed"] >= 1

        second.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(second.communicate(), timeout=5)
        assert second.returncode == 0, stderr.decode()
        final_health = json.loads(stdout.decode().strip().splitlines()[-1])
        assert final_health["state"] == "stopped"
        assert final_health["terminal_error"] is None

        facts = await _query_rows(
            f"""
            SELECT sequence_end, first_kafka_offset, envelope_sha256, payload_json
            FROM `{CLICKHOUSE_DATABASE}`.`{MARKET_EVENT_FACT_TABLE}` FINAL
            ORDER BY sequence_end
            FORMAT JSONEachRow
            """
        )
        conflicts = await _query_rows(
            f"""
            SELECT kafka_offset, existing_envelope_sha256, incoming_envelope_sha256
            FROM `{CLICKHOUSE_DATABASE}`.`{MARKET_EVENT_CONFLICT_TABLE}` FINAL
            ORDER BY kafka_offset
            FORMAT JSONEachRow
            """
        )
        assert [int(row["sequence_end"]) for row in facts] == [42, 43]
        assert [int(row["first_kafka_offset"]) for row in facts] == [0, 3]
        assert json.loads(facts[0]["payload_json"])["quantity"] == "0.02500000"
        assert len(conflicts) == 1
        assert int(conflicts[0]["kafka_offset"]) == 2
        assert (
            conflicts[0]["existing_envelope_sha256"]
            != conflicts[0]["incoming_envelope_sha256"]
        )
        assert await _committed_offset(group_id) == 4
    finally:
        await _terminate_process(first)
        if second is not None:
            await _terminate_process(second)


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


def _envelope(
    sequence: int,
    *,
    quantity: str = "0.02500000",
    previous_sequence: int | None = None,
) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        _market_event(sequence, quantity=quantity),
        previous_sequence=previous_sequence,
        published_at_ms=1_700_000_001_000 + sequence,
    )


async def _publish_gate_records() -> None:
    original = _envelope(42)
    conflict = _envelope(42, quantity="0.03000000")
    following = _envelope(43, previous_sequence=42)
    assert original.event_id == conflict.event_id
    publisher = KafkaMarketEventPublisher(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="phase1d-fixture-publisher",
    )
    await publisher.start()
    try:
        for envelope in (original, original, conflict, following):
            await publisher.publish((envelope,))
    finally:
        await publisher.stop()


async def _reset_clickhouse() -> None:
    await _execute_clickhouse(f"DROP DATABASE IF EXISTS `{CLICKHOUSE_DATABASE}`")
    projector = ClickHouseMarketEventProjector(
        url=CLICKHOUSE_URL,
        database=CLICKHOUSE_DATABASE,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    await projector.initialize_schema()


async def _execute_clickhouse(query: str) -> str:
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            CLICKHOUSE_URL,
            params={"query": query.strip()},
            headers={
                "X-ClickHouse-User": CLICKHOUSE_USER,
                "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
            },
        ) as response,
    ):
        body = await response.text()
        assert response.status == 200, body
        return body


async def _query_rows(query: str) -> list[dict[str, Any]]:
    body = await _execute_clickhouse(query)
    return [json.loads(line) for line in body.splitlines() if line.strip()]


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
    crash_after_project: bool,
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
        "--clickhouse-url",
        CLICKHOUSE_URL,
        "--clickhouse-user",
        CLICKHOUSE_USER,
        "--clickhouse-password",
        CLICKHOUSE_PASSWORD,
        "--health-file",
        str(health_file),
        "--crash-marker",
        str(crash_marker),
    ]
    if crash_after_project:
        command.append("--crash-after-project")
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
    if os.environ.get("CANDLESCOPE_PHASE1D_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1D_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its topic and ClickHouse database"
        )
    if BOOTSTRAP_SERVERS not in {"localhost:19092", "127.0.0.1:19092"}:
        raise RuntimeError("Phase 1D reset only accepts local Redpanda on port 19092")
    if CLICKHOUSE_URL not in {
        "http://localhost:18123",
        "http://127.0.0.1:18123",
    }:
        raise RuntimeError("Phase 1D reset only accepts local ClickHouse on port 18123")


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
