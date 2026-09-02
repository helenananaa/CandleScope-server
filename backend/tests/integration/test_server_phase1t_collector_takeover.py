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
import boto3
import psycopg
import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from app.data_engine.market_data import MarketChannel, MarketStreamKey
from app.replay.sources.trade_source import TradeReplaySource
from app.server_runtime.chain_health import (
    archive_health_from_wire,
    collector_health_from_wire,
    writer_health_from_wire,
)
from app.server_runtime.chain_reconciliation import (
    STATUS_CAUGHT_UP,
    ChainObservation,
    reconcile_chain,
)
from app.server_runtime.publishers import MARKET_EVENTS_TOPIC
from app.server_runtime.replay_snapshot import (
    ReplayServerSnapshotPin,
    ServerSnapshotTradeReader,
)
from app.server_runtime.storage import (
    MARKET_EVENT_FACT_TABLE,
    STREAM_LEASE_TABLE,
    ClickHouseMarketEventProjector,
    PostgresStreamLeaseStore,
)
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
    ParquetMarketEventQuery,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore
from botocore.client import Config
from psycopg.conninfo import conninfo_to_dict

BOOTSTRAP_SERVERS = os.environ.get(
    "CANDLESCOPE_PHASE1T_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)
POSTGRES_DSN = os.environ.get(
    "CANDLESCOPE_PHASE1T_POSTGRES_DSN",
    "postgresql://candlescope:phase1t-local-only@localhost:15432/candlescope",
)
CLICKHOUSE_URL = os.environ.get(
    "CANDLESCOPE_PHASE1T_CLICKHOUSE_URL",
    "http://localhost:18123",
)
CLICKHOUSE_USER = "candlescope"
CLICKHOUSE_PASSWORD = "phase1t-local-only"
CLICKHOUSE_DATABASE = "candlescope"
S3_ENDPOINT_URL = os.environ.get(
    "CANDLESCOPE_PHASE1T_S3_ENDPOINT_URL",
    "http://localhost:19000",
)
S3_BUCKET = "candlescope-phase1t-archive"
S3_PREFIX = "market-data"
S3_ACCESS_KEY_ID = "candlescope"
S3_SECRET_ACCESS_KEY = "phase1t-local-secret"
DATA_EPOCH = "phase1t-takeover-epoch"
SEGMENT_EVENT_COUNT = 4
FIRST_SEQUENCE = 42
START_MS = 1_700_000_000_000
SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
COLLECTOR_WORKER = SCRIPTS / "server_phase1t_collector_worker.py"
WRITER_WORKER = SCRIPTS / "server_phase1d_process_worker.py"
ARCHIVE_WORKER = SCRIPTS / "server_phase1e_process_worker.py"
STREAM = MarketStreamKey.build("binance", "futures", "BTCUSDT", MarketChannel.AGG_TRADE)

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1T_INTEGRATION") != "1",
    reason="requires the explicit Phase 1T Redpanda/PostgreSQL/ClickHouse/MinIO stack",
)


def test_collector_takeover_then_writer_and_archiver_restart_reconcile(
    tmp_path: Path,
) -> None:
    asyncio.run(_run_gate(tmp_path))


async def _run_gate(tmp_path: Path) -> None:
    _require_explicit_local_reset()
    await _reset_postgres()
    await _reset_topic()
    await _reset_clickhouse()
    await _reset_bucket()
    await PostgresStreamLeaseStore(POSTGRES_DSN).initialize_schema()

    collector_health = await _run_collector_takeover(tmp_path)
    writer_group = f"phase1t-writer-{uuid.uuid4()}"
    archive_group = f"phase1t-archive-{uuid.uuid4()}"
    writer_health = await _run_faulted_consumer(
        tmp_path,
        role="writer",
        group_id=writer_group,
        crash_exit=97,
    )
    archive_health = await _run_faulted_consumer(
        tmp_path,
        role="archive",
        group_id=archive_group,
        crash_exit=98,
    )
    snapshot = archive_health.current_snapshot
    assert snapshot is not None
    assert snapshot.snapshot_version == 4
    assert collector_health.last_sequence == 45
    assert collector_health.last_partition_offset == 3
    assert collector_health.producer_epoch == 1
    assert collector_health.pending_event_id is None
    assert writer_health.committed_next_offset == 4
    assert archive_health.committed_next_offset == 4

    facts = await _query_clickhouse(
        f"""
        SELECT sequence_end
        FROM `{CLICKHOUSE_DATABASE}`.`{MARKET_EVENT_FACT_TABLE}` FINAL
        ORDER BY sequence_end
        FORMAT JSONEachRow
        """
    )
    assert [int(row["sequence_end"]) for row in facts] == [42, 43, 44, 45]

    query = ParquetMarketEventQuery(
        archive=ImmutableParquetMarketEventArchive(
            object_store=_store(),
            segment_event_count=SEGMENT_EVENT_COUNT,
        ),
        max_page_rows=SEGMENT_EVENT_COUNT,
    )
    await query.start()
    try:
        pin = ReplayServerSnapshotPin(
            snapshot=snapshot,
            stream=STREAM,
            start_event_time_ms=START_MS + FIRST_SEQUENCE,
            end_event_time_ms=START_MS + FIRST_SEQUENCE + SEGMENT_EVENT_COUNT - 1,
            expected_first_agg_trade_id=FIRST_SEQUENCE,
            expected_last_agg_trade_id=FIRST_SEQUENCE + SEGMENT_EVENT_COUNT - 1,
            row_count=SEGMENT_EVENT_COUNT,
        )
        reader = await ServerSnapshotTradeReader.load(
            query,
            pin,
            query_page_limit=SEGMENT_EVENT_COUNT,
            page_rows=SEGMENT_EVENT_COUNT,
        )
    finally:
        await query.stop()
    source = TradeReplaySource(reader)
    replay_ids = tuple(
        item.agg_trade_id
        for item in (source.next() for _ in range(SEGMENT_EVENT_COUNT))
        if item is not None
    )
    assert replay_ids == (42, 43, 44, 45)
    assert source.exhausted()

    result = reconcile_chain(
        ChainObservation(
            collector=collector_health,
            writer=writer_health,
            archive=archive_health,
            query_snapshot=snapshot,
            query_sequences=replay_ids,
            replay_snapshot=pin.snapshot,
            replay_first_id=pin.expected_first_agg_trade_id,
            replay_last_id=pin.expected_last_agg_trade_id,
            replay_row_count=pin.row_count,
        ),
        require_caught_up=True,
    )
    assert result.status == STATUS_CAUGHT_UP
    assert result.physical_next_offset == 4
    assert result.logical_last_sequence == 45


async def _run_collector_takeover(tmp_path: Path) -> Any:
    leader_health = tmp_path / "collector-a.json"
    standby_health = tmp_path / "collector-b.json"
    leader = await _start_collector("collector-process-a", "42,43", leader_health)
    standby: asyncio.subprocess.Process | None = None
    try:
        first = await _wait_for_health(
            leader_health,
            lambda value: (
                value.get("last_sequence") == 43 and value.get("state") == "leader"
            ),
            timeout=8,
        )
        assert first["producer_epoch"] == 0
        assert first["pending_event_id"] is None
        standby = await _start_collector("collector-process-b", "44,45", standby_health)
        waiting = await _wait_for_health(
            standby_health,
            lambda value: value.get("state") == "standby",
            timeout=5,
        )
        assert waiting["events_published"] == 0
        leader.kill()
        await asyncio.wait_for(leader.wait(), timeout=3)
        taken = await _wait_for_health(
            standby_health,
            lambda value: (
                value.get("last_sequence") == 45 and value.get("state") == "leader"
            ),
            timeout=8,
        )
        assert taken["producer_epoch"] == 1
        assert taken["last_partition_offset"] == 3
        assert taken["pending_event_id"] is None
        health = collector_health_from_wire(taken)
        standby.send_signal(signal.SIGTERM)
        _stdout, stderr = await asyncio.wait_for(standby.communicate(), timeout=5)
        assert standby.returncode == 0, stderr.decode()
        return health
    finally:
        await _terminate_process(leader)
        if standby is not None:
            await _terminate_process(standby)


async def _run_faulted_consumer(
    tmp_path: Path,
    *,
    role: str,
    group_id: str,
    crash_exit: int,
) -> Any:
    first_health = tmp_path / f"{role}-first-health.json"
    crash_marker = tmp_path / f"{role}-crash.json"
    first = await _start_consumer(
        role=role,
        owner_id=f"{role}-process-a",
        group_id=group_id,
        health_file=first_health,
        crash_marker=crash_marker,
        crash=True,
    )
    second: asyncio.subprocess.Process | None = None
    try:
        stdout, stderr = await asyncio.wait_for(first.communicate(), timeout=20)
        assert first.returncode == crash_exit, (stdout.decode(), stderr.decode())
        marker = json.loads(crash_marker.read_text(encoding="utf-8"))
        assert marker["last_offset"] == 3
        second_health = tmp_path / f"{role}-second-health.json"
        second = await _start_consumer(
            role=role,
            owner_id=f"{role}-process-b",
            group_id=group_id,
            health_file=second_health,
            crash_marker=tmp_path / f"{role}-unused.json",
            crash=False,
        )
        recovered = await _wait_for_health(
            second_health,
            lambda value: value.get("committed_next_offset") == 4,
            timeout=25,
        )
        second.send_signal(signal.SIGTERM)
        _stdout, stderr = await asyncio.wait_for(second.communicate(), timeout=8)
        assert second.returncode == 0, stderr.decode()
        if role == "writer":
            return writer_health_from_wire(recovered)
        return archive_health_from_wire(recovered)
    finally:
        await _terminate_process(first)
        if second is not None:
            await _terminate_process(second)


async def _start_collector(
    owner_id: str,
    sequences: str,
    health_file: Path,
) -> asyncio.subprocess.Process:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(COLLECTOR_WORKER),
        "--postgres-dsn",
        POSTGRES_DSN,
        "--bootstrap-servers",
        BOOTSTRAP_SERVERS,
        "--owner-id",
        owner_id,
        "--sequences",
        sequences,
        "--health-file",
        str(health_file),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )


async def _start_consumer(
    *,
    role: str,
    owner_id: str,
    group_id: str,
    health_file: Path,
    crash_marker: Path,
    crash: bool,
) -> asyncio.subprocess.Process:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2])
    if role == "writer":
        command = [
            sys.executable,
            str(WRITER_WORKER),
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
        if crash:
            command.append("--crash-after-project")
    elif role == "archive":
        command = [
            sys.executable,
            str(ARCHIVE_WORKER),
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
        if crash:
            command.append("--crash-after-archive")
    else:
        raise ValueError(f"unsupported role: {role}")
    return await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )


async def _reset_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(POSTGRES_DSN) as connection:
        await connection.execute(f"DROP TABLE IF EXISTS {STREAM_LEASE_TABLE}")


async def _reset_clickhouse() -> None:
    last_error: Exception | None = None
    for _attempt in range(8):
        try:
            await _execute_clickhouse(
                f"DROP DATABASE IF EXISTS `{CLICKHOUSE_DATABASE}`",
                database="default",
            )
            last_error = None
            break
        except (aiohttp.ClientError, AssertionError) as exc:
            last_error = exc
            await asyncio.sleep(0.25)
    if last_error is not None:
        raise last_error
    projector = ClickHouseMarketEventProjector(
        url=CLICKHOUSE_URL,
        database=CLICKHOUSE_DATABASE,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    await projector.initialize_schema()


async def _execute_clickhouse(query: str, *, database: str | None = None) -> str:
    params: dict[str, str] = {"query": query.strip()}
    if database is not None:
        params["database"] = database
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            CLICKHOUSE_URL,
            params=params,
            headers={
                "X-ClickHouse-User": CLICKHOUSE_USER,
                "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
            },
        ) as response,
    ):
        body = await response.text()
        assert response.status == 200, body
        return body


async def _query_clickhouse(query: str) -> list[dict[str, Any]]:
    body = await _execute_clickhouse(query, database=CLICKHOUSE_DATABASE)
    return [json.loads(line) for line in body.splitlines() if line.strip()]


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
    if os.environ.get("CANDLESCOPE_PHASE1T_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1T_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its topic, lease table, ClickHouse database, "
            "and bucket"
        )
    connection = conninfo_to_dict(POSTGRES_DSN)
    if (
        connection.get("host", "localhost") not in {"127.0.0.1", "::1", "localhost"}
        or str(connection.get("port", "5432")) != "15432"
        or connection.get("dbname") != "candlescope"
        or connection.get("user") != "candlescope"
    ):
        raise RuntimeError(
            "Phase 1T reset only accepts local Compose PostgreSQL "
            "candlescope@localhost:15432/candlescope"
        )
    if BOOTSTRAP_SERVERS not in {"localhost:19092", "127.0.0.1:19092"}:
        raise RuntimeError("Phase 1T reset only accepts local Redpanda on port 19092")
    if CLICKHOUSE_URL not in {"http://localhost:18123", "http://127.0.0.1:18123"}:
        raise RuntimeError("Phase 1T reset only accepts local ClickHouse on port 18123")
    if S3_ENDPOINT_URL not in {"http://localhost:19000", "http://127.0.0.1:19000"}:
        raise RuntimeError("Phase 1T reset only accepts local MinIO on port 19000")
