from __future__ import annotations

import argparse
import ast
import asyncio
from pathlib import Path

import aiohttp
import pytest
from app.deployment.profile import (
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.chain_health import synthetic_publisher_collector_health
from app.server_runtime.health_http import (
    HealthHttpBindError,
    add_health_bind_argument,
    parse_health_bind,
    run_with_health_bind,
    start_health_observer,
)
from app.server_runtime.soak_supervisor import (
    SoakSupervisorSettings,
    run_soak_supervisor,
)
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)

BACKEND_ROOT = Path(__file__).parents[1]
SCRIPTS = BACKEND_ROOT / "scripts"
NOW_MS = 1_700_000_500_000


class _Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += int(seconds * 1000)


def test_parse_health_bind_accepts_loopback_and_rejects_everything_else() -> None:
    assert parse_health_bind("127.0.0.1:18121") == ("127.0.0.1", 18121)
    assert parse_health_bind("localhost:0") == ("localhost", 0)
    with pytest.raises(HealthHttpBindError, match="loopback"):
        parse_health_bind("0.0.0.0:18121")
    with pytest.raises(HealthHttpBindError, match="HOST:PORT"):
        parse_health_bind("127.0.0.1")
    with pytest.raises(HealthHttpBindError, match="HOST:PORT"):
        parse_health_bind("127.0.0.1:18121/health")
    with pytest.raises(HealthHttpBindError, match="out of range"):
        parse_health_bind("127.0.0.1:70000")


def test_cli_flag_defaults_to_env_and_stays_optional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND", "127.0.0.1:18121")
    parser = argparse.ArgumentParser()
    add_health_bind_argument(
        parser, env_name="CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND"
    )
    assert parser.parse_args([]).health_bind == "127.0.0.1:18121"
    monkeypatch.delenv("CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND")
    parser = argparse.ArgumentParser()
    add_health_bind_argument(
        parser, env_name="CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND"
    )
    assert parser.parse_args([]).health_bind is None


def test_blank_bind_does_not_start_http() -> None:
    async def run() -> None:
        inner_calls: list[object] = []

        async def inner(health: object) -> None:
            inner_calls.append(health)

        async def runner(on_health: object) -> None:
            await on_health(object())  # type: ignore[misc,operator]

        await run_with_health_bind(None, inner, runner)
        await run_with_health_bind("  ", inner, runner)
        assert len(inner_calls) == 2

    asyncio.run(run())


def test_observer_serves_exact_to_wire_and_chains_inner_log() -> None:
    async def run() -> None:
        inner_calls: list[object] = []

        async def inner(health: object) -> None:
            inner_calls.append(health)

        observer = await start_health_observer("127.0.0.1:0", inner)
        assert observer is not None
        try:
            health = synthetic_publisher_collector_health(
                last_sequence=45,
                last_partition_offset=3,
                events_published=4,
                started_at_ms=NOW_MS,
            )
            await observer(health)
            async with aiohttp.ClientSession() as session:
                response = await session.get(observer.server.health_url)
                assert response.status == 200
                payload = await response.json()
            assert payload == health.to_wire()
            assert inner_calls == [health]
        finally:
            await observer.stop()

    asyncio.run(run())


def test_supervisor_can_scrape_bound_observer() -> None:
    async def run() -> None:
        snapshot = MarketDataSnapshotRef(
            data_epoch="phase1v-bind-epoch",
            snapshot_version=4,
            manifest_uri="s3://archive/snapshot-4.json",
            manifest_sha256="d" * 64,
        )
        collector = synthetic_publisher_collector_health(
            last_sequence=45,
            last_partition_offset=3,
            events_published=4,
            started_at_ms=NOW_MS,
        )
        writer = ClickHouseWriterHealth(
            state=ClickHouseWriterState.RUNNING,
            ready=True,
            reason="caught up",
            owner_id="writer-b",
            kafka_group_id="phase1v-writer",
            committed_next_offset=4,
            batches_committed=1,
            inserted_events=4,
            duplicate_events=0,
            conflict_events=0,
            started_at_ms=NOW_MS,
            updated_at_ms=NOW_MS,
            terminal_error=None,
        )
        archive = ArchiveWriterHealth(
            state=ArchiveWriterState.RUNNING,
            ready=True,
            reason="caught up",
            owner_id="archiver-b",
            kafka_group_id="phase1v-archive",
            data_epoch=snapshot.data_epoch,
            committed_next_offset=4,
            segments_committed=1,
            events_archived=4,
            current_snapshot=snapshot,
            started_at_ms=NOW_MS,
            updated_at_ms=NOW_MS,
            terminal_error=None,
        )
        collector_obs = await start_health_observer("127.0.0.1:0")
        writer_obs = await start_health_observer("127.0.0.1:0")
        archive_obs = await start_health_observer("127.0.0.1:0")
        assert collector_obs is not None
        assert writer_obs is not None
        assert archive_obs is not None
        try:
            await collector_obs(collector)
            await writer_obs(writer)
            await archive_obs(archive)
            clock = _Clock(NOW_MS)
            result = await run_soak_supervisor(
                SoakSupervisorSettings(
                    collector_health_url=collector_obs.server.health_url,
                    writer_health_url=writer_obs.server.health_url,
                    archive_health_url=archive_obs.server.health_url,
                    query_sequences=(42, 43, 44, 45),
                    duration_ms=20,
                    scrape_interval_ms=10,
                    stale_after_ms=5_000,
                ),
                clock_ms=clock,
                sleep=clock.sleep,
            )
            assert result["phase1u_passed"] is True
            assert result["reconciliation"]["status"] == "caught_up"
        finally:
            await archive_obs.stop()
            await writer_obs.stop()
            await collector_obs.stop()

    asyncio.run(run())


def test_production_entrypoints_wire_optional_health_bind() -> None:
    required = {
        "add_health_bind_argument",
        "run_with_health_bind",
    }
    for name in (
        "server_agg_trade_collector.py",
        "server_clickhouse_writer.py",
        "server_parquet_archiver.py",
    ):
        path = SCRIPTS / name
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                names.update(alias.name for alias in node.names)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                names.add(node.func.id)
        missing = required - names
        assert missing == set(), f"{name} missing {sorted(missing)}"


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.profile is DeploymentProfile.SERVER
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
