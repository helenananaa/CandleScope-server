from __future__ import annotations

import asyncio
import json

import pytest
from app.deployment.profile import (
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.chain_health import synthetic_publisher_collector_health
from app.server_runtime.health import CollectorState
from app.server_runtime.health_http import (
    HealthHttpBindError,
    RoleHealthHttpServer,
)
from app.server_runtime.soak_supervisor import (
    MAX_SCRIPTED_DURATION_MS,
    SoakSupervisorError,
    SoakSupervisorSettings,
    public_24h_refusal,
    run_soak_supervisor,
)
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)
from scripts import server_phase1u_soak_supervisor

NOW_MS = 1_700_000_400_000


class _Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += int(seconds * 1000)


def _snapshot() -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="phase1u-soak-epoch",
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256="c" * 64,
    )


def _caught_up_roles(now_ms: int = NOW_MS) -> tuple[object, object, object]:
    snapshot = _snapshot()
    collector = synthetic_publisher_collector_health(
        last_sequence=45,
        last_partition_offset=3,
        events_published=4,
        started_at_ms=now_ms,
    )
    writer = ClickHouseWriterHealth(
        state=ClickHouseWriterState.RUNNING,
        ready=True,
        reason="caught up",
        owner_id="writer-b",
        kafka_group_id="phase1u-writer",
        committed_next_offset=4,
        batches_committed=1,
        inserted_events=4,
        duplicate_events=0,
        conflict_events=0,
        started_at_ms=now_ms,
        updated_at_ms=now_ms,
        terminal_error=None,
    )
    archive = ArchiveWriterHealth(
        state=ArchiveWriterState.RUNNING,
        ready=True,
        reason="caught up",
        owner_id="archiver-b",
        kafka_group_id="phase1u-archive",
        data_epoch=snapshot.data_epoch,
        committed_next_offset=4,
        segments_committed=1,
        events_archived=4,
        current_snapshot=snapshot,
        started_at_ms=now_ms,
        updated_at_ms=now_ms,
        terminal_error=None,
    )
    return collector, writer, archive


async def _serve_roles() -> tuple[
    RoleHealthHttpServer, RoleHealthHttpServer, RoleHealthHttpServer
]:
    collector_http = RoleHealthHttpServer()
    writer_http = RoleHealthHttpServer()
    archive_http = RoleHealthHttpServer()
    await collector_http.start()
    await writer_http.start()
    await archive_http.start()
    collector, writer, archive = _caught_up_roles()
    collector_http.publish(collector)
    writer_http.publish(writer)
    archive_http.publish(archive)
    return collector_http, writer_http, archive_http


def test_health_http_rejects_non_loopback_bind() -> None:
    with pytest.raises(HealthHttpBindError, match="loopback"):
        RoleHealthHttpServer(host="8.8.8.8")


def test_settings_fail_closed_on_duration_source_and_url() -> None:
    with pytest.raises(SoakSupervisorError) as duration_exc:
        SoakSupervisorSettings(
            collector_health_url="http://127.0.0.1:9/health",
            writer_health_url="http://127.0.0.1:9/health",
            archive_health_url="http://127.0.0.1:9/health",
            query_sequences=(42, 43, 44, 45),
            duration_ms=MAX_SCRIPTED_DURATION_MS + 1,
        )
    assert duration_exc.value.code == "DURATION_EXCEEDS_SCRIPTED_CAP"
    with pytest.raises(SoakSupervisorError) as source_exc:
        SoakSupervisorSettings(
            collector_health_url="http://127.0.0.1:9/health",
            writer_health_url="http://127.0.0.1:9/health",
            archive_health_url="http://127.0.0.1:9/health",
            query_sequences=(42, 43, 44, 45),
            source="binance",
        )
    assert source_exc.value.code == "BINANCE_SOURCE_NOT_AUTHORIZED"
    with pytest.raises(SoakSupervisorError) as url_exc:
        SoakSupervisorSettings(
            collector_health_url="http://example.com/health",
            writer_health_url="http://127.0.0.1:9/health",
            archive_health_url="http://127.0.0.1:9/health",
            query_sequences=(42, 43, 44, 45),
        )
    assert url_exc.value.code == "NON_LOOPBACK_HEALTH_URL"


def test_supervisor_scrapes_loopback_health_and_requires_caught_up() -> None:
    async def run() -> None:
        collector_http, writer_http, archive_http = await _serve_roles()
        try:
            clock = _Clock(NOW_MS)
            settings = SoakSupervisorSettings(
                collector_health_url=collector_http.health_url,
                writer_health_url=writer_http.health_url,
                archive_health_url=archive_http.health_url,
                query_sequences=(42, 43, 44, 45),
                duration_ms=40,
                scrape_interval_ms=20,
                stale_after_ms=5_000,
            )
            result = await run_soak_supervisor(
                settings,
                clock_ms=clock,
                sleep=clock.sleep,
            )
            assert result["phase1u_passed"] is True
            assert result["samples"] >= 2
            assert result["twenty_four_hour_public_continuity"] is False
            assert result["reconciliation"]["status"] == "caught_up"
            assert result["source"] == "scripted"
        finally:
            await archive_http.stop()
            await writer_http.stop()
            await collector_http.stop()

    asyncio.run(run())


def test_supervisor_fails_closed_on_stale_health() -> None:
    async def run() -> None:
        collector_http, writer_http, archive_http = await _serve_roles()
        try:
            clock = _Clock(NOW_MS + 20_000)
            settings = SoakSupervisorSettings(
                collector_health_url=collector_http.health_url,
                writer_health_url=writer_http.health_url,
                archive_health_url=archive_http.health_url,
                query_sequences=(42, 43, 44, 45),
                duration_ms=20,
                scrape_interval_ms=10,
                stale_after_ms=5_000,
            )
            with pytest.raises(SoakSupervisorError) as exc:
                await run_soak_supervisor(
                    settings,
                    clock_ms=clock,
                    sleep=clock.sleep,
                )
            assert exc.value.code == "HEALTH_STALE"
        finally:
            await archive_http.stop()
            await writer_http.stop()
            await collector_http.stop()

    asyncio.run(run())


def test_live_ready_and_health_routes() -> None:
    async def run() -> None:
        server = RoleHealthHttpServer()
        await server.start()
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                live = await session.get(f"{server.origin}/health/live")
                assert live.status == 200
                ready = await session.get(f"{server.origin}/health/ready")
                assert ready.status == 503
                collector, _writer, _archive = _caught_up_roles()
                server.publish(collector)
                ready = await session.get(f"{server.origin}/health/ready")
                assert ready.status == 200
                health = await session.get(server.health_url)
                payload = await health.json()
                assert payload["state"] == CollectorState.LEADER.value
                assert payload["last_sequence"] == 45
        finally:
            await server.stop()

    asyncio.run(run())


def test_public_24h_mode_does_not_claim_continuity() -> None:
    denied = public_24h_refusal(allow_public_soak=False)
    assert denied["phase1u_passed"] is False
    assert denied["code"] == "PUBLIC_SOAK_NOT_AUTHORIZED"
    allowed = public_24h_refusal(allow_public_soak=True)
    assert allowed["phase1u_passed"] is False
    assert allowed["code"] == "PUBLIC_SOAK_NOT_RUN"
    assert allowed["twenty_four_hour_public_continuity"] is False


def test_cli_public_24h_refuses(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert server_phase1u_soak_supervisor.main(["public-24h"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["phase1u_passed"] is False
    assert payload["code"] == "PUBLIC_SOAK_NOT_AUTHORIZED"


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.profile is DeploymentProfile.SERVER
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
