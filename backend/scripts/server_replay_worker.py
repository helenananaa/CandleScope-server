"""Phase 1AE Replay Worker CLI. Assignment is explicit; no scheduler."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from pathlib import Path

from aiohttp import web
from app.replay.broker.models import BrokerConfig
from app.replay.constants import REPLAY_PROTOCOL, CommandType
from app.replay.models import ReplayCommand, ReplaySessionConfig
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.health_http import parse_health_bind
from app.server_runtime.replay_scheduler import ReplayScheduler
from app.server_runtime.replay_session import ServerReplaySessionSpec
from app.server_runtime.replay_snapshot import ReplayServerSnapshotPin
from app.server_runtime.replay_worker import ReplayWorker
from app.server_runtime.replay_worker_pool import ReplayWorkerPoolLoop
from app.server_runtime.replay_worker_settings import ReplayWorkerSettings
from app.server_runtime.storage.postgres_replay_lease import (
    PostgresReplaySessionLeaseStore,
)
from app.server_runtime.storage.postgres_replay_scheduler import (
    PostgresReplaySchedulerStore,
)
from app.server_runtime.storage.postgres_replay_session import (
    PostgresReplaySessionStore,
)
from app.server_runtime.testing.frozen_agg_trade_query import FrozenAggTradeQuery


async def _run(args: argparse.Namespace) -> None:
    settings = ReplayWorkerSettings.from_env()
    assignment = json.loads(Path(args.assignment).read_text(encoding="utf-8"))
    query = FrozenAggTradeQuery(Path(assignment["query_path"]))
    lease_store = PostgresReplaySessionLeaseStore(settings.postgres_dsn)
    session_store = PostgresReplaySessionStore(settings.postgres_dsn)
    worker = ReplayWorker(
        settings,
        lease_store=lease_store,
        session_store=session_store,
        query=query,
        verify_privileges=session_store.verify_runtime_privileges,
    )
    snapshot = assignment["snapshot"]
    pin_payload = assignment["pin"]
    pin = ReplayServerSnapshotPin(
        snapshot=MarketDataSnapshotRef(
            data_epoch=str(snapshot["data_epoch"]),
            snapshot_version=int(snapshot["snapshot_version"]),
            manifest_uri=str(snapshot["manifest_uri"]),
            manifest_sha256=str(snapshot["manifest_sha256"]),
        ),
        start_event_time_ms=int(pin_payload["start_event_time_ms"]),
        end_event_time_ms=int(pin_payload["end_event_time_ms"]),
        expected_first_agg_trade_id=int(pin_payload["expected_first_agg_trade_id"]),
        expected_last_agg_trade_id=int(pin_payload["expected_last_agg_trade_id"]),
        row_count=int(pin_payload["row_count"]),
    )
    lease = await lease_store.acquire(
        session_id=str(assignment["session_id"]),
        worker_id=settings.worker_id,
        snapshot=pin.snapshot,
        organization_id=str(assignment["organization_id"]),
        workspace_id=str(assignment["workspace_id"]),
        lease_ttl_ms=settings.lease_ttl_ms,
    )
    spec = ServerReplaySessionSpec(
        lease=lease,
        pin=pin,
        config=ReplaySessionConfig.from_dict(assignment["config"]),
        broker_config=BrokerConfig.from_dict(assignment["broker_config"]),
        replay_start_ms=int(assignment["replay_start_ms"]),
        replay_end_time_ms=int(assignment["replay_end_time_ms"]),
        command_queue_size=32,
        event_buffer_size=64,
        max_emit_fps=30,
        controller_ttl_seconds=30.0,
        checkpoint_event_interval=1,
        checkpoint_virtual_ms=60_000,
        max_closed_bars=16,
        shutdown_timeout_seconds=max(0.2, settings.shutdown_timeout_ms / 1_000),
    )
    if args.mode == "recover":
        await worker.recover(spec)
    else:
        await worker.start_new(spec)

    host, port = parse_health_bind(args.control_bind)
    app = web.Application()

    async def health(_request: web.Request) -> web.Response:
        payload = worker.health().to_wire()
        return web.json_response(payload)

    async def snapshot_handler(request: web.Request) -> web.Response:
        _authorize(request, settings.worker_control_token)
        if worker.session is None:
            raise web.HTTPServiceUnavailable()
        return web.json_response(await worker.session.public_snapshot())

    async def command_handler(request: web.Request) -> web.Response:
        _authorize(request, settings.worker_control_token)
        body = await request.json()
        try:
            result = await worker.submit(
                ReplayCommand(
                    protocol=REPLAY_PROTOCOL,
                    command_id=str(body["command_id"]),
                    client_instance_id=str(
                        body.get("client_instance_id", "worker-client")
                    ),
                    expected_revision=int(body["expected_revision"]),
                    type=CommandType(str(body["type"])),
                    payload=dict(body.get("payload") or {}),
                )
            )
        except Exception as exc:  # noqa: BLE001
            details = getattr(exc, "details", None)
            return web.json_response(
                {
                    "error": type(exc).__name__,
                    "message": str(exc),
                    "code": getattr(getattr(exc, "code", None), "value", None),
                    "details": dict(details) if details else None,
                },
                status=500,
            )
        return web.json_response(
            {
                "command_id": result.command_id,
                "revision": result.revision,
                "sequence": result.sequence,
                "state": result.state.value,
                "state_hash": result.state_hash,
                "cursor": {
                    "source_sequence": result.cursor.source_sequence,
                    "last_agg_trade_id": result.cursor.last_agg_trade_id,
                },
            }
        )

    app.router.add_get("/health", health)
    app.router.add_get("/snapshot", snapshot_handler)
    app.router.add_post("/commands", command_handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await worker.stop()
        await runner.cleanup()


def _authorize(request: web.Request, token: str) -> None:
    header = request.headers.get("Authorization", "")
    if header != f"Bearer {token}":
        raise web.HTTPUnauthorized()


async def _run_pool(args: argparse.Namespace) -> None:
    settings = ReplayWorkerSettings.from_env()
    store = PostgresReplaySchedulerStore(settings.postgres_dsn)
    heartbeat_ttl_ms = int(
        os.environ.get("CANDLESCOPE_SERVER_REPLAY_SCHEDULER_HEARTBEAT_TTL_MS", "60000")
    )
    scheduler = ReplayScheduler(
        store,
        clock_ms=lambda: time.time_ns() // 1_000_000,
        heartbeat_ttl_ms=heartbeat_ttl_ms,
    )
    session_store = PostgresReplaySessionStore(settings.postgres_dsn)
    lease_store = PostgresReplaySessionLeaseStore(settings.postgres_dsn)
    pool = ReplayWorkerPoolLoop(
        settings,
        scheduler=scheduler,
        lease_store=lease_store,
        session_store=session_store,
    )
    host, port = parse_health_bind(args.control_bind)
    app = web.Application()

    async def health(_request: web.Request) -> web.Response:
        worker = pool._worker
        payload = (
            {"ready": False, "state": "idle"}
            if worker is None
            else worker.health().to_wire()
        )
        return web.json_response(payload)

    app.router.add_get("/health", health)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    run_task = asyncio.create_task(pool.run())
    try:
        await stop.wait()
    finally:
        pool.request_stop()
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
        await pool.stop()
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="CandleScope Replay Worker")
    parser.add_argument("--assignment")
    parser.add_argument("--mode", choices=("new", "recover"), default="new")
    parser.add_argument("--control-bind", required=True)
    parser.add_argument("--pool", action="store_true")
    args = parser.parse_args()
    if args.pool:
        asyncio.run(_run_pool(args))
        return
    if not args.assignment:
        parser.error("--assignment is required unless --pool is set")
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
