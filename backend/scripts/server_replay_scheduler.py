"""Phase 1AF scheduler CLI. Metadata/assignment only."""

from __future__ import annotations

import argparse
import asyncio
import signal
import time

from aiohttp import web
from app.server_runtime.health_http import parse_health_bind
from app.server_runtime.replay_scheduler import ReplayScheduler
from app.server_runtime.replay_scheduler_health import ReplaySchedulerHealth
from app.server_runtime.replay_scheduler_settings import ReplaySchedulerSettings
from app.server_runtime.storage.postgres_replay_scheduler import (
    PostgresReplaySchedulerStore,
)


async def _run(bind: str) -> None:
    settings = ReplaySchedulerSettings.from_env()
    store = PostgresReplaySchedulerStore(settings.postgres_dsn)
    scheduler = ReplayScheduler(store, clock_ms=lambda: time.time_ns() // 1_000_000)
    host, port = parse_health_bind(bind)
    app = web.Application()

    async def health(_request: web.Request) -> web.Response:
        return web.json_response(
            ReplaySchedulerHealth(
                ready=True,
                pending=0,
                running=0,
                last_scan_at_ms=time.time_ns() // 1_000_000,
                last_error_code=None,
            ).to_wire()
        )

    app.router.add_get("/health", health)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        while not stop.is_set():
            try:
                await scheduler.scan_timeouts()
            except Exception as exc:  # noqa: BLE001
                _last_error = type(exc).__name__
                del _last_error
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=settings.scan_interval_ms / 1000
                )
            except TimeoutError:
                continue
    finally:
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--health-bind", required=True)
    args = parser.parse_args()
    asyncio.run(_run(args.health_bind))


if __name__ == "__main__":
    main()
