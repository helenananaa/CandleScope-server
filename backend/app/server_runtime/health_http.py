"""Loopback-only HTTP health snapshots for soak supervision."""

from __future__ import annotations

import argparse
import inspect
import os
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

from aiohttp import web

from app.server_runtime.archive_health import ArchiveWriterHealth
from app.server_runtime.health import CollectorHealth
from app.server_runtime.writer_health import ClickHouseWriterHealth

RoleHealth = CollectorHealth | ClickHouseWriterHealth | ArchiveWriterHealth
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


class HealthHttpBindError(ValueError):
    """The health HTTP server refused a non-loopback or invalid bind."""


class RoleHealthHttpServer:
    """Serve one role's latest to_wire() snapshot on loopback HTTP."""

    def __init__(self, *, host: str = "127.0.0.1", port: int = 0) -> None:
        if host not in _LOOPBACK_HOSTS:
            raise HealthHttpBindError("health HTTP may bind only to loopback")
        if isinstance(port, bool) or not isinstance(port, int) or port < 0:
            raise HealthHttpBindError("port must be a non-negative integer")
        self._host = "127.0.0.1" if host == "localhost" else host
        self._port = port
        self._payload: dict[str, object] | None = None
        self._ready = False
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    @property
    def origin(self) -> str:
        if self._runner is None:
            raise HealthHttpBindError("health HTTP server is not started")
        addresses = getattr(self._runner, "addresses", None)
        if addresses:
            host, bound_port = addresses[0][0], addresses[0][1]
            display = "127.0.0.1" if host in {"::1", "localhost"} else host
            return f"http://{display}:{bound_port}"
        site = self._site
        server = getattr(site, "_server", None) if site is not None else None
        sockets = getattr(server, "sockets", None)
        if not sockets:
            raise HealthHttpBindError("health HTTP server has no bound socket")
        bound_port = int(sockets[0].getsockname()[1])
        display = "127.0.0.1" if self._host == "::1" else self._host
        return f"http://{display}:{bound_port}"

    @property
    def health_url(self) -> str:
        return f"{self.origin}/health"

    def publish(self, health: RoleHealth) -> None:
        if not isinstance(
            health, (CollectorHealth, ClickHouseWriterHealth, ArchiveWriterHealth)
        ):
            raise TypeError("health must be a collector, writer, or archive snapshot")
        self._payload = health.to_wire()
        self._ready = bool(health.ready)

    async def start(self) -> None:
        if self._runner is not None:
            raise HealthHttpBindError("health HTTP server is already started")
        app = web.Application()
        app.router.add_get("/health/live", self._live)
        app.router.add_get("/health/ready", self._ready_handler)
        app.router.add_get("/health", self._health)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()

    async def stop(self) -> None:
        site = self._site
        runner = self._runner
        self._site = None
        self._runner = None
        if site is not None:
            await site.stop()
        if runner is not None:
            await runner.cleanup()

    async def _live(self, request: web.Request) -> web.Response:
        del request
        return web.json_response({"status": "live"})

    async def _ready_handler(self, request: web.Request) -> web.Response:
        del request
        if self._payload is None or not self._ready:
            return web.json_response(
                {"status": "not_ready"},
                status=503,
            )
        return web.json_response({"status": "ready"})

    async def _health(self, request: web.Request) -> web.Response:
        del request
        if self._payload is None:
            return web.json_response(
                {"status": "not_ready"},
                status=503,
            )
        return web.json_response(self._payload)


HealthObserver = Callable[[RoleHealth], Awaitable[None] | None]


class HealthHttpObserver:
    """Publish each health snapshot to loopback HTTP, then the inner observer."""

    def __init__(
        self,
        server: RoleHealthHttpServer,
        inner: HealthObserver | None = None,
    ) -> None:
        if not isinstance(server, RoleHealthHttpServer):
            raise TypeError("server must be a RoleHealthHttpServer")
        self.server = server
        self._inner = inner

    async def __call__(self, health: RoleHealth) -> None:
        self.server.publish(health)
        if self._inner is None:
            return
        result = self._inner(health)
        if inspect.isawaitable(result):
            await result

    async def start(self) -> None:
        await self.server.start()

    async def stop(self) -> None:
        await self.server.stop()


def parse_health_bind(raw: str) -> tuple[str, int]:
    """Parse HOST:PORT and reject anything that is not loopback HTTP."""

    if not isinstance(raw, str) or not raw.strip():
        raise HealthHttpBindError("health bind cannot be blank")
    text = raw.strip()
    if "/" in text or "?" in text or "#" in text or "@" in text:
        raise HealthHttpBindError("health bind must be HOST:PORT")
    if text.count(":") != 1:
        raise HealthHttpBindError("health bind must be HOST:PORT")
    host, port_text = text.split(":", 1)
    if host not in _LOOPBACK_HOSTS:
        raise HealthHttpBindError("health HTTP may bind only to loopback")
    if not port_text.isdigit():
        raise HealthHttpBindError("health bind port must be a non-negative integer")
    port = int(port_text)
    if port > 65535:
        raise HealthHttpBindError("health bind port is out of range")
    return host, port


def add_health_bind_argument(parser: argparse.ArgumentParser, *, env_name: str) -> None:
    parser.add_argument(
        "--health-bind",
        default=os.environ.get(env_name),
        metavar="HOST:PORT",
        help="optional loopback health HTTP bind, for example 127.0.0.1:18121",
    )


async def start_health_observer(
    bind: str | None,
    inner: HealthObserver | None = None,
) -> HealthHttpObserver | None:
    if bind is None or not str(bind).strip():
        return None
    host, port = parse_health_bind(str(bind))
    observer = HealthHttpObserver(
        RoleHealthHttpServer(host=host, port=port),
        inner=inner,
    )
    await observer.start()
    return observer


async def run_with_health_bind(
    bind: str | None,
    inner: HealthObserver,
    runner: Callable[[HealthObserver], Awaitable[None]],
) -> None:
    observer = await start_health_observer(bind, inner)
    on_health: HealthObserver = observer if observer is not None else inner
    try:
        await runner(on_health)
    finally:
        if observer is not None:
            await observer.stop()


def is_loopback_http_health_url(url: str) -> bool:
    parsed = urlsplit(url)
    return (
        parsed.scheme == "http"
        and parsed.hostname in _LOOPBACK_HOSTS
        and parsed.path == "/health"
        and parsed.username is None
        and parsed.password is None
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.port is not None
    )


__all__ = [
    "HealthHttpBindError",
    "HealthHttpObserver",
    "HealthObserver",
    "RoleHealth",
    "RoleHealthHttpServer",
    "add_health_bind_argument",
    "is_loopback_http_health_url",
    "parse_health_bind",
    "run_with_health_bind",
    "start_health_observer",
]
