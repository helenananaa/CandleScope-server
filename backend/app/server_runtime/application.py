"""Server Profile FastAPI composition. Does not open SQLite or personal stores."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import aiohttp
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.deployment.profile import (
    PRODUCTION_READY_BLOCKERS,
    DeploymentSettings,
)
from app.server_runtime.access_identity import (
    ServerPrincipal,
    StaticTokenIdentityVerifier,
)
from app.server_runtime.composition import ServerDataPlaneComposition
from app.server_runtime.replay_api_composition import build_server_replay_app
from app.server_runtime.replay_api_service import ServerReplayApiService
from app.server_runtime.replay_event_stream import ReplayEventStream
from app.server_runtime.replay_scheduler import ReplayScheduler
from app.server_runtime.replay_scheduler_settings import ReplaySchedulerSettings
from app.server_runtime.storage.postgres_replay_scheduler import (
    PostgresReplaySchedulerStore,
)
from app.server_runtime.storage.postgres_replay_session import (
    PostgresReplaySessionStore,
)

STATUS = "SERVER_PROFILE_RUNTIME_COMPLETE_NOT_PRODUCTION_READY"
IDENTITY_ENV = "CANDLESCOPE_SERVER_API_STATIC_TOKENS_JSON"
MIN_LIVE_WORKERS = 1


class ServerProfileRuntimeError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class ServerProfileRuntime:
    settings: DeploymentSettings
    composition: ServerDataPlaneComposition
    scheduler: ReplayScheduler | None = None
    service: ServerReplayApiService | None = None
    ready: bool = False
    last_error: str | None = None
    _session: aiohttp.ClientSession | None = field(default=None, repr=False)

    def liveness(self) -> dict[str, object]:
        return {
            "status": "live",
            "profile": "server",
            "runtime_status": STATUS,
            "production_ready": False,
        }

    async def readiness(self) -> dict[str, object]:
        details: dict[str, object] = {
            "profile": "server",
            "runtime_status": STATUS,
            "production_ready": False,
            "production_ready_blockers": list(PRODUCTION_READY_BLOCKERS),
        }
        if self.scheduler is None:
            details["ready"] = False
            details["reason"] = "scheduler_unbound"
            return details
        try:
            workers = await self.scheduler.live_worker_count()
        except Exception as exc:  # noqa: BLE001
            self.last_error = type(exc).__name__
            details["ready"] = False
            details["reason"] = "scheduler_unavailable"
            return details
        health = await _observe_health_binds(self.composition.health_binds)
        details["workers"] = workers
        details["role_health"] = health
        ready = workers >= MIN_LIVE_WORKERS and all(
            item.get("ready") is True for item in health.values()
        )
        if not self.composition.health_binds:
            ready = workers >= MIN_LIVE_WORKERS
        self.ready = ready
        details["ready"] = ready
        return details

    async def stop(self) -> None:
        self.ready = False
        session = self._session
        self._session = None
        if session is not None:
            await session.close()


def load_static_identity(
    environment: Mapping[str, str] | None = None,
) -> StaticTokenIdentityVerifier:
    values = os.environ if environment is None else environment
    raw = values.get(IDENTITY_ENV)
    if not isinstance(raw, str) or not raw.strip():
        raise ServerProfileRuntimeError(
            "API_IDENTITY_MISSING",
            f"{IDENTITY_ENV} is required for server FastAPI",
        )
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not payload:
        raise ServerProfileRuntimeError(
            "API_IDENTITY_MISSING",
            f"{IDENTITY_ENV} must be a non-empty JSON object",
        )
    mapping: dict[str, ServerPrincipal] = {}
    for token, body in payload.items():
        if not isinstance(token, str) or not isinstance(body, dict):
            raise ServerProfileRuntimeError(
                "API_IDENTITY_INVALID",
                "static identity entries must be token to principal objects",
            )
        mapping[token] = ServerPrincipal(
            subject=str(body["subject"]),
            organization_id=str(body["organization_id"]),
            workspace_id=str(body["workspace_id"]),
            principal_type=str(body.get("principal_type") or "user"),
            role=str(body["role"]),
            credential_id=str(body["credential_id"]),
        )
    return StaticTokenIdentityVerifier(mapping)


def build_server_application(
    service: ServerReplayApiService,
    verifier: StaticTokenIdentityVerifier,
    runtime: ServerProfileRuntime,
) -> FastAPI:
    app = build_server_replay_app(service, verifier)
    _mount_profile_health(app, runtime)
    app.state.server_profile_runtime = runtime
    app.state.replay_service = service
    return app


async def attach_server_profile(
    app: FastAPI,
    settings: DeploymentSettings,
    composition: ServerDataPlaneComposition,
    *,
    environment: Mapping[str, str] | None = None,
) -> ServerProfileRuntime:
    values = os.environ if environment is None else environment
    scheduler_settings = ReplaySchedulerSettings.from_env(values)
    store = PostgresReplaySchedulerStore(scheduler_settings.postgres_dsn)
    session_store = PostgresReplaySessionStore(scheduler_settings.postgres_dsn)
    scheduler = ReplayScheduler(store, clock_ms=lambda: time.time_ns() // 1_000_000)
    service = ServerReplayApiService(
        scheduler,
        event_stream=ReplayEventStream(session_store),
        session_store=session_store,
    )
    verifier = load_static_identity(values)
    runtime = ServerProfileRuntime(
        settings=settings,
        composition=composition,
        scheduler=scheduler,
        service=service,
    )
    server_app = build_server_replay_app(service, verifier)
    app.router.routes[0:0] = list(server_app.router.routes)
    for exc_type, handler in server_app.exception_handlers.items():
        app.add_exception_handler(exc_type, handler)
    _mount_profile_health(app, runtime)
    app.state.server_profile_runtime = runtime
    app.state.replay_service = service
    app.state.replay_application = service
    app.state.identity_verifier = verifier
    return runtime


def _mount_profile_health(app: FastAPI, runtime: ServerProfileRuntime) -> None:
    @app.get("/health/live")
    async def live() -> dict[str, object]:
        return runtime.liveness()

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        payload = await runtime.readiness()
        status = 200 if payload.get("ready") is True else 503
        return JSONResponse(payload, status_code=status)


async def _observe_health_binds(
    binds: Mapping[str, str],
) -> dict[str, dict[str, object]]:
    observed: dict[str, dict[str, object]] = {}
    if not binds:
        return observed
    timeout = aiohttp.ClientTimeout(total=2)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for role, bind in binds.items():
            host, port = bind.split(":", 1)
            url = f"http://{host}:{port}/health/ready"
            parsed = urlsplit(url)
            if parsed.hostname not in {"127.0.0.1", "localhost"}:
                observed[role] = {"ready": False, "reason": "non_loopback"}
                continue
            try:
                async with session.get(url) as response:
                    body = await response.json()
                    observed[role] = {
                        "ready": response.status == 200
                        and bool(body.get("ready", True)),
                        "status": response.status,
                    }
            except (aiohttp.ClientError, TimeoutError, json.JSONDecodeError):
                observed[role] = {"ready": False, "reason": "unreachable"}
    return observed


__all__ = [
    "IDENTITY_ENV",
    "STATUS",
    "ServerProfileRuntime",
    "ServerProfileRuntimeError",
    "attach_server_profile",
    "build_server_application",
    "load_static_identity",
]
