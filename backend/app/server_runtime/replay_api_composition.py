"""Compose the server replay HTTP/WS app without unlocking the main Profile."""

from __future__ import annotations

from fastapi import FastAPI, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from app.replay.errors import ReplayDomainError
from app.replay.models import ReplayCommand
from app.server_runtime.access_identity import (
    IdentityError,
    IdentityVerifier,
    ServerPrincipal,
)
from app.server_runtime.replay_api_service import (
    CAPABILITY_UNAVAILABLE,
    ServerReplayApiService,
)
from app.server_runtime.replay_authorization import ReplayAuthorizationError

V2_UNAVAILABLE = {
    "GET /api/v1/replay/runs/{run_id}/tracks": "unavailable",
    "POST /api/v1/replay/runs/{run_id}/markets": "unavailable",
    "GET /api/v1/replay/runs/{run_id}/journal": "unavailable",
}


def build_server_replay_app(
    service: ServerReplayApiService,
    verifier: IdentityVerifier,
) -> FastAPI:
    app = FastAPI()
    app.state.replay_application = service
    app.state.identity_verifier = verifier

    async def principal(authorization: str | None) -> ServerPrincipal:
        if not authorization or not authorization.lower().startswith("bearer "):
            raise IdentityError("UNAUTHENTICATED", "missing bearer token")
        return await verifier.verify(authorization.split(" ", 1)[1])

    @app.exception_handler(IdentityError)
    async def identity_handler(_request, exc: IdentityError) -> JSONResponse:
        status = 401 if exc.code == "UNAUTHENTICATED" else 403
        return JSONResponse(
            {"error": {"code": exc.code, "message": exc.message}}, status
        )

    @app.exception_handler(ReplayAuthorizationError)
    async def authz_handler(_request, exc: ReplayAuthorizationError) -> JSONResponse:
        return JSONResponse({"error": {"code": exc.code, "message": exc.message}}, 403)

    @app.exception_handler(ReplayDomainError)
    async def domain_handler(_request, exc: ReplayDomainError) -> JSONResponse:
        payload = {
            "protocol": "replay.v1",
            "error": {
                "code": exc.code.value,
                "message": exc.message,
                "details": dict(exc.details),
            },
        }
        if exc.code.value == CAPABILITY_UNAVAILABLE:
            payload["error"]["code"] = CAPABILITY_UNAVAILABLE
        return JSONResponse(payload, exc.http_status)

    @app.get("/api/v1/replay/capabilities")
    async def capabilities() -> dict[str, object]:
        return dict(service.capabilities())

    @app.get("/api/v1/replay/catalog")
    async def catalog(
        authorization: str | None = Header(default=None),
    ) -> dict[str, object]:
        await principal(authorization)
        return await service.catalog()

    @app.post("/api/v1/replay/runs")
    async def create_run(
        payload: dict[str, object],
        authorization: str | None = Header(default=None),
    ) -> dict[str, object]:
        return await service.create_run(
            payload, principal=await principal(authorization)
        )

    @app.get("/api/v1/replay/runs")
    async def list_runs(
        authorization: str | None = Header(default=None),
    ) -> dict[str, object]:
        return await service.list_runs(principal=await principal(authorization))

    @app.get("/api/v1/replay/runs/{run_id}")
    async def get_run(
        run_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, object]:
        return await service.get_run(run_id, principal=await principal(authorization))

    @app.get("/api/v1/replay/runs/session/{session_id}")
    async def get_session(
        session_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, object]:
        return await service.get_session(
            session_id, principal=await principal(authorization)
        )

    @app.post("/api/v1/replay/runs/session/{session_id}/commands")
    async def submit_command(
        session_id: str,
        payload: dict[str, object],
        authorization: str | None = Header(default=None),
    ) -> dict[str, object]:
        command = ReplayCommand.from_dict(payload)
        result = await service.submit_command(
            session_id, command, principal=await principal(authorization)
        )
        return {
            "command_id": result.command_id,
            "revision": result.revision,
            "sequence": result.sequence,
            "state": result.state.value,
            "state_hash": result.state_hash,
        }

    @app.delete("/api/v1/replay/runs/{run_id}")
    async def cancel_run(
        run_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, object]:
        return await service.cancel_run(
            run_id, principal=await principal(authorization)
        )

    @app.get("/api/v1/replay/inventory")
    async def inventory() -> dict[str, object]:
        return {"endpoints": service.endpoint_inventory()}

    @app.websocket("/api/v1/stream/replay/{session_id}")
    async def stream(websocket: WebSocket, session_id: str) -> None:
        token = websocket.headers.get("authorization")
        ident = await principal(token)
        await websocket.accept()
        try:
            async for event in service.subscribe_events(
                session_id, after_sequence=None, principal=ident
            ):
                await websocket.send_json(event.to_dict())
        except WebSocketDisconnect:
            return

    return app
