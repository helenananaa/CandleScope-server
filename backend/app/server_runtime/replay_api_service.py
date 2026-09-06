"""Server replay API facade. Talks to scheduler/store, never a live Actor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Any

from app.replay.commands import CommandResult
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.replay.models import ReplayCommand
from app.server_runtime.access_identity import ServerPrincipal
from app.server_runtime.replay_authorization import (
    require_read,
    require_scope,
    require_write,
)
from app.server_runtime.replay_event_stream import ReplayEventStream
from app.server_runtime.replay_scheduler import ReplayScheduler, ReplaySchedulerRequest
from app.server_runtime.replay_session import command_result_from_payload

CAPABILITY_UNAVAILABLE = "CAPABILITY_UNAVAILABLE"
V2_UNAVAILABLE_ENDPOINTS = {
    "GET /api/v1/replay/runs/{run_id}/tracks": "unavailable",
    "POST /api/v1/replay/runs/{run_id}/markets": "unavailable",
    "GET /api/v1/replay/runs/{run_id}/journal": "unavailable",
}
FIRST_SLICE_ENDPOINTS = {
    "GET /api/v1/replay/capabilities": "implemented",
    "GET /api/v1/replay/catalog": "implemented",
    "POST /api/v1/replay/runs": "implemented",
    "GET /api/v1/replay/runs": "implemented",
    "GET /api/v1/replay/runs/{run_id}": "implemented",
    "GET /api/v1/replay/runs/session/{session_id}": "implemented",
    "POST /api/v1/replay/runs/session/{session_id}/commands": "implemented",
    "DELETE /api/v1/replay/runs/{run_id}": "implemented",
    "WS /api/v1/stream/replay/{session_id}": "implemented",
}


class ServerReplayApiService:
    def __init__(
        self,
        scheduler: ReplayScheduler,
        *,
        event_stream: ReplayEventStream,
        session_store: Any | None = None,
        command_wait_ms: int = 8_000,
        unavailable_endpoints: Mapping[str, str] | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._event_stream = event_stream
        self._session_store = session_store
        self._command_wait_ms = command_wait_ms
        inventory = dict(FIRST_SLICE_ENDPOINTS)
        inventory.update(V2_UNAVAILABLE_ENDPOINTS)
        inventory.update(dict(unavailable_endpoints or {}))
        self._inventory = inventory

    def capabilities(self) -> dict[str, object]:
        return {
            "protocol": "replay.v1",
            "enabled": True,
            "available": True,
            "profile": "server",
            "sources": {
                "agg_trade": {
                    "enabled": True,
                    "stream": "binance:futures:BTCUSDT@agg_trade",
                },
                "bar": {"enabled": False, "reason": CAPABILITY_UNAVAILABLE},
            },
            "quality_mode": "exact",
            "blind_mode": False,
            "warmup_bars": 0,
            "replay_v2": {"enabled": False, "reason": CAPABILITY_UNAVAILABLE},
            "endpoints": dict(self._inventory),
        }

    async def catalog(self) -> dict[str, object]:
        return {
            "protocol": "replay.v1",
            "markets": ["binance:futures:BTCUSDT@agg_trade"],
            "latest_forbidden": True,
        }

    async def create_run(
        self, payload: Mapping[str, object], *, principal: ServerPrincipal
    ) -> dict[str, object]:
        require_write(principal)
        if "query_path" in payload:
            raise ReplayDomainError(
                ReplayErrorCode.INVALID_STATE_TRANSITION,
                "server replay requests cannot select a local query path",
            )
        organization_id = str(
            payload.get("organization_id") or principal.organization_id
        )
        workspace_id = str(payload.get("workspace_id") or principal.workspace_id)
        require_scope(principal, organization_id, workspace_id)
        scheduler_payload = dict(payload)
        # This payload crosses into independently deployed Workers. Persist the
        # verified identity scope even when the caller omitted it.
        scheduler_payload["organization_id"] = principal.organization_id
        scheduler_payload["workspace_id"] = principal.workspace_id
        request = await self._scheduler.create_request(
            organization_id=organization_id,
            workspace_id=workspace_id,
            idempotency_key=str(
                payload.get("idempotency_key") or payload.get("client_key") or "default"
            ),
            payload=scheduler_payload,
            priority=int(payload.get("priority") or 0),
        )
        return _request_payload(request)

    async def list_runs(self, *, principal: ServerPrincipal) -> dict[str, object]:
        require_read(principal)
        requests = await self._scheduler.list_requests(
            organization_id=principal.organization_id,
            workspace_id=principal.workspace_id,
        )
        return {
            "protocol": "replay.v1",
            "runs": [_request_payload(request) for request in requests],
        }

    async def get_run(
        self, run_id: str, *, principal: ServerPrincipal
    ) -> dict[str, object]:
        require_read(principal)
        request = await self._scheduler.get_request(run_id)
        if request is None:
            raise ReplayDomainError(
                ReplayErrorCode.SESSION_NOT_FOUND, "replay run does not exist"
            )
        require_scope(principal, request.organization_id, request.workspace_id)
        return _request_payload(request)

    async def get_session(
        self, session_id: str, *, principal: ServerPrincipal
    ) -> dict[str, object]:
        require_read(principal)
        request = await self._scheduler.get_by_session(session_id)
        if request is None:
            raise ReplayDomainError(
                ReplayErrorCode.SESSION_NOT_FOUND,
                "replay session does not exist",
            )
        require_scope(principal, request.organization_id, request.workspace_id)
        state = request.state.value
        if await self._scheduler.assignment_is_recovering(session_id):
            state = "RECOVERING"
        return {
            "protocol": "replay.v1",
            "session_id": session_id,
            "run_id": request.request_id,
            "state": state,
            "organization_id": request.organization_id,
            "workspace_id": request.workspace_id,
            "attempt": request.attempt,
        }

    async def submit_command(
        self,
        session_id: str,
        command: ReplayCommand,
        *,
        principal: ServerPrincipal,
    ) -> CommandResult:
        require_write(principal)
        request = await self._scheduler.get_by_session(session_id)
        if request is None:
            raise ReplayDomainError(
                ReplayErrorCode.SESSION_NOT_FOUND,
                "command journal is bound to a leased worker session",
                details={"session_id": session_id, "command_id": command.command_id},
            )
        require_scope(principal, request.organization_id, request.workspace_id)
        await self._scheduler.enqueue_command(session_id, command.to_dict())
        if self._session_store is None:
            raise ReplayDomainError(
                ReplayErrorCode.SESSION_NOT_FOUND,
                "command journal is bound to a leased worker session",
                details={"session_id": session_id, "command_id": command.command_id},
            )
        deadline = asyncio.get_running_loop().time() + (self._command_wait_ms / 1000)
        while asyncio.get_running_loop().time() < deadline:
            payload = await self._session_store.get_command_result(
                session_id, command.command_id
            )
            if payload is not None:
                if payload.get("rejected") is True:
                    try:
                        code = ReplayErrorCode(str(payload.get("error_code")))
                    except ValueError:
                        code = ReplayErrorCode.INVALID_STATE_TRANSITION
                    raise ReplayDomainError(
                        code,
                        str(payload.get("error_message") or "command was rejected"),
                        details={
                            "session_id": session_id,
                            "command_id": command.command_id,
                        },
                    )
                return command_result_from_payload(payload)
            await asyncio.sleep(0.05)
        raise ReplayDomainError(
            ReplayErrorCode.SESSION_NOT_FOUND,
            "command result was not durable before the wait bound",
            details={"session_id": session_id, "command_id": command.command_id},
        )

    async def cancel_run(
        self, run_id: str, *, principal: ServerPrincipal
    ) -> dict[str, object]:
        require_write(principal)
        request = await self._scheduler.get_request(run_id)
        if request is None:
            raise ReplayDomainError(
                ReplayErrorCode.SESSION_NOT_FOUND,
                "replay run does not exist",
            )
        require_scope(principal, request.organization_id, request.workspace_id)
        request = await self._scheduler.cancel(run_id)
        return _request_payload(request)

    def endpoint_inventory(self) -> dict[str, str]:
        return dict(self._inventory)

    async def subscribe_events(
        self,
        session_id: str,
        *,
        after_sequence: int | None,
        principal: ServerPrincipal,
    ) -> AsyncIterator:
        require_read(principal)
        async for event in self._event_stream.subscribe(
            session_id,
            after_sequence=after_sequence,
            organization_id=principal.organization_id,
            workspace_id=principal.workspace_id,
        ):
            yield event


def _request_payload(request: ReplaySchedulerRequest) -> dict[str, object]:
    return {
        "protocol": "replay.v1",
        "run_id": request.request_id,
        "session_id": request.session_id,
        "state": request.state.value,
        "organization_id": request.organization_id,
        "workspace_id": request.workspace_id,
        "attempt": request.attempt,
    }
