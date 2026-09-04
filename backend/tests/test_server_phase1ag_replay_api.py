from __future__ import annotations

import ast
from pathlib import Path

import pytest
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.replay.constants import REPLAY_PROTOCOL, CommandType
from app.replay.models import ReplayCommand
from app.server_runtime.access_identity import (
    ServerPrincipal,
    StaticTokenIdentityVerifier,
)
from app.server_runtime.replay_api_composition import build_server_replay_app
from app.server_runtime.replay_api_service import (
    CAPABILITY_UNAVAILABLE,
    ServerReplayApiService,
)
from app.server_runtime.replay_event_stream import ReplayEventStream
from app.server_runtime.replay_scheduler import ReplayScheduler
from app.server_runtime.testing.in_memory_replay_scheduler import (
    InMemoryReplaySchedulerStore,
)
from fastapi.testclient import TestClient

REPLAY_API = Path(__file__).resolve().parents[1] / "app" / "api" / "v1" / "replay.py"


class _EmptyOutbox:
    async def events_after(self, session_id: str, after_sequence: int):
        del session_id, after_sequence
        return ()


def _principal(role: str, token_suffix: str) -> tuple[str, ServerPrincipal]:
    token = f"token-{role}-{token_suffix}"
    principal = ServerPrincipal(
        subject=f"user-{role}",
        organization_id="org-alpha",
        workspace_id="ws-research",
        principal_type="user",
        role=role,
        credential_id=f"cred-{role}",
    )
    return token, principal


def _app() -> tuple[TestClient, ReplayScheduler]:
    now = [1_000]
    store = InMemoryReplaySchedulerStore(clock_ms=lambda: now[0])
    scheduler = ReplayScheduler(store, clock_ms=lambda: now[0])
    tokens = {}
    for role in ("admin", "researcher", "trader", "read-only"):
        token, principal = _principal(role, "a")
        tokens[token] = principal
    other = ServerPrincipal(
        subject="other",
        organization_id="org-beta",
        workspace_id="ws-research",
        principal_type="user",
        role="admin",
        credential_id="cred-other",
    )
    tokens["token-other"] = other
    service = ServerReplayApiService(
        scheduler, event_stream=ReplayEventStream(_EmptyOutbox())
    )
    app = build_server_replay_app(service, StaticTokenIdentityVerifier(tokens))
    return TestClient(app), scheduler


def test_capabilities_and_auth_matrix() -> None:
    client, _scheduler = _app()
    caps = client.get("/api/v1/replay/capabilities").json()
    assert caps["profile"] == "server"
    assert caps["replay_v2"]["reason"] == CAPABILITY_UNAVAILABLE
    assert caps["sources"]["agg_trade"]["enabled"] is True
    denied = client.post("/api/v1/replay/runs", json={"idempotency_key": "k1"})
    assert denied.status_code == 401
    readonly = client.post(
        "/api/v1/replay/runs",
        json={"idempotency_key": "k1"},
        headers={"Authorization": "Bearer token-read-only-a"},
    )
    assert readonly.status_code == 403
    created = client.post(
        "/api/v1/replay/runs",
        json={"idempotency_key": "k1", "source_kind": "agg_trade"},
        headers={"Authorization": "Bearer token-trader-a"},
    )
    assert created.status_code == 200
    body = created.json()
    assert body["state"] in {"PENDING", "ASSIGNED"}
    other = client.get(
        f"/api/v1/replay/runs/{body['run_id']}",
        headers={"Authorization": "Bearer token-other"},
    )
    assert other.status_code == 403
    listed = client.get(
        "/api/v1/replay/runs",
        headers={"Authorization": "Bearer token-read-only-a"},
    )
    assert listed.status_code == 200
    inventory = client.get("/api/v1/replay/inventory").json()
    assert inventory["endpoints"]["GET /api/v1/replay/capabilities"] == "implemented"


def test_endpoint_inventory_covers_replay_router_and_marks_v2_unavailable() -> None:
    tree = ast.parse(REPLAY_API.read_text(encoding="utf-8"))
    routes: list[str] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for deco in node.decorator_list:
            if not isinstance(deco, ast.Call):
                continue
            func = deco.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr in {"get", "post", "delete"}
                and deco.args
                and isinstance(deco.args[0], ast.Constant)
            ):
                routes.append(str(deco.args[0].value))
    assert "/capabilities" in routes
    assert "/runs" in routes
    client, _scheduler = _app()
    inventory = client.get("/api/v1/replay/inventory").json()["endpoints"]
    assert "POST /api/v1/replay/runs/{run_id}/markets" in inventory
    assert inventory["POST /api/v1/replay/runs/{run_id}/markets"] == "unavailable"


def test_server_profile_remains_refused_and_replay_package_has_application_port() -> (
    None
):
    from app.deployment import FastAPISqliteBootError, refuse_server_sqlite_boot

    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.runtime_supported is True
    settings.require_runtime_support()
    with pytest.raises(FastAPISqliteBootError):
        refuse_server_sqlite_boot(settings)
    path = Path(__file__).resolve().parents[1] / "app" / "replay" / "application.py"
    assert path.is_file()
    text = path.read_text(encoding="utf-8")
    assert "class ReplayApplication" in text
    assert "app.server_runtime" not in text


def test_command_requires_write_role() -> None:
    client, _scheduler = _app()
    command = ReplayCommand(
        protocol=REPLAY_PROTOCOL,
        command_id="c1",
        client_instance_id="ui",
        expected_revision=0,
        type=CommandType.STEP,
        payload={"count": 1},
    )
    response = client.post(
        "/api/v1/replay/runs/session/sess-1/commands",
        json=command.to_dict(),
        headers={"Authorization": "Bearer token-read-only-a"},
    )
    assert response.status_code == 403
