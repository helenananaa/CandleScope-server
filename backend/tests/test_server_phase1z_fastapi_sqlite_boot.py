from __future__ import annotations

import ast
import asyncio
import inspect
import json
from pathlib import Path

import pytest
from app.deployment import (
    FASTAPI_SQLITE_BOOT_PATHS,
    FASTAPI_UNLOCK_BLOCKERS,
    SQLITE_BOOT_BLOCKER,
    DeploymentSettings,
    FastAPISqliteBootError,
    fastapi_sqlite_boot_inventory,
    load_deployment_settings,
    refuse_server_sqlite_boot,
)
from app.replay.runtime import start_replay_runtime
from app.replay.storage import ReplaySQLiteStore
from app.server_runtime.composition import fastapi_unlock_refusal

BACKEND_ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = BACKEND_ROOT / "app" / "main.py"
REPLAY_RUNTIME_PATH = BACKEND_ROOT / "app" / "replay" / "runtime.py"


def _module_ast(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _function_def(
    tree: ast.Module, name: str
) -> ast.AsyncFunctionDef | ast.FunctionDef:
    for node in tree.body:
        if (
            isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
            and node.name == name
        ):
            return node
    raise AssertionError(f"missing function {name}")


def _ordered_call_names(function: ast.AST) -> list[str]:
    names: list[str] = []
    for statement in getattr(function, "body", ()):
        for child in ast.walk(statement):
            if not isinstance(child, ast.Call):
                continue
            func = child.func
            if isinstance(func, ast.Name):
                names.append(func.id)
            elif isinstance(func, ast.Attribute):
                names.append(func.attr)
    return names


def test_sqlite_boot_inventory_is_frozen_and_not_profile_gated() -> None:
    wire = fastapi_sqlite_boot_inventory()
    assert wire["schema_version"] == "candlescope.fastapi-sqlite-boot-inventory.v1"
    assert wire["blocker"] == SQLITE_BOOT_BLOCKER
    assert SQLITE_BOOT_BLOCKER not in FASTAPI_UNLOCK_BLOCKERS
    assert wire["server_boot_allowed"] is False
    assert wire["profile_gated"] is True
    assert [path.initializer for path in FASTAPI_SQLITE_BOOT_PATHS] == [
        "init_klines_storage",
        "init_market_metrics_storage",
        "init_trade_flow_storage",
        "init_liquidation_storage",
        "ReplaySQLiteStore",
    ]
    assert [path.module for path in FASTAPI_SQLITE_BOOT_PATHS[:4]] == [
        "app.deployment.personal_runtime"
    ] * 4
    assert wire["paths"] == [path.to_wire() for path in FASTAPI_SQLITE_BOOT_PATHS]
    dumped = json.dumps(wire)
    assert "password" not in dumped
    assert "secret" not in dumped
    assert "token" not in dumped


def test_personal_may_open_sqlite_and_server_cannot() -> None:
    personal = load_deployment_settings({})
    refuse_server_sqlite_boot(personal)
    server = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    with pytest.raises(FastAPISqliteBootError) as exc:
        refuse_server_sqlite_boot(server)
    assert exc.value.code == "FASTAPI_SQLITE_CONTROL_OR_MARKET_PATH"
    assert exc.value.details["server_boot_allowed"] is False
    assert "token" not in json.dumps(exc.value.to_wire())
    server.require_runtime_support()


def test_server_startup_still_refuses_sqlite_if_runtime_support_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from app import main as main_module
    from app.deployment import personal_runtime as personal_module

    storage_calls: list[str] = []
    monkeypatch.setenv("CANDLESCOPE_PROFILE", "server")
    monkeypatch.setattr(
        DeploymentSettings, "require_runtime_support", lambda self: None
    )
    monkeypatch.setattr(
        personal_module,
        "init_klines_storage",
        lambda: storage_calls.append("klines"),
    )
    monkeypatch.setattr(
        personal_module,
        "init_market_metrics_storage",
        lambda: storage_calls.append("metrics"),
    )
    monkeypatch.setattr(
        personal_module,
        "init_trade_flow_storage",
        lambda _path: storage_calls.append("trade_flow"),
    )
    monkeypatch.setattr(
        personal_module,
        "init_liquidation_storage",
        lambda _path: storage_calls.append("liquidation"),
    )

    with pytest.raises(FastAPISqliteBootError) as exc:
        asyncio.run(main_module.startup_event())
    assert exc.value.code == "FASTAPI_SQLITE_CONTROL_OR_MARKET_PATH"
    assert storage_calls == []


def test_startup_calls_sqlite_inits_after_both_server_guards() -> None:
    startup = _function_def(_module_ast(MAIN_PATH), "startup_event")
    names = _ordered_call_names(startup)
    required = [
        "load_deployment_settings",
        "require_runtime_support",
        "refuse_server_sqlite_boot",
        "start_personal_runtime",
        "start_server_runtime",
    ]
    indexes = [names.index(name) for name in required]
    assert indexes == sorted(indexes)
    personal = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "deployment"
        / "personal_runtime.py"
    )
    personal_names = _ordered_call_names(
        _function_def(_module_ast(personal), "start_personal_runtime")
    )
    for name in (
        "init_klines_storage",
        "init_market_metrics_storage",
        "init_trade_flow_storage",
        "init_liquidation_storage",
    ):
        assert name in personal_names
    server_source = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "deployment"
        / "server_runtime.py"
    ).read_text(encoding="utf-8")
    assert "init_klines_storage" not in server_source


def test_replay_runtime_still_defaults_to_sqlite_and_fastapi_stays_locked() -> None:
    source = inspect.getsource(start_replay_runtime)
    assert "store_factory or ReplaySQLiteStore" in source
    assert "ReplaySQLiteStore" in REPLAY_RUNTIME_PATH.read_text(encoding="utf-8")
    assert ReplaySQLiteStore.__module__ == "app.replay.storage.sqlite_store"
    refusal = fastapi_unlock_refusal()
    assert refusal["fastapi_runtime_supported"] is False
    assert refusal["production_ready"] is False
    assert refusal["details"]["sqlite_boot"]["server_boot_allowed"] is False
    assert refusal["details"]["sqlite_boot"]["paths"] == [
        path.to_wire() for path in FASTAPI_SQLITE_BOOT_PATHS
    ]
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    settings.require_runtime_support()
    with pytest.raises(FastAPISqliteBootError):
        refuse_server_sqlite_boot(settings)
