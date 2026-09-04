"""Inventory of FastAPI SQLite control/market/replay boot paths.

Personal Profile may open these stores. Server FastAPI must prove a
composition that does not use SQLite, local files, or in-process fallbacks.
This is not a complete sqlite3.connect census of the repository.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.deployment.profile import (
    DeploymentProfile,
    DeploymentSettings,
)

FASTAPI_SQLITE_BOOT_SCHEMA_VERSION = "candlescope.fastapi-sqlite-boot-inventory.v1"
SQLITE_BOOT_BLOCKER = "fastapi_must_not_boot_sqlite_control_or_market_paths"
_ROLES = frozenset({"control", "market", "replay"})
_SAFE_NAME = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._"
)


class FastAPISqliteBootError(RuntimeError):
    """Server FastAPI would still open a personal SQLite control or market path."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = _required_text(code, field="code")
        self.message = _required_text(message, field="message")
        self.details = dict(details or {})

    def to_wire(self) -> dict[str, object]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class FastAPISqliteBootPath:
    role: Literal["control", "market", "replay"]
    initializer: str
    module: str

    def __post_init__(self) -> None:
        if self.role not in _ROLES:
            raise ValueError("sqlite boot path role is not recognized")
        object.__setattr__(
            self,
            "initializer",
            _safe_identifier(self.initializer, field="initializer"),
        )
        object.__setattr__(
            self,
            "module",
            _safe_identifier(self.module, field="module"),
        )

    def to_wire(self) -> dict[str, str]:
        return {
            "role": self.role,
            "initializer": self.initializer,
            "module": self.module,
        }


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _safe_identifier(value: object, *, field: str) -> str:
    identifier = _required_text(value, field=field)
    if (
        len(identifier) > 128
        or not identifier.isascii()
        or identifier[0] not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        or any(character not in _SAFE_NAME for character in identifier)
    ):
        raise ValueError(f"{field} must use a bounded dotted identifier")
    return identifier


FASTAPI_SQLITE_BOOT_PATHS = (
    FastAPISqliteBootPath(
        "control", "init_klines_storage", "app.deployment.personal_runtime"
    ),
    FastAPISqliteBootPath(
        "market", "init_market_metrics_storage", "app.deployment.personal_runtime"
    ),
    FastAPISqliteBootPath(
        "market", "init_trade_flow_storage", "app.deployment.personal_runtime"
    ),
    FastAPISqliteBootPath(
        "market", "init_liquidation_storage", "app.deployment.personal_runtime"
    ),
    FastAPISqliteBootPath("replay", "ReplaySQLiteStore", "app.replay.runtime"),
)
_SERVER_FORBIDDEN_BINDINGS = frozenset(
    {"sqlite", "in_process", "local_filesystem"}
)


def fastapi_sqlite_boot_inventory() -> dict[str, object]:
    return {
        "schema_version": FASTAPI_SQLITE_BOOT_SCHEMA_VERSION,
        "blocker": SQLITE_BOOT_BLOCKER,
        "server_boot_allowed": False,
        "profile_gated": True,
        "paths": [path.to_wire() for path in FASTAPI_SQLITE_BOOT_PATHS],
    }


def refuse_server_sqlite_boot(
    settings: DeploymentSettings,
    *,
    composition: object | None = None,
) -> None:
    """No-op for personal. Server FastAPI must prove it will not open SQLite paths."""

    if not isinstance(settings, DeploymentSettings):
        raise TypeError("settings must be a DeploymentSettings")
    if settings.profile is DeploymentProfile.PERSONAL:
        return
    details = fastapi_sqlite_boot_inventory()
    if composition is None:
        raise FastAPISqliteBootError(
            "FASTAPI_SQLITE_CONTROL_OR_MARKET_PATH",
            "server FastAPI cannot prove it will not initialize SQLite "
            "control, market, or replay stores",
            details=details,
        )
    bindings = settings.bindings
    forbidden = {
        "control_store": bindings.control_store,
        "market_event_log": bindings.market_event_log,
        "analytical_store": bindings.analytical_store,
        "immutable_archive": bindings.immutable_archive,
        "live_event_transport": bindings.live_event_transport,
    }
    sqlite_hits = {
        role: value
        for role, value in forbidden.items()
        if value in _SERVER_FORBIDDEN_BINDINGS
    }
    if sqlite_hits:
        details["forbidden_bindings"] = sqlite_hits
        raise FastAPISqliteBootError(
            "FASTAPI_SQLITE_CONTROL_OR_MARKET_PATH",
            "server composition still selects SQLite, local, or in-process fallbacks",
            details=details,
        )


__all__ = [
    "FASTAPI_SQLITE_BOOT_PATHS",
    "FASTAPI_SQLITE_BOOT_SCHEMA_VERSION",
    "SQLITE_BOOT_BLOCKER",
    "FastAPISqliteBootError",
    "FastAPISqliteBootPath",
    "fastapi_sqlite_boot_inventory",
    "refuse_server_sqlite_boot",
]
