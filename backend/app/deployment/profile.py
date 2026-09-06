"""Strict deployment-profile selection for CandleScope.

Personal remains the default. Server FastAPI may boot after composition and
the SQLite negative inventory pass; it is not production-ready until public
24-hour continuity exists.
"""

from __future__ import annotations

import enum
import os
from collections.abc import Mapping
from dataclasses import dataclass

DEPLOYMENT_PROFILE_ENV = "CANDLESCOPE_PROFILE"
SERVER_FOUNDATION_CONTRACT_VERSION = "candlescope.server-foundation.v1"
FASTAPI_UNLOCK_BLOCKERS: tuple[str, ...] = ()
PRODUCTION_READY_BLOCKERS = ("twenty_four_hour_public_continuity",)


class DeploymentProfile(str, enum.Enum):
    """Supported logical deployment shapes."""

    PERSONAL = "personal"
    SERVER = "server"


@dataclass(frozen=True, slots=True)
class BackendRoleBindings:
    """Technology-neutral storage and transport roles for one profile."""

    control_store: str
    market_event_log: str
    analytical_store: str
    immutable_archive: str
    live_event_transport: str


_PERSONAL_BINDINGS = BackendRoleBindings(
    control_store="sqlite",
    market_event_log="in_process",
    analytical_store="sqlite",
    immutable_archive="local_filesystem",
    live_event_transport="in_process",
)
_SERVER_BINDINGS = BackendRoleBindings(
    control_store="postgresql",
    market_event_log="kafka_compatible",
    analytical_store="clickhouse",
    immutable_archive="object_storage_parquet",
    live_event_transport="kafka_compatible",
)


class ServerRuntimeUnavailableError(RuntimeError):
    """Raised when Phase 0 contracts are mistaken for a runnable server."""


@dataclass(frozen=True, slots=True)
class DeploymentSettings:
    """Resolved deployment intent and its required backend roles."""

    profile: DeploymentProfile
    bindings: BackendRoleBindings
    contract_version: str = SERVER_FOUNDATION_CONTRACT_VERSION

    @property
    def runtime_supported(self) -> bool:
        """Whether the current codebase can boot this profile.

        Server boot is not production-ready: public 24h continuity is still
        outstanding. ``refuse_server_sqlite_boot`` remains the negative SQLite
        inventory that must pass after this property is true.
        """

        return True

    def require_runtime_support(self) -> None:
        """Fail closed until the selected runtime has implemented its roles."""

        if self.runtime_supported:
            return
        raise ServerRuntimeUnavailableError(
            "CANDLESCOPE_PROFILE=server is not implemented"
        )


def load_deployment_settings(
    environment: Mapping[str, str] | None = None,
) -> DeploymentSettings:
    """Resolve CANDLESCOPE_PROFILE strictly, defaulting to personal mode."""

    values = os.environ if environment is None else environment
    raw_profile = values.get(DEPLOYMENT_PROFILE_ENV, DeploymentProfile.PERSONAL.value)
    normalized = raw_profile.strip().lower()
    try:
        profile = DeploymentProfile(normalized)
    except ValueError as exc:
        supported = ", ".join(item.value for item in DeploymentProfile)
        raise ValueError(
            f"{DEPLOYMENT_PROFILE_ENV} must be one of: {supported}"
        ) from exc
    bindings = (
        _PERSONAL_BINDINGS
        if profile is DeploymentProfile.PERSONAL
        else _SERVER_BINDINGS
    )
    return DeploymentSettings(profile=profile, bindings=bindings)
