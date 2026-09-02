"""Deployment-profile contracts shared by personal and server runtimes."""

from .fastapi_sqlite_boot import (
    FASTAPI_SQLITE_BOOT_PATHS,
    FASTAPI_SQLITE_BOOT_SCHEMA_VERSION,
    SQLITE_BOOT_BLOCKER,
    FastAPISqliteBootError,
    FastAPISqliteBootPath,
    fastapi_sqlite_boot_inventory,
    refuse_server_sqlite_boot,
)
from .profile import (
    DEPLOYMENT_PROFILE_ENV,
    FASTAPI_UNLOCK_BLOCKERS,
    SERVER_FOUNDATION_CONTRACT_VERSION,
    BackendRoleBindings,
    DeploymentProfile,
    DeploymentSettings,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)

__all__ = [
    "DEPLOYMENT_PROFILE_ENV",
    "FASTAPI_SQLITE_BOOT_PATHS",
    "FASTAPI_SQLITE_BOOT_SCHEMA_VERSION",
    "FASTAPI_UNLOCK_BLOCKERS",
    "SERVER_FOUNDATION_CONTRACT_VERSION",
    "SQLITE_BOOT_BLOCKER",
    "BackendRoleBindings",
    "DeploymentProfile",
    "DeploymentSettings",
    "FastAPISqliteBootError",
    "FastAPISqliteBootPath",
    "ServerRuntimeUnavailableError",
    "fastapi_sqlite_boot_inventory",
    "load_deployment_settings",
    "refuse_server_sqlite_boot",
]
