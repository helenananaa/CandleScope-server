"""Fail-closed data-plane composition inventory.

Phase 1AH unlocks FastAPI ``CANDLESCOPE_PROFILE=server`` after this inventory
and the SQLite negative gate pass. Public 24-hour continuity is still absent,
so the composition is not production-ready.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from app.deployment.fastapi_sqlite_boot import fastapi_sqlite_boot_inventory
from app.deployment.profile import (
    FASTAPI_UNLOCK_BLOCKERS,
    PRODUCTION_READY_BLOCKERS,
)
from app.server_runtime.archive_settings import (
    ArchiveWriterConfigurationError,
    ArchiveWriterSettings,
)
from app.server_runtime.health_http import HealthHttpBindError, parse_health_bind
from app.server_runtime.query_settings import (
    QueryServiceConfigurationError,
    QueryServiceSettings,
)
from app.server_runtime.settings import (
    ServerCollectorConfigurationError,
    ServerCollectorSettings,
)
from app.server_runtime.writer_settings import (
    ClickHouseWriterConfigurationError,
    ClickHouseWriterSettings,
)

_ROLE_CONFIG_ERRORS = (
    ServerCollectorConfigurationError,
    ClickHouseWriterConfigurationError,
    ArchiveWriterConfigurationError,
    QueryServiceConfigurationError,
    ValueError,
    TypeError,
)

COMPOSITION_SCHEMA_VERSION = "candlescope.server-data-plane-composition.v1"
COLLECTOR_HEALTH_BIND_ENV = "CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND"
WRITER_HEALTH_BIND_ENV = "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_HEALTH_BIND"
ARCHIVE_HEALTH_BIND_ENV = "CANDLESCOPE_SERVER_ARCHIVE_WRITER_HEALTH_BIND"
_HEALTH_BIND_ENV = (
    ("collector", COLLECTOR_HEALTH_BIND_ENV),
    ("clickhouse_writer", WRITER_HEALTH_BIND_ENV),
    ("parquet_archiver", ARCHIVE_HEALTH_BIND_ENV),
)


class ServerCompositionError(RuntimeError):
    """The independent server processes are not configured as one composition."""

    def __init__(
        self, code: str, message: str, *, details: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": COMPOSITION_SCHEMA_VERSION,
            "status": "incomplete",
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "fastapi_runtime_supported": False,
            "fastapi_unlock_blockers": list(FASTAPI_UNLOCK_BLOCKERS),
            "production_ready": False,
            "production_ready_blockers": list(PRODUCTION_READY_BLOCKERS),
        }


@dataclass(frozen=True, slots=True)
class ServerDataPlaneComposition:
    collector: ServerCollectorSettings
    clickhouse_writer: ClickHouseWriterSettings
    parquet_archiver: ArchiveWriterSettings
    snapshot_query: QueryServiceSettings
    health_binds: dict[str, str]

    def to_public_wire(self) -> dict[str, object]:
        return {
            "schema_version": COMPOSITION_SCHEMA_VERSION,
            "status": "configured",
            "roles": [
                "collector",
                "clickhouse_writer",
                "parquet_archiver",
                "snapshot_query",
            ],
            "kafka_bootstrap_servers": list(self.collector.kafka_bootstrap_servers),
            "clickhouse_url": self.clickhouse_writer.clickhouse_url,
            "s3_endpoint_url": self.parquet_archiver.s3_endpoint_url,
            "s3_bucket": self.parquet_archiver.s3_bucket,
            "health_binds": dict(self.health_binds),
            "fastapi_runtime_supported": True,
            "fastapi_unlock_blockers": list(FASTAPI_UNLOCK_BLOCKERS),
            "production_ready": False,
            "production_ready_blockers": list(PRODUCTION_READY_BLOCKERS),
        }


def load_server_data_plane_composition(
    environment: Mapping[str, str] | None = None,
) -> ServerDataPlaneComposition:
    """Load and cross-check independent process settings. Does not connect."""

    values = os.environ if environment is None else environment
    failures: list[dict[str, str]] = []
    collector = _load_role(
        "collector",
        lambda: ServerCollectorSettings.from_env(values),
        failures,
    )
    writer = _load_role(
        "clickhouse_writer",
        lambda: ClickHouseWriterSettings.from_env(values),
        failures,
    )
    archive = _load_role(
        "parquet_archiver",
        lambda: ArchiveWriterSettings.from_env(values),
        failures,
    )
    query = _load_role(
        "snapshot_query",
        lambda: QueryServiceSettings.from_env(values),
        failures,
    )
    if failures:
        raise ServerCompositionError(
            "ROLE_CONFIG_INCOMPLETE",
            "one or more data-plane roles are missing required configuration",
            details={"roles": failures},
        )
    assert collector is not None
    assert writer is not None
    assert archive is not None
    assert query is not None
    if (
        len(
            {
                collector.kafka_bootstrap_servers,
                writer.kafka_bootstrap_servers,
                archive.kafka_bootstrap_servers,
                query.kafka_bootstrap_servers,
            }
        )
        != 1
    ):
        raise ServerCompositionError(
            "KAFKA_BOOTSTRAP_DRIFT",
            "collector, writer, archiver, and query must share kafka bootstrap servers",
        )
    if writer.clickhouse_url != query.clickhouse_url:
        raise ServerCompositionError(
            "CLICKHOUSE_URL_DRIFT",
            "ClickHouse writer and snapshot query must share clickhouse_url",
        )
    if (
        archive.s3_endpoint_url != query.s3_endpoint_url
        or archive.s3_bucket != query.s3_bucket
    ):
        raise ServerCompositionError(
            "OBJECT_STORE_DRIFT",
            "Parquet archiver and snapshot query must share S3 endpoint and bucket",
        )
    if query.auth_organization_id is None or query.auth_workspace_id is None:
        raise ServerCompositionError(
            "QUERY_SCOPE_UNBOUND",
            "Server Profile query credentials must bind an organization and workspace",
            details={"role": "snapshot_query"},
        )
    health_binds = _optional_health_binds(values)
    # The query API is a mandatory part of every Server Profile replay path,
    # so readiness must observe its own bind even when optional role-health
    # environment variables are absent.
    query_health_bind = f"{query.bind_host}:{query.bind_port}"
    try:
        parse_health_bind(query_health_bind)
    except HealthHttpBindError as exc:
        raise ServerCompositionError(
            "HEALTH_BIND_INVALID",
            "snapshot query bind is not a loopback HOST:PORT",
            details={"role": "snapshot_query", "message": str(exc)},
        ) from exc
    health_binds["snapshot_query"] = query_health_bind
    return ServerDataPlaneComposition(
        collector=collector,
        clickhouse_writer=writer,
        parquet_archiver=archive,
        snapshot_query=query,
        health_binds=health_binds,
    )


def fastapi_unlock_refusal() -> dict[str, object]:
    return ServerCompositionError(
        "FASTAPI_SERVER_PROFILE_LOCKED",
        "data-plane composition is incomplete; CANDLESCOPE_PROFILE=server stays locked",
        details={
            "fastapi_unlock_blockers": list(FASTAPI_UNLOCK_BLOCKERS),
            "production_ready_blockers": list(PRODUCTION_READY_BLOCKERS),
            "sqlite_boot": fastapi_sqlite_boot_inventory(),
        },
    ).to_wire()


def fastapi_unlock_status(composition: ServerDataPlaneComposition) -> dict[str, object]:
    wire = composition.to_public_wire()
    wire["sqlite_boot"] = fastapi_sqlite_boot_inventory()
    return wire


def _load_role(
    role: str,
    loader: Callable[[], object],
    failures: list[dict[str, str]],
) -> object:
    try:
        return loader()
    except _ROLE_CONFIG_ERRORS as exc:
        failures.append(
            {
                "role": role,
                "error_type": type(exc).__name__,
                "message": str(exc),
            }
        )
        return None


def _optional_health_binds(values: Mapping[str, str]) -> dict[str, str]:
    binds: dict[str, str] = {}
    for role, env_name in _HEALTH_BIND_ENV:
        raw = values.get(env_name)
        if raw is None or not raw.strip():
            continue
        try:
            parse_health_bind(raw)
        except HealthHttpBindError as exc:
            raise ServerCompositionError(
                "HEALTH_BIND_INVALID",
                f"{role} health bind is not a loopback HOST:PORT",
                details={"role": role, "message": str(exc)},
            ) from exc
        binds[role] = raw.strip()
    return binds


__all__ = [
    "COMPOSITION_SCHEMA_VERSION",
    "ServerCompositionError",
    "ServerDataPlaneComposition",
    "fastapi_unlock_refusal",
    "fastapi_unlock_status",
    "load_server_data_plane_composition",
]
