"""Fail-closed configuration for the standalone server collector process."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

ENV_PREFIX = "CANDLESCOPE_SERVER_COLLECTOR_"


class ServerCollectorConfigurationError(ValueError):
    """Raised before any external connection when configuration is unsafe."""


@dataclass(frozen=True, slots=True)
class ServerCollectorSettings:
    postgres_dsn: str = field(repr=False)
    kafka_bootstrap_servers: tuple[str, ...]
    owner_id: str
    lease_ttl_ms: int = 15_000
    heartbeat_interval_ms: int = 5_000
    leadership_retry_ms: int = 1_000
    shutdown_timeout_ms: int = 10_000

    def __post_init__(self) -> None:
        dsn = _required_text(self.postgres_dsn, field="postgres_dsn")
        owner = _required_text(self.owner_id, field="owner_id")
        if not isinstance(self.kafka_bootstrap_servers, tuple):
            raise ServerCollectorConfigurationError(
                "kafka_bootstrap_servers must be a tuple of server strings"
            )
        servers = tuple(
            _required_text(server, field="kafka_bootstrap_servers")
            for server in self.kafka_bootstrap_servers
        )
        if not servers:
            raise ServerCollectorConfigurationError(
                "kafka_bootstrap_servers must contain at least one server"
            )
        object.__setattr__(self, "postgres_dsn", dsn)
        object.__setattr__(self, "owner_id", owner)
        object.__setattr__(self, "kafka_bootstrap_servers", servers)
        for name in (
            "lease_ttl_ms",
            "heartbeat_interval_ms",
            "leadership_retry_ms",
            "shutdown_timeout_ms",
        ):
            _positive_int(getattr(self, name), field=name)
        if self.heartbeat_interval_ms * 3 > self.lease_ttl_ms:
            raise ServerCollectorConfigurationError(
                "heartbeat_interval_ms must be at most one third of lease_ttl_ms"
            )

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ServerCollectorSettings:
        values = os.environ if environ is None else environ
        postgres_dsn = _required_env(values, "POSTGRES_DSN")
        bootstrap = _required_env(values, "KAFKA_BOOTSTRAP_SERVERS")
        owner_id = _required_env(values, "OWNER_ID")
        servers = tuple(part.strip() for part in bootstrap.split(","))
        if any(not server for server in servers):
            raise ServerCollectorConfigurationError(
                f"{ENV_PREFIX}KAFKA_BOOTSTRAP_SERVERS contains a blank server"
            )
        return cls(
            postgres_dsn=postgres_dsn,
            kafka_bootstrap_servers=servers,
            owner_id=owner_id,
            lease_ttl_ms=_optional_int(values, "LEASE_TTL_MS", 15_000),
            heartbeat_interval_ms=_optional_int(
                values,
                "HEARTBEAT_INTERVAL_MS",
                5_000,
            ),
            leadership_retry_ms=_optional_int(
                values,
                "LEADERSHIP_RETRY_MS",
                1_000,
            ),
            shutdown_timeout_ms=_optional_int(
                values,
                "SHUTDOWN_TIMEOUT_MS",
                10_000,
            ),
        )


def _required_env(values: Mapping[str, str], suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = values.get(name)
    if value is None or not value.strip():
        raise ServerCollectorConfigurationError(f"required setting {name} is missing")
    return value.strip()


def _optional_int(values: Mapping[str, str], suffix: str, default: int) -> int:
    name = f"{ENV_PREFIX}{suffix}"
    raw = values.get(name)
    if raw is None:
        return default
    if not raw or raw != raw.strip() or not raw.isascii() or not raw.isdecimal():
        raise ServerCollectorConfigurationError(
            f"setting {name} must be an unsigned base-10 integer"
        )
    return int(raw)


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ServerCollectorConfigurationError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ServerCollectorConfigurationError(f"{field} must be a positive integer")
    return value
