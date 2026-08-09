"""Strict environment settings for the Phase 1D ClickHouse writer."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

ENV_PREFIX = "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_"
DEFAULT_GROUP_ID = "candlescope-clickhouse-writer-v1"


class ClickHouseWriterConfigurationError(ValueError):
    """Configuration is missing or unsafe before external I/O begins."""


@dataclass(frozen=True, slots=True)
class ClickHouseWriterSettings:
    kafka_bootstrap_servers: tuple[str, ...]
    owner_id: str
    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str = field(repr=False)
    clickhouse_database: str = "candlescope"
    kafka_group_id: str = DEFAULT_GROUP_ID
    batch_size: int = 500
    poll_timeout_ms: int = 1_000
    clickhouse_request_timeout_ms: int = 10_000
    kafka_session_timeout_ms: int = 10_000
    kafka_heartbeat_interval_ms: int = 3_000
    shutdown_timeout_ms: int = 10_000

    def __post_init__(self) -> None:
        if not isinstance(self.kafka_bootstrap_servers, tuple):
            raise ClickHouseWriterConfigurationError(
                "kafka_bootstrap_servers must be a tuple"
            )
        servers = tuple(
            _required_text(value, field="kafka_bootstrap_servers")
            for value in self.kafka_bootstrap_servers
        )
        if not servers:
            raise ClickHouseWriterConfigurationError(
                "kafka_bootstrap_servers cannot be empty"
            )
        object.__setattr__(self, "kafka_bootstrap_servers", servers)
        for name in (
            "owner_id",
            "clickhouse_url",
            "clickhouse_user",
            "clickhouse_password",
            "clickhouse_database",
            "kafka_group_id",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(getattr(self, name), field=name),
            )
        for name in (
            "batch_size",
            "poll_timeout_ms",
            "clickhouse_request_timeout_ms",
            "kafka_session_timeout_ms",
            "kafka_heartbeat_interval_ms",
            "shutdown_timeout_ms",
        ):
            _positive_int(getattr(self, name), field=name)
        if self.kafka_heartbeat_interval_ms * 3 > self.kafka_session_timeout_ms:
            raise ClickHouseWriterConfigurationError(
                "kafka_heartbeat_interval_ms must be at most one third of "
                "kafka_session_timeout_ms"
            )

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ClickHouseWriterSettings:
        values = os.environ if environ is None else environ
        bootstrap = _required_env(values, "KAFKA_BOOTSTRAP_SERVERS")
        servers = tuple(part.strip() for part in bootstrap.split(","))
        if any(not server for server in servers):
            raise ClickHouseWriterConfigurationError(
                f"{ENV_PREFIX}KAFKA_BOOTSTRAP_SERVERS contains a blank server"
            )
        return cls(
            kafka_bootstrap_servers=servers,
            owner_id=_required_env(values, "OWNER_ID"),
            clickhouse_url=_required_env(values, "CLICKHOUSE_URL"),
            clickhouse_user=_required_env(values, "CLICKHOUSE_USER"),
            clickhouse_password=_required_env(values, "CLICKHOUSE_PASSWORD"),
            clickhouse_database=values.get(
                f"{ENV_PREFIX}CLICKHOUSE_DATABASE",
                "candlescope",
            ),
            kafka_group_id=values.get(
                f"{ENV_PREFIX}KAFKA_GROUP_ID",
                DEFAULT_GROUP_ID,
            ),
            batch_size=_optional_int(values, "BATCH_SIZE", 500),
            poll_timeout_ms=_optional_int(values, "POLL_TIMEOUT_MS", 1_000),
            clickhouse_request_timeout_ms=_optional_int(
                values,
                "CLICKHOUSE_REQUEST_TIMEOUT_MS",
                10_000,
            ),
            kafka_session_timeout_ms=_optional_int(
                values,
                "KAFKA_SESSION_TIMEOUT_MS",
                10_000,
            ),
            kafka_heartbeat_interval_ms=_optional_int(
                values,
                "KAFKA_HEARTBEAT_INTERVAL_MS",
                3_000,
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
        raise ClickHouseWriterConfigurationError(f"required setting {name} is missing")
    return value.strip()


def _optional_int(values: Mapping[str, str], suffix: str, default: int) -> int:
    name = f"{ENV_PREFIX}{suffix}"
    raw = values.get(name)
    if raw is None:
        return default
    if not raw or raw != raw.strip() or not raw.isascii() or not raw.isdecimal():
        raise ClickHouseWriterConfigurationError(
            f"setting {name} must be an unsigned base-10 integer"
        )
    return int(raw)


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ClickHouseWriterConfigurationError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ClickHouseWriterConfigurationError(f"{field} must be positive")
    return value
