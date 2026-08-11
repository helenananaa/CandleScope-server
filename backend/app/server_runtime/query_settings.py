"""Strict standalone settings for the Phase 1H snapshot query service."""

from __future__ import annotations

import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field

from app.server_runtime.writer_settings import DEFAULT_GROUP_ID

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_"


class QueryServiceConfigurationError(ValueError):
    """Query service configuration is missing or unsafe."""


@dataclass(frozen=True, slots=True)
class QueryServiceSettings:
    kafka_bootstrap_servers: tuple[str, ...]
    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str = field(repr=False)
    s3_endpoint_url: str
    s3_bucket: str
    s3_access_key_id: str = field(repr=False)
    s3_secret_access_key: str = field(repr=False)
    auth_bearer_token: str = field(repr=False)
    postgres_dsn: str | None = field(repr=False)
    control_bearer_token: str | None = field(repr=False)
    instance_id: str
    control_backend: str = "postgres"
    clickhouse_database: str = "candlescope"
    clickhouse_writer_group_id: str = DEFAULT_GROUP_ID
    auth_principal: str = "candlescope-api-gateway"
    control_principal: str = "candlescope-query-operator"
    hot_backend_id: str = "clickhouse-market-events-v1"
    s3_region: str = "us-east-1"
    s3_prefix: str = "market-data"
    bind_host: str = "127.0.0.1"
    bind_port: int = 8110
    max_page_rows: int = 1_000
    max_scan_rows: int = 100_000
    max_manifest_depth: int = 10_000
    max_concurrent_queries: int = 16
    query_queue_timeout_ms: int = 1_000
    clickhouse_request_timeout_ms: int = 10_000
    clickhouse_max_execution_time_ms: int = 5_000
    clickhouse_max_memory_usage_bytes: int = 268_435_456
    clickhouse_max_bytes_to_read: int = 536_870_912
    clickhouse_max_threads: int = 4
    s3_request_timeout_ms: int = 10_000
    kafka_request_timeout_ms: int = 10_000
    postgres_connect_timeout_ms: int = 5_000
    postgres_request_timeout_ms: int = 5_000
    parity_sample_interval_ms: int = 30_000
    parity_probe_capacity: int = 128

    def __post_init__(self) -> None:
        if not isinstance(self.kafka_bootstrap_servers, tuple):
            raise QueryServiceConfigurationError(
                "kafka_bootstrap_servers must be a tuple"
            )
        servers = tuple(
            _required_text(item, field="kafka_bootstrap_servers")
            for item in self.kafka_bootstrap_servers
        )
        if not servers:
            raise QueryServiceConfigurationError(
                "kafka_bootstrap_servers cannot be empty"
            )
        object.__setattr__(self, "kafka_bootstrap_servers", servers)
        for name in (
            "clickhouse_url",
            "clickhouse_user",
            "clickhouse_password",
            "clickhouse_database",
            "clickhouse_writer_group_id",
            "auth_principal",
            "control_principal",
            "hot_backend_id",
            "instance_id",
            "s3_endpoint_url",
            "s3_region",
            "s3_bucket",
            "s3_access_key_id",
            "s3_secret_access_key",
            "bind_host",
        ):
            object.__setattr__(
                self,
                name,
                _required_text(getattr(self, name), field=name),
            )
        token = _required_text(self.auth_bearer_token, field="auth_bearer_token")
        if len(token) < 32:
            raise QueryServiceConfigurationError(
                "auth_bearer_token must contain at least 32 characters"
            )
        object.__setattr__(self, "auth_bearer_token", token)
        if not isinstance(self.control_backend, str):
            raise QueryServiceConfigurationError("control_backend must be a string")
        control_backend = self.control_backend.strip().lower()
        if control_backend not in {"postgres", "process"}:
            raise QueryServiceConfigurationError(
                "control_backend must be postgres or process"
            )
        object.__setattr__(self, "control_backend", control_backend)
        if control_backend == "postgres":
            postgres_dsn = _required_text(self.postgres_dsn, field="postgres_dsn")
            control_token = _required_text(
                self.control_bearer_token,
                field="control_bearer_token",
            )
            if len(control_token) < 32:
                raise QueryServiceConfigurationError(
                    "control_bearer_token must contain at least 32 characters"
                )
            if secrets.compare_digest(token, control_token):
                raise QueryServiceConfigurationError(
                    "query and control bearer tokens must be distinct"
                )
            object.__setattr__(self, "postgres_dsn", postgres_dsn)
            object.__setattr__(self, "control_bearer_token", control_token)
        elif self.postgres_dsn is not None or self.control_bearer_token is not None:
            raise QueryServiceConfigurationError(
                "process control_backend cannot accept PostgreSQL/control credentials"
            )
        if not isinstance(self.s3_prefix, str):
            raise QueryServiceConfigurationError("s3_prefix must be a string")
        object.__setattr__(self, "s3_prefix", self.s3_prefix.strip("/"))
        for name in (
            "bind_port",
            "max_page_rows",
            "max_scan_rows",
            "max_manifest_depth",
            "max_concurrent_queries",
            "query_queue_timeout_ms",
            "clickhouse_request_timeout_ms",
            "clickhouse_max_execution_time_ms",
            "clickhouse_max_memory_usage_bytes",
            "clickhouse_max_bytes_to_read",
            "clickhouse_max_threads",
            "s3_request_timeout_ms",
            "kafka_request_timeout_ms",
            "postgres_connect_timeout_ms",
            "postgres_request_timeout_ms",
            "parity_sample_interval_ms",
            "parity_probe_capacity",
        ):
            _positive_int(getattr(self, name), field=name)
        if self.bind_port > 65_535:
            raise QueryServiceConfigurationError("bind_port must be at most 65535")
        if self.max_page_rows > self.max_scan_rows:
            raise QueryServiceConfigurationError(
                "max_page_rows cannot exceed max_scan_rows"
            )

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> QueryServiceSettings:
        values = os.environ if environ is None else environ
        bootstrap = _required_env(values, "KAFKA_BOOTSTRAP_SERVERS")
        servers = tuple(part.strip() for part in bootstrap.split(","))
        if any(not server for server in servers):
            raise QueryServiceConfigurationError(
                f"{ENV_PREFIX}KAFKA_BOOTSTRAP_SERVERS contains a blank server"
            )
        return cls(
            kafka_bootstrap_servers=servers,
            clickhouse_url=_required_env(values, "CLICKHOUSE_URL"),
            clickhouse_user=_required_env(values, "CLICKHOUSE_USER"),
            clickhouse_password=_required_env(values, "CLICKHOUSE_PASSWORD"),
            clickhouse_database=values.get(
                f"{ENV_PREFIX}CLICKHOUSE_DATABASE",
                "candlescope",
            ),
            clickhouse_writer_group_id=values.get(
                f"{ENV_PREFIX}CLICKHOUSE_WRITER_GROUP_ID",
                DEFAULT_GROUP_ID,
            ),
            s3_endpoint_url=_required_env(values, "S3_ENDPOINT_URL"),
            s3_region=values.get(f"{ENV_PREFIX}S3_REGION", "us-east-1"),
            s3_bucket=_required_env(values, "S3_BUCKET"),
            s3_prefix=values.get(f"{ENV_PREFIX}S3_PREFIX", "market-data"),
            s3_access_key_id=_required_env(values, "S3_ACCESS_KEY_ID"),
            s3_secret_access_key=_required_env(values, "S3_SECRET_ACCESS_KEY"),
            auth_bearer_token=_required_env(values, "AUTH_BEARER_TOKEN"),
            postgres_dsn=_optional_env(values, "POSTGRES_DSN"),
            control_bearer_token=_optional_env(values, "CONTROL_BEARER_TOKEN"),
            instance_id=_required_env(values, "INSTANCE_ID"),
            control_backend=values.get(
                f"{ENV_PREFIX}CONTROL_BACKEND",
                "postgres",
            ),
            auth_principal=values.get(
                f"{ENV_PREFIX}AUTH_PRINCIPAL",
                "candlescope-api-gateway",
            ),
            control_principal=values.get(
                f"{ENV_PREFIX}CONTROL_PRINCIPAL",
                "candlescope-query-operator",
            ),
            hot_backend_id=values.get(
                f"{ENV_PREFIX}HOT_BACKEND_ID",
                "clickhouse-market-events-v1",
            ),
            bind_host=values.get(f"{ENV_PREFIX}BIND_HOST", "127.0.0.1"),
            bind_port=_optional_int(values, "BIND_PORT", 8110),
            max_page_rows=_optional_int(values, "MAX_PAGE_ROWS", 1_000),
            max_scan_rows=_optional_int(values, "MAX_SCAN_ROWS", 100_000),
            max_manifest_depth=_optional_int(
                values,
                "MAX_MANIFEST_DEPTH",
                10_000,
            ),
            max_concurrent_queries=_optional_int(
                values,
                "MAX_CONCURRENT_QUERIES",
                16,
            ),
            query_queue_timeout_ms=_optional_int(
                values,
                "QUERY_QUEUE_TIMEOUT_MS",
                1_000,
            ),
            clickhouse_request_timeout_ms=_optional_int(
                values,
                "CLICKHOUSE_REQUEST_TIMEOUT_MS",
                10_000,
            ),
            clickhouse_max_execution_time_ms=_optional_int(
                values,
                "CLICKHOUSE_MAX_EXECUTION_TIME_MS",
                5_000,
            ),
            clickhouse_max_memory_usage_bytes=_optional_int(
                values,
                "CLICKHOUSE_MAX_MEMORY_USAGE_BYTES",
                268_435_456,
            ),
            clickhouse_max_bytes_to_read=_optional_int(
                values,
                "CLICKHOUSE_MAX_BYTES_TO_READ",
                536_870_912,
            ),
            clickhouse_max_threads=_optional_int(
                values,
                "CLICKHOUSE_MAX_THREADS",
                4,
            ),
            s3_request_timeout_ms=_optional_int(
                values,
                "S3_REQUEST_TIMEOUT_MS",
                10_000,
            ),
            kafka_request_timeout_ms=_optional_int(
                values,
                "KAFKA_REQUEST_TIMEOUT_MS",
                10_000,
            ),
            postgres_connect_timeout_ms=_optional_int(
                values,
                "POSTGRES_CONNECT_TIMEOUT_MS",
                5_000,
            ),
            postgres_request_timeout_ms=_optional_int(
                values,
                "POSTGRES_REQUEST_TIMEOUT_MS",
                5_000,
            ),
            parity_sample_interval_ms=_optional_int(
                values,
                "PARITY_SAMPLE_INTERVAL_MS",
                30_000,
            ),
            parity_probe_capacity=_optional_int(
                values,
                "PARITY_PROBE_CAPACITY",
                128,
            ),
        )


def _required_env(values: Mapping[str, str], suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = values.get(name)
    if value is None or not value.strip():
        raise QueryServiceConfigurationError(f"required setting {name} is missing")
    return value.strip()


def _optional_int(values: Mapping[str, str], suffix: str, default: int) -> int:
    name = f"{ENV_PREFIX}{suffix}"
    raw = values.get(name)
    if raw is None:
        return default
    if not raw or raw != raw.strip() or not raw.isascii() or not raw.isdecimal():
        raise QueryServiceConfigurationError(
            f"setting {name} must be an unsigned base-10 integer"
        )
    return int(raw)


def _optional_env(values: Mapping[str, str], suffix: str) -> str | None:
    name = f"{ENV_PREFIX}{suffix}"
    value = values.get(name)
    if value is None:
        return None
    if not value.strip():
        raise QueryServiceConfigurationError(f"setting {name} cannot be blank")
    return value.strip()


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QueryServiceConfigurationError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise QueryServiceConfigurationError(f"{field} must be positive")
    return value
