"""Strict environment settings for the Phase 1E Parquet archive writer."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

ENV_PREFIX = "CANDLESCOPE_SERVER_ARCHIVE_WRITER_"
DEFAULT_ARCHIVE_GROUP_ID = "candlescope-parquet-archiver-v1"
_DATA_EPOCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ArchiveWriterConfigurationError(ValueError):
    """Archive configuration is missing or unsafe before external I/O."""


@dataclass(frozen=True, slots=True)
class ArchiveWriterSettings:
    kafka_bootstrap_servers: tuple[str, ...]
    owner_id: str
    data_epoch: str
    s3_endpoint_url: str
    s3_region: str
    s3_bucket: str
    s3_prefix: str
    s3_access_key_id: str = field(repr=False)
    s3_secret_access_key: str = field(repr=False)
    kafka_group_id: str = DEFAULT_ARCHIVE_GROUP_ID
    segment_event_count: int = 10_000
    poll_timeout_ms: int = 1_000
    s3_request_timeout_ms: int = 10_000
    kafka_session_timeout_ms: int = 10_000
    kafka_heartbeat_interval_ms: int = 3_000
    shutdown_timeout_ms: int = 10_000

    def __post_init__(self) -> None:
        if not isinstance(self.kafka_bootstrap_servers, tuple):
            raise ArchiveWriterConfigurationError(
                "kafka_bootstrap_servers must be a tuple"
            )
        servers = tuple(
            _required_text(server, field="kafka_bootstrap_servers")
            for server in self.kafka_bootstrap_servers
        )
        if not servers:
            raise ArchiveWriterConfigurationError(
                "kafka_bootstrap_servers cannot be empty"
            )
        object.__setattr__(self, "kafka_bootstrap_servers", servers)
        for field_name in (
            "owner_id",
            "s3_endpoint_url",
            "s3_region",
            "s3_bucket",
            "s3_access_key_id",
            "s3_secret_access_key",
            "kafka_group_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field=field_name),
            )
        data_epoch = _required_text(self.data_epoch, field="data_epoch")
        if not _DATA_EPOCH.fullmatch(data_epoch):
            raise ArchiveWriterConfigurationError(
                "data_epoch must be a path-safe identifier of at most 128 characters"
            )
        object.__setattr__(self, "data_epoch", data_epoch)
        if not isinstance(self.s3_prefix, str):
            raise ArchiveWriterConfigurationError("s3_prefix must be a string")
        object.__setattr__(self, "s3_prefix", self.s3_prefix.strip("/"))
        for field_name in (
            "segment_event_count",
            "poll_timeout_ms",
            "s3_request_timeout_ms",
            "kafka_session_timeout_ms",
            "kafka_heartbeat_interval_ms",
            "shutdown_timeout_ms",
        ):
            _positive_int(getattr(self, field_name), field=field_name)
        if self.kafka_heartbeat_interval_ms * 3 > self.kafka_session_timeout_ms:
            raise ArchiveWriterConfigurationError(
                "kafka_heartbeat_interval_ms must be at most one third of "
                "kafka_session_timeout_ms"
            )

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ArchiveWriterSettings:
        values = os.environ if environ is None else environ
        bootstrap = _required_env(values, "KAFKA_BOOTSTRAP_SERVERS")
        servers = tuple(part.strip() for part in bootstrap.split(","))
        if any(not server for server in servers):
            raise ArchiveWriterConfigurationError(
                f"{ENV_PREFIX}KAFKA_BOOTSTRAP_SERVERS contains a blank server"
            )
        return cls(
            kafka_bootstrap_servers=servers,
            owner_id=_required_env(values, "OWNER_ID"),
            data_epoch=_required_env(values, "DATA_EPOCH"),
            s3_endpoint_url=_required_env(values, "S3_ENDPOINT_URL"),
            s3_region=values.get(f"{ENV_PREFIX}S3_REGION", "us-east-1"),
            s3_bucket=_required_env(values, "S3_BUCKET"),
            s3_prefix=values.get(f"{ENV_PREFIX}S3_PREFIX", "market-data"),
            s3_access_key_id=_required_env(values, "S3_ACCESS_KEY_ID"),
            s3_secret_access_key=_required_env(values, "S3_SECRET_ACCESS_KEY"),
            kafka_group_id=values.get(
                f"{ENV_PREFIX}KAFKA_GROUP_ID",
                DEFAULT_ARCHIVE_GROUP_ID,
            ),
            segment_event_count=_optional_int(values, "SEGMENT_EVENT_COUNT", 10_000),
            poll_timeout_ms=_optional_int(values, "POLL_TIMEOUT_MS", 1_000),
            s3_request_timeout_ms=_optional_int(
                values,
                "S3_REQUEST_TIMEOUT_MS",
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
        raise ArchiveWriterConfigurationError(f"required setting {name} is missing")
    return value.strip()


def _optional_int(values: Mapping[str, str], suffix: str, default: int) -> int:
    name = f"{ENV_PREFIX}{suffix}"
    raw = values.get(name)
    if raw is None:
        return default
    if not raw or raw != raw.strip() or not raw.isascii() or not raw.isdecimal():
        raise ArchiveWriterConfigurationError(
            f"setting {name} must be an unsigned base-10 integer"
        )
    return int(raw)


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ArchiveWriterConfigurationError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ArchiveWriterConfigurationError(f"{field} must be positive")
    return value
