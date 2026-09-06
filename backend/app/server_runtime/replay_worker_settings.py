"""Fail-closed configuration for a single Replay Worker process."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from app.server_runtime.health_http import parse_health_bind
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)
from app.server_runtime.replay_lease import normalize_replay_worker_id

ENV_PREFIX = "CANDLESCOPE_SERVER_REPLAY_WORKER_"


class ReplayWorkerConfigurationError(ValueError):
    """Raised before any external connection when Worker configuration is unsafe."""


@dataclass(frozen=True, slots=True)
class ReplayWorkerSettings:
    worker_id: str
    postgres_dsn: str = field(repr=False)
    query_credential: str = field(repr=False)
    organization_id: str
    workspace_id: str
    query_url: str = "http://127.0.0.1:8110"
    query_request_timeout_ms: int = 10_000
    lease_ttl_ms: int = 15_000
    renew_interval_ms: int = 5_000
    max_actors: int = 1
    max_concurrent_recoveries: int = 1
    poll_interval_ms: int = 200
    shutdown_timeout_ms: int = 5_000
    health_bind: str | None = None
    worker_control_token: str = field(repr=False, default="")

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "worker_id",
            normalize_replay_worker_id(self.worker_id),
        )
        dsn = _required_text(self.postgres_dsn, field="postgres_dsn")
        credential = _required_text(self.query_credential, field="query_credential")
        token = _required_text(
            self.worker_control_token,
            field="worker_control_token",
        )
        object.__setattr__(self, "postgres_dsn", dsn)
        object.__setattr__(self, "query_credential", credential)
        object.__setattr__(self, "worker_control_token", token)
        object.__setattr__(
            self,
            "organization_id",
            normalize_organization_id(self.organization_id),
        )
        object.__setattr__(
            self,
            "workspace_id",
            normalize_workspace_id(self.workspace_id),
        )
        query_url = _absolute_http_url(self.query_url, field="query_url")
        object.__setattr__(self, "query_url", query_url)
        for name in (
            "query_request_timeout_ms",
            "lease_ttl_ms",
            "renew_interval_ms",
            "max_actors",
            "max_concurrent_recoveries",
            "poll_interval_ms",
            "shutdown_timeout_ms",
        ):
            _positive_int(getattr(self, name), field=name)
        if self.max_actors != 1:
            raise ReplayWorkerConfigurationError(
                "Phase 1AE workers accept exactly one actor"
            )
        if self.lease_ttl_ms < 3 * self.renew_interval_ms:
            raise ReplayWorkerConfigurationError(
                "lease_ttl_ms must be at least 3 * renew_interval_ms"
            )
        if self.health_bind is not None:
            parse_health_bind(self.health_bind)
        if self.postgres_dsn in (self.query_credential, self.worker_control_token):
            raise ReplayWorkerConfigurationError(
                "query, worker, and database credentials must be distinct"
            )

    def __repr__(self) -> str:
        return (
            "ReplayWorkerSettings("
            f"worker_id={self.worker_id!r}, "
            f"organization_id={self.organization_id!r}, "
            f"workspace_id={self.workspace_id!r}, "
            f"lease_ttl_ms={self.lease_ttl_ms}, "
            f"renew_interval_ms={self.renew_interval_ms}, "
            f"health_bind={self.health_bind!r})"
        )

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ReplayWorkerSettings:
        values = os.environ if environ is None else environ
        health_bind = values.get(f"{ENV_PREFIX}HEALTH_BIND")
        return cls(
            worker_id=_required_env(values, "WORKER_ID"),
            postgres_dsn=_required_env(values, "POSTGRES_DSN"),
            query_credential=_required_env(values, "QUERY_CREDENTIAL"),
            organization_id=_required_env(values, "ORGANIZATION_ID"),
            workspace_id=_required_env(values, "WORKSPACE_ID"),
            query_url=_required_env(values, "QUERY_URL"),
            worker_control_token=_required_env(values, "CONTROL_TOKEN"),
            query_request_timeout_ms=_optional_int(
                values, "QUERY_REQUEST_TIMEOUT_MS", 10_000
            ),
            lease_ttl_ms=_optional_int(values, "LEASE_TTL_MS", 15_000),
            renew_interval_ms=_optional_int(values, "RENEW_INTERVAL_MS", 5_000),
            max_actors=_optional_int(values, "MAX_ACTORS", 1),
            max_concurrent_recoveries=_optional_int(
                values, "MAX_CONCURRENT_RECOVERIES", 1
            ),
            poll_interval_ms=_optional_int(values, "POLL_INTERVAL_MS", 200),
            shutdown_timeout_ms=_optional_int(values, "SHUTDOWN_TIMEOUT_MS", 5_000),
            health_bind=None if not health_bind else health_bind.strip(),
        )


def _required_env(values: Mapping[str, str], suffix: str) -> str:
    key = f"{ENV_PREFIX}{suffix}"
    if key not in values:
        raise ReplayWorkerConfigurationError(f"{key} is required")
    return _required_text(values[key], field=key)


def _optional_int(values: Mapping[str, str], suffix: str, default: int) -> int:
    key = f"{ENV_PREFIX}{suffix}"
    if key not in values:
        return default
    raw = values[key].strip()
    if not raw.isdigit():
        raise ReplayWorkerConfigurationError(f"{key} must be a positive integer")
    return _positive_int(int(raw), field=key)


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReplayWorkerConfigurationError(f"{field} must be a non-blank string")
    return value.strip()


def _absolute_http_url(value: object, *, field: str) -> str:
    normalized = _required_text(value, field=field).rstrip("/")
    parsed = urlsplit(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ReplayWorkerConfigurationError(f"{field} must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ReplayWorkerConfigurationError(
            f"{field} cannot contain credentials, query, or fragment"
        )
    return normalized


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReplayWorkerConfigurationError(f"{field} must be a positive integer")
    return value
