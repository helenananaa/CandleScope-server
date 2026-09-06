"""Fail-closed scheduler process settings."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

ENV_PREFIX = "CANDLESCOPE_SERVER_REPLAY_SCHEDULER_"


class ReplaySchedulerConfigurationError(ValueError):
    """Invalid scheduler configuration."""


@dataclass(frozen=True, slots=True)
class ReplaySchedulerSettings:
    postgres_dsn: str = field(repr=False)
    scan_interval_ms: int = 1_000
    heartbeat_ttl_ms: int = 60_000
    health_bind: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.postgres_dsn, str) or not self.postgres_dsn.strip():
            raise ReplaySchedulerConfigurationError("postgres_dsn is required")
        object.__setattr__(self, "postgres_dsn", self.postgres_dsn.strip())
        if (
            isinstance(self.scan_interval_ms, bool)
            or not isinstance(self.scan_interval_ms, int)
            or self.scan_interval_ms <= 0
        ):
            raise ReplaySchedulerConfigurationError("scan_interval_ms must be positive")

    def __repr__(self) -> str:
        return (
            "ReplaySchedulerSettings("
            f"scan_interval_ms={self.scan_interval_ms}, "
            f"health_bind={self.health_bind!r})"
        )

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None
    ) -> ReplaySchedulerSettings:
        values = os.environ if environ is None else environ
        dsn = values.get(f"{ENV_PREFIX}POSTGRES_DSN")
        if not dsn:
            raise ReplaySchedulerConfigurationError(
                f"{ENV_PREFIX}POSTGRES_DSN is required"
            )
        return cls(
            postgres_dsn=dsn,
            scan_interval_ms=int(values.get(f"{ENV_PREFIX}SCAN_INTERVAL_MS", "1000")),
            heartbeat_ttl_ms=int(
                values.get(f"{ENV_PREFIX}HEARTBEAT_TTL_MS", "60000")
            ),
            health_bind=values.get(f"{ENV_PREFIX}HEALTH_BIND"),
        )
