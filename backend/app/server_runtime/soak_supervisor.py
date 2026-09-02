"""Bounded soak supervisor: scrape loopback health, then caught-up reconcile."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from app.server_runtime.chain_health import (
    archive_health_from_wire,
    collector_health_from_wire,
    writer_health_from_wire,
)
from app.server_runtime.chain_reconciliation import (
    STATUS_CAUGHT_UP,
    ChainObservation,
    reconcile_chain,
)
from app.server_runtime.health_http import is_loopback_http_health_url
from app.server_runtime.soak_rehearsal import PUBLIC_SOAK_DURATION_MS

SOAK_SUPERVISOR_SCHEMA_VERSION = "candlescope.server-phase1u-soak-result.v1"
PUBLIC_SOAK_ENV = "CANDLESCOPE_PHASE1U_ALLOW_PUBLIC_SOAK"
MAX_SCRIPTED_DURATION_MS = 300_000
DEFAULT_DURATION_MS = 5_000
DEFAULT_SCRAPE_INTERVAL_MS = 250
DEFAULT_STALE_AFTER_MS = 5_000
DEFAULT_SCRAPE_TIMEOUT_MS = 2_000
DEFAULT_MAX_HEALTH_BYTES = 16_384
SOURCE_SCRIPTED = "scripted"
SOURCE_BINANCE = "binance"
_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}

ClockMs = Callable[[], int]
Sleeper = Callable[[float], Awaitable[None]]


class SoakSupervisorError(RuntimeError):
    """The soak supervisor refused to start, scrape, or claim success."""

    def __init__(
        self, code: str, message: str, *, details: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": SOAK_SUPERVISOR_SCHEMA_VERSION,
            "phase1u_passed": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "twenty_four_hour_public_continuity": False,
            "main_fastapi_server_profile_unlocked": False,
        }


@dataclass(frozen=True, slots=True)
class SoakSupervisorSettings:
    collector_health_url: str
    writer_health_url: str
    archive_health_url: str
    query_sequences: tuple[int, ...]
    duration_ms: int = DEFAULT_DURATION_MS
    scrape_interval_ms: int = DEFAULT_SCRAPE_INTERVAL_MS
    stale_after_ms: int = DEFAULT_STALE_AFTER_MS
    scrape_timeout_ms: int = DEFAULT_SCRAPE_TIMEOUT_MS
    source: str = SOURCE_SCRIPTED
    allow_public_soak: bool = False

    def __post_init__(self) -> None:
        for field_name, url in (
            ("collector_health_url", self.collector_health_url),
            ("writer_health_url", self.writer_health_url),
            ("archive_health_url", self.archive_health_url),
        ):
            if not is_loopback_http_health_url(url):
                raise SoakSupervisorError(
                    "NON_LOOPBACK_HEALTH_URL",
                    f"{field_name} must be loopback HTTP /health without query or credentials",
                    details={"url": url},
                )
        sequences = tuple(self.query_sequences)
        if not sequences:
            raise SoakSupervisorError(
                "QUERY_SEQUENCE_GAP",
                "query_sequences cannot be empty",
            )
        expected = sequences[0]
        for value in sequences:
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise SoakSupervisorError(
                    "QUERY_SEQUENCE_GAP",
                    "query_sequences must be positive integers",
                )
            if value != expected:
                raise SoakSupervisorError(
                    "QUERY_SEQUENCE_GAP",
                    "query_sequences must be contiguous",
                )
            expected += 1
        object.__setattr__(self, "query_sequences", sequences)
        duration_ms = _positive_int(self.duration_ms, field="duration_ms")
        scrape_interval_ms = _positive_int(
            self.scrape_interval_ms, field="scrape_interval_ms"
        )
        stale_after_ms = _positive_int(self.stale_after_ms, field="stale_after_ms")
        scrape_timeout_ms = _positive_int(
            self.scrape_timeout_ms, field="scrape_timeout_ms"
        )
        object.__setattr__(self, "duration_ms", duration_ms)
        object.__setattr__(self, "scrape_interval_ms", scrape_interval_ms)
        object.__setattr__(self, "stale_after_ms", stale_after_ms)
        object.__setattr__(self, "scrape_timeout_ms", scrape_timeout_ms)
        source = self.source.strip().lower()
        if source not in {SOURCE_SCRIPTED, SOURCE_BINANCE}:
            raise SoakSupervisorError(
                "UNSUPPORTED_SOURCE",
                "source must be scripted or binance",
            )
        object.__setattr__(self, "source", source)
        if not isinstance(self.allow_public_soak, bool):
            raise TypeError("allow_public_soak must be a boolean")
        if source == SOURCE_BINANCE and not self.allow_public_soak:
            raise SoakSupervisorError(
                "BINANCE_SOURCE_NOT_AUTHORIZED",
                "binance source requires CANDLESCOPE_PHASE1U_ALLOW_PUBLIC_SOAK=1",
            )
        if source == SOURCE_BINANCE and duration_ms < PUBLIC_SOAK_DURATION_MS:
            raise SoakSupervisorError(
                "BINANCE_SOURCE_REQUIRES_24H",
                "binance source requires duration_ms of at least 24 hours",
                details={"duration_ms": duration_ms},
            )
        if not self.allow_public_soak and duration_ms > MAX_SCRIPTED_DURATION_MS:
            raise SoakSupervisorError(
                "DURATION_EXCEEDS_SCRIPTED_CAP",
                "scripted soak duration cannot exceed 300 seconds without the public switch",
                details={"duration_ms": duration_ms},
            )


async def run_soak_supervisor(
    settings: SoakSupervisorSettings,
    *,
    clock_ms: ClockMs,
    sleep: Sleeper,
) -> dict[str, object]:
    """Scrape loopback health until duration elapses, then require caught-up."""

    if not isinstance(settings, SoakSupervisorSettings):
        raise TypeError("settings must be SoakSupervisorSettings")
    started_at_ms = clock_ms()
    deadline_ms = started_at_ms + settings.duration_ms
    samples = 0
    while True:
        now_ms = clock_ms()
        observation = await _scrape_observation(settings, now_ms=now_ms)
        require_caught_up = now_ms >= deadline_ms
        reconciliation = reconcile_chain(
            observation,
            require_caught_up=require_caught_up,
        )
        samples += 1
        if require_caught_up:
            finished_at_ms = clock_ms()
            elapsed_ms = finished_at_ms - started_at_ms
            public_continuity = (
                settings.source == SOURCE_BINANCE
                and elapsed_ms >= PUBLIC_SOAK_DURATION_MS
                and reconciliation.status == STATUS_CAUGHT_UP
            )
            return {
                "schema_version": SOAK_SUPERVISOR_SCHEMA_VERSION,
                "phase1u_passed": True,
                "mode": "supervised-scrape",
                "source": settings.source,
                "samples": samples,
                "duration_ms": settings.duration_ms,
                "elapsed_ms": elapsed_ms,
                "reconciliation": reconciliation.to_wire(),
                "twenty_four_hour_public_continuity": public_continuity,
                "main_fastapi_server_profile_unlocked": False,
                "claims_not_made": [
                    "FastAPI server profile startup",
                    "ReplayService composition-root wiring",
                    *(
                        []
                        if public_continuity
                        else ["Binance public 24 hour continuity"]
                    ),
                ],
            }
        remaining_ms = deadline_ms - clock_ms()
        if remaining_ms <= 0:
            continue
        await sleep(min(settings.scrape_interval_ms, remaining_ms) / 1000)


def public_24h_refusal(*, allow_public_soak: bool) -> dict[str, object]:
    if not allow_public_soak:
        return SoakSupervisorError(
            "PUBLIC_SOAK_NOT_AUTHORIZED",
            "CANDLESCOPE_PHASE1U_ALLOW_PUBLIC_SOAK=1 is required for public-24h",
        ).to_wire()
    return SoakSupervisorError(
        "PUBLIC_SOAK_NOT_RUN",
        "Phase 1U can scrape loopback health for a bounded scripted duration; "
        "it does not start Binance collectors or run a 24 hour wall clock",
    ).to_wire()


async def _scrape_observation(
    settings: SoakSupervisorSettings,
    *,
    now_ms: int,
) -> ChainObservation:
    collector_wire = await _scrape_json(
        settings.collector_health_url,
        timeout_ms=settings.scrape_timeout_ms,
    )
    writer_wire = await _scrape_json(
        settings.writer_health_url,
        timeout_ms=settings.scrape_timeout_ms,
    )
    archive_wire = await _scrape_json(
        settings.archive_health_url,
        timeout_ms=settings.scrape_timeout_ms,
    )
    collector = collector_health_from_wire(collector_wire)
    writer = writer_health_from_wire(writer_wire)
    archive = archive_health_from_wire(archive_wire)
    for role, updated_at_ms in (
        ("collector", collector.updated_at_ms),
        ("writer", writer.updated_at_ms),
        ("archive", archive.updated_at_ms),
    ):
        if updated_at_ms > now_ms + 1_000:
            raise SoakSupervisorError(
                "HEALTH_CLOCK_SKEW",
                f"{role} health updated_at_ms is in the future",
                details={"updated_at_ms": updated_at_ms, "now_ms": now_ms},
            )
        if now_ms - updated_at_ms > settings.stale_after_ms:
            raise SoakSupervisorError(
                "HEALTH_STALE",
                f"{role} health snapshot is stale",
                details={
                    "updated_at_ms": updated_at_ms,
                    "now_ms": now_ms,
                    "stale_after_ms": settings.stale_after_ms,
                },
            )
    snapshot = archive.current_snapshot
    if snapshot is None:
        raise SoakSupervisorError(
            "SNAPSHOT_MISSING",
            "archive health has no current snapshot",
        )
    sequences = settings.query_sequences
    return ChainObservation(
        collector=collector,
        writer=writer,
        archive=archive,
        query_snapshot=snapshot,
        query_sequences=sequences,
        replay_snapshot=snapshot,
        replay_first_id=sequences[0],
        replay_last_id=sequences[-1],
        replay_row_count=len(sequences),
    )


async def _scrape_json(url: str, *, timeout_ms: int) -> Mapping[str, Any]:
    parsed = urlsplit(url)
    if parsed.hostname not in _LOOPBACK_HOSTS or parsed.scheme != "http":
        raise SoakSupervisorError(
            "NON_LOOPBACK_HEALTH_URL",
            "health scrape URLs must be loopback HTTP",
            details={"url": url},
        )
    timeout = aiohttp.ClientTimeout(total=timeout_ms / 1000)
    try:
        async with (
            aiohttp.ClientSession(trust_env=False) as session,
            session.get(
                url,
                timeout=timeout,
                allow_redirects=False,
            ) as response,
        ):
            if 300 <= response.status < 400:
                raise SoakSupervisorError(
                    "HEALTH_REDIRECT_FORBIDDEN",
                    "health scrape must not follow redirects",
                )
            if response.status != 200:
                raise SoakSupervisorError(
                    "HEALTH_SCRAPE_FAILED",
                    "health scrape returned a non-200 status",
                    details={"status": response.status},
                )
            raw = await response.content.read(DEFAULT_MAX_HEALTH_BYTES + 1)
    except SoakSupervisorError:
        raise
    except Exception as exc:
        raise SoakSupervisorError(
            "HEALTH_SCRAPE_FAILED",
            "health scrape failed",
        ) from exc
    if len(raw) > DEFAULT_MAX_HEALTH_BYTES:
        raise SoakSupervisorError(
            "HEALTH_SCRAPE_FAILED",
            "health scrape exceeded the response byte limit",
        )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SoakSupervisorError(
            "HEALTH_SCRAPE_FAILED",
            "health scrape is not strict JSON",
        ) from exc
    if not isinstance(payload, dict):
        raise SoakSupervisorError(
            "HEALTH_SCRAPE_FAILED",
            "health scrape must be a JSON object",
        )
    return payload


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 1:
        raise ValueError(f"{field} must be greater than zero")
    return value


__all__ = [
    "DEFAULT_DURATION_MS",
    "DEFAULT_SCRAPE_INTERVAL_MS",
    "MAX_SCRIPTED_DURATION_MS",
    "PUBLIC_SOAK_ENV",
    "SOAK_SUPERVISOR_SCHEMA_VERSION",
    "SOURCE_BINANCE",
    "SOURCE_SCRIPTED",
    "SoakSupervisorError",
    "SoakSupervisorSettings",
    "public_24h_refusal",
    "run_soak_supervisor",
]
