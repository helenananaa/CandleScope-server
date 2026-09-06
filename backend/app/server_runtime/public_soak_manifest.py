"""Fail-closed Phase 1AI public-soak run manifest.

The same immutable model backs ``development-smoke`` and ``run``. Mode is an
entry-point argument, not a field that can downgrade a formal 24h start.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from app.replay.canonical import canonical_json
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)

SCHEMA_VERSION = "candlescope.server-phase1ai-public-soak-manifest.v1"
MODE_DEVELOPMENT_SMOKE = "development-smoke"
MODE_RUN = "run"
SOURCE_BINANCE = "binance"
FROZEN_EXCHANGE = "binance"
FROZEN_MARKET = "futures"
FROZEN_SYMBOL = "BTCUSDT"
FROZEN_EVENT_KIND = "agg_trade"
PUBLIC_SOAK_DURATION_MS = 86_400_000
SMOKE_MIN_DURATION_MS = 300_000
GIT_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
HEALTH_PATHS = frozenset({"/health", "/health/live", "/health/ready"})
REQUIRED_HEALTH_ROLES = (
    "collector",
    "writer",
    "archiver",
    "query",
    "scheduler",
    "worker_a",
    "worker_b",
    "api",
)
FAULT_METHOD_TARGETS = {
    "collector_sigkill": "collector",
    "writer_pre_commit_exit": "writer",
    "archiver_pre_commit_exit": "archiver",
    "worker_sigkill": "worker_a",
    "scheduler_restart": "scheduler",
    "api_restart": "api",
}
REQUIRED_RUN_FAULT_METHODS = tuple(FAULT_METHOD_TARGETS)
SMOKE_ALLOWED_FAULT_METHODS = ("worker_sigkill",)
SECRET_KEY_RE = re.compile(
    r"(token|secret|password|passwd|dsn|credential|api[_-]?key)",
    re.IGNORECASE,
)
SECRET_VALUE_RE = re.compile(
    r"(?:"
    r"(?:postgres(?:ql)?|mysql|amqp|redis|mongodb)://"
    r"|password\s*="
    r"|secret\s*="
    r"|Bearer\s+[A-Za-z0-9._\-+=/]{8,}"
    r")",
    re.IGNORECASE,
)
USERINFO_RE = re.compile(r":[^/@]+@")
_SAFE_ID = frozenset("abcdefghijklmnopqrstuvwxyz0123456789._:-")

_LIMIT_BOUNDS = {
    "scrape_interval_ms": (100, 60_000),
    "stale_after_ms": (1_000, 600_000),
    "quiet_checkpoint_timeout_ms": (1_000, 3_600_000),
    "max_active_replays": (1, 2),
    "max_queued_replays": (1, 8),
    "max_actors_per_worker": (1, 1),
    "command_timeout_ms": (100, 60_000),
    "step_interval_ms": (100, 60_000),
    "max_commands_per_replay": (1, 100_000),
    "max_log_bytes": (4_096, 104_857_600),
    "max_health_bytes": (1_024, 1_048_576),
    "max_sample_payload_bytes": (1_024, 1_048_576),
}


class PublicSoakManifestError(ValueError):
    """The run manifest was rejected before any soak work started."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class FaultSpec:
    fault_id: str
    target_role: str
    method: str
    scheduled_elapsed_ms: int
    observation_timeout_ms: int
    recovery_timeout_ms: int


@dataclass(frozen=True, slots=True)
class InfrastructureEndpoints:
    postgres_bind: str
    redpanda_bind: str
    clickhouse_bind: str
    minio_bind: str
    compose_project: str
    compose_file: str


@dataclass(frozen=True, slots=True)
class ReplayWorkloadLimits:
    max_active_replays: int
    max_queued_replays: int
    max_actors_per_worker: int
    command_timeout_ms: int
    step_interval_ms: int
    max_commands_per_replay: int


@dataclass(frozen=True, slots=True)
class AcceptanceThresholds:
    max_unresolved_gaps: int
    max_hash_conflicts: int
    max_producer_epoch_rollbacks: int
    require_caught_up: bool


@dataclass(frozen=True, slots=True)
class OutputPolicy:
    result_path: str
    sample_dir: str
    log_dir: str
    max_log_bytes: int
    max_health_bytes: int
    max_sample_payload_bytes: int
    exclusive_create: bool


@dataclass(frozen=True, slots=True)
class PublicSoakManifest:
    schema_version: str
    run_id: str
    source: str
    exchange: str
    market: str
    symbol: str
    event_kind: str
    organization_id: str
    workspace_id: str
    duration_ms: int
    scrape_interval_ms: int
    stale_after_ms: int
    quiet_checkpoint_timeout_ms: int
    started_not_before_utc: str
    git_commit: str
    require_clean_worktree: bool
    infrastructure: InfrastructureEndpoints
    health_endpoints: dict[str, str]
    replay_limits: ReplayWorkloadLimits
    fault_plan: tuple[FaultSpec, ...]
    acceptance: AcceptanceThresholds
    output: OutputPolicy

    def to_canonical_dict(self) -> dict[str, object]:
        payload = _as_canonical(self)
        if not isinstance(payload, dict):
            raise TypeError("manifest canonical form must be an object")
        return payload

    def sha256(self) -> str:
        return hashlib.sha256(
            canonical_json(self.to_canonical_dict()).encode("utf-8")
        ).hexdigest()

    def dumps(self) -> str:
        return canonical_json(self.to_canonical_dict())


def load_manifest(
    path: str | Path,
    *,
    mode: str,
) -> PublicSoakManifest:
    raw_path = _absolute_existing_file(path, field="manifest_path")
    try:
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PublicSoakManifestError(
            "MANIFEST_JSON_INVALID",
            "manifest is not valid JSON",
            details={"error": str(exc)},
        ) from exc
    if not isinstance(payload, dict):
        raise PublicSoakManifestError(
            "MANIFEST_JSON_INVALID",
            "manifest root must be a JSON object",
        )
    return parse_manifest(payload, mode=mode)


def parse_manifest(
    payload: Mapping[str, Any],
    *,
    mode: str,
) -> PublicSoakManifest:
    if not isinstance(payload, Mapping):
        raise PublicSoakManifestError(
            "MANIFEST_JSON_INVALID",
            "manifest root must be an object",
        )
    normalized_mode = _require_mode(mode)
    unknown = sorted(set(payload) - _MANIFEST_KEYS)
    if unknown:
        raise PublicSoakManifestError(
            "UNKNOWN_MANIFEST_FIELD",
            "manifest contains unsupported fields",
            details={"fields": unknown},
        )
    schema_version = _required_text(payload.get("schema_version"), "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise PublicSoakManifestError(
            "SCHEMA_VERSION_MISMATCH",
            "schema_version does not match the Phase 1AI contract",
            details={"schema_version": schema_version},
        )
    source = _required_text(payload.get("source"), "source").lower()
    if source != SOURCE_BINANCE:
        raise PublicSoakManifestError(
            "SOURCE_NOT_BINANCE",
            "source must be binance",
            details={"source": source},
        )
    exchange = _required_text(payload.get("exchange"), "exchange").lower()
    market = _required_text(payload.get("market"), "market").lower()
    symbol = _required_text(payload.get("symbol"), "symbol").upper()
    event_kind = _required_text(payload.get("event_kind"), "event_kind").lower()
    if exchange != FROZEN_EXCHANGE:
        raise PublicSoakManifestError(
            "UNSUPPORTED_EXCHANGE",
            "first public soak exchange is frozen to binance",
        )
    if market != FROZEN_MARKET:
        raise PublicSoakManifestError(
            "UNSUPPORTED_MARKET",
            "first public soak market is frozen to futures",
        )
    if symbol != FROZEN_SYMBOL:
        raise PublicSoakManifestError(
            "UNSUPPORTED_SYMBOL",
            "first public soak symbol is frozen to BTCUSDT",
        )
    if event_kind != FROZEN_EVENT_KIND:
        raise PublicSoakManifestError(
            "UNSUPPORTED_EVENT_KIND",
            "first public soak event_kind is frozen to agg_trade",
        )
    duration_ms = _positive_int(payload.get("duration_ms"), "duration_ms")
    if normalized_mode == MODE_RUN and duration_ms < PUBLIC_SOAK_DURATION_MS:
        raise PublicSoakManifestError(
            "DURATION_BELOW_PUBLIC_SOAK",
            "run mode cannot be downgraded below 86_400_000 ms",
            details={"duration_ms": duration_ms},
        )
    if normalized_mode == MODE_DEVELOPMENT_SMOKE:
        if duration_ms < SMOKE_MIN_DURATION_MS:
            raise PublicSoakManifestError(
                "DURATION_BELOW_SMOKE",
                "development-smoke duration_ms must be at least 300000",
                details={"duration_ms": duration_ms},
            )
        if duration_ms >= PUBLIC_SOAK_DURATION_MS:
            raise PublicSoakManifestError(
                "SMOKE_CANNOT_CLAIM_PUBLIC_SOAK",
                "development-smoke cannot use a 24h duration",
                details={"duration_ms": duration_ms},
            )
    scrape_interval_ms = _bounded_int(
        payload.get("scrape_interval_ms"),
        "scrape_interval_ms",
    )
    stale_after_ms = _bounded_int(payload.get("stale_after_ms"), "stale_after_ms")
    if stale_after_ms <= scrape_interval_ms:
        raise PublicSoakManifestError(
            "LIMIT_OUT_OF_BOUNDS",
            "stale_after_ms must be greater than scrape_interval_ms",
        )
    quiet_checkpoint_timeout_ms = _bounded_int(
        payload.get("quiet_checkpoint_timeout_ms"),
        "quiet_checkpoint_timeout_ms",
    )
    started_not_before_utc = _utc_timestamp(
        payload.get("started_not_before_utc"),
        "started_not_before_utc",
    )
    git_commit = _required_text(payload.get("git_commit"), "git_commit").lower()
    if GIT_COMMIT_RE.fullmatch(git_commit) is None:
        raise PublicSoakManifestError(
            "INVALID_GIT_COMMIT",
            "git_commit must be a 40-character lowercase SHA-1",
        )
    require_clean = payload.get("require_clean_worktree")
    if not isinstance(require_clean, bool):
        raise PublicSoakManifestError(
            "INVALID_REQUIRE_CLEAN_WORKTREE",
            "require_clean_worktree must be a boolean",
        )
    infrastructure = _parse_infrastructure(payload.get("infrastructure"))
    health_endpoints = _parse_health_endpoints(payload.get("health_endpoints"))
    replay_limits = _parse_replay_limits(payload.get("replay_limits"))
    output = _parse_output(payload.get("output"))
    acceptance = _parse_acceptance(payload.get("acceptance"))
    fault_plan = _parse_fault_plan(
        payload.get("fault_plan"),
        mode=normalized_mode,
        duration_ms=duration_ms,
    )
    try:
        organization_id = normalize_organization_id(payload.get("organization_id"))
        workspace_id = normalize_workspace_id(payload.get("workspace_id"))
    except (TypeError, ValueError) as exc:
        raise PublicSoakManifestError(
            "INVALID_TENANT_SCOPE",
            str(exc),
        ) from exc
    manifest = PublicSoakManifest(
        schema_version=schema_version,
        run_id=_safe_id(payload.get("run_id"), "run_id"),
        source=source,
        exchange=exchange,
        market=market,
        symbol=symbol,
        event_kind=event_kind,
        organization_id=organization_id,
        workspace_id=workspace_id,
        duration_ms=duration_ms,
        scrape_interval_ms=scrape_interval_ms,
        stale_after_ms=stale_after_ms,
        quiet_checkpoint_timeout_ms=quiet_checkpoint_timeout_ms,
        started_not_before_utc=started_not_before_utc,
        git_commit=git_commit,
        require_clean_worktree=require_clean,
        infrastructure=infrastructure,
        health_endpoints=health_endpoints,
        replay_limits=replay_limits,
        fault_plan=fault_plan,
        acceptance=acceptance,
        output=output,
    )
    _reject_secrets(manifest.to_canonical_dict())
    return manifest


def _parse_infrastructure(raw: object) -> InfrastructureEndpoints:
    body = _object(raw, "infrastructure")
    unknown = sorted(set(body) - {item.name for item in fields(InfrastructureEndpoints)})
    if unknown:
        raise PublicSoakManifestError(
            "UNKNOWN_MANIFEST_FIELD",
            "infrastructure contains unsupported fields",
            details={"fields": unknown},
        )
    compose_file = _absolute_path_text(body.get("compose_file"), "compose_file")
    compose_project = _safe_id(body.get("compose_project"), "compose_project")
    if compose_project != "candlescope-phase1ai":
        raise PublicSoakManifestError(
            "UNSUPPORTED_COMPOSE_PROJECT",
            "compose_project must be candlescope-phase1ai",
        )
    return InfrastructureEndpoints(
        postgres_bind=_loopback_bind(body.get("postgres_bind"), "postgres_bind"),
        redpanda_bind=_loopback_bind(body.get("redpanda_bind"), "redpanda_bind"),
        clickhouse_bind=_loopback_bind(body.get("clickhouse_bind"), "clickhouse_bind"),
        minio_bind=_loopback_bind(body.get("minio_bind"), "minio_bind"),
        compose_project=compose_project,
        compose_file=compose_file,
    )


def _parse_health_endpoints(raw: object) -> dict[str, str]:
    body = _object(raw, "health_endpoints")
    missing = [role for role in REQUIRED_HEALTH_ROLES if role not in body]
    extra = sorted(set(body) - set(REQUIRED_HEALTH_ROLES))
    if missing or extra:
        raise PublicSoakManifestError(
            "HEALTH_ENDPOINTS_INVALID",
            "health_endpoints must list exactly the eight soak roles",
            details={"missing": missing, "extra": extra},
        )
    parsed: dict[str, str] = {}
    for role in REQUIRED_HEALTH_ROLES:
        parsed[role] = _health_url(body.get(role), field=f"health_endpoints.{role}")
    return parsed


def _parse_replay_limits(raw: object) -> ReplayWorkloadLimits:
    body = _object(raw, "replay_limits")
    unknown = sorted(set(body) - {item.name for item in fields(ReplayWorkloadLimits)})
    if unknown:
        raise PublicSoakManifestError(
            "UNKNOWN_MANIFEST_FIELD",
            "replay_limits contains unsupported fields",
            details={"fields": unknown},
        )
    return ReplayWorkloadLimits(
        max_active_replays=_bounded_int(
            body.get("max_active_replays"),
            "max_active_replays",
        ),
        max_queued_replays=_bounded_int(
            body.get("max_queued_replays"),
            "max_queued_replays",
        ),
        max_actors_per_worker=_bounded_int(
            body.get("max_actors_per_worker"),
            "max_actors_per_worker",
        ),
        command_timeout_ms=_bounded_int(
            body.get("command_timeout_ms"),
            "command_timeout_ms",
        ),
        step_interval_ms=_bounded_int(
            body.get("step_interval_ms"),
            "step_interval_ms",
        ),
        max_commands_per_replay=_bounded_int(
            body.get("max_commands_per_replay"),
            "max_commands_per_replay",
        ),
    )


def _parse_acceptance(raw: object) -> AcceptanceThresholds:
    body = _object(raw, "acceptance")
    unknown = sorted(set(body) - {item.name for item in fields(AcceptanceThresholds)})
    if unknown:
        raise PublicSoakManifestError(
            "UNKNOWN_MANIFEST_FIELD",
            "acceptance contains unsupported fields",
            details={"fields": unknown},
        )
    require_caught_up = body.get("require_caught_up")
    if not isinstance(require_caught_up, bool):
        raise PublicSoakManifestError(
            "INVALID_ACCEPTANCE",
            "require_caught_up must be a boolean",
        )
    gaps = body.get("max_unresolved_gaps")
    conflicts = body.get("max_hash_conflicts")
    rollbacks = body.get("max_producer_epoch_rollbacks")
    for name, value in (
        ("max_unresolved_gaps", gaps),
        ("max_hash_conflicts", conflicts),
        ("max_producer_epoch_rollbacks", rollbacks),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value != 0:
            raise PublicSoakManifestError(
                "INVALID_ACCEPTANCE",
                f"{name} must be 0",
            )
    return AcceptanceThresholds(
        max_unresolved_gaps=0,
        max_hash_conflicts=0,
        max_producer_epoch_rollbacks=0,
        require_caught_up=require_caught_up,
    )


def _parse_output(raw: object) -> OutputPolicy:
    body = _object(raw, "output")
    unknown = sorted(set(body) - {item.name for item in fields(OutputPolicy)})
    if unknown:
        raise PublicSoakManifestError(
            "UNKNOWN_MANIFEST_FIELD",
            "output contains unsupported fields",
            details={"fields": unknown},
        )
    exclusive = body.get("exclusive_create")
    if exclusive is not True:
        raise PublicSoakManifestError(
            "OUTPUT_MUST_BE_EXCLUSIVE",
            "output.exclusive_create must be true",
        )
    result_path = _absolute_path_text(body.get("result_path"), "result_path")
    if Path(result_path).exists():
        raise PublicSoakManifestError(
            "OUTPUT_EXISTS",
            "output result_path already exists and must not be overwritten",
            details={"result_path": result_path},
        )
    return OutputPolicy(
        result_path=result_path,
        sample_dir=_absolute_path_text(body.get("sample_dir"), "sample_dir"),
        log_dir=_absolute_path_text(body.get("log_dir"), "log_dir"),
        max_log_bytes=_bounded_int(body.get("max_log_bytes"), "max_log_bytes"),
        max_health_bytes=_bounded_int(
            body.get("max_health_bytes"),
            "max_health_bytes",
        ),
        max_sample_payload_bytes=_bounded_int(
            body.get("max_sample_payload_bytes"),
            "max_sample_payload_bytes",
        ),
        exclusive_create=True,
    )


def _parse_fault_plan(
    raw: object,
    *,
    mode: str,
    duration_ms: int,
) -> tuple[FaultSpec, ...]:
    if not isinstance(raw, list) or not raw:
        raise PublicSoakManifestError(
            "REQUIRED_FAULTS_MISSING",
            "fault_plan must be a non-empty array",
        )
    specs: list[FaultSpec] = []
    seen_ids: set[str] = set()
    previous_elapsed = 0
    for index, item in enumerate(raw):
        body = _object(item, f"fault_plan[{index}]")
        fault_id = _safe_id(body.get("fault_id"), "fault_id")
        if fault_id in seen_ids:
            raise PublicSoakManifestError(
                "FAULT_ID_DUPLICATE",
                "fault_id values must be unique",
                details={"fault_id": fault_id},
            )
        seen_ids.add(fault_id)
        method = _required_text(body.get("method"), "method")
        target_role = _required_text(body.get("target_role"), "target_role")
        expected_target = FAULT_METHOD_TARGETS.get(method)
        if expected_target is None:
            raise PublicSoakManifestError(
                "UNSUPPORTED_FAULT_METHOD",
                "fault method is not in the Phase 1AI set",
                details={"method": method},
            )
        if target_role != expected_target:
            raise PublicSoakManifestError(
                "FAULT_TARGET_MISMATCH",
                "fault target_role does not match method",
                details={"method": method, "target_role": target_role},
            )
        scheduled = _positive_int(
            body.get("scheduled_elapsed_ms"),
            "scheduled_elapsed_ms",
        )
        if scheduled >= duration_ms:
            raise PublicSoakManifestError(
                "FAULT_TIME_OUTSIDE_WINDOW",
                "fault scheduled_elapsed_ms must be inside the run window",
                details={"scheduled_elapsed_ms": scheduled},
            )
        if scheduled <= previous_elapsed:
            raise PublicSoakManifestError(
                "FAULT_TIME_NOT_INCREASING",
                "fault scheduled_elapsed_ms must strictly increase",
                details={"scheduled_elapsed_ms": scheduled},
            )
        previous_elapsed = scheduled
        specs.append(
            FaultSpec(
                fault_id=fault_id,
                target_role=target_role,
                method=method,
                scheduled_elapsed_ms=scheduled,
                observation_timeout_ms=_positive_int(
                    body.get("observation_timeout_ms"),
                    "observation_timeout_ms",
                ),
                recovery_timeout_ms=_positive_int(
                    body.get("recovery_timeout_ms"),
                    "recovery_timeout_ms",
                ),
            )
        )
    methods = tuple(item.method for item in specs)
    if mode == MODE_RUN:
        missing = [
            method for method in REQUIRED_RUN_FAULT_METHODS if method not in methods
        ]
        extra = [method for method in methods if method not in REQUIRED_RUN_FAULT_METHODS]
        if missing or extra or len(methods) != len(REQUIRED_RUN_FAULT_METHODS):
            raise PublicSoakManifestError(
                "REQUIRED_FAULTS_MISSING",
                "run mode requires the six frozen faults and no others",
                details={"missing": missing, "extra": extra},
            )
        if methods != REQUIRED_RUN_FAULT_METHODS:
            raise PublicSoakManifestError(
                "REQUIRED_FAULTS_MISSING",
                "run mode faults must appear in the frozen order",
                details={"methods": list(methods)},
            )
    else:
        if methods != SMOKE_ALLOWED_FAULT_METHODS:
            raise PublicSoakManifestError(
                "SMOKE_FAULT_NOT_WORKER_SIGKILL",
                "development-smoke may only plan worker_sigkill",
                details={"methods": list(methods)},
            )
    return tuple(specs)


def _require_mode(mode: object) -> str:
    if not isinstance(mode, str):
        raise PublicSoakManifestError("INVALID_MODE", "mode must be a string")
    normalized = mode.strip()
    if normalized not in {MODE_DEVELOPMENT_SMOKE, MODE_RUN}:
        raise PublicSoakManifestError(
            "INVALID_MODE",
            "mode must be development-smoke or run",
            details={"mode": normalized},
        )
    return normalized


def _health_url(value: object, *, field: str) -> str:
    text = _required_text(value, field)
    parsed = urlsplit(text)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in LOOPBACK_HOSTS
        or parsed.path not in HEALTH_PATHS
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query != ""
        or parsed.fragment != ""
        or parsed.port is None
    ):
        raise PublicSoakManifestError(
            "HEALTH_URL_UNSAFE",
            f"{field} must be loopback HTTP health without query or credentials",
            details={"url": text},
        )
    return text


def _loopback_bind(value: object, field: str) -> str:
    text = _required_text(value, field)
    if "/" in text or "?" in text or "#" in text or "@" in text:
        raise PublicSoakManifestError(
            "ENDPOINT_HAS_CREDENTIALS",
            f"{field} must be HOST:PORT without credentials",
            details={"value": text},
        )
    if text.count(":") != 1:
        raise PublicSoakManifestError(
            "ENDPOINT_HAS_CREDENTIALS",
            f"{field} must be HOST:PORT",
        )
    host, port_text = text.split(":", 1)
    if host not in LOOPBACK_HOSTS:
        raise PublicSoakManifestError(
            "HEALTH_URL_UNSAFE",
            f"{field} must bind only to loopback",
            details={"value": text},
        )
    if not port_text.isdigit() or not (1 <= int(port_text) <= 65535):
        raise PublicSoakManifestError(
            "LIMIT_OUT_OF_BOUNDS",
            f"{field} port is out of range",
        )
    return f"{host}:{int(port_text)}"


def _absolute_path_text(value: object, field: str) -> str:
    text = _required_text(value, field)
    path = Path(text)
    if not path.is_absolute():
        raise PublicSoakManifestError(
            "RELATIVE_PATH",
            f"{field} must be an absolute path",
            details={"path": text},
        )
    return str(path)


def _absolute_existing_file(path: str | Path, *, field: str) -> Path:
    if not isinstance(path, (str, Path)):
        raise PublicSoakManifestError("RELATIVE_PATH", f"{field} must be a path")
    resolved = Path(path)
    if not resolved.is_absolute():
        raise PublicSoakManifestError(
            "RELATIVE_PATH",
            f"{field} must be an absolute path",
            details={"path": str(resolved)},
        )
    if not resolved.is_file():
        raise PublicSoakManifestError(
            "MANIFEST_NOT_FOUND",
            "manifest file does not exist",
            details={"path": str(resolved)},
        )
    return resolved


def _utc_timestamp(value: object, field: str) -> str:
    text = _required_text(value, field)
    if not text.endswith("Z"):
        raise PublicSoakManifestError(
            "INVALID_UTC",
            f"{field} must be an RFC3339 UTC timestamp ending with Z",
        )
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PublicSoakManifestError(
            "INVALID_UTC",
            f"{field} is not a valid UTC timestamp",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise PublicSoakManifestError(
            "INVALID_UTC",
            f"{field} must be UTC",
        )
    return text


def _reject_secrets(payload: object, *, path: str = "$") -> None:
    if isinstance(payload, dict):
        for key, child in payload.items():
            if SECRET_KEY_RE.search(str(key)):
                raise PublicSoakManifestError(
                    "SECRET_IN_MANIFEST",
                    "manifest must not contain token, secret, password, or DSN keys",
                    details={"path": f"{path}.{key}"},
                )
            _reject_secrets(child, path=f"{path}.{key}")
        return
    if isinstance(payload, list):
        for index, child in enumerate(payload):
            _reject_secrets(child, path=f"{path}[{index}]")
        return
    if isinstance(payload, str):
        if SECRET_VALUE_RE.search(payload) or USERINFO_RE.search(payload):
            raise PublicSoakManifestError(
                "SECRET_IN_MANIFEST",
                "manifest must not contain token, secret, password, or DSN values",
                details={"path": path},
            )


def _object(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PublicSoakManifestError(
            "MANIFEST_JSON_INVALID",
            f"{field} must be an object",
        )
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PublicSoakManifestError(
            "MANIFEST_JSON_INVALID",
            f"{field} must be a non-empty string",
        )
    return value.strip()


def _safe_id(value: object, field: str) -> str:
    text = _required_text(value, field).lower()
    if len(text) > 128 or any(char not in _SAFE_ID for char in text):
        raise PublicSoakManifestError(
            "INVALID_IDENTIFIER",
            f"{field} contains unsupported characters",
        )
    return text


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PublicSoakManifestError(
            "LIMIT_OUT_OF_BOUNDS",
            f"{field} must be a positive integer",
        )
    return value


def _bounded_int(value: object, field: str) -> int:
    number = _positive_int(value, field)
    bounds = _LIMIT_BOUNDS.get(field)
    if bounds is None:
        return number
    low, high = bounds
    if number < low or number > high:
        raise PublicSoakManifestError(
            "LIMIT_OUT_OF_BOUNDS",
            f"{field} is outside the frozen bounds",
            details={"value": number, "min": low, "max": high},
        )
    return number


def _as_canonical(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {key: _as_canonical(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _as_canonical(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_as_canonical(item) for item in value]
    if isinstance(value, list):
        return [_as_canonical(item) for item in value]
    return value


_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "run_id",
        "source",
        "exchange",
        "market",
        "symbol",
        "event_kind",
        "organization_id",
        "workspace_id",
        "duration_ms",
        "scrape_interval_ms",
        "stale_after_ms",
        "quiet_checkpoint_timeout_ms",
        "started_not_before_utc",
        "git_commit",
        "require_clean_worktree",
        "infrastructure",
        "health_endpoints",
        "replay_limits",
        "fault_plan",
        "acceptance",
        "output",
    }
)


__all__ = [
    "AcceptanceThresholds",
    "FAULT_METHOD_TARGETS",
    "FaultSpec",
    "FROZEN_EVENT_KIND",
    "FROZEN_EXCHANGE",
    "FROZEN_MARKET",
    "FROZEN_SYMBOL",
    "InfrastructureEndpoints",
    "MODE_DEVELOPMENT_SMOKE",
    "MODE_RUN",
    "OutputPolicy",
    "PUBLIC_SOAK_DURATION_MS",
    "PublicSoakManifest",
    "PublicSoakManifestError",
    "REQUIRED_RUN_FAULT_METHODS",
    "ReplayWorkloadLimits",
    "SCHEMA_VERSION",
    "SMOKE_MIN_DURATION_MS",
    "SOURCE_BINANCE",
    "load_manifest",
    "parse_manifest",
]
