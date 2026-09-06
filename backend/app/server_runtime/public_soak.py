"""Phase 1AI sampling, hash chain, and exclusive evidence writer."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from app.replay.canonical import canonical_json
from app.server_runtime.public_soak_manifest import (
    MODE_DEVELOPMENT_SMOKE,
    MODE_RUN,
    PUBLIC_SOAK_DURATION_MS,
    SECRET_KEY_RE,
    SECRET_VALUE_RE,
    USERINFO_RE,
    PublicSoakManifest,
    PublicSoakManifestError,
)

SAMPLE_SCHEMA_VERSION = "candlescope.server-phase1ai-sample.v1"
RESULT_SCHEMA_VERSION = "candlescope.server-phase1ai-result.v1"
GENESIS_SHA256 = "0" * 64
PRIVATE_PATH_RE = re.compile(r"^/(?:home|root|var|opt|tmp|usr|etc|private)/")
MARKET_KEYS = frozenset(
    {
        "price",
        "qty",
        "quantity",
        "agg_trade_id",
        "trade_id",
        "p",
        "q",
        "a",
    }
)
ROLE_FIELDS = {
    "collector": (
        "ready",
        "state",
        "owner_id",
        "producer_epoch",
        "last_partition_offset",
        "updated_at_ms",
    ),
    "writer": (
        "ready",
        "state",
        "owner_id",
        "committed_next_offset",
        "duplicate_events",
        "conflict_events",
        "updated_at_ms",
    ),
    "archiver": (
        "ready",
        "state",
        "owner_id",
        "committed_next_offset",
        "current_snapshot",
        "updated_at_ms",
    ),
    "query": ("ready", "status", "snapshot", "updated_at_ms"),
    "scheduler": (
        "ready",
        "pending",
        "running",
        "live_workers",
        "updated_at_ms",
    ),
    "worker_a": (
        "ready",
        "state",
        "owner_id",
        "fencing_epoch",
        "active_actors",
        "updated_at_ms",
    ),
    "worker_b": (
        "ready",
        "state",
        "owner_id",
        "fencing_epoch",
        "active_actors",
        "updated_at_ms",
    ),
    "api": ("ready", "status", "updated_at_ms"),
}


class SoakObservationError(RuntimeError):
    """A sample, hash chain, or evidence write was rejected."""

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
class SampleRecord:
    sequence: int
    observed_at_utc: str
    monotonic_elapsed_ms: int
    payload: dict[str, object]
    payload_sha256: str
    previous_sample_sha256: str
    sample_sha256: str

    def to_canonical_dict(self) -> dict[str, object]:
        return {
            "schema_version": SAMPLE_SCHEMA_VERSION,
            "sequence": self.sequence,
            "observed_at_utc": self.observed_at_utc,
            "monotonic_elapsed_ms": self.monotonic_elapsed_ms,
            "payload": self.payload,
            "payload_sha256": self.payload_sha256,
            "previous_sample_sha256": self.previous_sample_sha256,
            "sample_sha256": self.sample_sha256,
        }


def sha256_canonical(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def redact_for_evidence(value: object, *, max_bytes: int) -> object:
    redacted = _redact(value)
    encoded = canonical_json(redacted).encode("utf-8")
    if len(encoded) > max_bytes:
        raise SoakObservationError(
            "HEALTH_PAYLOAD_TOO_LARGE",
            "redacted sample exceeded max_sample_payload_bytes",
            details={"bytes": len(encoded), "max_bytes": max_bytes},
        )
    return redacted


def build_sample_record(
    payload: Mapping[str, Any],
    *,
    sequence: int,
    observed_at_utc: str,
    monotonic_elapsed_ms: int,
    previous_sample_sha256: str | None,
    max_bytes: int,
) -> SampleRecord:
    if sequence < 1:
        raise SoakObservationError("INVALID_SAMPLE_SEQUENCE", "sequence must be >= 1")
    redacted = redact_for_evidence(dict(payload), max_bytes=max_bytes)
    if not isinstance(redacted, dict):
        raise SoakObservationError(
            "HEALTH_JSON_INVALID", "sample payload must be an object"
        )
    payload_sha256 = sha256_canonical(redacted)
    previous = previous_sample_sha256 or GENESIS_SHA256
    unsigned = {
        "schema_version": SAMPLE_SCHEMA_VERSION,
        "sequence": sequence,
        "observed_at_utc": observed_at_utc,
        "monotonic_elapsed_ms": monotonic_elapsed_ms,
        "payload": redacted,
        "payload_sha256": payload_sha256,
        "previous_sample_sha256": previous,
    }
    return SampleRecord(
        sequence=sequence,
        observed_at_utc=observed_at_utc,
        monotonic_elapsed_ms=monotonic_elapsed_ms,
        payload=redacted,
        payload_sha256=payload_sha256,
        previous_sample_sha256=previous,
        sample_sha256=sha256_canonical(unsigned),
    )


def parse_health_bytes(raw: bytes, *, max_bytes: int, role: str) -> dict[str, object]:
    if len(raw) > max_bytes:
        raise SoakObservationError(
            "HEALTH_PAYLOAD_TOO_LARGE",
            f"{role} health payload exceeded max_health_bytes",
            details={"role": role, "bytes": len(raw)},
        )
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SoakObservationError(
            "HEALTH_JSON_INVALID",
            f"{role} health payload is not valid JSON",
        ) from exc
    if not isinstance(payload, dict):
        raise SoakObservationError(
            "HEALTH_JSON_INVALID",
            f"{role} health payload must be a JSON object",
        )
    return payload


def summarize_role_health(role: str, body: Mapping[str, Any]) -> dict[str, object]:
    fields = ROLE_FIELDS.get(role)
    if fields is None:
        raise SoakObservationError("UNKNOWN_ROLE", f"unsupported soak role {role}")
    summary: dict[str, object] = {"role": role}
    for field in fields:
        if field in body:
            summary[field] = body[field]
    if role == "writer":
        summary.setdefault("unresolved_gaps", body.get("unresolved_gaps", 0))
    if role == "archiver" and "current_snapshot" in body:
        snapshot = body["current_snapshot"]
        if isinstance(snapshot, Mapping):
            summary["snapshot_version"] = snapshot.get("snapshot_version")
            summary["manifest_sha256"] = snapshot.get("manifest_sha256")
    return summary


class SoakSampler:
    def __init__(
        self,
        manifest: PublicSoakManifest,
        *,
        clock_ms: Callable[[], int],
        now_utc: Callable[[], str] | None = None,
    ) -> None:
        self._manifest = manifest
        self._clock_ms = clock_ms
        self._now_utc = now_utc or (
            lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        self._started_at_ms: int | None = None
        self._last_clock_ms: int | None = None
        self._previous_sha256: str | None = None
        self._sequence = 0
        self.records: list[SampleRecord] = []

    def observe(
        self,
        role_bodies: Mapping[str, Mapping[str, Any]],
        *,
        replays: Mapping[str, Mapping[str, Any]] | None = None,
        process_exits: Mapping[str, int | None] | None = None,
        last_fault: Mapping[str, Any] | None = None,
        raw_bytes_by_role: Mapping[str, int] | None = None,
    ) -> SampleRecord:
        now_ms = self._clock_ms()
        if self._started_at_ms is None:
            self._started_at_ms = now_ms
        if self._last_clock_ms is not None and now_ms < self._last_clock_ms:
            raise SoakObservationError(
                "CLOCK_ROLLBACK",
                "monotonic clock moved backwards",
                details={"previous_ms": self._last_clock_ms, "now_ms": now_ms},
            )
        max_health = self._manifest.output.max_health_bytes
        if raw_bytes_by_role:
            for role, size in raw_bytes_by_role.items():
                if size > max_health:
                    raise SoakObservationError(
                        "HEALTH_PAYLOAD_TOO_LARGE",
                        f"{role} health payload exceeded max_health_bytes",
                        details={"role": role, "bytes": size},
                    )
        roles: dict[str, object] = {}
        for role, body in role_bodies.items():
            if not isinstance(body, Mapping):
                raise SoakObservationError(
                    "HEALTH_JSON_INVALID",
                    f"{role} health JSON must be an object",
                )
            updated = body.get("updated_at_ms")
            if isinstance(updated, int):
                age = now_ms - updated
                if age > self._manifest.stale_after_ms:
                    raise SoakObservationError(
                        "HEALTH_STALE",
                        f"{role} health is older than stale_after_ms",
                        details={"role": role, "age_ms": age},
                    )
            roles[role] = summarize_role_health(role, body)
        self._sequence += 1
        payload = {
            "roles": roles,
            "replays": dict(replays or {}),
            "process_exits": dict(process_exits or {}),
            "last_fault": dict(last_fault or {}),
        }
        record = build_sample_record(
            payload,
            sequence=self._sequence,
            observed_at_utc=self._now_utc(),
            monotonic_elapsed_ms=now_ms - self._started_at_ms,
            previous_sample_sha256=self._previous_sha256,
            max_bytes=self._manifest.output.max_sample_payload_bytes,
        )
        self._previous_sha256 = record.sample_sha256
        self._last_clock_ms = now_ms
        self.records.append(record)
        return record


class EvidenceWriter:
    def __init__(
        self,
        manifest: PublicSoakManifest,
        *,
        mode: str,
        sample_path: str | Path,
    ) -> None:
        if mode not in {MODE_DEVELOPMENT_SMOKE, MODE_RUN}:
            raise SoakObservationError("INVALID_MODE", "evidence mode is invalid")
        self._manifest = manifest
        self._mode = mode
        self._sample_path = Path(sample_path)
        if not self._sample_path.is_absolute():
            raise PublicSoakManifestError(
                "RELATIVE_PATH",
                "sample_path must be absolute",
            )
        self._partial_path = Path(str(manifest.output.result_path) + ".partial")
        self._result_path = Path(manifest.output.result_path)
        self._sample_path.parent.mkdir(parents=True, exist_ok=True)
        self._partial_path.parent.mkdir(parents=True, exist_ok=True)
        _exclusive_write(self._partial_path, b"")
        _exclusive_write(self._sample_path, b"")

    @property
    def partial_path(self) -> Path:
        return self._partial_path

    def append_sample(self, record: SampleRecord, *, fsync: bool = False) -> None:
        line = canonical_json(record.to_canonical_dict()) + "\n"
        with self._sample_path.open("ab") as handle:
            handle.write(line.encode("utf-8"))
            handle.flush()
            if fsync:
                os.fsync(handle.fileno())

    def write_partial_snapshot(self, payload: Mapping[str, Any]) -> None:
        encoded = canonical_json(dict(payload)).encode("utf-8")
        with self._partial_path.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

    def finalize(
        self,
        *,
        phase_passed: bool,
        elapsed_ms: int,
        error_code: str | None = None,
        error_message: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, object]:
        samples = _reread_samples(self._sample_path)
        final_hash = samples[-1]["sample_sha256"] if samples else GENESIS_SHA256
        _verify_sample_chain(samples)
        continuity = (
            self._mode == MODE_RUN
            and phase_passed is True
            and elapsed_ms >= PUBLIC_SOAK_DURATION_MS
        )
        if self._mode == MODE_DEVELOPMENT_SMOKE:
            continuity = False
        result = {
            "schema_version": RESULT_SCHEMA_VERSION,
            "run_id": self._manifest.run_id,
            "mode": self._mode,
            "phase_passed": phase_passed,
            "phase1ai_passed": phase_passed if continuity else False,
            "twenty_four_hour_public_continuity": continuity,
            "production_ready": False,
            "elapsed_ms": elapsed_ms,
            "manifest_sha256": self._manifest.sha256(),
            "sample_file": self._sample_path.name,
            "sample_count": len(samples),
            "final_sample_sha256": final_hash,
            "error": None
            if phase_passed
            else {"code": error_code or "PHASE1AI_FAILED", "message": error_message},
        }
        if extra:
            redacted_extra = redact_for_evidence(
                dict(extra),
                max_bytes=self._manifest.output.max_sample_payload_bytes,
            )
            if isinstance(redacted_extra, dict):
                result["details"] = redacted_extra
        encoded = canonical_json(result).encode("utf-8")
        try:
            _exclusive_write(self._result_path, encoded)
        except FileExistsError as exc:
            raise SoakObservationError(
                "OUTPUT_EXISTS",
                "final evidence already exists",
                details={"path": str(self._result_path)},
            ) from exc
        self.write_partial_snapshot(result)
        return result


def _reread_samples(path: Path) -> list[dict[str, object]]:
    if not path.is_file() or path.stat().st_size == 0:
        return []
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SoakObservationError(
                "HEALTH_JSON_INVALID",
                "sample file contains invalid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise SoakObservationError(
                "HEALTH_JSON_INVALID",
                "sample line must be an object",
            )
        records.append(payload)
    return records


def _verify_sample_chain(samples: list[dict[str, object]]) -> None:
    previous = GENESIS_SHA256
    expected_sequence = 1
    for item in samples:
        if item.get("sequence") != expected_sequence:
            raise SoakObservationError(
                "SAMPLE_CHAIN_BROKEN",
                "sample sequence is not contiguous",
            )
        unsigned = {key: value for key, value in item.items() if key != "sample_sha256"}
        expected = sha256_canonical(unsigned)
        if item.get("sample_sha256") != expected:
            raise SoakObservationError(
                "SAMPLE_HASH_MISMATCH",
                "stored sample_sha256 does not match canonical payload",
            )
        if item.get("previous_sample_sha256") != previous:
            raise SoakObservationError(
                "SAMPLE_CHAIN_BROKEN",
                "previous_sample_sha256 does not match the prior sample",
            )
        previous = str(item["sample_sha256"])
        expected_sequence += 1


def _exclusive_write(path: Path, data: bytes) -> None:
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(path, flags, 0o644)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _redact(value: object) -> object:
    if isinstance(value, Mapping):
        redacted: dict[str, object] = {}
        for key, child in value.items():
            name = str(key)
            if SECRET_KEY_RE.search(name) or name in {"lease_token", "authorization"}:
                redacted[name] = "<redacted>"
            else:
                redacted[name] = _redact(child)
        return redacted
    if isinstance(value, list):
        if _looks_like_market_payload(value):
            return {"dropped": len(value), "kind": "market_payload"}
        return [_redact(item) for item in value[:32]]
    if isinstance(value, str):
        if SECRET_VALUE_RE.search(value) or USERINFO_RE.search(value):
            return "<redacted>"
        parsed = urlsplit(value)
        if parsed.scheme in {"http", "https"} and parsed.username:
            return "<redacted>"
        if PRIVATE_PATH_RE.match(value):
            return Path(value).name
        return value
    return value


def _looks_like_market_payload(value: list[object]) -> bool:
    if len(value) < 2:
        return False
    sample = value[0]
    if not isinstance(sample, Mapping):
        return False
    keys = {str(key).lower() for key in sample}
    return bool(keys & MARKET_KEYS)


async def execute_public_soak(
    manifest: PublicSoakManifest,
    *,
    mode: str,
    environ: Mapping[str, str],
    repo_root: Path,
    python_executable: str,
) -> dict[str, object]:
    """Run the public soak controller. Always writes immutable evidence."""

    import time

    import aiohttp

    from app.server_runtime.public_soak_processes import RoleProcessManager
    from app.server_runtime.soak_faults import SoakFaultMachine

    sample_path = Path(manifest.output.sample_dir) / f"{manifest.run_id}.samples.jsonl"
    writer = EvidenceWriter(manifest, mode=mode, sample_path=sample_path)
    manager = RoleProcessManager()
    clock = lambda: time.time_ns() // 1_000_000
    sampler = SoakSampler(manifest, clock_ms=clock)
    started_at_ms = clock()
    error_code = None
    error_message = None
    phase_passed = False
    replay_extra: dict[str, object] = {}
    takeover_observed = False
    try:
        _compose_up(manifest, environ)
        await _run_inits(
            manager,
            manifest,
            environ,
            python_executable=python_executable,
            repo_root=repo_root,
        )
        specs = build_role_specs(
            manifest,
            python_executable=python_executable,
            repo_root=repo_root,
        )
        environments = {
            spec.name: _role_child_env(spec.name, environ, manifest) for spec in specs
        }
        data_plane = [spec for spec in specs if spec.name != "api"]
        api_specs = [spec for spec in specs if spec.name == "api"]
        await manager.start_in_order(data_plane, environments)
        await _wait_archive_caught_up(manifest, timeout_ms=120_000)
        await manager.start_in_order(api_specs, environments)
        actuator = ProcessFaultActuator(manager, specs, environ, manifest)
        faults = SoakFaultMachine(
            manifest,
            actuator,
            clock_ms=clock,
            sleep=asyncio.sleep,
            hook_dir=manifest.output.log_dir,
        )
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            trust_env=False,
        ) as session:
            workload = await _start_replay_workload(
                manifest, environ, session, actuator, manager
            )
            replay_extra = {
                "replay_a": workload.replay_a.run_id,
                "replay_b": workload.replay_b.run_id,
                "replay_queued": workload.replay_queued.run_id,
                "replay_queued_observed": workload.replay_queued.observed_queued,
                "replay_a_session": workload.replay_a.session_id,
                "replay_a_owner": actuator.owner_worker_id,
                "kill_role": actuator.kill_role_override,
            }
            while clock() - started_at_ms < manifest.duration_ms:
                bodies, raw_sizes = await _scrape_roles(
                    session, manifest.health_endpoints
                )
                actuator.last_bodies = bodies
                sampler.observe(
                    bodies,
                    process_exits={
                        name: role.exit_code for name, role in manager.roles.items()
                    },
                    last_fault=_fault_evidence(faults),
                    raw_bytes_by_role=raw_sizes,
                )
                writer.append_sample(sampler.records[-1], fsync=False)
                await faults.run_due(started_at_ms=started_at_ms)
                await asyncio.sleep(manifest.scrape_interval_ms / 1000)
        takeover_observed = actuator.takeover_observed
        if not takeover_observed:
            raise SoakObservationError(
                "WORKER_TAKEOVER_NOT_OBSERVED",
                "Worker SIGKILL did not transfer the live Actor to the sibling",
            )
        phase_passed = True
    except Exception as exc:  # noqa: BLE001
        error_code = getattr(exc, "code", type(exc).__name__)
        error_message = str(exc)
        phase_passed = False
    finally:
        try:
            await manager.stop_in_reverse(
                build_role_specs(
                    manifest,
                    python_executable=python_executable,
                    repo_root=repo_root,
                )
            )
        except Exception as stop_exc:  # noqa: BLE001
            if error_code is None:
                error_code = type(stop_exc).__name__
                error_message = str(stop_exc)
    elapsed_ms = max(0, clock() - started_at_ms)
    return writer.finalize(
        phase_passed=phase_passed,
        elapsed_ms=elapsed_ms,
        error_code=None if phase_passed else str(error_code),
        error_message=error_message,
        extra={
            "events": manager.events[-32:],
            "roles": [role.to_evidence() for role in manager.roles.values()],
            "replay": replay_extra,
            "worker_takeover_observed": takeover_observed,
        },
    )


def build_role_specs(
    manifest: PublicSoakManifest,
    *,
    python_executable: str,
    repo_root: Path,
) -> list:
    from app.server_runtime.public_soak_processes import START_ORDER, RoleSpec

    scripts = repo_root / "backend" / "scripts"
    log_dir = Path(manifest.output.log_dir)
    max_log = manifest.output.max_log_bytes
    specs = []
    mapping = {
        "collector": (
            python_executable,
            str(scripts / "server_agg_trade_collector.py"),
            "run",
        ),
        "writer": (
            python_executable,
            str(scripts / "server_clickhouse_writer.py"),
            "run",
        ),
        "archiver": (
            python_executable,
            str(scripts / "server_parquet_archiver.py"),
            "run",
        ),
        "query": (python_executable, str(scripts / "server_snapshot_query.py")),
        "scheduler": (
            python_executable,
            str(scripts / "server_replay_scheduler.py"),
            "--health-bind",
            _bind(manifest.health_endpoints["scheduler"]),
        ),
        "worker_a": (
            python_executable,
            str(scripts / "server_replay_worker.py"),
            "--pool",
            "--control-bind",
            _bind(manifest.health_endpoints["worker_a"]),
        ),
        "worker_b": (
            python_executable,
            str(scripts / "server_replay_worker.py"),
            "--pool",
            "--control-bind",
            _bind(manifest.health_endpoints["worker_b"]),
        ),
        "api": (
            python_executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            _bind(manifest.health_endpoints["api"]).split(":")[1],
        ),
    }
    extra_args = {
        "collector": (
            "--health-bind",
            _bind(manifest.health_endpoints["collector"]),
        ),
        "writer": ("--health-bind", _bind(manifest.health_endpoints["writer"])),
        "archiver": ("--health-bind", _bind(manifest.health_endpoints["archiver"])),
    }
    for name in START_ORDER:
        argv = mapping[name] + extra_args.get(name, ())
        specs.append(
            RoleSpec(
                name=name,
                argv=tuple(argv),
                sanitized_environment_keys=_SANITIZED_KEYS,
                health_url=manifest.health_endpoints[name],
                startup_timeout_ms=120_000 if name == "api" else 60_000,
                shutdown_timeout_ms=8_000,
                stdout_log=str(log_dir / f"{name}.stdout.log"),
                stderr_log=str(log_dir / f"{name}.stderr.log"),
                max_log_bytes=max_log,
            )
        )
    return specs


def _fault_evidence(machine) -> dict[str, object]:
    records = getattr(machine, "records", None) or []
    if not records:
        return {}
    index = int(getattr(machine, "_index", 0) or 0)
    if index >= len(records):
        return records[-1].to_evidence()
    return records[index].to_evidence()


def repo_root_from_manifest(manifest: PublicSoakManifest) -> Path:
    return Path(manifest.infrastructure.compose_file).resolve().parents[2]


async def _start_replay_workload(
    manifest: PublicSoakManifest,
    environ: Mapping[str, str],
    session,
    actuator: ProcessFaultActuator,
    manager,
):
    from app.server_runtime.public_soak_replay import (
        FROZEN_IDEMPOTENT_COMMAND_ID,
        HttpSoakReplayTransport,
        PublicSoakReplayDriver,
        assignment_worker_id,
        durable_command_count_from_store,
        health_origin,
        pin_from_query_events,
        worker_role_from_id,
    )

    pin = await _wait_snapshot_pin(manifest, session)
    api_token = _api_bearer_token(environ)
    query_token = environ.get("CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN") or ""
    transport = HttpSoakReplayTransport(
        api_origin=health_origin(manifest.health_endpoints["api"]),
        query_origin=health_origin(manifest.health_endpoints["query"]),
        api_token=api_token,
        query_token=query_token,
        organization_id=manifest.organization_id,
        workspace_id=manifest.workspace_id,
        session=session,
    )
    driver = PublicSoakReplayDriver(manifest, transport, sleep=asyncio.sleep)
    queried = await transport.cold_query_snapshot(pin.to_payload()["snapshot"])
    events = queried.get("events")
    if not isinstance(events, list) or not events:
        raise SoakObservationError(
            "QUERY_NOT_COLD",
            "cold Query returned no events for the pinned snapshot",
        )
    typed_events = [item for item in events if isinstance(item, Mapping)]
    pin = pin_from_query_events(pin.to_payload()["snapshot"], typed_events)
    workload = await driver.create_three_tasks(pin)
    commands = await driver.drive_commands(workload)
    await driver.probe_idempotency(
        workload,
        expected_revision=commands["replay-b-resume"].revision,
    )
    if workload.replay_a.session_id is None:
        raise SoakObservationError("SESSION_NOT_ASSIGNED", "replay-a has no session")
    dsn = environ.get("CANDLESCOPE_SERVER_REPLAY_WORKER_POSTGRES_DSN") or ""
    durable = await durable_command_count_from_store(dsn, FROZEN_IDEMPOTENT_COMMAND_ID)
    if durable != 1:
        raise SoakObservationError(
            "COMMAND_NOT_IDEMPOTENT",
            "PostgreSQL must contain exactly one durable command result",
        )
    owner = await assignment_worker_id(dsn, workload.replay_a.session_id)
    if not owner:
        raise SoakObservationError(
            "WORKER_TAKEOVER_NOT_OBSERVED",
            "could not resolve the Worker that owns replay-a",
        )
    kill_role = worker_role_from_id(owner)
    actuator.kill_role_override = kill_role
    actuator.owner_worker_id = owner
    sibling = "worker_b" if kill_role == "worker_a" else "worker_a"
    await transport.cancel_run(workload.replay_queued.run_id)
    await transport.cancel_run(workload.replay_b.run_id)
    await _wait_run_terminal(transport, workload.replay_queued.run_id)
    await _wait_run_terminal(transport, workload.replay_b.run_id)
    await _wait_worker_idle(session, manifest.health_endpoints[sibling])

    async def _takeover() -> bool:
        process = manager._live.get(kill_role)
        dead = process is not None and process.returncode is not None
        if not dead:
            return False
        sibling_proc = manager._live.get(sibling)
        sibling_alive = sibling_proc is not None and sibling_proc.returncode is None
        if not sibling_alive:
            return False
        try:
            session_body = await transport.get_session(
                workload.replay_a.session_id or ""
            )
        except Exception:  # noqa: BLE001
            return False
        running = str(session_body.get("state") or "") == "RUNNING"
        attempt = int(session_body.get("attempt") or 0)
        observed = dead and running and attempt >= 2 and sibling_alive
        if observed:
            actuator.takeover_observed = True
        return observed

    actuator.takeover_check = _takeover
    return workload


async def _wait_snapshot_pin(manifest: PublicSoakManifest, session):
    import time

    from app.server_runtime.public_soak_replay import parse_snapshot_pin

    deadline = time.time_ns() // 1_000_000 + 120_000
    while time.time_ns() // 1_000_000 < deadline:
        bodies, _sizes = await _scrape_roles(session, manifest.health_endpoints)
        archive = bodies.get("archiver") or {}
        snapshot = archive.get("current_snapshot")
        if (
            isinstance(snapshot, Mapping)
            and int(snapshot.get("snapshot_version") or 0) > 0
        ):
            start_ms = int(archive.get("updated_at_ms") or 0)
            return parse_snapshot_pin(
                {
                    "snapshot": dict(snapshot),
                    "pin": {
                        "start_event_time_ms": max(0, start_ms - 60_000),
                        "end_event_time_ms": start_ms + 60_000,
                        "expected_first_agg_trade_id": 1,
                        "expected_last_agg_trade_id": 2,
                        "row_count": 2,
                    },
                }
            )
        await asyncio.sleep(0.5)
    raise SoakObservationError(
        "SNAPSHOT_MISSING",
        "archiver did not publish an immutable snapshot",
    )


def _api_bearer_token(environ: Mapping[str, str]) -> str:
    token = environ.get("CANDLESCOPE_PHASE1AI_API_TOKEN") or environ.get(
        "CANDLESCOPE_SERVER_API_TOKEN", ""
    )
    if str(token).strip():
        return str(token).strip()
    raw = environ.get("CANDLESCOPE_SERVER_API_STATIC_TOKENS_JSON") or ""
    if not str(raw).strip():
        return ""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if isinstance(payload, dict) and payload:
        return str(next(iter(payload)))
    return ""


async def _wait_run_terminal(transport, run_id: str, *, attempts: int = 150) -> None:
    current = ""
    for _ in range(attempts):
        body = await transport.get_run(run_id)
        current = str(body.get("state") or "")
        if current in {"CANCELLED", "FAILED", "COMPLETED"}:
            return
        await asyncio.sleep(0.2)
    raise SoakObservationError(
        "WORKER_TAKEOVER_NOT_OBSERVED",
        f"{run_id} did not reach a terminal scheduler state",
    )


async def _wait_worker_idle(session, health_url: str, *, attempts: int = 150) -> None:
    for _ in range(attempts):
        try:
            async with session.get(health_url) as response:
                body = await response.json(content_type=None)
        except Exception:  # noqa: BLE001
            body = {}
        if not isinstance(body, Mapping):
            body = {}
        actors = int(body.get("active_actors") or 0)
        if actors == 0:
            return
        await asyncio.sleep(0.2)
    raise SoakObservationError(
        "WORKER_TAKEOVER_NOT_OBSERVED",
        "sibling Worker did not release its Actor after cancel",
    )


async def _wait_archive_caught_up(
    manifest: PublicSoakManifest,
    *,
    timeout_ms: int,
) -> None:
    import time

    import aiohttp

    deadline = time.time_ns() // 1_000_000 + timeout_ms
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=2),
        trust_env=False,
    ) as session:
        while time.time_ns() // 1_000_000 < deadline:
            bodies, _sizes = await _scrape_roles(session, manifest.health_endpoints)
            writer = bodies.get("writer") or {}
            archive = bodies.get("archiver") or {}
            writer_next = writer.get("committed_next_offset")
            archive_next = archive.get("committed_next_offset")
            archive_error = archive.get("terminal_error") or archive.get("error")
            if archive.get("ready") is False and archive_error:
                raise SoakObservationError(
                    "ARCHIVER_TERMINAL",
                    "archiver stopped before catch-up",
                    details={"error": str(archive_error)},
                )
            if (
                isinstance(writer_next, int)
                and isinstance(archive_next, int)
                and archive_next >= max(0, writer_next - 20)
            ):
                return
            await asyncio.sleep(0.5)
    raise SoakObservationError(
        "ARCHIVE_CATCHUP_TIMEOUT",
        "archiver did not catch up before API start",
    )


def _bind(health_url: str) -> str:
    parsed = urlsplit(health_url)
    return f"{parsed.hostname}:{parsed.port}"


_SANITIZED_KEYS = (
    "PATH",
    "PYTHONPATH",
    "CANDLESCOPE_PROFILE",
    "CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN",
    "CANDLESCOPE_SERVER_COLLECTOR_KAFKA_BOOTSTRAP_SERVERS",
    "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_PASSWORD",
    "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_SECRET_ACCESS_KEY",
    "CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN",
    "CANDLESCOPE_SERVER_REPLAY_WORKER_POSTGRES_DSN",
    "CANDLESCOPE_SERVER_API_STATIC_TOKENS_JSON",
)


def _compose_up(manifest: PublicSoakManifest, environ: Mapping[str, str]) -> None:
    import subprocess

    env_file = environ.get("CANDLESCOPE_PHASE1AI_ENV_FILE")
    command = [
        "docker",
        "compose",
        "-p",
        manifest.infrastructure.compose_project,
        "-f",
        manifest.infrastructure.compose_file,
        "up",
        "-d",
        "--wait",
    ]
    if env_file:
        command[4:4] = ["--env-file", env_file]
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise SoakObservationError(
            "COMPOSE_UP_FAILED",
            "docker compose up --wait failed",
            details={"stderr": (result.stderr or "")[-500:]},
        )


async def _run_inits(
    manager,
    manifest: PublicSoakManifest,
    environ: Mapping[str, str],
    *,
    python_executable: str,
    repo_root: Path,
) -> None:
    from app.server_runtime.public_soak_processes import InitCommand

    scripts = repo_root / "backend" / "scripts"
    log_dir = Path(manifest.output.log_dir)
    commands = [
        InitCommand(
            name="collector-init-schema",
            argv=(
                python_executable,
                str(scripts / "server_agg_trade_collector.py"),
                "init-schema",
            ),
            sanitized_environment_keys=_SANITIZED_KEYS,
            timeout_ms=60_000,
            stdout_log=str(log_dir / "collector-init.stdout.log"),
            stderr_log=str(log_dir / "collector-init.stderr.log"),
            max_log_bytes=manifest.output.max_log_bytes,
        ),
        InitCommand(
            name="writer-init-schema",
            argv=(
                python_executable,
                str(scripts / "server_clickhouse_writer.py"),
                "init-schema",
            ),
            sanitized_environment_keys=_SANITIZED_KEYS,
            timeout_ms=60_000,
            stdout_log=str(log_dir / "writer-init.stdout.log"),
            stderr_log=str(log_dir / "writer-init.stderr.log"),
            max_log_bytes=manifest.output.max_log_bytes,
        ),
        InitCommand(
            name="archiver-init-bucket",
            argv=(
                python_executable,
                str(scripts / "server_parquet_archiver.py"),
                "init-bucket",
            ),
            sanitized_environment_keys=_SANITIZED_KEYS,
            timeout_ms=60_000,
            stdout_log=str(log_dir / "archiver-init.stdout.log"),
            stderr_log=str(log_dir / "archiver-init.stderr.log"),
            max_log_bytes=manifest.output.max_log_bytes,
        ),
        InitCommand(
            name="postgres-query-control",
            argv=(
                python_executable,
                "-c",
                "from app.server_runtime.public_soak_processes import bootstrap_postgres_main; bootstrap_postgres_main()",
            ),
            sanitized_environment_keys=_SANITIZED_KEYS,
            timeout_ms=60_000,
            stdout_log=str(log_dir / "postgres-init.stdout.log"),
            stderr_log=str(log_dir / "postgres-init.stderr.log"),
            max_log_bytes=manifest.output.max_log_bytes,
        ),
    ]
    child_env = dict(environ)
    for command in commands:
        await manager.run_init(command, child_env)


async def _scrape_roles(session, endpoints: Mapping[str, str]):
    bodies: dict[str, dict[str, object]] = {}
    sizes: dict[str, int] = {}
    for role, url in endpoints.items():
        try:
            async with session.get(url) as response:
                raw = await response.read()
                sizes[role] = len(raw)
                if response.status != 200:
                    bodies[role] = {"ready": False, "status": response.status}
                    continue
                payload = json.loads(raw.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise SoakObservationError(
                        "HEALTH_JSON_INVALID",
                        f"{role} health must be an object",
                    )
                bodies[role] = payload
        except SoakObservationError:
            raise
        except Exception as exc:  # noqa: BLE001
            bodies[role] = {
                "ready": False,
                "status": "unreachable",
                "error": type(exc).__name__,
            }
            sizes[role] = 0
    return bodies, sizes


def _role_child_env(
    name: str, environ: Mapping[str, str], manifest: PublicSoakManifest
) -> dict[str, str]:
    env = dict(environ)
    env.setdefault(
        "PYTHONPATH", str(Path(repo_root_from_manifest(manifest)) / "backend")
    )
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("CANDLESCOPE_SERVER_REPLAY_WORKER_LEASE_TTL_MS", "8000")
    env.setdefault("CANDLESCOPE_SERVER_REPLAY_WORKER_RENEW_INTERVAL_MS", "2000")
    env.setdefault("CANDLESCOPE_SERVER_REPLAY_SCHEDULER_HEARTBEAT_TTL_MS", "5000")
    env["CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH"] = manifest.run_id
    env["CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_GROUP_ID"] = (
        f"{manifest.run_id}-writer"
    )
    env["CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_GROUP_ID"] = (
        f"{manifest.run_id}-archiver"
    )
    if name == "api":
        env["CANDLESCOPE_PROFILE"] = "server"
    if name == "worker_b":
        env["CANDLESCOPE_SERVER_REPLAY_WORKER_WORKER_ID"] = env.get(
            "CANDLESCOPE_SERVER_REPLAY_WORKER_B_WORKER_ID",
            "worker-b",
        )
        if "CANDLESCOPE_SERVER_REPLAY_WORKER_B_CONTROL_TOKEN" in env:
            env["CANDLESCOPE_SERVER_REPLAY_WORKER_CONTROL_TOKEN"] = env[
                "CANDLESCOPE_SERVER_REPLAY_WORKER_B_CONTROL_TOKEN"
            ]
    if name == "worker_a":
        env["CANDLESCOPE_SERVER_REPLAY_WORKER_WORKER_ID"] = env.get(
            "CANDLESCOPE_SERVER_REPLAY_WORKER_A_WORKER_ID",
            "worker-a",
        )
        if "CANDLESCOPE_SERVER_REPLAY_WORKER_A_CONTROL_TOKEN" in env:
            env["CANDLESCOPE_SERVER_REPLAY_WORKER_CONTROL_TOKEN"] = env[
                "CANDLESCOPE_SERVER_REPLAY_WORKER_A_CONTROL_TOKEN"
            ]
    return env


class ProcessFaultActuator:
    def __init__(
        self,
        manager,
        specs,
        environ: Mapping[str, str],
        manifest: PublicSoakManifest,
    ) -> None:
        self._manager = manager
        self._specs = {spec.name: spec for spec in specs}
        self._environ = environ
        self._manifest = manifest
        self._triggered: set[str] = set()
        self._restarted: set[str] = set()
        self.last_bodies: dict[str, dict[str, object]] = {}
        self.kill_role_override: str | None = None
        self.owner_worker_id: str | None = None
        self.takeover_check = None
        self.takeover_observed = False

    async def trigger(self, spec) -> None:
        from app.server_runtime.soak_faults import (
            arm_precommit_hook,
            precommit_hook_name,
        )

        if precommit_hook_name(spec.method):
            arm_precommit_hook(
                Path(self._environ.get("CANDLESCOPE_PHASE1AI_HOOK_DIR", "/tmp")),
                spec,
            )
            self._triggered.add(spec.fault_id)
            return
        role = self.kill_role_override or spec.target_role
        process = self._manager._live.get(role)
        if process is None:
            raise SoakObservationError("ROLE_NOT_STARTED", role)
        if spec.method.endswith("sigkill"):
            process.kill()
        else:
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            pass
        self._triggered.add(spec.fault_id)

    async def trigger_observed(self, spec) -> bool:
        role = self.kill_role_override or spec.target_role
        process = self._manager._live.get(role)
        if spec.method.endswith("sigkill"):
            return process is not None and process.returncode is not None
        return spec.fault_id in self._triggered

    async def recovery_observed(self, spec) -> bool:
        if spec.method.endswith("sigkill") and self.takeover_check is not None:
            return await self.takeover_check()
        if not spec.method.endswith("sigkill"):
            return True
        role = self.kill_role_override or spec.target_role
        process = self._manager._live.get(role)
        if process is not None and process.returncode is None:
            return True
        if role in self._restarted:
            live = self._manager._live.get(role)
            return live is not None and live.returncode is None
        if role in self._manager._live:
            await self._manager._finalize(role)
        env = _role_child_env(role, self._environ, self._manifest)
        await self._manager.start_role(self._specs[role], env)
        self._restarted.add(role)
        live = self._manager._live.get(role)
        return live is not None and live.returncode is None

    async def quiet_observation(self):
        import aiohttp

        from app.server_runtime.soak_faults import QuietObservation

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=2),
            trust_env=False,
        ) as session:
            bodies, _sizes = await _scrape_roles(
                session, self._manifest.health_endpoints
            )
        self.last_bodies = bodies
        collector = self._offset("collector", "last_partition_offset")
        if collector is not None:
            collector += 1
        writer = self._offset("writer", "committed_next_offset")
        archive = self._offset("archiver", "committed_next_offset")
        snapshot = self._snapshot()
        return QuietObservation(
            collector_durable_next_offset=int(collector or 0),
            writer_committed_next_offset=int(writer or 0),
            archiver_covered_next_offset=int(archive or 0),
            query_snapshot=snapshot,
            replay_pinned_snapshot=snapshot,
            unresolved_gaps=int(
                self.last_bodies.get("writer", {}).get("unresolved_gaps") or 0
            ),
            hash_conflicts=int(
                self.last_bodies.get("writer", {}).get("conflict_events") or 0
            ),
            producer_epoch_rollback=0,
        )

    def _offset(self, role: str, field: str) -> int | None:
        body = self.last_bodies.get(role) or {}
        value = body.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        return None

    def _snapshot(self) -> dict[str, object]:
        body = self.last_bodies.get("archiver") or {}
        snapshot = body.get("current_snapshot")
        if isinstance(snapshot, dict):
            return {
                "snapshot_version": snapshot.get("snapshot_version"),
                "manifest_sha256": snapshot.get("manifest_sha256"),
            }
        return {}


__all__ = [
    "GENESIS_SHA256",
    "RESULT_SCHEMA_VERSION",
    "SAMPLE_SCHEMA_VERSION",
    "EvidenceWriter",
    "ProcessFaultActuator",
    "SampleRecord",
    "SoakObservationError",
    "SoakSampler",
    "build_role_specs",
    "build_sample_record",
    "execute_public_soak",
    "parse_health_bytes",
    "redact_for_evidence",
    "sha256_canonical",
    "summarize_role_health",
]
