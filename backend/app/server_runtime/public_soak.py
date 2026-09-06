"""Phase 1AI sampling, hash chain, and exclusive evidence writer."""

from __future__ import annotations

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
    PublicSoakManifest,
    PublicSoakManifestError,
    SECRET_KEY_RE,
    SECRET_VALUE_RE,
    USERINFO_RE,
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
        raise SoakObservationError("HEALTH_JSON_INVALID", "sample payload must be an object")
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
        unsigned = {
            key: value for key, value in item.items() if key != "sample_sha256"
        }
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


__all__ = [
    "EvidenceWriter",
    "GENESIS_SHA256",
    "RESULT_SCHEMA_VERSION",
    "SAMPLE_SCHEMA_VERSION",
    "SampleRecord",
    "SoakObservationError",
    "SoakSampler",
    "build_sample_record",
    "parse_health_bytes",
    "redact_for_evidence",
    "sha256_canonical",
    "summarize_role_health",
]
