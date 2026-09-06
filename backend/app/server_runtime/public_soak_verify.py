"""Independent Phase 1AI verifier. Reads frozen files only."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from app.replay.canonical import canonical_json
from app.server_runtime.public_soak import (
    GENESIS_SHA256,
    RESULT_SCHEMA_VERSION,
    sha256_canonical,
)
from app.server_runtime.public_soak_manifest import (
    MODE_DEVELOPMENT_SMOKE,
    MODE_RUN,
    PUBLIC_SOAK_DURATION_MS,
    SECRET_KEY_RE,
    SECRET_VALUE_RE,
    USERINFO_RE,
    PublicSoakManifestError,
    load_manifest,
)


class SoakVerifyError(RuntimeError):
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


def verify_evidence(
    *,
    manifest_path: str | Path,
    result_path: str | Path,
    sample_path: str | Path | None = None,
) -> dict[str, object]:
    manifest_file = _absolute_file(manifest_path, "manifest")
    result_file = _absolute_file(result_path, "result")
    result = _load_json_object(result_file)
    mode = str(result.get("mode") or MODE_RUN)
    try:
        manifest = load_manifest(
            manifest_file,
            mode=mode,
            allow_existing_output=True,
        )
    except PublicSoakManifestError as exc:
        raise SoakVerifyError(exc.code, exc.message, details=exc.details) from exc
    if result.get("schema_version") != RESULT_SCHEMA_VERSION:
        raise SoakVerifyError(
            "RESULT_SCHEMA_MISMATCH",
            "result schema_version does not match the Phase 1AI contract",
        )
    if result.get("manifest_sha256") != manifest.sha256():
        raise SoakVerifyError(
            "MANIFEST_HASH_MISMATCH",
            "result manifest_sha256 does not match the frozen manifest",
        )
    samples_file = (
        Path(sample_path)
        if sample_path is not None
        else result_file.parent / str(result.get("sample_file") or "")
    )
    if not samples_file.is_absolute():
        raise SoakVerifyError("RELATIVE_PATH", "sample_path must be absolute")
    samples = _load_samples(samples_file)
    final_hash = _verify_chain(samples)
    if int(result.get("sample_count") or -1) != len(samples):
        raise SoakVerifyError(
            "SAMPLE_COUNT_MISMATCH",
            "result sample_count does not match the sample file",
        )
    if samples and result.get("final_sample_sha256") != final_hash:
        raise SoakVerifyError(
            "SAMPLE_HASH_MISMATCH",
            "result final_sample_sha256 does not match recomputed chain",
        )
    _scan_for_secrets(result)
    _scan_for_secrets({"samples": samples})
    continuity = result.get("twenty_four_hour_public_continuity") is True
    production_ready = result.get("production_ready") is True
    if mode == MODE_DEVELOPMENT_SMOKE and continuity:
        raise SoakVerifyError(
            "SMOKE_CLAIMED_PUBLIC_CONTINUITY",
            "development-smoke cannot claim 24h continuity",
        )
    if production_ready:
        raise SoakVerifyError(
            "PRODUCTION_READY_CLAIMED",
            "verifier refuses production_ready=true without a release review",
        )
    if continuity and int(result.get("elapsed_ms") or 0) < PUBLIC_SOAK_DURATION_MS:
        raise SoakVerifyError(
            "CONTINUITY_ELAPSED_TOO_SHORT",
            "24h continuity was claimed without 86_400_000 ms elapsed",
        )
    return {
        "verified": True,
        "manifest_sha256": manifest.sha256(),
        "final_sample_sha256": final_hash,
        "sample_count": len(samples),
        "phase_passed": result.get("phase_passed") is True,
        "twenty_four_hour_public_continuity": False if mode == MODE_DEVELOPMENT_SMOKE else continuity,
        "production_ready": False,
        "mode": mode,
    }


def _verify_chain(samples: list[dict[str, object]]) -> str:
    previous = GENESIS_SHA256
    expected_sequence = 1
    final_hash = GENESIS_SHA256
    for item in samples:
        if item.get("sequence") != expected_sequence:
            raise SoakVerifyError("SAMPLE_CHAIN_BROKEN", "sample sequence is not contiguous")
        unsigned = {key: value for key, value in item.items() if key != "sample_sha256"}
        expected = sha256_canonical(unsigned)
        if item.get("sample_sha256") != expected:
            raise SoakVerifyError(
                "SAMPLE_HASH_MISMATCH",
                "stored sample_sha256 does not match canonical payload",
            )
        if item.get("previous_sample_sha256") != previous:
            raise SoakVerifyError(
                "SAMPLE_CHAIN_BROKEN",
                "previous_sample_sha256 does not match the prior sample",
            )
        previous = str(item["sample_sha256"])
        final_hash = previous
        expected_sequence += 1
    return final_hash


def _load_samples(path: Path) -> list[dict[str, object]]:
    if not path.is_file():
        return []
    records: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SoakVerifyError(
                "HEALTH_JSON_INVALID",
                "sample file contains invalid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise SoakVerifyError("HEALTH_JSON_INVALID", "sample line must be an object")
        records.append(payload)
    return records


def _load_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SoakVerifyError(
            "RESULT_JSON_INVALID",
            "result is not valid JSON",
        ) from exc
    if not isinstance(payload, dict):
        raise SoakVerifyError("RESULT_JSON_INVALID", "result must be a JSON object")
    return payload


def _absolute_file(path: str | Path, field: str) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        raise SoakVerifyError("RELATIVE_PATH", f"{field} must be an absolute path")
    if not resolved.is_file():
        raise SoakVerifyError("FILE_NOT_FOUND", f"{field} does not exist")
    return resolved


def _scan_for_secrets(payload: object, *, path: str = "$") -> None:
    if isinstance(payload, Mapping):
        for key, child in payload.items():
            name = str(key)
            if SECRET_KEY_RE.search(name) and name not in {
                "manifest_sha256",
                "payload_sha256",
                "sample_sha256",
                "previous_sample_sha256",
                "final_sample_sha256",
            }:
                raise SoakVerifyError(
                    "SECRET_IN_EVIDENCE",
                    "evidence contains a secret-like key",
                    details={"path": f"{path}.{name}"},
                )
            _scan_for_secrets(child, path=f"{path}.{name}")
        return
    if isinstance(payload, list):
        for index, child in enumerate(payload):
            _scan_for_secrets(child, path=f"{path}[{index}]")
        return
    if isinstance(payload, str) and (
        SECRET_VALUE_RE.search(payload) or USERINFO_RE.search(payload)
    ):
        raise SoakVerifyError(
            "SECRET_IN_EVIDENCE",
            "evidence contains a secret-like value",
            details={"path": path},
        )


def dumps_report(report: Mapping[str, Any]) -> str:
    return canonical_json(dict(report))


__all__ = ["SoakVerifyError", "dumps_report", "verify_evidence"]
