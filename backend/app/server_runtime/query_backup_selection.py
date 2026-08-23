"""Bounded recovery selection from receipts reconciled to signed manifests."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import rfc8785

from app.server_runtime.query_backup_catalog import PublishedPhysicalBackup

BACKUP_JOB_RECEIPT_SCHEMA_VERSION = "candlescope.query-backup-job-receipt.v1"
BACKUP_JOB_RESULT_SCHEMA_VERSION = "candlescope.query-backup-job-result.v2"
RECOVERY_SELECTION_SCHEMA_VERSION = "candlescope.query-backup-recovery-selection.v1"
DEFAULT_MAXIMUM_CANDIDATES = 32
DEFAULT_MAXIMUM_AGE_MS = 30 * 60 * 60 * 1_000
DEFAULT_MAXIMUM_FUTURE_SKEW_MS = 5 * 60 * 1_000
_LSN = re.compile(r"^[0-9A-F]+/[0-9A-F]+$")
_WAL_FILENAME = re.compile(r"^[0-9A-F]{24}$")
_RECEIPT_FIELDS = {
    "schema_version",
    "status",
    "run_id",
    "backup_id",
    "started_at_ms",
    "completed_at_ms",
    "duration_ms",
    "manifest_uri",
    "manifest_sha256",
    "audit_anchor_uri",
    "audit_anchor_sha256",
    "recovery_target_time",
    "recovery_target_lsn",
    "recovery_target_wal_filename",
    "wal_coverage_segment_count",
    "write_fence_id",
    "operator_id",
}
_RESULT_FIELDS = _RECEIPT_FIELDS | {
    "success_history_schema_version",
    "success_history_uri",
    "success_history_sha256",
    "success_history_created",
}


class RecoverySelectionError(RuntimeError):
    """A candidate set cannot prove a unique fresh recovery selection."""


@dataclass(frozen=True, slots=True)
class BackupJobReceipt:
    run_id: str
    backup_id: str
    started_at_ms: int
    completed_at_ms: int
    duration_ms: int
    manifest_uri: str
    manifest_sha256: str
    audit_anchor_uri: str
    audit_anchor_sha256: str
    recovery_target_time: str
    recovery_target_lsn: str
    recovery_target_wal_filename: str
    wal_coverage_segment_count: int
    write_fence_id: str
    operator_id: str
    schema_version: str = BACKUP_JOB_RECEIPT_SCHEMA_VERSION
    status: str = "succeeded"

    def __post_init__(self) -> None:
        if self.schema_version != BACKUP_JOB_RECEIPT_SCHEMA_VERSION:
            raise ValueError("backup job receipt schema_version has drifted")
        if self.status != "succeeded":
            raise ValueError("backup job receipt is not successful")
        object.__setattr__(self, "run_id", _uuid(self.run_id, field="run_id"))
        object.__setattr__(
            self,
            "backup_id",
            _uuid(self.backup_id, field="backup_id"),
        )
        if self.run_id != self.backup_id:
            raise ValueError("backup job run_id differs from backup_id")
        object.__setattr__(
            self,
            "write_fence_id",
            _uuid(self.write_fence_id, field="write_fence_id"),
        )
        for field in ("started_at_ms", "completed_at_ms"):
            object.__setattr__(self, field, _positive_int(getattr(self, field), field))
        object.__setattr__(
            self,
            "duration_ms",
            _non_negative_int(self.duration_ms, "duration_ms"),
        )
        if self.completed_at_ms < self.started_at_ms:
            raise ValueError("backup job completion precedes its start")
        object.__setattr__(
            self,
            "manifest_uri",
            _s3_uri(self.manifest_uri, field="manifest_uri"),
        )
        object.__setattr__(
            self,
            "audit_anchor_uri",
            _s3_uri(self.audit_anchor_uri, field="audit_anchor_uri"),
        )
        object.__setattr__(
            self,
            "manifest_sha256",
            _sha256(self.manifest_sha256, field="manifest_sha256"),
        )
        object.__setattr__(
            self,
            "audit_anchor_sha256",
            _sha256(self.audit_anchor_sha256, field="audit_anchor_sha256"),
        )
        object.__setattr__(
            self,
            "recovery_target_time",
            _utc_timestamp(self.recovery_target_time),
        )
        object.__setattr__(
            self,
            "recovery_target_lsn",
            _lsn(self.recovery_target_lsn),
        )
        if not isinstance(self.recovery_target_wal_filename, str) or not (
            _WAL_FILENAME.fullmatch(self.recovery_target_wal_filename)
        ):
            raise ValueError("recovery_target_wal_filename is invalid")
        object.__setattr__(
            self,
            "wal_coverage_segment_count",
            _positive_int(
                self.wal_coverage_segment_count,
                "wal_coverage_segment_count",
            ),
        )
        object.__setattr__(
            self,
            "operator_id",
            _bounded_text(self.operator_id, field="operator_id"),
        )

    @classmethod
    def from_canonical_bytes(cls, data: bytes) -> BackupJobReceipt:
        if not isinstance(data, bytes) or not data:
            raise RecoverySelectionError("backup job receipt is empty")
        try:
            text = data.decode("utf-8")
            wire = json.loads(text, object_pairs_hook=_strict_object)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise RecoverySelectionError(
                "backup job receipt is not strict JSON"
            ) from exc
        if not isinstance(wire, dict) or text != _canonical_json(wire) + "\n":
            raise RecoverySelectionError(
                "backup job receipt is not canonical or has an unexpected shape"
            )
        if set(wire) == _RESULT_FIELDS:
            try:
                if wire["schema_version"] != BACKUP_JOB_RESULT_SCHEMA_VERSION:
                    raise ValueError("result schema")
                if (
                    wire["success_history_schema_version"]
                    != "candlescope.query-backup-run-history.v1"
                ):
                    raise ValueError("history schema")
                _s3_uri(wire["success_history_uri"], field="success_history_uri")
                _sha256(
                    wire["success_history_sha256"],
                    field="success_history_sha256",
                )
                if not isinstance(wire["success_history_created"], bool):
                    raise TypeError("success_history_created")
            except (TypeError, ValueError) as exc:
                raise RecoverySelectionError(
                    "backup job result history reference is invalid"
                ) from exc
            wire = {
                key: value
                for key, value in wire.items()
                if key not in _RESULT_FIELDS - _RECEIPT_FIELDS
            }
            wire["schema_version"] = BACKUP_JOB_RECEIPT_SCHEMA_VERSION
        elif set(wire) != _RECEIPT_FIELDS:
            raise RecoverySelectionError(
                "backup job receipt is not canonical or has an unexpected shape"
            )
        return cls.from_wire(wire)

    @classmethod
    def from_wire(cls, wire: object) -> BackupJobReceipt:
        if not isinstance(wire, dict) or set(wire) != _RECEIPT_FIELDS:
            raise RecoverySelectionError("backup job receipt shape is invalid")
        try:
            return cls(**wire)
        except (TypeError, ValueError) as exc:
            raise RecoverySelectionError(
                "backup job receipt fields are invalid"
            ) from exc

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "run_id": self.run_id,
            "backup_id": self.backup_id,
            "started_at_ms": self.started_at_ms,
            "completed_at_ms": self.completed_at_ms,
            "duration_ms": self.duration_ms,
            "manifest_uri": self.manifest_uri,
            "manifest_sha256": self.manifest_sha256,
            "audit_anchor_uri": self.audit_anchor_uri,
            "audit_anchor_sha256": self.audit_anchor_sha256,
            "recovery_target_time": self.recovery_target_time,
            "recovery_target_lsn": self.recovery_target_lsn,
            "recovery_target_wal_filename": self.recovery_target_wal_filename,
            "wal_coverage_segment_count": self.wal_coverage_segment_count,
            "write_fence_id": self.write_fence_id,
            "operator_id": self.operator_id,
        }


@dataclass(frozen=True, slots=True)
class RecoverySelection:
    receipt: BackupJobReceipt
    published: PublishedPhysicalBackup
    expected_cluster_id: str
    expected_system_identifier: int
    expected_timeline: int
    candidate_count: int
    now_ms: int
    age_ms: int
    maximum_age_ms: int
    maximum_future_skew_ms: int
    selection_scope_sha256: str

    def to_wire(self) -> dict[str, object]:
        request = self.published.manifest.request
        return {
            "schema_version": RECOVERY_SELECTION_SCHEMA_VERSION,
            "status": "selected-and-metadata-authenticated",
            "latest_within_supplied_receipts": True,
            "global_latest_proven": False,
            "candidate_count": self.candidate_count,
            "selection_scope_sha256": self.selection_scope_sha256,
            "evaluated_at_ms": self.now_ms,
            "maximum_age_ms": self.maximum_age_ms,
            "maximum_future_skew_ms": self.maximum_future_skew_ms,
            "selected_age_ms": self.age_ms,
            "expected": {
                "cluster_id": self.expected_cluster_id,
                "system_identifier": str(self.expected_system_identifier),
                "timeline": self.expected_timeline,
            },
            "selected": {
                "backup_id": request.backup_id,
                "cluster_id": request.cluster_id,
                "system_identifier": str(request.system_identifier),
                "timeline": request.timeline,
                "manifest_uri": self.published.manifest_uri,
                "manifest_sha256": self.published.manifest_sha256,
                "audit_anchor_uri": request.audit_anchor_uri,
                "audit_anchor_sha256": request.audit_anchor_sha256,
                "recovery_target_time": request.recovery_target_time,
                "recovery_target_lsn": request.recovery_target_lsn,
                "recovery_target_wal_filename": request.wal_coverage[-1].filename,
                "wal_coverage_segment_count": len(request.wal_coverage),
                "write_fence_id": request.write_fence_id,
            },
        }


def select_recovery_candidate(
    candidates: tuple[tuple[BackupJobReceipt, PublishedPhysicalBackup], ...],
    *,
    expected_cluster_id: str,
    expected_system_identifier: int,
    expected_timeline: int,
    now_ms: int,
    maximum_age_ms: int = DEFAULT_MAXIMUM_AGE_MS,
    maximum_future_skew_ms: int = DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
    maximum_candidates: int = DEFAULT_MAXIMUM_CANDIDATES,
) -> RecoverySelection:
    expected_cluster_id = _bounded_text(
        expected_cluster_id,
        field="expected_cluster_id",
    )
    expected_system_identifier = _positive_int(
        expected_system_identifier,
        "expected_system_identifier",
    )
    expected_timeline = _positive_int(expected_timeline, "expected_timeline")
    now_ms = _positive_int(now_ms, "now_ms")
    maximum_age_ms = _positive_int(maximum_age_ms, "maximum_age_ms")
    maximum_future_skew_ms = _non_negative_int(
        maximum_future_skew_ms,
        "maximum_future_skew_ms",
    )
    maximum_candidates = _positive_int(maximum_candidates, "maximum_candidates")
    if not isinstance(candidates, tuple) or not candidates:
        raise RecoverySelectionError("at least one recovery candidate is required")
    if len(candidates) > maximum_candidates:
        raise RecoverySelectionError("recovery candidate count exceeds its bound")

    backup_ids: set[str] = set()
    manifest_uris: set[str] = set()
    reconciled: list[tuple[int, BackupJobReceipt, PublishedPhysicalBackup]] = []
    for receipt, published in candidates:
        if not isinstance(receipt, BackupJobReceipt) or not isinstance(
            published,
            PublishedPhysicalBackup,
        ):
            raise TypeError(
                "candidates must contain receipt and published backup pairs"
            )
        if receipt.backup_id in backup_ids or receipt.manifest_uri in manifest_uris:
            raise RecoverySelectionError("recovery candidate is duplicated")
        backup_ids.add(receipt.backup_id)
        manifest_uris.add(receipt.manifest_uri)
        request = published.manifest.request
        if request.cluster_id != expected_cluster_id:
            raise RecoverySelectionError(
                "recovery candidate belongs to another cluster"
            )
        if request.system_identifier != expected_system_identifier:
            raise RecoverySelectionError(
                "recovery candidate belongs to another PostgreSQL system"
            )
        if request.timeline != expected_timeline:
            raise RecoverySelectionError(
                "recovery candidate belongs to another PostgreSQL timeline"
            )
        _require_receipt_match(receipt, published)
        target_ms = _timestamp_ms(request.recovery_target_time)
        if target_ms > now_ms + maximum_future_skew_ms:
            raise RecoverySelectionError("recovery candidate target is in the future")
        reconciled.append((target_ms, receipt, published))

    reconciled.sort(key=lambda item: (item[0], item[1].backup_id))
    latest_target_ms = reconciled[-1][0]
    if sum(target_ms == latest_target_ms for target_ms, _, _ in reconciled) != 1:
        raise RecoverySelectionError("latest recovery target is ambiguous")
    _, receipt, published = reconciled[-1]
    age_ms = max(0, now_ms - latest_target_ms)
    if age_ms > maximum_age_ms:
        raise RecoverySelectionError("latest recovery candidate is stale")
    scope = [
        item[1].to_wire()
        for item in sorted(reconciled, key=lambda value: value[1].backup_id)
    ]
    return RecoverySelection(
        receipt=receipt,
        published=published,
        expected_cluster_id=expected_cluster_id,
        expected_system_identifier=expected_system_identifier,
        expected_timeline=expected_timeline,
        candidate_count=len(reconciled),
        now_ms=now_ms,
        age_ms=age_ms,
        maximum_age_ms=maximum_age_ms,
        maximum_future_skew_ms=maximum_future_skew_ms,
        selection_scope_sha256=hashlib.sha256(rfc8785.dumps(scope)).hexdigest(),
    )


def _require_receipt_match(
    receipt: BackupJobReceipt,
    published: PublishedPhysicalBackup,
) -> None:
    request = published.manifest.request
    observed = (
        receipt.backup_id,
        receipt.manifest_uri,
        receipt.manifest_sha256,
        receipt.audit_anchor_uri,
        receipt.audit_anchor_sha256,
        receipt.recovery_target_time,
        receipt.recovery_target_lsn,
        receipt.recovery_target_wal_filename,
        receipt.wal_coverage_segment_count,
        receipt.write_fence_id,
    )
    expected = (
        request.backup_id,
        published.manifest_uri,
        published.manifest_sha256,
        request.audit_anchor_uri,
        request.audit_anchor_sha256,
        request.recovery_target_time,
        request.recovery_target_lsn,
        request.wal_coverage[-1].filename,
        len(request.wal_coverage),
        request.write_fence_id,
    )
    if observed != expected:
        raise RecoverySelectionError(
            "backup job receipt differs from its signed physical manifest"
        )
    if not (receipt.started_at_ms <= request.created_at_ms <= receipt.completed_at_ms):
        raise RecoverySelectionError(
            "signed physical manifest was created outside its job interval"
        )
    if not (
        receipt.started_at_ms
        <= request.write_fence_acquired_at_ms
        <= receipt.completed_at_ms
    ):
        raise RecoverySelectionError(
            "write fence was acquired outside its job interval"
        )


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _uuid(value: object, *, field: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc
    rendered = str(parsed)
    if value != rendered:
        raise ValueError(f"{field} must be a canonical UUID")
    return rendered


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _bounded_text(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or value != value.strip()
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{field} must be bounded printable text")
    return value


def _s3_uri(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.path in {"", "/"}
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field} must be a credential-free S3 URI")
    return value


def _sha256(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


def _lsn(value: object) -> str:
    if not isinstance(value, str) or not _LSN.fullmatch(value):
        raise ValueError("recovery_target_lsn must be an upper-case PostgreSQL LSN")
    high, low = (int(component, 16) for component in value.split("/", 1))
    if high > 0xFFFFFFFF or low > 0xFFFFFFFF:
        raise ValueError("recovery_target_lsn exceeds PostgreSQL's 64-bit range")
    rendered = f"{high:X}/{low:X}"
    if rendered != value:
        raise ValueError("recovery_target_lsn is not canonical")
    return rendered


def _utc_timestamp(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("recovery_target_time must be a string")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f+00").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError(
            "recovery_target_time must be canonical UTC with microseconds"
        ) from exc
    rendered = parsed.strftime("%Y-%m-%d %H:%M:%S.%f+00")
    if rendered != value:
        raise ValueError("recovery_target_time is not canonical")
    return rendered


def _timestamp_ms(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f+00").replace(
        tzinfo=timezone.utc
    )
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000
