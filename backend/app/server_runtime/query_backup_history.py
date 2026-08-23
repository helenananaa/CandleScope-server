"""Signed immutable success history and bounded backup-cadence verification."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from itertools import pairwise
from typing import Any

import rfc8785

from app.server_runtime.object_store import ImmutableObjectStore
from app.server_runtime.query_backup_selection import BackupJobReceipt

BACKUP_RUN_HISTORY_SCHEMA_VERSION = "candlescope.query-backup-run-history.v1"
BACKUP_RUN_HISTORY_ALGORITHM = "hmac-sha256"
BACKUP_SUCCESS_CADENCE_SCHEMA_VERSION = "candlescope.query-backup-success-cadence.v1"
DEFAULT_MAXIMUM_HISTORIES = 64
DEFAULT_MAXIMUM_GAP_MS = 30 * 60 * 60 * 1_000
DEFAULT_MAXIMUM_FUTURE_SKEW_MS = 5 * 60 * 1_000
_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class BackupRunHistoryError(RuntimeError):
    """A signed run history or cadence proof is invalid or unavailable."""


@dataclass(frozen=True, slots=True)
class SignedBackupRunHistory:
    key_id: str
    cluster_id: str
    receipt: BackupJobReceipt
    signature: str
    schema_version: str = BACKUP_RUN_HISTORY_SCHEMA_VERSION
    algorithm: str = BACKUP_RUN_HISTORY_ALGORITHM

    def __post_init__(self) -> None:
        if self.schema_version != BACKUP_RUN_HISTORY_SCHEMA_VERSION:
            raise ValueError("backup run history schema_version has drifted")
        if self.algorithm != BACKUP_RUN_HISTORY_ALGORITHM:
            raise ValueError("backup run history algorithm has drifted")
        object.__setattr__(self, "key_id", _token(self.key_id, field="key_id"))
        object.__setattr__(
            self,
            "cluster_id",
            _token(self.cluster_id, field="cluster_id"),
        )
        if not isinstance(self.receipt, BackupJobReceipt):
            raise TypeError("receipt must be a BackupJobReceipt")
        object.__setattr__(
            self,
            "signature",
            _sha256(self.signature, field="signature"),
        )

    def unsigned_wire(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "algorithm": self.algorithm,
            "key_id": self.key_id,
            "cluster_id": self.cluster_id,
            "receipt": self.receipt.to_wire(),
        }

    def to_wire(self) -> dict[str, object]:
        return {**self.unsigned_wire(), "signature": self.signature}

    def canonical_bytes(self) -> bytes:
        return rfc8785.dumps(self.to_wire())


@dataclass(frozen=True, slots=True)
class PublishedBackupRunHistory:
    uri: str
    content_sha256: str
    history: SignedBackupRunHistory
    created: bool


class BackupRunHistorySigner:
    def __init__(self, *, key_id: str, secret: bytes) -> None:
        self._key_id = _token(key_id, field="key_id")
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError(
                "backup run history HMAC secret must contain at least 32 bytes"
            )
        self._secret = secret

    def sign(
        self,
        receipt: BackupJobReceipt,
        *,
        cluster_id: str,
    ) -> SignedBackupRunHistory:
        placeholder = SignedBackupRunHistory(
            key_id=self._key_id,
            cluster_id=cluster_id,
            receipt=receipt,
            signature="0" * 64,
        )
        signature = hmac.digest(
            self._secret,
            rfc8785.dumps(placeholder.unsigned_wire()),
            "sha256",
        ).hex()
        return SignedBackupRunHistory(
            key_id=placeholder.key_id,
            cluster_id=placeholder.cluster_id,
            receipt=placeholder.receipt,
            signature=signature,
        )

    def verify(self, history: SignedBackupRunHistory) -> None:
        if not isinstance(history, SignedBackupRunHistory):
            raise TypeError("history must be a SignedBackupRunHistory")
        if history.key_id != self._key_id:
            raise BackupRunHistoryError("backup run history key_id is not configured")
        expected = hmac.digest(
            self._secret,
            rfc8785.dumps(history.unsigned_wire()),
            "sha256",
        ).hex()
        if not hmac.compare_digest(expected, history.signature):
            raise BackupRunHistoryError("backup run history HMAC verification failed")


class ImmutableBackupRunHistoryRepository:
    def __init__(
        self,
        *,
        object_store: ImmutableObjectStore,
        signer: BackupRunHistorySigner,
    ) -> None:
        if not isinstance(object_store, ImmutableObjectStore):
            raise TypeError("object_store must implement ImmutableObjectStore")
        if not isinstance(signer, BackupRunHistorySigner):
            raise TypeError("signer must be a BackupRunHistorySigner")
        self._object_store = object_store
        self._signer = signer

    async def publish(
        self,
        receipt: BackupJobReceipt,
        *,
        cluster_id: str,
    ) -> PublishedBackupRunHistory:
        history = self._signer.sign(receipt, cluster_id=cluster_id)
        data = history.canonical_bytes()
        key = _history_key(history)
        await self._object_store.check_bucket()
        created = await self._object_store.put_if_absent(
            key,
            data,
            content_type="application/json",
            metadata={
                "schema-version": history.schema_version,
                "cluster-id": history.cluster_id,
                "backup-id": history.receipt.backup_id,
                "key-id": history.key_id,
            },
        )
        if not created:
            existing = await self._object_store.get(key)
            if existing.data != data:
                raise BackupRunHistoryError(
                    "immutable backup run history contains different bytes"
                )
        return PublishedBackupRunHistory(
            uri=self._object_store.uri_for(key),
            content_sha256=hashlib.sha256(data).hexdigest(),
            history=history,
            created=created,
        )

    async def verify(self, uri: str) -> PublishedBackupRunHistory:
        stored = await self._object_store.get_uri(uri)
        history = _history_from_bytes(stored.data)
        self._signer.verify(history)
        if uri != self._object_store.uri_for(_history_key(history)):
            raise BackupRunHistoryError("backup run history URI has drifted")
        return PublishedBackupRunHistory(
            uri=uri,
            content_sha256=hashlib.sha256(stored.data).hexdigest(),
            history=history,
            created=False,
        )


@dataclass(frozen=True, slots=True)
class BackupSuccessCadence:
    expected_cluster_id: str
    window_start_ms: int
    window_end_ms: int
    evaluated_at_ms: int
    maximum_gap_ms: int
    maximum_future_skew_ms: int
    maximum_observed_gap_ms: int
    histories: tuple[PublishedBackupRunHistory, ...]
    scope_sha256: str

    def to_wire(self) -> dict[str, object]:
        first = self.histories[0].history.receipt
        last = self.histories[-1].history.receipt
        return {
            "schema_version": BACKUP_SUCCESS_CADENCE_SCHEMA_VERSION,
            "status": "success-cadence-within-explicit-window",
            "global_history_completeness_proven": False,
            "scheduled_slot_execution_proven": False,
            "cluster_id": self.expected_cluster_id,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "evaluated_at_ms": self.evaluated_at_ms,
            "maximum_gap_ms": self.maximum_gap_ms,
            "maximum_future_skew_ms": self.maximum_future_skew_ms,
            "maximum_observed_gap_ms": self.maximum_observed_gap_ms,
            "successful_run_count": len(self.histories),
            "first_success_completed_at_ms": first.completed_at_ms,
            "last_success_completed_at_ms": last.completed_at_ms,
            "scope_sha256": self.scope_sha256,
            "runs": [
                {
                    "backup_id": item.history.receipt.backup_id,
                    "completed_at_ms": item.history.receipt.completed_at_ms,
                    "history_uri": item.uri,
                    "history_sha256": item.content_sha256,
                }
                for item in self.histories
            ],
        }


def verify_success_cadence(
    histories: tuple[PublishedBackupRunHistory, ...],
    *,
    expected_cluster_id: str,
    window_start_ms: int,
    window_end_ms: int,
    evaluated_at_ms: int,
    maximum_gap_ms: int = DEFAULT_MAXIMUM_GAP_MS,
    maximum_future_skew_ms: int = DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
    maximum_histories: int = DEFAULT_MAXIMUM_HISTORIES,
) -> BackupSuccessCadence:
    expected_cluster_id = _token(expected_cluster_id, field="expected_cluster_id")
    window_start_ms = _positive_int(window_start_ms, field="window_start_ms")
    window_end_ms = _positive_int(window_end_ms, field="window_end_ms")
    evaluated_at_ms = _positive_int(evaluated_at_ms, field="evaluated_at_ms")
    maximum_gap_ms = _positive_int(maximum_gap_ms, field="maximum_gap_ms")
    maximum_future_skew_ms = _non_negative_int(
        maximum_future_skew_ms,
        field="maximum_future_skew_ms",
    )
    maximum_histories = _positive_int(
        maximum_histories,
        field="maximum_histories",
    )
    if window_end_ms <= window_start_ms:
        raise BackupRunHistoryError("cadence window must have positive duration")
    if window_end_ms > evaluated_at_ms + maximum_future_skew_ms:
        raise BackupRunHistoryError("cadence window ends in the future")
    if not isinstance(histories, tuple) or not histories:
        raise BackupRunHistoryError("at least one signed success history is required")
    if len(histories) > maximum_histories:
        raise BackupRunHistoryError("backup run history count exceeds its bound")

    backup_ids: set[str] = set()
    uris: set[str] = set()
    ordered: list[PublishedBackupRunHistory] = []
    for published in histories:
        if not isinstance(published, PublishedBackupRunHistory):
            raise TypeError("histories must contain PublishedBackupRunHistory")
        history = published.history
        receipt = history.receipt
        if history.cluster_id != expected_cluster_id:
            raise BackupRunHistoryError("backup run history belongs to another cluster")
        if receipt.backup_id in backup_ids or published.uri in uris:
            raise BackupRunHistoryError("backup run history is duplicated")
        if not window_start_ms <= receipt.completed_at_ms <= window_end_ms:
            raise BackupRunHistoryError(
                "backup success lies outside the cadence window"
            )
        backup_ids.add(receipt.backup_id)
        uris.add(published.uri)
        ordered.append(published)
    ordered.sort(
        key=lambda item: (
            item.history.receipt.completed_at_ms,
            item.history.receipt.backup_id,
        )
    )
    points = [
        window_start_ms,
        *(item.history.receipt.completed_at_ms for item in ordered),
        window_end_ms,
    ]
    gaps = tuple(right - left for left, right in pairwise(points))
    maximum_observed_gap_ms = max(gaps)
    if maximum_observed_gap_ms > maximum_gap_ms:
        raise BackupRunHistoryError("backup success cadence exceeds its maximum gap")
    scope = [
        {
            "uri": item.uri,
            "content_sha256": item.content_sha256,
            "backup_id": item.history.receipt.backup_id,
            "completed_at_ms": item.history.receipt.completed_at_ms,
        }
        for item in ordered
    ]
    return BackupSuccessCadence(
        expected_cluster_id=expected_cluster_id,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        evaluated_at_ms=evaluated_at_ms,
        maximum_gap_ms=maximum_gap_ms,
        maximum_future_skew_ms=maximum_future_skew_ms,
        maximum_observed_gap_ms=maximum_observed_gap_ms,
        histories=tuple(ordered),
        scope_sha256=hashlib.sha256(rfc8785.dumps(scope)).hexdigest(),
    )


def _history_key(history: SignedBackupRunHistory) -> str:
    receipt = history.receipt
    return (
        f"backup-runs/v1/{history.cluster_id}/"
        f"{receipt.completed_at_ms:020d}-{receipt.backup_id}.json"
    )


def _history_from_bytes(data: bytes) -> SignedBackupRunHistory:
    if not isinstance(data, bytes) or not data:
        raise BackupRunHistoryError("backup run history is empty")
    try:
        wire = json.loads(data, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BackupRunHistoryError("backup run history is not strict JSON") from exc
    if not isinstance(wire, dict) or set(wire) != {
        "schema_version",
        "algorithm",
        "key_id",
        "cluster_id",
        "receipt",
        "signature",
    }:
        raise BackupRunHistoryError("backup run history shape is invalid")
    try:
        history = SignedBackupRunHistory(
            schema_version=wire["schema_version"],
            algorithm=wire["algorithm"],
            key_id=wire["key_id"],
            cluster_id=wire["cluster_id"],
            receipt=BackupJobReceipt.from_wire(wire["receipt"]),
            signature=wire["signature"],
        )
    except (KeyError, RuntimeError, TypeError, ValueError) as exc:
        raise BackupRunHistoryError("backup run history fields are invalid") from exc
    if history.canonical_bytes() != data:
        raise BackupRunHistoryError("backup run history is not RFC 8785 canonical JSON")
    return history


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _token(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValueError(f"{field} must be a bounded ASCII token")
    return value


def _sha256(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value
