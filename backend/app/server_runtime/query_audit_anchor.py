"""Immutable HMAC anchors for PostgreSQL query-audit chain heads."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass

import rfc8785

from app.server_runtime.object_store import ImmutableObjectStore
from app.server_runtime.storage.postgres_query_control import (
    QueryAuditChainVerification,
)

QUERY_AUDIT_ANCHOR_SCHEMA_VERSION = "candlescope.query-audit-anchor.v1"
QUERY_AUDIT_ANCHOR_ALGORITHM = "hmac-sha256"


class QueryAuditAnchorError(RuntimeError):
    """An external query-audit anchor is missing, invalid, or inconsistent."""


@dataclass(frozen=True, slots=True)
class QueryAuditAnchor:
    key_id: str
    record_count: int
    head_audit_sequence: int
    head_event_hash: str
    head_updated_at_ms: int
    migration_version: int
    migration_sha256: str
    signature: str
    schema_version: str = QUERY_AUDIT_ANCHOR_SCHEMA_VERSION
    algorithm: str = QUERY_AUDIT_ANCHOR_ALGORITHM

    def __post_init__(self) -> None:
        if self.schema_version != QUERY_AUDIT_ANCHOR_SCHEMA_VERSION:
            raise ValueError("query audit anchor schema_version has drifted")
        if self.algorithm != QUERY_AUDIT_ANCHOR_ALGORITHM:
            raise ValueError("query audit anchor algorithm has drifted")
        object.__setattr__(self, "key_id", _safe_token(self.key_id, field="key_id"))
        for field in (
            "record_count",
            "head_audit_sequence",
            "head_updated_at_ms",
            "migration_version",
        ):
            object.__setattr__(
                self,
                field,
                _non_negative_int(getattr(self, field), field=field),
            )
        if self.migration_version == 0:
            raise ValueError("migration_version must be positive")
        object.__setattr__(
            self,
            "head_event_hash",
            _sha256(self.head_event_hash, field="head_event_hash"),
        )
        object.__setattr__(
            self,
            "migration_sha256",
            _sha256(self.migration_sha256, field="migration_sha256"),
        )
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
            "payload": {
                "record_count": str(self.record_count),
                "head_audit_sequence": str(self.head_audit_sequence),
                "head_event_hash": self.head_event_hash,
                "head_updated_at_ms": str(self.head_updated_at_ms),
                "migration_version": str(self.migration_version),
                "migration_sha256": self.migration_sha256,
            },
        }

    def to_wire(self) -> dict[str, object]:
        return {**self.unsigned_wire(), "signature": self.signature}

    def canonical_bytes(self) -> bytes:
        return rfc8785.dumps(self.to_wire())


@dataclass(frozen=True, slots=True)
class PublishedQueryAuditAnchor:
    uri: str
    content_sha256: str
    anchor: QueryAuditAnchor


class QueryAuditAnchorSigner:
    def __init__(self, *, key_id: str, secret: bytes) -> None:
        self._key_id = _safe_token(key_id, field="key_id")
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("anchor HMAC secret must contain at least 32 bytes")
        self._secret = secret

    @property
    def key_id(self) -> str:
        return self._key_id

    def sign(self, verification: QueryAuditChainVerification) -> QueryAuditAnchor:
        if not isinstance(verification, QueryAuditChainVerification):
            raise TypeError("verification must be a QueryAuditChainVerification")
        placeholder = QueryAuditAnchor(
            key_id=self._key_id,
            record_count=verification.record_count,
            head_audit_sequence=verification.head_audit_sequence,
            head_event_hash=verification.head_event_hash,
            head_updated_at_ms=verification.head_updated_at_ms,
            migration_version=verification.migration_version,
            migration_sha256=verification.migration_sha256,
            signature="0" * 64,
        )
        signature = hmac.digest(
            self._secret,
            rfc8785.dumps(placeholder.unsigned_wire()),
            "sha256",
        ).hex()
        return QueryAuditAnchor(
            key_id=placeholder.key_id,
            record_count=placeholder.record_count,
            head_audit_sequence=placeholder.head_audit_sequence,
            head_event_hash=placeholder.head_event_hash,
            head_updated_at_ms=placeholder.head_updated_at_ms,
            migration_version=placeholder.migration_version,
            migration_sha256=placeholder.migration_sha256,
            signature=signature,
        )

    def verify(self, anchor: QueryAuditAnchor) -> None:
        if not isinstance(anchor, QueryAuditAnchor):
            raise TypeError("anchor must be a QueryAuditAnchor")
        if anchor.key_id != self._key_id:
            raise QueryAuditAnchorError("anchor key_id is not configured")
        expected = hmac.digest(
            self._secret,
            rfc8785.dumps(anchor.unsigned_wire()),
            "sha256",
        ).hex()
        if not hmac.compare_digest(expected, anchor.signature):
            raise QueryAuditAnchorError("anchor HMAC verification failed")


class ImmutableQueryAuditAnchorRepository:
    def __init__(
        self,
        *,
        object_store: ImmutableObjectStore,
        signer: QueryAuditAnchorSigner,
    ) -> None:
        if not isinstance(object_store, ImmutableObjectStore):
            raise TypeError("object_store must implement ImmutableObjectStore")
        if not isinstance(signer, QueryAuditAnchorSigner):
            raise TypeError("signer must be a QueryAuditAnchorSigner")
        self._object_store = object_store
        self._signer = signer

    async def publish(
        self,
        verification: QueryAuditChainVerification,
    ) -> PublishedQueryAuditAnchor:
        anchor = self._signer.sign(verification)
        data = anchor.canonical_bytes()
        key = _anchor_key(anchor)
        await self._object_store.check_bucket()
        created = await self._object_store.put_if_absent(
            key,
            data,
            content_type="application/json",
            metadata={
                "schema-version": anchor.schema_version,
                "head-event-hash": anchor.head_event_hash,
                "key-id": anchor.key_id,
            },
        )
        if not created:
            existing = await self._object_store.get(key)
            if existing.data != data:
                raise QueryAuditAnchorError(
                    "immutable anchor key contains different bytes"
                )
        return PublishedQueryAuditAnchor(
            uri=self._object_store.uri_for(key),
            content_sha256=hashlib.sha256(data).hexdigest(),
            anchor=anchor,
        )

    async def verify(
        self,
        uri: str,
        *,
        database: QueryAuditChainVerification | None = None,
    ) -> PublishedQueryAuditAnchor:
        stored = await self._object_store.get_uri(uri)
        anchor = _anchor_from_bytes(stored.data)
        self._signer.verify(anchor)
        if database is not None:
            _require_database_match(anchor, database)
        return PublishedQueryAuditAnchor(
            uri=uri,
            content_sha256=hashlib.sha256(stored.data).hexdigest(),
            anchor=anchor,
        )


def _anchor_key(anchor: QueryAuditAnchor) -> str:
    return (
        f"anchors/v1/{anchor.key_id}/"
        f"{anchor.head_audit_sequence:020d}-{anchor.head_event_hash}.json"
    )


def _anchor_from_bytes(data: bytes) -> QueryAuditAnchor:
    if not isinstance(data, bytes) or not data:
        raise QueryAuditAnchorError("anchor object is empty")
    try:
        wire = json.loads(data, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise QueryAuditAnchorError("anchor object is not strict JSON") from exc
    if not isinstance(wire, dict) or set(wire) != {
        "schema_version",
        "algorithm",
        "key_id",
        "payload",
        "signature",
    }:
        raise QueryAuditAnchorError("anchor top-level shape is invalid")
    payload = wire["payload"]
    if not isinstance(payload, dict) or set(payload) != {
        "record_count",
        "head_audit_sequence",
        "head_event_hash",
        "head_updated_at_ms",
        "migration_version",
        "migration_sha256",
    }:
        raise QueryAuditAnchorError("anchor payload shape is invalid")
    try:
        anchor = QueryAuditAnchor(
            schema_version=wire["schema_version"],
            algorithm=wire["algorithm"],
            key_id=wire["key_id"],
            record_count=_decimal(payload["record_count"], field="record_count"),
            head_audit_sequence=_decimal(
                payload["head_audit_sequence"],
                field="head_audit_sequence",
            ),
            head_event_hash=payload["head_event_hash"],
            head_updated_at_ms=_decimal(
                payload["head_updated_at_ms"],
                field="head_updated_at_ms",
            ),
            migration_version=_decimal(
                payload["migration_version"],
                field="migration_version",
            ),
            migration_sha256=payload["migration_sha256"],
            signature=wire["signature"],
        )
    except (TypeError, ValueError) as exc:
        raise QueryAuditAnchorError("anchor fields are invalid") from exc
    if anchor.canonical_bytes() != data:
        raise QueryAuditAnchorError("anchor object is not RFC 8785 canonical JSON")
    return anchor


def _require_database_match(
    anchor: QueryAuditAnchor,
    database: QueryAuditChainVerification,
) -> None:
    if not isinstance(database, QueryAuditChainVerification):
        raise TypeError("database must be a QueryAuditChainVerification")
    if (
        anchor.record_count,
        anchor.head_audit_sequence,
        anchor.head_event_hash,
        anchor.head_updated_at_ms,
        anchor.migration_version,
        anchor.migration_sha256,
    ) != (
        database.record_count,
        database.head_audit_sequence,
        database.head_event_hash,
        database.head_updated_at_ms,
        database.migration_version,
        database.migration_sha256,
    ):
        raise QueryAuditAnchorError("database audit head differs from external anchor")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _decimal(value: object, *, field: str) -> int:
    if (
        not isinstance(value, str)
        or not value
        or not value.isascii()
        or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        raise ValueError(f"{field} must be a canonical decimal string")
    return int(value)


def _safe_token(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise ValueError(f"{field} must contain 1-128 characters")
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789._:-"
    if value[0] not in "abcdefghijklmnopqrstuvwxyz0123456789" or any(
        character not in allowed for character in value
    ):
        raise ValueError(f"{field} must use lower-case safe ASCII")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    value = value.lower()
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value
