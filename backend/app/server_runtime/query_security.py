"""Internal authentication and redacted query-audit contracts for Phase 1G."""

from __future__ import annotations

import json
import logging
import secrets
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

QUERY_AUDIT_SCHEMA_VERSION = "candlescope.snapshot-query-audit.v1"


class QueryAuthenticationError(RuntimeError):
    """A caller did not prove possession of the internal query credential."""


class BearerTokenAuthenticator:
    """Authenticate one internal principal with constant-time token comparison."""

    def __init__(self, *, token: str, principal: str) -> None:
        token = _required_text(token, field="token")
        if len(token) < 32:
            raise ValueError("token must contain at least 32 characters")
        self._token = token
        self._principal = _required_text(principal, field="principal")

    @property
    def principal(self) -> str:
        return self._principal

    def authenticate(self, credential: object) -> str:
        if not isinstance(credential, str) or not secrets.compare_digest(
            credential,
            self._token,
        ):
            raise QueryAuthenticationError("invalid bearer credential")
        return self._principal


@dataclass(frozen=True, slots=True)
class QueryAuditEvent:
    request_id: str
    action: str
    outcome: str
    status_code: int
    timestamp_ms: int
    latency_ms: int
    principal: str | None = None
    snapshot_version: int | None = None
    manifest_sha256: str | None = None
    partition_key: str | None = None
    preference: str | None = None
    backend: str | None = None
    schema_version: str = QUERY_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != QUERY_AUDIT_SCHEMA_VERSION:
            raise ValueError("query audit schema_version has drifted")
        for field in ("request_id", "action", "outcome"):
            object.__setattr__(
                self,
                field,
                _required_text(getattr(self, field), field=field),
            )
        for field in ("status_code", "timestamp_ms", "latency_ms"):
            value = _non_negative_int(getattr(self, field), field=field)
            object.__setattr__(self, field, value)
        if not 100 <= self.status_code <= 599:
            raise ValueError("status_code must be a valid HTTP status")
        for field in ("principal", "partition_key", "preference", "backend"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field, _required_text(value, field=field))
        if self.snapshot_version is not None:
            snapshot_version = _non_negative_int(
                self.snapshot_version,
                field="snapshot_version",
            )
            if snapshot_version == 0:
                raise ValueError("snapshot_version must be positive")
            object.__setattr__(self, "snapshot_version", snapshot_version)
        if self.manifest_sha256 is not None:
            object.__setattr__(
                self,
                "manifest_sha256",
                _sha256(self.manifest_sha256, field="manifest_sha256"),
            )

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "principal": self.principal,
            "action": self.action,
            "outcome": self.outcome,
            "status_code": self.status_code,
            "timestamp_ms": self.timestamp_ms,
            "latency_ms": self.latency_ms,
            "snapshot_version": self.snapshot_version,
            "manifest_sha256": self.manifest_sha256,
            "partition_key": self.partition_key,
            "preference": self.preference,
            "backend": self.backend,
        }


@runtime_checkable
class QueryAuditSink(Protocol):
    async def emit(self, event: QueryAuditEvent) -> None: ...


class StructuredLogQueryAuditSink:
    """Emit one canonical-shape JSON object for external durable collection."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("candlescope.query.audit")

    async def emit(self, event: QueryAuditEvent) -> None:
        if not isinstance(event, QueryAuditEvent):
            raise TypeError("event must be a QueryAuditEvent")
        self._logger.warning(
            "snapshot_query_audit=%s",
            json.dumps(
                event.to_wire(),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _sha256(value: object, *, field: str) -> str:
    value = _required_text(value, field=field).lower()
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value
