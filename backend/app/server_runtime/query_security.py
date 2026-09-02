"""Internal authentication and redacted query-control audit contracts."""

from __future__ import annotations

import json
import logging
import secrets
import uuid
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Protocol, runtime_checkable

from app.server_runtime.query_identity import QueryCallerIdentity

QUERY_AUDIT_SCHEMA_VERSION = "candlescope.query-control-audit.v2"


class QueryAuthenticationError(RuntimeError):
    """A caller did not prove possession of the internal query credential."""


class BearerTokenAuthenticator:
    """Authenticate one internal principal with constant-time token comparison."""

    def __init__(
        self,
        *,
        token: str,
        principal: str,
        organization_id: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        token = _required_text(token, field="token")
        if len(token) < 32:
            raise ValueError("token must contain at least 32 characters")
        self._token = token
        self._principal = _required_text(principal, field="principal")
        if organization_id is None and workspace_id is None:
            self._identity: QueryCallerIdentity | None = None
        elif organization_id is None or workspace_id is None:
            raise ValueError("organization_id and workspace_id must be bound together")
        else:
            self._identity = QueryCallerIdentity(
                principal=self._principal,
                organization_id=organization_id,
                workspace_id=workspace_id,
            )

    @property
    def principal(self) -> str:
        return self._principal

    @property
    def identity(self) -> QueryCallerIdentity | None:
        return self._identity

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
    control_generation: int | None = None
    reason_code: str | None = None
    organization_id: str | None = None
    workspace_id: str | None = None
    event_id: str = dataclass_field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = QUERY_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != QUERY_AUDIT_SCHEMA_VERSION:
            raise ValueError("query audit schema_version has drifted")
        try:
            event_id = str(uuid.UUID(self.event_id))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("event_id must be a UUID") from exc
        object.__setattr__(self, "event_id", event_id)
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
        for field in (
            "principal",
            "partition_key",
            "preference",
            "backend",
            "organization_id",
            "workspace_id",
        ):
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
        if self.control_generation is not None:
            generation = _non_negative_int(
                self.control_generation,
                field="control_generation",
            )
            if generation == 0:
                raise ValueError("control_generation must be positive")
            object.__setattr__(self, "control_generation", generation)
        if self.reason_code is not None:
            reason_code = _required_text(self.reason_code, field="reason_code")
            if (
                len(reason_code) > 128
                or not reason_code.isascii()
                or reason_code[0] not in "abcdefghijklmnopqrstuvwxyz0123456789"
                or not all(
                    character in "abcdefghijklmnopqrstuvwxyz0123456789._:-"
                    for character in reason_code
                )
            ):
                raise ValueError("reason_code must use lower-case safe ASCII")
            object.__setattr__(self, "reason_code", reason_code)

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "request_id": self.request_id,
            "principal": self.principal,
            "action": self.action,
            "outcome": self.outcome,
            "status_code": self.status_code,
            "timestamp_ms": self.timestamp_ms,
            "latency_ms": self.latency_ms,
            "snapshot_version": (
                None if self.snapshot_version is None else str(self.snapshot_version)
            ),
            "manifest_sha256": self.manifest_sha256,
            "partition_key": self.partition_key,
            "preference": self.preference,
            "backend": self.backend,
            "control_generation": (
                None
                if self.control_generation is None
                else str(self.control_generation)
            ),
            "reason_code": self.reason_code,
            "organization_id": self.organization_id,
            "workspace_id": self.workspace_id,
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
