"""Verified server principal. Does not retain the original token."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)

SERVER_PRINCIPAL_SCHEMA = "candlescope.server-principal.v1"


class IdentityError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True, slots=True)
class ServerPrincipal:
    subject: str
    organization_id: str
    workspace_id: str
    principal_type: str
    role: str
    credential_id: str
    team_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "organization_id", normalize_organization_id(self.organization_id)
        )
        object.__setattr__(
            self, "workspace_id", normalize_workspace_id(self.workspace_id)
        )
        if self.role not in {"admin", "researcher", "trader", "read-only"}:
            raise IdentityError("INVALID_ROLE", "unsupported principal role")
        if self.principal_type not in {"user", "service-account"}:
            raise IdentityError("INVALID_PRINCIPAL_TYPE", "unsupported principal type")

    def to_public_ref(self) -> dict[str, object]:
        return {
            "schema_version": SERVER_PRINCIPAL_SCHEMA,
            "subject": self.subject,
            "organization_id": self.organization_id,
            "workspace_id": self.workspace_id,
            "principal_type": self.principal_type,
            "role": self.role,
            "credential_id": self.credential_id,
        }


class IdentityVerifier(Protocol):
    async def verify(self, token: str) -> ServerPrincipal: ...


class StaticTokenIdentityVerifier:
    """Test/dev verifier. Production must use JWKS with issuer/audience checks."""

    def __init__(self, mapping: dict[str, ServerPrincipal]) -> None:
        self._mapping = dict(mapping)

    async def verify(self, token: str) -> ServerPrincipal:
        if not isinstance(token, str) or not token.strip():
            raise IdentityError("UNAUTHENTICATED", "missing bearer token")
        principal = self._mapping.get(token.strip())
        if principal is None:
            raise IdentityError("UNAUTHENTICATED", "bearer token was rejected")
        return principal
