"""Server-bound query caller identity with organization and workspace scope."""

from __future__ import annotations

from dataclasses import dataclass

QUERY_CALLER_IDENTITY_SCHEMA_VERSION = "candlescope.query-caller-identity.v2"
_RESERVED_SCOPE_IDS = frozenset(
    {
        "*",
        "all",
        "any",
        "default",
        "global",
        "public",
        "shared",
        "wildcard",
    }
)
_SAFE_ID = frozenset("abcdefghijklmnopqrstuvwxyz0123456789._:-")


class QueryCallerScopeError(RuntimeError):
    """A query did not match the scope bound to the caller credential."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class QueryOrganizationScopeError(QueryCallerScopeError):
    """Organization scope mismatch or omission."""


class QueryWorkspaceScopeError(QueryCallerScopeError):
    """Workspace scope mismatch or omission."""


@dataclass(frozen=True, slots=True)
class QueryCallerIdentity:
    principal: str
    organization_id: str
    workspace_id: str
    schema_version: str = QUERY_CALLER_IDENTITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != QUERY_CALLER_IDENTITY_SCHEMA_VERSION:
            raise ValueError("query caller identity schema_version has drifted")
        object.__setattr__(
            self,
            "principal",
            _safe_identifier(self.principal, field="principal"),
        )
        object.__setattr__(
            self,
            "organization_id",
            normalize_organization_id(self.organization_id),
        )
        object.__setattr__(
            self,
            "workspace_id",
            normalize_workspace_id(self.workspace_id),
        )

    def to_public_ref(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "principal": self.principal,
            "organization_id": self.organization_id,
            "workspace_id": self.workspace_id,
        }


def normalize_organization_id(value: object) -> str:
    return _normalize_scope_id(value, field="organization_id")


def normalize_workspace_id(value: object) -> str:
    return _normalize_scope_id(value, field="workspace_id")


def require_organization_scope(
    identity: QueryCallerIdentity,
    requested_organization_id: object,
) -> str:
    """Require the request org to equal the token-bound org. No client override."""

    return _require_scope(
        bound=identity.organization_id,
        requested=requested_organization_id,
        field="organization_id",
        error_type=QueryOrganizationScopeError,
        required_code="ORGANIZATION_SCOPE_REQUIRED",
        invalid_code="ORGANIZATION_SCOPE_INVALID",
        denied_code="ORGANIZATION_SCOPE_DENIED",
    )


def require_workspace_scope(
    identity: QueryCallerIdentity,
    requested_workspace_id: object,
) -> str:
    """Require the request workspace to equal the token-bound workspace."""

    return _require_scope(
        bound=identity.workspace_id,
        requested=requested_workspace_id,
        field="workspace_id",
        error_type=QueryWorkspaceScopeError,
        required_code="WORKSPACE_SCOPE_REQUIRED",
        invalid_code="WORKSPACE_SCOPE_INVALID",
        denied_code="WORKSPACE_SCOPE_DENIED",
    )


def _normalize_scope_id(value: object, *, field: str) -> str:
    identifier = _safe_identifier(value, field=field)
    if identifier in _RESERVED_SCOPE_IDS:
        raise ValueError(f"{field} cannot be a wildcard or reserved name")
    return identifier


def _require_scope(
    *,
    bound: str,
    requested: object,
    field: str,
    error_type: type[QueryCallerScopeError],
    required_code: str,
    invalid_code: str,
    denied_code: str,
) -> str:
    if requested is None:
        raise error_type(required_code, f"{field} is required for this credential")
    try:
        normalized = _normalize_scope_id(requested, field=field)
    except (TypeError, ValueError) as exc:
        raise error_type(
            invalid_code,
            f"{field} is not a valid identifier",
        ) from exc
    if normalized != bound:
        raise error_type(
            denied_code,
            f"{field} does not match the credential scope",
        )
    return normalized


def _safe_identifier(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    normalized = value.strip().lower()
    if (
        len(normalized) > 64
        or not normalized.isascii()
        or normalized[0] not in "abcdefghijklmnopqrstuvwxyz0123456789"
        or any(character not in _SAFE_ID for character in normalized)
    ):
        raise ValueError(f"{field} must use a bounded lower-case identifier")
    return normalized


__all__ = [
    "QUERY_CALLER_IDENTITY_SCHEMA_VERSION",
    "QueryCallerIdentity",
    "QueryCallerScopeError",
    "QueryOrganizationScopeError",
    "QueryWorkspaceScopeError",
    "normalize_organization_id",
    "normalize_workspace_id",
    "require_organization_scope",
    "require_workspace_scope",
]
