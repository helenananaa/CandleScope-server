"""Role matrix for the first-slice server replay API."""

from __future__ import annotations

from app.server_runtime.access_identity import IdentityError, ServerPrincipal

_WRITE_ROLES = frozenset({"admin", "researcher", "trader"})
_READ_ROLES = frozenset({"admin", "researcher", "trader", "read-only"})


class ReplayAuthorizationError(IdentityError):
    def __init__(self, message: str = "principal is not allowed") -> None:
        super().__init__("FORBIDDEN", message)


def require_scope(
    principal: ServerPrincipal, organization_id: str, workspace_id: str
) -> None:
    if (
        principal.organization_id != organization_id
        or principal.workspace_id != workspace_id
    ):
        raise ReplayAuthorizationError(
            "request organization/workspace does not match the principal"
        )


def require_read(principal: ServerPrincipal) -> None:
    if principal.role not in _READ_ROLES:
        raise ReplayAuthorizationError()


def require_write(principal: ServerPrincipal) -> None:
    if principal.role not in _WRITE_ROLES:
        raise ReplayAuthorizationError("read-only principal cannot mutate replay")
