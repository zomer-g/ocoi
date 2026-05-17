"""Bearer-token auth middleware for the mounted MCP ASGI app.

Each MCP request must arrive with ``Authorization: Bearer <jwt>``. The
JWT was minted by ``ocoi_api.routers.oauth.token`` and carries
``aud="mcp"`` + ``sub=<user_id>`` + ``client_id``. We verify it once
here, load the ``User`` row + ``BillingAccount`` row, and stash both in
a ContextVar so tool implementations can read them without plumbing.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass

from sqlalchemy import select
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from ocoi_api.auth import decode_mcp_token
from ocoi_db.engine import async_session_factory
from ocoi_db.models import BillingAccount, User


@dataclass
class MCPCaller:
    user_id: str
    email: str
    name: str
    client_id: str | None
    plan: str
    monthly_quota: int | None


_current_caller: contextvars.ContextVar[MCPCaller | None] = contextvars.ContextVar(
    "mcp_caller", default=None,
)


def get_current_caller() -> MCPCaller:
    """Read the caller stashed by the middleware. Must only be called
    from inside an MCP tool implementation."""
    caller = _current_caller.get()
    if caller is None:
        # This is an internal invariant — middleware sets it before any
        # tool runs. If it's None we'd rather crash loudly than serve
        # data as an anonymous user.
        raise RuntimeError("MCP tool called outside an authenticated request")
    return caller


def set_current_caller(caller: MCPCaller) -> contextvars.Token:
    return _current_caller.set(caller)


def reset_current_caller(token: contextvars.Token) -> None:
    _current_caller.reset(token)


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Validate ``Authorization: Bearer <jwt>`` and load the caller.

    * Missing or malformed header → 401 with WWW-Authenticate pointing at
      our OAuth metadata, per MCP spec 2025-06-18.
    * Valid JWT but unknown user → 401 (the row could have been deleted).
    * Valid → ContextVar set for the duration of the request.
    """

    async def dispatch(self, request: Request, call_next):
        # GET probes for capability discovery — keep them simple. The MCP
        # spec recommends responding to unauthenticated requests with a
        # 401 that advertises the resource metadata endpoint.
        header = request.headers.get("authorization") or request.headers.get("Authorization")
        if not header or not header.lower().startswith("bearer "):
            return _unauth("missing_or_malformed_bearer")

        token = header.split(" ", 1)[1].strip()
        try:
            payload = decode_mcp_token(token)
        except Exception as exc:
            return _unauth(str(getattr(exc, "detail", "invalid_token")))

        user_id = payload.get("sub")
        client_id = payload.get("client_id")
        if not user_id:
            return _unauth("token_missing_subject")

        async with async_session_factory() as session:
            user = await session.get(User, user_id)
            if user is None:
                return _unauth("user_not_found")
            billing = await session.get(BillingAccount, user_id)
            plan = billing.plan if billing is not None else "free"
            quota = billing.monthly_quota if billing is not None else None

        caller = MCPCaller(
            user_id=str(user.id),
            email=user.email,
            name=user.name or user.email,
            client_id=client_id,
            plan=plan,
            monthly_quota=quota,
        )
        token_handle = set_current_caller(caller)
        try:
            response = await call_next(request)
        finally:
            reset_current_caller(token_handle)
        return response


def _unauth(reason: str) -> JSONResponse:
    """401 response per MCP spec 2025-06-18: WWW-Authenticate MUST carry
    a ``resource_metadata`` URL so the client can discover where to
    authenticate. Path matches main.py's `/.well-known/...` registration."""
    from ocoi_common.config import settings
    base = settings.mcp_public_url.rstrip("/")
    return JSONResponse(
        {"error": "invalid_token", "error_description": reason},
        status_code=401,
        headers={
            "WWW-Authenticate": (
                f'Bearer realm="mcp", '
                f'resource_metadata="{base}/.well-known/oauth-protected-resource/mcp"'
            ),
        },
    )
