"""JWT cookie auth + section-aware permission gating.

Two roles live in the ``users`` table:

* ``admin``            — implicit access to every admin endpoint, including
                         user management.
* ``content_manager``  — has a per-user permission list. The dependency
                         below maps the request path to a required
                         permission (see ``ocoi_common.permissions``) and
                         enforces it.

Emails in the ``ADMIN_EMAILS`` env var are auto-bootstrapped to a
``role='admin'`` row on first login, so an empty ``users`` table still
works the moment a whitelisted admin signs in.
"""

from datetime import datetime, timedelta, timezone

import json
import jwt
from fastapi import HTTPException, Request
from sqlalchemy import select

from ocoi_common.config import settings
from ocoi_common.permissions import (
    ALL_PERMISSIONS,
    required_permission_for_path,
)


def create_access_token(email: str, name: str) -> str:
    """Admin-cookie token. Audience is omitted so the MCP verifier (which
    requires ``aud=mcp``) will reject it if it ever leaks into a Bearer
    header — and vice-versa."""
    payload = {
        "sub": email,
        "name": name,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_expire_minutes),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    """Decode an admin-cookie token. Rejects MCP tokens (which carry an
    ``aud`` claim) so a stolen MCP bearer can't be replayed as a cookie."""
    try:
        decoded = jwt.decode(
            token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm],
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")
    # Admin cookie tokens never carry an audience. MCP tokens always do.
    if decoded.get("aud"):
        raise HTTPException(401, "Wrong token audience")
    return decoded


def create_mcp_access_token(user_id: str, client_id: str, scope: str = "mcp") -> str:
    """Short-lived bearer token for MCP tool calls. Audience separates it
    from the admin cookie."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "aud": settings.mcp_jwt_audience,
        "client_id": client_id,
        "scope": scope,
        "iat": now,
        "exp": now + timedelta(minutes=settings.mcp_access_token_minutes),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_mcp_token(token: str) -> dict:
    """Verify an MCP bearer token. Raises HTTPException(401) on failure."""
    try:
        return jwt.decode(
            token,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
            audience=settings.mcp_jwt_audience,
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidAudienceError:
        raise HTTPException(401, "Wrong token audience")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


def user_permissions(user) -> set[str]:
    """Resolve the effective permission set for a User row. Admins get
    every key in ALL_PERMISSIONS; content managers get whatever's stored
    in their JSON list."""
    if getattr(user, "role", None) == "admin":
        return set(ALL_PERMISSIONS)
    raw = getattr(user, "permissions", None)
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
    except Exception:
        return set()
    if not isinstance(parsed, list):
        return set()
    return {p for p in parsed if isinstance(p, str)}


async def get_or_bootstrap_user(
    email: str,
    name: str | None = None,
    *,
    for_mcp: bool = False,
):
    """Find or create the user row for the given Google email.

    * Existing row → returned as-is, ``last_login_at`` touched.
    * No row, email on ``ADMIN_EMAILS`` whitelist → new ``role='admin'`` row.
    * No row, ``for_mcp=True`` and ``MCP_INVITE_ONLY=False`` → new
      ``role='mcp_user'`` row.
    * No row, ``for_mcp=True`` and ``MCP_INVITE_ONLY=True`` (the default) →
      ``None``. The Google callback turns this into an "you need an
      invite" error page so closed-beta access is enforced.
    * No row, ``for_mcp=False`` → ``None`` (admin panel stays locked down).
    """
    from ocoi_db.engine import async_session_factory
    from ocoi_db.models import User

    email = (email or "").strip().lower()
    if not email:
        return None

    async with async_session_factory() as session:
        result = await session.execute(select(User).where(User.email == email))
        user = result.scalars().first()
        if user is not None:
            user.last_login_at = datetime.utcnow()
            if name and not user.name:
                user.name = name
            await session.commit()
            await session.refresh(user)
            return user

        # No row yet — bootstrap admins listed in ADMIN_EMAILS first so a
        # whitelisted admin signing in via MCP still gets full admin role.
        if email in settings.admin_email_set:
            user = User(
                email=email,
                name=name or email,
                role="admin",
                permissions=None,
                last_login_at=datetime.utcnow(),
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user

        if for_mcp and not settings.mcp_invite_only:
            user = User(
                email=email,
                name=name or email,
                role="mcp_user",
                permissions=None,
                last_login_at=datetime.utcnow(),
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user

    return None


async def get_current_admin(request: Request):
    """FastAPI dependency for the admin router. Resolves the User row from
    the JWT cookie, then enforces the section-specific permission for the
    request path.

    * Admin-only paths (e.g. ``/admin/users``) → ``role='admin'`` required.
    * Permission-gated paths → either ``role='admin'`` or the right key in
      the user's permission set.
    * Returns the ``User`` ORM row on success — downstream handlers can
      use ``request.state.user`` if they prefer.
    """
    token = request.cookies.get("ocoi_auth")
    if not token:
        raise HTTPException(401, "Not authenticated")
    payload = decode_token(token)
    email = payload.get("sub", "").lower()
    user = await get_or_bootstrap_user(email, payload.get("name"))
    if user is None:
        raise HTTPException(403, "Not authorized")

    admin_only, required = required_permission_for_path(request.url.path)
    if admin_only:
        if user.role != "admin":
            raise HTTPException(403, "Admin only section")
        return user
    if required is not None:
        if user.role != "admin" and required not in user_permissions(user):
            raise HTTPException(403, f"Missing permission: {required}")
    return user
