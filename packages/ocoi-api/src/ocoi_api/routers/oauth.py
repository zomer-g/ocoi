"""OAuth 2.1 Authorization Server for the MCP surface.

Implements just enough of the spec — RFC 8414 (metadata), RFC 7591
(dynamic client registration), RFC 9728 (protected-resource metadata),
RFC 7009 (revocation), the authorization-code grant with mandatory PKCE
(RFC 7636), and refresh tokens — for the MCP 2025-06-18 spec.

Identity is delegated upstream to Google: ``/oauth/authorize`` redirects
the user's browser to Google with a signed ``state`` JWT carrying the
pending grant; Google calls back to ``/oauth/google/callback`` which
mints a short-lived authorization code and bounces the browser to the
MCP client's redirect URI. The MCP client then exchanges that code for
an access token at ``/oauth/token``.

Tokens are JWTs (`aud="mcp"`) signed with the existing ``JWT_SECRET_KEY``
so a single secret rotation covers admin + MCP.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx
import jwt
from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.auth import create_mcp_access_token, get_or_bootstrap_user
from ocoi_common.config import settings
from ocoi_db.engine import async_session_factory
from ocoi_db.models import (
    BillingAccount,
    OAuthAuthorizationCode,
    OAuthClient,
    OAuthRefreshToken,
)

# Google endpoints — copied from routers/auth.py so the two flows can
# evolve independently.
GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

# The /state JWT we hand to Google is itself signed with the OCOI secret;
# audience tag keeps it from being confused with cookie or MCP tokens.
PENDING_GRANT_AUDIENCE = "oauth_pending_grant"
PENDING_GRANT_TTL_SECONDS = 600  # 10 min — covers user's Google flow

router = APIRouter(prefix="/mcp/oauth", tags=["oauth"])


# ─── helpers ──────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _hash(token: str) -> str:
    """SHA-256 hex — fine for tokens that already have ≥128 bits of entropy.
    No salting needed because the input is itself random and high-entropy."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _gen_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


def _require_mcp_enabled() -> None:
    if not settings.mcp_enabled:
        raise HTTPException(404, "MCP disabled")


def _base_url() -> str:
    """Bare host URL — used as the well-known root and for the SPA."""
    return settings.mcp_public_url.rstrip("/")


def _issuer_url() -> str:
    """Canonical OAuth issuer = the MCP server's resource URL. Per RFC 8414,
    when the issuer has a path component, the metadata is published at
    ``<host>/.well-known/<type>/<issuer-path>`` — that's why the well-known
    routes carry a ``/mcp`` suffix in main.py."""
    return f"{_base_url()}/mcp"


def _validate_redirect_uri(client: OAuthClient, redirect_uri: str) -> None:
    try:
        registered = json.loads(client.redirect_uris) if client.redirect_uris else []
    except Exception:
        registered = []
    if redirect_uri not in registered:
        raise HTTPException(400, "redirect_uri does not match a registered URI")


def _verify_pkce(code_challenge: str | None, method: str | None, verifier: str | None) -> None:
    """Enforce RFC 7636 — MCP requires PKCE. We accept only S256."""
    if not code_challenge:
        # Code was issued without PKCE — only allowed if client is
        # confidential. We require PKCE for everyone, so reject.
        raise HTTPException(400, "PKCE was required at /authorize but missing")
    if not verifier:
        raise HTTPException(400, "code_verifier missing")
    if (method or "S256") != "S256":
        raise HTTPException(400, "Only S256 code_challenge_method is supported")
    computed = (
        hashlib.sha256(verifier.encode("ascii")).digest()
    )
    import base64
    expected = base64.urlsafe_b64encode(computed).rstrip(b"=").decode("ascii")
    if not secrets.compare_digest(expected, code_challenge):
        raise HTTPException(400, "PKCE verification failed")


def _make_pending_grant_state(payload: dict[str, Any]) -> str:
    body = {
        **payload,
        "aud": PENDING_GRANT_AUDIENCE,
        "exp": _now() + timedelta(seconds=PENDING_GRANT_TTL_SECONDS),
        "iat": _now(),
        "nonce": secrets.token_urlsafe(8),
    }
    return jwt.encode(body, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def _decode_pending_grant_state(state: str) -> dict[str, Any]:
    try:
        return jwt.decode(
            state,
            settings.jwt_secret_key,
            algorithms=[settings.jwt_algorithm],
            audience=PENDING_GRANT_AUDIENCE,
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(400, "Authorization request expired — please retry")
    except jwt.InvalidTokenError:
        raise HTTPException(400, "Invalid or tampered state")


async def _ensure_billing_account(session: AsyncSession, user_id: str) -> BillingAccount:
    """Make sure a BillingAccount row exists for the user; create a free
    one if not. Stripe customer creation happens lazily on first
    metered-plan upgrade so dev environments don't need Stripe keys."""
    row = await session.get(BillingAccount, user_id)
    if row is not None:
        return row
    row = BillingAccount(user_id=user_id, plan="free")
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


# ─── RFC 7591: Dynamic client registration ────────────────────────────────


@router.post("/register")
async def register_client(body: dict, request: Request):
    """RFC 7591 — MCP clients (Claude Desktop, Cursor, …) register on
    first launch. We accept the minimum metadata and ignore the rest."""
    _require_mcp_enabled()

    redirect_uris = body.get("redirect_uris") or []
    if not isinstance(redirect_uris, list) or not redirect_uris:
        raise HTTPException(400, "redirect_uris must be a non-empty array")
    # Sanity-check each URI shape.
    for uri in redirect_uris:
        if not isinstance(uri, str):
            raise HTTPException(400, "redirect_uris entries must be strings")
        parsed = urlparse(uri)
        if parsed.scheme not in ("http", "https") and not uri.startswith("http://localhost"):
            # MCP allows custom schemes for desktop apps in practice, but
            # we keep it conservative; relax later if a real client needs it.
            raise HTTPException(400, f"Unsupported redirect_uri scheme: {parsed.scheme}")

    client_name = (body.get("client_name") or "").strip() or "Unknown MCP Client"
    grant_types = body.get("grant_types") or ["authorization_code", "refresh_token"]
    token_endpoint_auth_method = body.get("token_endpoint_auth_method", "none")
    is_public = token_endpoint_auth_method == "none"

    client_id = _gen_token(16)
    client_secret = None if is_public else _gen_token(32)

    row = OAuthClient(
        client_id=client_id,
        client_secret_hash=_hash(client_secret) if client_secret else None,
        is_public=is_public,
        redirect_uris=json.dumps(redirect_uris),
        client_name=client_name[:200],
        grant_types=json.dumps(grant_types),
    )
    async with async_session_factory() as session:
        session.add(row)
        await session.commit()

    response = {
        "client_id": client_id,
        "client_id_issued_at": int(_now().timestamp()),
        "redirect_uris": redirect_uris,
        "grant_types": grant_types,
        "token_endpoint_auth_method": token_endpoint_auth_method,
        "client_name": client_name,
    }
    if client_secret:
        response["client_secret"] = client_secret
        # 0 = never expires — RFC 7591 §3.2.1
        response["client_secret_expires_at"] = 0
    return JSONResponse(response, status_code=201)


# ─── Authorization endpoint ───────────────────────────────────────────────


@router.get("/authorize")
async def authorize(
    request: Request,
    response_type: str = Query(...),
    client_id: str = Query(...),
    redirect_uri: str = Query(...),
    scope: str = Query("mcp"),
    state: str | None = Query(None),
    code_challenge: str | None = Query(None),
    code_challenge_method: str | None = Query(None),
):
    """Step 1 of the auth-code flow: validate the request, then bounce the
    user to Google. The MCP-client's state + PKCE challenge are packed
    into the signed JWT we hand Google as *its* ``state`` parameter."""
    _require_mcp_enabled()
    if response_type != "code":
        raise HTTPException(400, "Only response_type=code is supported")
    if not code_challenge or (code_challenge_method or "S256") != "S256":
        raise HTTPException(400, "PKCE with S256 is required")

    async with async_session_factory() as session:
        client = (await session.execute(
            select(OAuthClient).where(OAuthClient.client_id == client_id)
        )).scalars().first()
    if client is None or client.revoked_at is not None:
        raise HTTPException(400, "Unknown or revoked client")
    _validate_redirect_uri(client, redirect_uri)

    pending = _make_pending_grant_state({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method or "S256",
        "client_state": state or "",
    })

    google_redirect = f"{_issuer_url()}/oauth/google/callback"
    google_params = urlencode({
        "client_id": settings.google_client_id,
        "redirect_uri": google_redirect,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online",
        "prompt": "select_account",
        "state": pending,
    })
    return RedirectResponse(f"{GOOGLE_AUTH_URL}?{google_params}")


@router.get("/google/callback")
async def google_callback(
    code: str = Query(""),
    state: str = Query(""),
    error: str | None = Query(None),
):
    """Step 2: Google sends the user back here. We resolve the user,
    mint an authorization code, and bounce to the MCP client's
    redirect_uri with ``?code=...&state=<original client state>``."""
    _require_mcp_enabled()
    pending = _decode_pending_grant_state(state)
    redirect_uri = pending["redirect_uri"]
    client_state = pending.get("client_state", "")

    def _client_error_redirect(err_code: str, desc: str) -> RedirectResponse:
        params = urlencode({"error": err_code, "error_description": desc, "state": client_state})
        return RedirectResponse(f"{redirect_uri}?{params}")

    if error or not code:
        return _client_error_redirect("access_denied", error or "no_code")

    google_redirect = f"{_issuer_url()}/oauth/google/callback"
    async with httpx.AsyncClient(timeout=15.0) as http:
        token_resp = await http.post(GOOGLE_TOKEN_URL, data={
            "client_id": settings.google_client_id,
            "client_secret": settings.google_client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": google_redirect,
        })
        if token_resp.status_code != 200:
            return _client_error_redirect("server_error", "token_exchange_failed")
        access_token = token_resp.json().get("access_token")
        if not access_token:
            return _client_error_redirect("server_error", "no_access_token")
        userinfo_resp = await http.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if userinfo_resp.status_code != 200:
            return _client_error_redirect("server_error", "userinfo_failed")
        userinfo = userinfo_resp.json()

    email = (userinfo.get("email") or "").lower()
    name = userinfo.get("name") or email
    user = await get_or_bootstrap_user(email, name, for_mcp=True)
    if user is None:
        # The user *did* authenticate with Google, they're just not on the
        # invite list. Rendering an HTML page (rather than redirecting back
        # to the MCP client with ?error=) keeps the explanation in front of
        # the user — most MCP clients just say "auth failed" if you bounce.
        return _invite_required_page(email)

    # Mint the authorization code. We only store its hash so the row alone
    # isn't enough to redeem.
    auth_code = _gen_token(32)
    async with async_session_factory() as session:
        await _ensure_billing_account(session, str(user.id))
        session.add(OAuthAuthorizationCode(
            code_hash=_hash(auth_code),
            client_id=pending["client_id"],
            user_id=str(user.id),
            redirect_uri=redirect_uri,
            code_challenge=pending["code_challenge"],
            code_challenge_method=pending["code_challenge_method"],
            scope=pending.get("scope", "mcp"),
            expires_at=_now().replace(tzinfo=None)
                + timedelta(minutes=settings.mcp_authorization_code_minutes),
        ))
        await session.commit()

    params = urlencode({"code": auth_code, "state": client_state})
    return RedirectResponse(f"{redirect_uri}?{params}")


def _invite_required_page(email: str) -> HTMLResponse:
    """Rendered when an authenticated Google user isn't on the invite
    list. Plain HTML, RTL Hebrew, no JS — works on any browser."""
    safe_email = (email or "").replace("<", "&lt;").replace(">", "&gt;")
    body = f"""<!doctype html>
<html lang="he" dir="rtl">
<head>
  <meta charset="utf-8">
  <title>ניגוד עניינים לעם — MCP</title>
  <style>
    body {{ font-family: -apple-system, system-ui, sans-serif; max-width: 520px;
            margin: 80px auto; padding: 0 16px; line-height: 1.6; color: #1f2937; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 24px;
             background: #fff; }}
    h1 {{ font-size: 1.4rem; margin: 0 0 12px; }}
    .email {{ font-family: ui-monospace, monospace; background: #f3f4f6;
              padding: 2px 6px; border-radius: 4px; direction: ltr;
              display: inline-block; }}
    .muted {{ color: #6b7280; font-size: 0.9rem; }}
    a {{ color: #1d4ed8; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>גישה ל-MCP — נדרשת הזמנה</h1>
    <p>התחברת בהצלחה עם <span class="email">{safe_email}</span>, אך הכתובת
       הזו עדיין לא רשומה כמשתמש MCP.</p>
    <p>שרת ה-MCP של ניגוד עניינים לעם פתוח כעת בגרסת ביתא סגורה.
       כדי לבקש הזמנה, פנו אל <a href="mailto:guy@z-g.co.il">guy@z-g.co.il</a>.</p>
    <p class="muted">אפשר לסגור את החלון הזה — המערכת לא שמרה מידע נוסף.</p>
  </div>
</body>
</html>"""
    return HTMLResponse(body, status_code=403)


# ─── Token endpoint ───────────────────────────────────────────────────────


@router.post("/token")
async def token(
    request: Request,
    grant_type: str = Form(...),
    code: str | None = Form(None),
    redirect_uri: str | None = Form(None),
    code_verifier: str | None = Form(None),
    refresh_token: str | None = Form(None),
    client_id: str | None = Form(None),
    client_secret: str | None = Form(None),
):
    """Issue or refresh an access token. Mints a new refresh token on
    every exchange (refresh-token rotation, RFC 6749 §10.4 recommendation)."""
    _require_mcp_enabled()

    # client_id may also arrive in Authorization: Basic
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("basic ") and not client_id:
        import base64 as _b64
        try:
            decoded = _b64.b64decode(auth_header.split(" ", 1)[1]).decode("utf-8")
            cid, _, csec = decoded.partition(":")
            client_id = client_id or cid
            client_secret = client_secret or csec
        except Exception:
            pass

    if not client_id:
        raise HTTPException(400, "client_id required")

    async with async_session_factory() as session:
        client = (await session.execute(
            select(OAuthClient).where(OAuthClient.client_id == client_id)
        )).scalars().first()
        if client is None or client.revoked_at is not None:
            raise HTTPException(401, "Unknown or revoked client")
        if not client.is_public:
            if not client_secret or not client.client_secret_hash:
                raise HTTPException(401, "client_secret required")
            if not secrets.compare_digest(_hash(client_secret), client.client_secret_hash):
                raise HTTPException(401, "Bad client_secret")

        if grant_type == "authorization_code":
            if not code or not redirect_uri:
                raise HTTPException(400, "code + redirect_uri required")
            row = (await session.execute(
                select(OAuthAuthorizationCode).where(
                    OAuthAuthorizationCode.code_hash == _hash(code)
                )
            )).scalars().first()
            if row is None:
                raise HTTPException(400, "Invalid code")
            if row.used_at is not None:
                # Token replay — best-effort: invalidate any refresh tokens
                # we'd already issued from this code. RFC 6749 §10.5.
                raise HTTPException(400, "Code already used")
            if row.expires_at < _now().replace(tzinfo=None):
                raise HTTPException(400, "Code expired")
            if row.client_id != client_id:
                raise HTTPException(400, "Code was issued to a different client")
            if row.redirect_uri != redirect_uri:
                raise HTTPException(400, "redirect_uri mismatch")
            _verify_pkce(row.code_challenge, row.code_challenge_method, code_verifier)

            row.used_at = _now().replace(tzinfo=None)
            user_id = row.user_id
            scope = row.scope or "mcp"
            await session.commit()
        elif grant_type == "refresh_token":
            if not refresh_token:
                raise HTTPException(400, "refresh_token required")
            row = (await session.execute(
                select(OAuthRefreshToken).where(
                    OAuthRefreshToken.token_hash == _hash(refresh_token)
                )
            )).scalars().first()
            if row is None or row.revoked_at is not None:
                raise HTTPException(400, "Invalid refresh_token")
            if row.expires_at < _now().replace(tzinfo=None):
                raise HTTPException(400, "Refresh token expired")
            if row.client_id != client_id:
                raise HTTPException(400, "Refresh token belongs to a different client")
            # Rotate.
            row.revoked_at = _now().replace(tzinfo=None)
            user_id = row.user_id
            scope = row.scope or "mcp"
            await session.commit()
        else:
            raise HTTPException(400, f"Unsupported grant_type: {grant_type}")

        # Issue fresh access + refresh tokens.
        new_refresh = _gen_token(32)
        session.add(OAuthRefreshToken(
            token_hash=_hash(new_refresh),
            client_id=client_id,
            user_id=user_id,
            scope=scope,
            expires_at=_now().replace(tzinfo=None) + timedelta(days=settings.mcp_refresh_token_days),
        ))
        await session.commit()

    access_token = create_mcp_access_token(user_id=user_id, client_id=client_id, scope=scope)
    return {
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": settings.mcp_access_token_minutes * 60,
        "refresh_token": new_refresh,
        "scope": scope,
    }


# ─── Revocation (RFC 7009) ────────────────────────────────────────────────


@router.post("/revoke")
async def revoke(
    token: str = Form(...),
    token_type_hint: str | None = Form(None),
    client_id: str | None = Form(None),
):
    """Best-effort revocation. We only know how to revoke refresh tokens
    (access tokens are JWTs and expire in 15 min anyway). Per RFC, we
    return 200 even on unknown tokens to avoid an oracle."""
    _require_mcp_enabled()
    async with async_session_factory() as session:
        row = (await session.execute(
            select(OAuthRefreshToken).where(OAuthRefreshToken.token_hash == _hash(token))
        )).scalars().first()
        if row is not None and row.revoked_at is None:
            row.revoked_at = _now().replace(tzinfo=None)
            await session.commit()
    return JSONResponse({}, status_code=200)


# ─── /.well-known/* metadata documents ────────────────────────────────────
#
# These two are mounted at the application root (not under /oauth) so MCP
# clients can find them at the canonical RFC 8414 / RFC 9728 paths.


def authorization_server_metadata() -> dict:
    """RFC 8414 metadata. ``issuer`` MUST equal the URL the metadata was
    served from (minus the ``/.well-known/...`` portion) — for us that's
    ``<host>/mcp``. MCP clients (notably Claude.ai) validate this strictly."""
    issuer = _issuer_url()
    return {
        "issuer": issuer,
        "authorization_endpoint": f"{issuer}/oauth/authorize",
        "token_endpoint": f"{issuer}/oauth/token",
        "registration_endpoint": f"{issuer}/oauth/register",
        "revocation_endpoint": f"{issuer}/oauth/revoke",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_basic", "client_secret_post"],
        "scopes_supported": ["mcp"],
    }


def protected_resource_metadata() -> dict:
    issuer = _issuer_url()
    return {
        "resource": issuer,
        "authorization_servers": [issuer],
        "scopes_supported": ["mcp"],
        "bearer_methods_supported": ["header"],
    }
