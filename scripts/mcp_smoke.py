"""Local smoke test for the MCP wiring — no Render, no Google.

Verifies:
* The new SQLAlchemy models load and create cleanly on a fresh SQLite.
* The OAuth router imports + metadata documents have the required fields.
* The MCP server module can build its Starlette app.
* The FastAPI app composes (mounted /mcp + /oauth + /.well-known).
* /.well-known/oauth-authorization-server/mcp returns valid JSON over Starlette TestClient.
* /api/v1/search still works (public-API regression).

Run with:
    .venv\\Scripts\\python scripts\\mcp_smoke.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Inject ocoi-api source onto sys.path so we don't have to pip-install it
# (avoids dragging in pdfplumber / pymupdf for a smoke test).
sys.path.insert(0, str(ROOT / "packages" / "ocoi-api" / "src"))

# Point everything at a throwaway SQLite db. Set BEFORE importing models.
tmp_db = Path(tempfile.mkdtemp()) / "smoke.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_db}"
os.environ["JWT_SECRET_KEY"] = "smoke-test-secret"
os.environ["MCP_ENABLED"] = "true"
os.environ["MCP_PUBLIC_URL"] = "http://localhost:8000"
os.environ["GOOGLE_CLIENT_ID"] = "fake"
os.environ["GOOGLE_CLIENT_SECRET"] = "fake"


def check(label: str, fn):
    print(f"\n-- {label} --")
    try:
        result = fn()
        print(f"PASS  {label}")
        return result
    except Exception:
        print(f"FAIL  {label}")
        traceback.print_exc()
        raise SystemExit(1)


def step_models_create():
    import asyncio

    from ocoi_db.engine import create_all_tables, async_engine
    from sqlalchemy import inspect, text as sa_text

    asyncio.run(create_all_tables())

    async def list_tables():
        async with async_engine.connect() as conn:
            return await conn.run_sync(lambda c: inspect(c).get_table_names())

    tables = asyncio.run(list_tables())
    required = {
        "oauth_clients", "oauth_authorization_codes", "oauth_refresh_tokens",
        "usage_events", "billing_accounts",
    }
    missing = required - set(tables)
    assert not missing, f"missing tables: {missing}"
    print(f"  Created {len(tables)} tables (incl. {sorted(required)})")


def step_oauth_metadata():
    from ocoi_api.routers.oauth import (
        authorization_server_metadata,
        protected_resource_metadata,
    )
    as_meta = authorization_server_metadata()
    pr_meta = protected_resource_metadata()
    for key in ("issuer", "authorization_endpoint", "token_endpoint",
                "registration_endpoint", "revocation_endpoint",
                "code_challenge_methods_supported"):
        assert key in as_meta, f"AS metadata missing '{key}'"
    assert "S256" in as_meta["code_challenge_methods_supported"]
    # Trap #3: issuer MUST be `<base>/mcp` to match the well-known path.
    assert as_meta["issuer"] == "http://localhost:8000/mcp", as_meta["issuer"]
    assert as_meta["authorization_endpoint"] == "http://localhost:8000/mcp/oauth/authorize"
    assert as_meta["token_endpoint"] == "http://localhost:8000/mcp/oauth/token"
    assert as_meta["registration_endpoint"] == "http://localhost:8000/mcp/oauth/register"
    assert pr_meta["resource"] == "http://localhost:8000/mcp"
    assert pr_meta["authorization_servers"] == ["http://localhost:8000/mcp"]
    print("  AS metadata:", json.dumps(as_meta, indent=2))
    print("  PR metadata:", json.dumps(pr_meta, indent=2))


def step_mcp_app_builds():
    from ocoi_api.mcp import build_mcp_app
    app = build_mcp_app()
    assert app is not None
    print("  build_mcp_app() returned:", type(app).__name__)


def step_fastapi_composes():
    # Stub the heavy admin router import chain — we only care about routes.
    from ocoi_api.main import create_app
    app = create_app()
    paths = sorted({getattr(r, "path", str(r)) for r in app.routes})
    interesting = [
        p for p in paths
        if "/oauth" in p or "/.well-known/" in p or p == "/mcp"
    ]
    print("  Interesting paths:", interesting)
    assert "/.well-known/oauth-authorization-server/mcp" in paths
    assert "/.well-known/oauth-protected-resource/mcp" in paths
    assert "/mcp/oauth/authorize" in paths
    assert "/mcp/oauth/token" in paths
    assert "/mcp/oauth/register" in paths
    assert "/mcp/oauth/google/callback" in paths


def step_test_client_well_known():
    from fastapi.testclient import TestClient
    from ocoi_api.main import create_app
    client = TestClient(create_app())

    r = client.get("/.well-known/oauth-authorization-server/mcp")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["issuer"] == "http://localhost:8000/mcp"
    assert body["authorization_endpoint"] == "http://localhost:8000/mcp/oauth/authorize"

    r2 = client.get("/.well-known/oauth-protected-resource/mcp")
    assert r2.status_code == 200
    pr = r2.json()
    assert pr["resource"] == "http://localhost:8000/mcp"
    assert pr["authorization_servers"] == ["http://localhost:8000/mcp"]

    # Public API regression
    r3 = client.get("/api/v1/search?q=test")
    assert r3.status_code == 200, r3.text


def step_cors_preflight():
    """Trap #1 — claude.ai (cross-origin) hits /mcp with an OPTIONS
    preflight. Must succeed with Allow-Origin: * even though the global
    CORS middleware only allows configured origins."""
    from fastapi.testclient import TestClient
    from ocoi_api.main import create_app
    client = TestClient(create_app())
    r = client.options(
        "/mcp",
        headers={
            "Origin": "https://claude.ai",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Authorization, Content-Type",
        },
    )
    assert r.status_code in (200, 204), f"preflight status {r.status_code}"
    assert r.headers.get("access-control-allow-origin") == "*"
    allowed_methods = r.headers.get("access-control-allow-methods", "")
    assert "POST" in allowed_methods
    print("  /mcp preflight Allow-Origin:", r.headers.get("access-control-allow-origin"))

    # Same for the well-known path.
    r2 = client.options(
        "/.well-known/oauth-protected-resource/mcp",
        headers={
            "Origin": "https://claude.ai",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert r2.status_code in (200, 204)
    assert r2.headers.get("access-control-allow-origin") == "*"


def step_unauth_mcp_returns_401():
    from fastapi.testclient import TestClient
    from ocoi_api.main import create_app
    client = TestClient(create_app())
    # No Authorization header → must be 401 with WWW-Authenticate pointing
    # at the resource metadata, per MCP spec 2025-06-18.
    r = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text[:300]}"
    www = r.headers.get("www-authenticate", "")
    assert "Bearer" in www and "resource_metadata=" in www, f"bad WWW-Authenticate: {www}"
    assert "/.well-known/oauth-protected-resource/mcp" in www, www
    print("  WWW-Authenticate:", www)


def step_dynamic_client_registration():
    """RFC 7591 — Claude.ai POSTs to /mcp/oauth/register on first connect.
    Must return a client_id."""
    from fastapi.testclient import TestClient
    from ocoi_api.main import create_app
    client = TestClient(create_app())
    r = client.post(
        "/mcp/oauth/register",
        json={
            "client_name": "Smoke Test Client",
            "redirect_uris": ["https://claude.ai/api/mcp/auth_callback"],
        },
    )
    assert r.status_code == 201, f"DCR status {r.status_code}: {r.text[:300]}"
    body = r.json()
    assert "client_id" in body and len(body["client_id"]) > 10
    assert body["client_name"] == "Smoke Test Client"
    print("  Issued client_id:", body["client_id"])


def step_invite_only_gate():
    """Without an existing User row, get_or_bootstrap_user(for_mcp=True)
    must return None — the Google callback then renders the invite page."""
    import asyncio
    from ocoi_api.auth import get_or_bootstrap_user
    from ocoi_common.config import settings

    assert settings.mcp_invite_only is True, "invite-only must be on by default"

    async def run():
        user = await get_or_bootstrap_user("nobody-invited@example.com", "Nobody", for_mcp=True)
        return user

    result = asyncio.run(run())
    assert result is None, f"invite-only failed: bootstrapped {result}"
    print("  Uninvited email correctly rejected (returned None)")


def step_mcp_disabled_404():
    """Kill switch — MCP_ENABLED=false → /.well-known/* 404."""
    from ocoi_common.config import settings
    original = settings.mcp_enabled
    settings.mcp_enabled = False
    try:
        from importlib import reload
        import ocoi_api.main as main_mod
        reload(main_mod)
        from fastapi.testclient import TestClient
        client = TestClient(main_mod.create_app(), raise_server_exceptions=False)
        r = client.get("/.well-known/oauth-authorization-server/mcp")
        # No /mcp routes registered → /.well-known/... falls to the SPA
        # fallback whose deny-list raises HTTPException(404). FastAPI
        # converts that into a JSON 404.
        assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text[:200]}"
        print("  /.well-known is 404 when disabled — kill switch works")
    finally:
        settings.mcp_enabled = original


if __name__ == "__main__":
    check("models load + tables create", step_models_create)
    check("oauth metadata helpers", step_oauth_metadata)
    check("mcp server builds", step_mcp_app_builds)
    check("fastapi app composes", step_fastapi_composes)
    check("/.well-known + public API via TestClient", step_test_client_well_known)
    check("CORS preflight (trap #1)", step_cors_preflight)
    check("/mcp without bearer returns 401 + WWW-Authenticate", step_unauth_mcp_returns_401)
    check("dynamic client registration", step_dynamic_client_registration)
    check("invite-only gate rejects uninvited", step_invite_only_gate)
    check("MCP_ENABLED=false kill switch", step_mcp_disabled_404)
    print("\nALL SMOKE TESTS PASSED")
