"""FastAPI application factory."""

import os
from pathlib import Path
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from ocoi_api.routers import search, entities, connections, documents, external, auth, admin, push, site, suggestions, oauth
from ocoi_api.auth import get_current_admin
from ocoi_common.config import settings


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        # SAMEORIGIN (not DENY) so the public document page can embed our
        # own /api/v1/documents/{id}/pdf in a side-by-side viewer.
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response


# ---------------------------------------------------------------------------
# MCP-specific CORS middleware
# ---------------------------------------------------------------------------
# External MCP clients (claude.ai, chatgpt.com, cursor.com, the MCP Inspector
# at modelcontextprotocol.io) live on different origins than the API. The
# global CORSMiddleware below is locked to ALLOWED_ORIGINS — fine for the
# admin UI, fatal for /mcp. This middleware short-circuits CORS preflight
# for the MCP + well-known paths and lets ANY origin through. Safety is
# preserved: /mcp is auth-gated by Bearer JWT and the OAuth flow runs
# through Google + the invite list.
# ---------------------------------------------------------------------------
_MCP_CORS_PATHS = (
    "/mcp",
    "/.well-known/oauth-authorization-server/mcp",
    "/.well-known/oauth-protected-resource/mcp",
)
_MCP_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": (
        "Authorization, Content-Type, Accept, Mcp-Session-Id, Last-Event-Id"
    ),
    "Access-Control-Expose-Headers": "Mcp-Session-Id, WWW-Authenticate",
    "Access-Control-Max-Age": "86400",
}


def _is_mcp_path(path: str) -> bool:
    return any(path == p or path.startswith(p + "/") for p in _MCP_CORS_PATHS)


class MCPCorsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not _is_mcp_path(request.url.path):
            return await call_next(request)
        # Preflight: short-circuit so we never hit our routes for OPTIONS.
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_MCP_CORS_HEADERS)
        response = await call_next(request)
        for k, v in _MCP_CORS_HEADERS.items():
            response.headers[k] = v
        return response


class MCPPathNormalizerMiddleware:
    """Pure ASGI middleware that rewrites the bare `/mcp` path to `/mcp/`
    so Starlette's Mount (which only matches `/mcp/...`) catches it.

    Without this, MCP clients that hit the canonical URL `https://host/mcp`
    (no trailing slash — which is what Claude.ai's "MCP server URL" field
    encourages) would fall through to the SPA catch-all for GET and 405
    for POST. We rewrite at the ASGI scope level — no body consumed, no
    redirect issued (POST requests can't reliably follow redirects)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and scope.get("path") == "/mcp":
            scope = dict(scope)
            scope["path"] = "/mcp/"
            raw = scope.get("raw_path") or b"/mcp"
            if not raw.endswith(b"/"):
                scope["raw_path"] = raw + b"/"
        await self.app(scope, receive, send)


async def _init_db():
    """Initialize DB tables and run migrations with retries."""
    import asyncio
    from ocoi_db.engine import create_all_tables, run_migrations
    for attempt in range(5):
        try:
            await create_all_tables()
            await run_migrations()
            print("DB initialized successfully.")
            # Schedule showcase pre-warm AFTER the DB is reachable. The
            # first visitor of the day otherwise pays ~1-2s computing the
            # "two suns" pair while staring at a "טוען המחשה…" spinner.
            try:
                from ocoi_api.routers.connections import prewarm_showcase_cache
                asyncio.ensure_future(prewarm_showcase_cache())
            except Exception as e:
                print(f"Showcase pre-warm scheduling failed: {e}")
            break
        except Exception as e:
            if attempt < 4:
                wait = 2 ** attempt
                print(f"DB connect attempt {attempt + 1}/5 failed: {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"DB connection failed after 5 attempts: {e}.")



@asynccontextmanager
async def lifespan(app: FastAPI):
    """Yield immediately so the server binds its port, then init DB in background."""
    import asyncio
    settings.ensure_dirs()
    asyncio.ensure_future(_init_db())
    # MCP session manager must be started BEFORE any /mcp request can be
    # served — its handle_request() needs an active anyio task group.
    # The sub-app's own lifespan isn't called when it's mounted on a
    # parent FastAPI app, so we drive it from here.
    mcp_handle = getattr(app.state, "mcp_handle", None)
    if mcp_handle is not None:
        try:
            await mcp_handle.startup()
        except Exception as e:
            print(f"MCP startup failed: {e}")
    # Stripe metered-billing batcher. No-op when MCP is disabled or
    # STRIPE_SECRET_KEY isn't configured (dev environments).
    if settings.mcp_enabled:
        try:
            from ocoi_api.mcp.billing import start_billing_batcher
            start_billing_batcher()
        except Exception as e:
            print(f"Billing batcher failed to start: {e}")
    yield
    # On shutdown: signal extraction + billing batcher to stop gracefully.
    try:
        from ocoi_api.services.extraction_service import stop_extraction
        stop_extraction()
    except Exception:
        pass
    try:
        from ocoi_api.mcp.billing import stop_billing_batcher
        stop_billing_batcher()
    except Exception:
        pass
    if mcp_handle is not None:
        try:
            await mcp_handle.shutdown()
        except Exception:
            pass


def _get_allowed_origins() -> list[str]:
    """Return allowed origins from env var for external API consumers."""
    origins_env = os.getenv("ALLOWED_ORIGINS", "")
    origins = [o.strip() for o in origins_env.split(",") if o.strip()] if origins_env else []
    # In dev mode, allow the frontend dev server
    if os.getenv("ENV", "dev") != "production" and not origins:
        origins = ["http://localhost:3000", "http://localhost"]
    return origins


def _get_static_dir() -> Path | None:
    """Return the static files directory if it exists."""
    static_dir = Path(os.getenv("STATIC_DIR", "./frontend/out"))
    if static_dir.is_dir() and (static_dir / "index.html").exists():
        return static_dir
    return None


def _build_public_openapi(full_schema: dict) -> dict:
    """Filter the full OpenAPI schema to include only public read-only endpoints."""
    public_prefixes = ("/api/v1/search", "/api/v1/persons", "/api/v1/companies",
                       "/api/v1/associations", "/api/v1/domains", "/api/v1/documents",
                       "/api/v1/graph", "/api/v1/external", "/api/v1/lookup",
                       "/api/v1/registry/lookup", "/api/health")
    admin_prefixes = ("/api/v1/admin",)

    filtered_paths = {}
    for path, methods in full_schema.get("paths", {}).items():
        # Skip admin routes
        if any(path.startswith(p) for p in admin_prefixes):
            continue
        # Only include GET methods from public routes
        if any(path.startswith(p) for p in public_prefixes):
            get_op = methods.get("get")
            if get_op:
                filtered_paths[path] = {"get": get_op}

    public_schema = {
        **full_schema,
        "info": {
            **full_schema.get("info", {}),
            "title": "ניגוד עניינים לעם — Public API",
            "description": "Read-only public API for querying entities, relationships, documents and registry data.",
        },
        "paths": filtered_paths,
    }
    # Remove admin tag
    if "tags" in public_schema:
        public_schema["tags"] = [t for t in public_schema["tags"] if t.get("name") != "admin"]
    return public_schema


def create_app() -> FastAPI:
    app = FastAPI(
        title="ניגוד עניינים לעם API",
        description="Conflict of Interest Transparency Platform API",
        version="0.1.0",
        docs_url=None,        # Disable default docs — we serve custom ones
        redoc_url=None,
        openapi_url=None,     # We serve openapi.json manually (admin-protected)
        lifespan=lifespan,
    )

    # Security headers
    app.add_middleware(SecurityHeadersMiddleware)

    # Global CORS — only needed for external API consumers (frontend is same-origin)
    allowed_origins = _get_allowed_origins()
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Content-Type", "Authorization"],
        )

    # MCP path normalizer — must run BEFORE the Mount router, which
    # means it has to be the outermost middleware. add_middleware
    # prepends, so register it after MCPCorsMiddleware below so this
    # ends up at the very outside.
    if settings.mcp_enabled:
        app.add_middleware(MCPCorsMiddleware)
        app.add_middleware(MCPPathNormalizerMiddleware)

    # ── API routers (matched first) ──────────────────────────────────────
    app.include_router(search.router, prefix="/api/v1")
    app.include_router(entities.router, prefix="/api/v1")
    app.include_router(connections.router, prefix="/api/v1")
    app.include_router(documents.router, prefix="/api/v1")
    app.include_router(external.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")
    app.include_router(push.router, prefix="/api/v1")
    app.include_router(site.router, prefix="/api/v1")
    app.include_router(suggestions.router, prefix="/api/v1")
    # MCP OAuth 2.1 authorization server endpoints. These live at /oauth/*
    # (not under /api/v1) because the well-known metadata advertises them
    # at the root issuer URL.
    if settings.mcp_enabled:
        app.include_router(oauth.router)

        # ── /.well-known metadata (RFC 8414 + RFC 9728) ─────────────────
        # The issuer is `<host>/mcp`, so per RFC 8414 §3 the metadata
        # MUST be served at `<host>/.well-known/<type>/mcp` (root host
        # + the issuer's path as suffix). Claude.ai validates this path
        # exactly — putting metadata under `/mcp/.well-known/...` breaks
        # discovery silently.
        @app.get("/.well-known/oauth-authorization-server/mcp", include_in_schema=False)
        async def oauth_as_metadata():
            return JSONResponse(oauth.authorization_server_metadata())

        @app.get("/.well-known/oauth-protected-resource/mcp", include_in_schema=False)
        async def oauth_pr_metadata():
            return JSONResponse(oauth.protected_resource_metadata())

        # ── Mount the MCP Streamable HTTP transport at /mcp ─────────────
        # Stash the handle on app.state so the lifespan hook above can
        # drive the session manager's startup/shutdown lifecycle.
        try:
            from ocoi_api.mcp import build_mcp_app
            mcp_handle = build_mcp_app()
            app.state.mcp_handle = mcp_handle
            app.mount("/mcp", mcp_handle.app)
        except Exception as e:
            print(f"MCP mount failed: {e}")

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

    @app.get("/api/db-health", include_in_schema=False)
    async def db_health():
        """Diagnostic: try a trivial DB query, return exact error on failure."""
        import traceback
        from ocoi_db.engine import async_session_factory
        from sqlalchemy import text as sa_text
        try:
            async with async_session_factory() as session:
                result = await session.execute(sa_text("SELECT 1"))
                val = result.scalar()
            return {"status": "ok", "db": "reachable", "select_1": val}
        except Exception as e:
            return JSONResponse(
                {
                    "status": "error",
                    "error_type": type(e).__name__,
                    "error_msg": str(e)[:1000],
                    "traceback": traceback.format_exc()[:3000],
                },
                status_code=500,
            )

    # ── Admin-only full OpenAPI schema + Swagger UI ────────────────────
    @app.get("/api/openapi.json", include_in_schema=False)
    async def full_openapi_schema(request: Request):
        """Full OpenAPI schema — admin-only."""
        try:
            await get_current_admin(request)
        except Exception:
            return JSONResponse({"detail": "Admin access required"}, status_code=403)
        return JSONResponse(app.openapi())

    @app.get("/api/admin-docs", include_in_schema=False)
    async def admin_docs(request: Request):
        """Full Swagger UI — requires admin auth (cookie)."""
        try:
            await get_current_admin(request)
        except Exception:
            return HTMLResponse(
                "<html><body style='text-align:center;margin-top:100px;font-family:sans-serif'>"
                "<h2>🔒 Admin access required</h2>"
                "<p><a href='/admin/login'>Login</a> to view full API documentation.</p>"
                "</body></html>",
                status_code=403,
            )
        from fastapi.openapi.docs import get_swagger_ui_html
        return get_swagger_ui_html(
            openapi_url="/api/openapi.json",
            title="Admin API Docs — ניגוד עניינים לעם",
        )

    # ── Public read-only API docs ────────────────────────────────────────
    @app.get("/api/public-openapi.json", include_in_schema=False)
    async def public_openapi():
        """Filtered OpenAPI schema with only public GET endpoints."""
        full_schema = app.openapi()
        return JSONResponse(_build_public_openapi(full_schema))

    @app.get("/api/docs", include_in_schema=False)
    async def public_docs():
        """Public Swagger UI — read-only endpoints only."""
        from fastapi.openapi.docs import get_swagger_ui_html
        return get_swagger_ui_html(
            openapi_url="/api/public-openapi.json",
            title="Public API — ניגוד עניינים לעם",
        )

    # ── Static frontend (SPA fallback) ───────────────────────────────────
    static_dir = _get_static_dir()
    if static_dir:
        # Root must be registered before catch-all
        @app.get("/", include_in_schema=False)
        async def serve_root():
            return FileResponse(static_dir / "index.html", media_type="text/html")

        # Catch-all: serve static files or SPA fallback
        @app.get("/{path:path}", include_in_schema=False)
        async def spa_fallback(request: Request, path: str):
            # Never intercept API / MCP / OAuth / well-known routes — let
            # the explicit routers / mounts handle them (or 404 cleanly).
            if path.startswith(("api/", "oauth/", "mcp/", ".well-known/")):
                raise HTTPException(status_code=404, detail="Not found")

            # Normalise trailing slashes — Next.js `output: "export"` with
            # the default `trailingSlash: false` writes a flat ``admin.html``
            # for the ``/admin`` route, *not* ``admin/index.html``.
            # A user visiting ``/admin/`` (trailing slash) would otherwise
            # miss the ``.html`` lookup below and fall through to the home
            # page. Strip the trailing slash before the file checks.
            normalised = path.rstrip("/")

            # Try to serve the exact file first
            file_path = static_dir / normalised
            if file_path.is_file():
                return FileResponse(file_path)

            # Try path + .html (Next.js static export generates graph.html, entity.html, etc.)
            if normalised:
                html_path = static_dir / f"{normalised}.html"
                if html_path.is_file():
                    return FileResponse(html_path, media_type="text/html")

            # Try path/index.html (some routes may still use the nested form)
            index_path = static_dir / normalised / "index.html"
            if index_path.is_file():
                return FileResponse(index_path, media_type="text/html")

            # Fallback to index.html for SPA client-side routing
            return FileResponse(static_dir / "index.html", media_type="text/html")

    return app


app = create_app()


def run():
    is_dev = os.getenv("ENV", "development") == "development"
    uvicorn.run(
        "ocoi_api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=is_dev,
    )


if __name__ == "__main__":
    run()
