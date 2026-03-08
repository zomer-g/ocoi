"""FastAPI application factory."""

import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from ocoi_api.routers import search, entities, connections, documents, external
from ocoi_common.config import settings


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create database tables on startup (for local SQLite dev)."""
    settings.ensure_dirs()
    from ocoi_db.engine import create_all_tables
    await create_all_tables()
    yield


def _get_allowed_origins() -> list[str]:
    """Return allowed origins from env var, defaulting to localhost for dev."""
    origins_env = os.getenv("ALLOWED_ORIGINS", "")
    if origins_env:
        return [o.strip() for o in origins_env.split(",") if o.strip()]
    # Default: local development origins only
    return [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]


def create_app() -> FastAPI:
    app = FastAPI(
        title="אינטרסים לעם API",
        description="Conflict of Interest Transparency Platform API",
        version="0.1.0",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )

    # Security headers
    app.add_middleware(SecurityHeadersMiddleware)

    # CORS — restricted to explicit origins, no wildcard credentials
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_allowed_origins(),
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization"],
    )

    app.include_router(search.router, prefix="/api/v1")
    app.include_router(entities.router, prefix="/api/v1")
    app.include_router(connections.router, prefix="/api/v1")
    app.include_router(documents.router, prefix="/api/v1")
    app.include_router(external.router, prefix="/api/v1")

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

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
