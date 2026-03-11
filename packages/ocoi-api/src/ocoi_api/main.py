"""FastAPI application factory."""

import os
from pathlib import Path
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from ocoi_api.routers import search, entities, connections, documents, external, auth, admin
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
    """Create database tables on startup. Retries on cold-start when DB may still be waking up."""
    import asyncio
    settings.ensure_dirs()
    from ocoi_db.engine import create_all_tables
    for attempt in range(5):
        try:
            await create_all_tables()
            break
        except Exception as e:
            if attempt < 4:
                wait = 2 ** attempt
                print(f"DB connect attempt {attempt + 1}/5 failed: {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"DB connection failed after 5 attempts: {e}. Starting anyway.")

    # Cleanup: bulk-delete metadata-only documents (no PDF content)
    try:
        from sqlalchemy import select, delete, func
        from ocoi_db.engine import async_session_factory
        from ocoi_db.models import Document, ExtractionRun, EntityRelationship
        async with async_session_factory() as session:
            # Get IDs only (not full objects) to minimize memory
            empty_filter = (Document.markdown_content.is_(None)) | (Document.markdown_content == "")
            count_result = await session.execute(select(func.count()).select_from(Document).where(empty_filter))
            count = count_result.scalar() or 0
            if count > 0:
                id_subq = select(Document.id).where(empty_filter)
                await session.execute(delete(ExtractionRun).where(ExtractionRun.document_id.in_(id_subq)))
                await session.execute(delete(EntityRelationship).where(EntityRelationship.document_id.in_(id_subq)))
                await session.execute(delete(Document).where(empty_filter))
                await session.commit()
                print(f"Startup cleanup: deleted {count} metadata-only documents")
    except Exception as e:
        print(f"Startup cleanup skipped: {e}")

    # One-shot Gov.il import: if govil_records.json exists, import and delete
    import json
    govil_file = Path("/app/data/govil_records.json")
    if govil_file.exists():
        print(f"Found govil_records.json — starting one-shot import...")
        try:
            raw_items = json.loads(govil_file.read_text(encoding="utf-8"))
            print(f"Loaded {len(raw_items)} Gov.il records, starting background import...")
            from ocoi_api.services.import_service import run_govil_with_records
            asyncio.ensure_future(run_govil_with_records(raw_items))
            govil_file.unlink()
            print("govil_records.json deleted (import running in background)")
        except Exception as e:
            print(f"Gov.il one-shot import failed: {e}")

    yield


def _get_allowed_origins() -> list[str]:
    """Return allowed origins from env var for external API consumers."""
    origins_env = os.getenv("ALLOWED_ORIGINS", "")
    if origins_env:
        return [o.strip() for o in origins_env.split(",") if o.strip()]
    return []


def _get_static_dir() -> Path | None:
    """Return the static files directory if it exists."""
    static_dir = Path(os.getenv("STATIC_DIR", "./frontend/out"))
    if static_dir.is_dir() and (static_dir / "index.html").exists():
        return static_dir
    return None


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

    # CORS — only needed for external API consumers (frontend is same-origin)
    allowed_origins = _get_allowed_origins()
    if allowed_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Content-Type", "Authorization"],
        )

    # ── API routers (matched first) ──────────────────────────────────────
    app.include_router(search.router, prefix="/api/v1")
    app.include_router(entities.router, prefix="/api/v1")
    app.include_router(connections.router, prefix="/api/v1")
    app.include_router(documents.router, prefix="/api/v1")
    app.include_router(external.router, prefix="/api/v1")
    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(admin.router, prefix="/api/v1")

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

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
            # Try to serve the exact file first
            file_path = static_dir / path
            if file_path.is_file():
                return FileResponse(file_path)

            # Try path + .html (Next.js static export generates graph.html, entity.html, etc.)
            html_path = static_dir / f"{path}.html"
            if html_path.is_file():
                return FileResponse(html_path, media_type="text/html")

            # Try path/index.html
            index_path = static_dir / path / "index.html"
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
