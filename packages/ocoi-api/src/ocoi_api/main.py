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
    from ocoi_db.engine import create_all_tables, run_migrations
    for attempt in range(5):
        try:
            await create_all_tables()
            await run_migrations()
            break
        except Exception as e:
            if attempt < 4:
                wait = 2 ** attempt
                print(f"DB connect attempt {attempt + 1}/5 failed: {e}. Retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"DB connection failed after 5 attempts: {e}. Starting anyway.")

    # NOTE: Auto-delete of metadata-only docs was removed — it destroyed
    # scanned PDFs that hadn't been OCR'd yet. Use DELETE /admin/documents/purge/metadata-only instead.

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

    @app.post("/api/debug/test-upload")
    async def debug_test_upload():
        """Temporary diagnostic: test PDF conversion pipeline on Render."""
        import hashlib
        import traceback as tb
        results = {}
        try:
            # Test 1: import pdf_converter
            from ocoi_api.services.pdf_converter import convert_pdf_bytes
            results["import"] = "ok"
            # Test 2: convert a tiny fake PDF
            fake_pdf = b"%PDF-1.0\n1 0 obj<</Pages 2 0 R>>endobj 2 0 obj<</Kids[]>>endobj\nxref\n0 3\ntrailer<</Root 1 0 R>>\nstartxref\n9\n%%EOF"
            try:
                md = convert_pdf_bytes(fake_pdf, "test")
                results["convert"] = f"ok, md={'yes' if md else 'no'}"
            except Exception as e:
                results["convert"] = f"error: {e}"
            # Test 3: DB write test
            from ocoi_db.engine import async_session_factory
            from ocoi_db.models import Document
            from sqlalchemy import select, text
            async with async_session_factory() as session:
                # Check pdf_content column exists
                r = await session.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='documents' AND column_name='pdf_content'"
                ))
                col = r.scalar_one_or_none()
                results["pdf_content_column"] = "exists" if col else "MISSING"
                # Check content_hash column
                r2 = await session.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name='documents' AND column_name='content_hash'"
                ))
                col2 = r2.scalar_one_or_none()
                results["content_hash_column"] = "exists" if col2 else "MISSING"
        except Exception as e:
            results["error"] = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
        return results

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
