# ── Stage 1: Build frontend ──────────────────────────────────────────────
FROM node:20-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ── Stage 2: Python application ─────────────────────────────────────────
FROM python:3.13-slim AS runtime
WORKDIR /app

# Install lightweight PDF tools (poppler pdftotext ~5MB RSS vs pymupdf ~150MB)
# and Tesseract OCR with Hebrew model for scanned PDFs
RUN apt-get update && \
    apt-get install -y --no-install-recommends poppler-utils tesseract-ocr curl && \
    rm -rf /var/lib/apt/lists/* && \
    TESS_DIR=$(find /usr/share/tesseract-ocr -name "tessdata" -type d 2>/dev/null | head -1) && \
    curl -sL -o "$TESS_DIR/heb.traineddata" \
      https://github.com/tesseract-ocr/tessdata_best/raw/main/heb.traineddata && \
    echo "Hebrew tessdata installed successfully"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Create venv
RUN uv venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV VIRTUAL_ENV="/app/.venv"

# Copy packages the API needs (skip heavy extractor/converter)
COPY packages/ocoi-common/ ./packages/ocoi-common/
COPY packages/ocoi-db/ ./packages/ocoi-db/
COPY packages/ocoi-api/ ./packages/ocoi-api/
COPY packages/ocoi-importer/ ./packages/ocoi-importer/
COPY packages/ocoi-matcher/ ./packages/ocoi-matcher/

# Strip [tool.uv.sources] workspace refs (not needed outside workspace)
RUN sed -i '/^\[tool\.uv/,$d' packages/*/pyproject.toml

# Strip heavy Python PDF libs — we use poppler pdftotext CLI instead (~5MB vs ~150MB)
RUN sed -i '/"pdfplumber/d; /"pymupdf/d' packages/ocoi-api/pyproject.toml

# Strip playwright — Gov.il scraping is a one-time op, saves ~15MB idle memory
RUN sed -i '/"playwright/d' packages/ocoi-importer/pyproject.toml

# Install via pip (bypasses workspace resolution — no torch/transformers)
RUN uv pip install ./packages/ocoi-common ./packages/ocoi-db ./packages/ocoi-api ./packages/ocoi-importer ./packages/ocoi-matcher

# Copy built frontend from stage 1
COPY --from=frontend-build /app/frontend/out /app/static

# Create data directories
RUN mkdir -p /app/data /app/data/pdfs /app/data/markdown

# Environment
ENV STATIC_DIR=/app/static
ENV ENV=production
ENV API_HOST=0.0.0.0
ENV API_PORT=8000
ENV JWT_ALGORITHM=HS256
ENV JWT_EXPIRE_MINUTES=480

EXPOSE 8000

# Run uvicorn directly (no uv run which triggers re-sync)
CMD ["uvicorn", "ocoi_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
