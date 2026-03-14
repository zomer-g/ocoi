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

# Install Tesseract OCR with best-quality Hebrew model (for scanned PDFs)
RUN apt-get update && \
    apt-get install -y --no-install-recommends tesseract-ocr curl && \
    rm -rf /var/lib/apt/lists/* && \
    TESS_DIR=$(find /usr/share/tesseract-ocr -name "tessdata" -type d 2>/dev/null | head -1) && \
    echo "Tessdata directory: $TESS_DIR" && \
    curl -sL -o "$TESS_DIR/heb.traineddata" \
      https://github.com/tesseract-ocr/tessdata_best/raw/main/heb.traineddata && \
    ls -la "$TESS_DIR/heb.traineddata" && \
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

# Install via pip (bypasses workspace resolution — no torch/transformers)
RUN uv pip install ./packages/ocoi-common ./packages/ocoi-db ./packages/ocoi-api ./packages/ocoi-importer ./packages/ocoi-matcher

# Copy built frontend from stage 1
COPY --from=frontend-build /app/frontend/out /app/static

# Create data directories
RUN mkdir -p /app/data /app/data/pdfs /app/data/markdown

# Gov.il records already imported — no longer copy on every build
# COPY data/govil_records.json /app/data/govil_records.json

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
