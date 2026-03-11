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

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy Python project files
COPY pyproject.toml ./
COPY packages/ ./packages/

# Install only API + its deps (skip heavy extractor/converter packages)
RUN uv sync --no-dev --no-editable --package ocoi-api

# Copy built frontend from stage 1
COPY --from=frontend-build /app/frontend/out /app/static

# Create data directory for SQLite (if needed)
RUN mkdir -p /app/data

# Environment
ENV STATIC_DIR=/app/static
ENV ENV=production
ENV API_HOST=0.0.0.0
ENV API_PORT=8000

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "ocoi_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
