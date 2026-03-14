<div align="center" dir="rtl">

# אינטרסים לעם

**פלטפורמת שקיפות להסדרי ניגוד עניינים של בעלי תפקידים ציבוריים בישראל**

[ocoi.org.il](https://ocoi.org.il)

</div>

---

OCOI aggregates, processes, and visualizes conflict-of-interest declarations of Israeli public officials. It extracts structured data from PDF documents using Hebrew NLP and LLMs, maps relationships between officials and corporate entities, and presents them through a searchable web interface with interactive graph visualizations.

## Architecture

Monorepo managed with [uv](https://docs.astral.sh/uv/) workspaces:

```
packages/
  ocoi-api/         FastAPI server — search, graph queries, admin panel, auth
  ocoi-common/      Shared config, Pydantic models, utilities
  ocoi-db/          SQLAlchemy ORM, async DB engine, graph CTEs, migrations
  ocoi-importer/    Data ingestion from CKAN (odata.org.il) and gov.il
  ocoi-converter/   PDF → Markdown conversion with Hebrew OCR
  ocoi-extractor/   Hebrew NER (DictaBERT) + LLM-based entity extraction
  ocoi-matcher/     Fuzzy matching of entities to official registries
frontend/           Next.js 15 / React 19 / Tailwind 4 — Hebrew RTL UI
tools/              Local processor for offline PDF processing
```

### Data Pipeline

```
Import (CKAN / gov.il PDFs)
  → Convert (PDF → Markdown via OCR)
  → Extract (Hebrew NER + DeepSeek LLM → entities & relationships)
  → Match (fuzzy-match to company/association registries)
  → Serve (search, graph queries, admin UI)
```

## Tech Stack

| Layer | Stack |
|-------|-------|
| **API** | Python 3.12, FastAPI, SQLAlchemy 2 (async), PostgreSQL / SQLite |
| **Frontend** | Next.js 15, React 19, TypeScript, Tailwind CSS 4, Cytoscape.js |
| **NLP** | DictaBERT (Hebrew NER), DeepSeek LLM, rapidfuzz |
| **PDF** | PyMuPDF, marker-pdf, pdfplumber |
| **Auth** | Google OAuth 2.0, JWT (httpOnly cookies), admin email whitelist |
| **Infra** | Render (production), Cloudflare (DNS/CDN) |

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Node.js 18+
- PostgreSQL (production) or SQLite (development)

### Setup

```bash
# Clone
git clone https://github.com/zomer-g/ocoi.git
cd ocoi

# Install Python dependencies
uv sync

# Copy environment config
cp .env.example .env
# Edit .env with your settings (database URL, API keys, etc.)

# Install frontend dependencies
cd frontend
npm install
cd ..
```

### Run (Development)

```bash
# Start API server (http://localhost:8000)
uv run ocoi-api

# In another terminal — start frontend dev server (http://localhost:3000)
cd frontend
npm run dev
```

### Run the Pipeline

```bash
# Import PDFs from CKAN
uv run ocoi-import

# Convert PDFs to Markdown
uv run ocoi-convert

# Extract entities and relationships
uv run ocoi-extract

# Match entities to official registries
uv run ocoi-match
```

## Configuration

Key environment variables (see `.env.example` for full list):

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Async SQLAlchemy URI (`sqlite+aiosqlite:///` or `postgresql+asyncpg://`) |
| `DEEPSEEK_API_KEY` | LLM API key for entity extraction |
| `JWT_SECRET_KEY` | Token signing secret (**must** be randomized in production) |
| `GOOGLE_CLIENT_ID` | OAuth 2.0 client ID for admin login |
| `GOOGLE_CLIENT_SECRET` | OAuth 2.0 client secret |
| `ADMIN_EMAILS` | Comma-separated Google email whitelist for admin access |
| `ENV` | `development` or `production` |

## Local Processor

For offline PDF processing without a server, use the standalone local tool:

```bash
cd tools
pip install httpx pymupdf openai python-dotenv
python local_app.py
# Open http://localhost:5555
```

## API

Public read-only API documentation is available at [`/api/docs`](https://ocoi.org.il/api/docs).

Key endpoints:
- `GET /api/v1/search?q=...` — Full-text search across all entities
- `GET /api/v1/graph/neighbors/{id}?type=...&depth=1` — Entity relationship graph
- `GET /api/v1/graph/path?from_id=...&to_id=...` — Find path between entities
- `GET /api/v1/persons/{id}`, `/companies/{id}`, `/associations/{id}` — Entity details

## License

This project is currently published without a formal license. All rights reserved by the author. If you'd like to use or contribute to this project, please reach out.

## Contact

zomer@octopus.org.il
