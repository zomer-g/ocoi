from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # Database (defaults to local SQLite — set to postgresql+asyncpg://... for production)
    database_url: str = "sqlite+aiosqlite:///./data/ocoi.db"
    database_url_sync: str = "sqlite:///./data/ocoi.db"

    @model_validator(mode="after")
    def _fix_pg_urls(self):
        """Render provides postgres:// — convert to SQLAlchemy-compatible schemes."""
        if self.database_url.startswith("postgres://"):
            self.database_url = self.database_url.replace(
                "postgres://", "postgresql+asyncpg://", 1
            )
            self.database_url_sync = self.database_url.replace(
                "postgresql+asyncpg://", "postgresql://", 1
            )
        elif self.database_url.startswith("postgresql://"):
            self.database_url_sync = self.database_url
            self.database_url = self.database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )
        return self

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # DeepSeek API
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"

    # OpenCorporates
    opencorporates_api_key: str = ""

    # File storage
    data_dir: Path = Path("./data")
    pdf_dir: Path = Path("./data/pdfs")
    markdown_dir: Path = Path("./data/markdown")

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    env: str = "development"

    # Google OAuth
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8000/api/v1/auth/callback"

    # JWT
    jwt_secret_key: str = "change-me-to-a-random-secret"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 480  # 8 hours

    @model_validator(mode="after")
    def _check_jwt_secret(self):
        if self.env == "production" and self.jwt_secret_key == "change-me-to-a-random-secret":
            raise ValueError(
                "JWT_SECRET_KEY must be set to a strong random value in production"
            )
        return self

    # Admin whitelist (comma-separated Google email addresses)
    admin_emails: str = ""

    @property
    def admin_email_set(self) -> set[str]:
        if not self.admin_emails:
            return set()
        return {e.strip().lower() for e in self.admin_emails.split(",") if e.strip()}

    @property
    def is_production(self) -> bool:
        return self.env == "production"

    # CKAN source
    ckan_base_url: str = "https://www.odata.org.il"
    ckan_search_query: str = "ניגוד עניינים"

    # odata.org.il snapshot dataset (replaces direct gov.il scraping)
    odata_dataset_page: str = "https://www.odata.org.il/dataset/gov-versions-scraper-ministers_conflict-274fd0fe"

    # Push API (local processor → server)
    push_api_key: str = ""

    # Israeli Government Data Registry (DATAGOV)
    datagov_base_url: str = "https://data.gov.il"
    registry_match_threshold: float = 0.85
    registry_sync_batch_size: int = 2000

    # ── MCP server ──────────────────────────────────────────────────────
    # Kill switch — when False the /mcp endpoint + OAuth metadata return
    # 404/503 so we can disable the surface without redeploying.
    mcp_enabled: bool = True
    # Closed-beta gate. When True, Google sign-in succeeds only for
    # emails that an admin has pre-invited (= a User row already exists
    # with role 'mcp_user' or higher). When False, any Google identity
    # gets auto-provisioned on first login — appropriate for fully-public
    # data, not for anything we plan to bill.
    mcp_invite_only: bool = True
    # Canonical public URL OAuth clients see — used as ``issuer`` in the
    # /.well-known metadata and as the redirect target Google calls back
    # to. In dev defaults to localhost; production sets this to the apex.
    mcp_public_url: str = "http://localhost:8000"
    # Access tokens issued for the MCP audience live this long. Short by
    # design — refresh tokens cover the long tail.
    mcp_access_token_minutes: int = 15
    mcp_refresh_token_days: int = 30
    # Authorization codes are single-use; this is just a safety upper bound.
    mcp_authorization_code_minutes: int = 10
    # OAuth audience claim that separates MCP tokens from the admin
    # cookie tokens — both are signed by ``jwt_secret_key`` but cross-use
    # is rejected by the verifier.
    mcp_jwt_audience: str = "mcp"
    # Machine-to-machine bypass. A shared secret that lets a trusted
    # gateway (the "חיפוש רוחבי" discovery gateway) query /mcp without an
    # interactive Google login: a request whose Bearer token equals this
    # value skips JWT verification and runs as a synthetic service
    # principal. Empty (the default) disables the bypass entirely — the
    # only auth path is then OAuth. Equivalent to full MCP access; keep it
    # in secrets only (Render sync:false / .env), never in the repo.
    mcp_service_token: str = ""

    # ── Stripe (metered billing) ────────────────────────────────────────
    stripe_secret_key: str = ""
    # The price ID for the per-tool-call metered product. When unset the
    # billing batcher is a no-op (handy for dev — usage_events still get
    # written, just never pushed).
    stripe_metered_price_id: str = ""

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
