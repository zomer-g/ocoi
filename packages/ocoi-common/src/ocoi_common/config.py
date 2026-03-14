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

    # Gov.il source
    govil_collector_url: str = "https://www.gov.il/he/api/DynamicCollector"

    # Israeli Government Data Registry (DATAGOV)
    datagov_base_url: str = "https://data.gov.il"
    registry_match_threshold: float = 0.85
    registry_sync_batch_size: int = 2000

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
