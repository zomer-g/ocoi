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

    # CKAN source
    ckan_base_url: str = "https://www.odata.org.il"
    ckan_search_query: str = "ניגוד עניינים"

    # Gov.il source
    govil_collector_url: str = "https://www.gov.il/he/api/DynamicCollector"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.pdf_dir.mkdir(parents=True, exist_ok=True)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
