"""Local processor configuration — loads from .env file."""

import os
from dataclasses import dataclass, field
from pathlib import Path

# Try dotenv, fall back gracefully
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore


@dataclass
class LocalConfig:
    server_url: str = "https://www.ocoi.org.il"
    push_api_key: str = ""
    deepseek_api_key: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    ckan_base_url: str = "https://www.odata.org.il"
    ckan_query: str = "ניגוד עניינים"
    cache_dir: Path = field(default_factory=lambda: Path("./cache"))
    batch_size: int = 10

    def validate(self) -> list[str]:
        """Return list of missing required settings."""
        errors = []
        if not self.push_api_key:
            errors.append("PUSH_API_KEY is required")
        if not self.deepseek_api_key:
            errors.append("DEEPSEEK_API_KEY is required (for extraction phase)")
        return errors


def load_config() -> LocalConfig:
    """Load config from .env file next to this module, then from env vars."""
    env_file = Path(__file__).parent / ".env"
    if load_dotenv and env_file.exists():
        load_dotenv(env_file, override=True)

    return LocalConfig(
        server_url=os.getenv("OCOI_SERVER_URL", "https://www.ocoi.org.il").rstrip("/"),
        push_api_key=os.getenv("PUSH_API_KEY", ""),
        deepseek_api_key=os.getenv("DEEPSEEK_API_KEY", ""),
        deepseek_base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        ckan_base_url=os.getenv("CKAN_BASE_URL", "https://www.odata.org.il"),
        ckan_query=os.getenv("CKAN_QUERY", "ניגוד עניינים"),
        cache_dir=Path(os.getenv("LOCAL_CACHE_DIR", "./cache")),
        batch_size=int(os.getenv("BATCH_SIZE", "10")),
    )
