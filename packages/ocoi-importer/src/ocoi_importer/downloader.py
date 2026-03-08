"""PDF download manager with retry logic and deduplication."""

import hashlib
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ocoi_common.config import settings
from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.importer.downloader")


class Downloader:
    """Downloads PDF files with retry logic and deduplication."""

    def __init__(self, dest_dir: Path | None = None):
        self.dest_dir = dest_dir or settings.pdf_dir
        self.dest_dir.mkdir(parents=True, exist_ok=True)

    def _url_to_filename(self, url: str) -> str:
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        # Extract original filename if possible
        parts = url.rstrip("/").split("/")
        original = parts[-1] if parts else "document"
        # Clean filename
        original = original.split("?")[0]
        if not original.endswith(".pdf"):
            original += ".pdf"
        return f"{url_hash}_{original}"

    def get_local_path(self, url: str) -> Path:
        return self.dest_dir / self._url_to_filename(url)

    def is_downloaded(self, url: str) -> bool:
        path = self.get_local_path(url)
        return path.exists() and path.stat().st_size > 0

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    async def download(self, url: str, force: bool = False) -> Path | None:
        """Download a file from URL. Returns local path or None on failure."""
        local_path = self.get_local_path(url)

        if not force and self.is_downloaded(url):
            logger.debug(f"Already downloaded: {local_path.name}")
            return local_path

        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()

                local_path.write_bytes(resp.content)
                logger.info(f"Downloaded: {local_path.name} ({len(resp.content)} bytes)")
                return local_path
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            raise

    async def download_batch(
        self,
        urls: list[str],
        max_concurrent: int = 5,
    ) -> dict[str, Path | None]:
        """Download multiple files. Returns mapping of url -> local_path."""
        import asyncio

        semaphore = asyncio.Semaphore(max_concurrent)
        results: dict[str, Path | None] = {}

        async def _download_one(url: str):
            async with semaphore:
                try:
                    path = await self.download(url)
                    results[url] = path
                except Exception:
                    results[url] = None

        tasks = [_download_one(url) for url in urls]
        await asyncio.gather(*tasks)

        success = sum(1 for v in results.values() if v is not None)
        logger.info(f"Downloaded {success}/{len(urls)} files")
        return results
