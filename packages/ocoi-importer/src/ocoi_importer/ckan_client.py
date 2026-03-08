"""Client for the CKAN API at odata.org.il."""

import httpx
from ocoi_common.config import settings
from ocoi_common.logging import setup_logging
from ocoi_common.models import CkanDataset, ImportedDocument

logger = setup_logging("ocoi.importer.ckan")


class CkanClient:
    """Fetches conflict of interest datasets from odata.org.il CKAN API."""

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or settings.ckan_base_url).rstrip("/")
        self.search_url = f"{self.base_url}/api/3/action/package_search"

    async def get_total_count(self, query: str | None = None) -> int:
        q = query or settings.ckan_search_query
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(self.search_url, params={"q": q, "rows": 0})
            resp.raise_for_status()
            return resp.json()["result"]["count"]

    async def search_datasets(
        self,
        query: str | None = None,
        rows: int = 100,
        start: int = 0,
    ) -> list[CkanDataset]:
        q = query or settings.ckan_search_query
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                self.search_url,
                params={"q": q, "rows": rows, "start": start},
            )
            resp.raise_for_status()
            results = resp.json()["result"]["results"]
            return [CkanDataset(**r) for r in results]

    async def fetch_all_datasets(
        self,
        query: str | None = None,
        batch_size: int = 100,
    ) -> list[CkanDataset]:
        total = await self.get_total_count(query)
        logger.info(f"Found {total} CKAN datasets for query")
        all_datasets = []
        for start in range(0, total, batch_size):
            batch = await self.search_datasets(query, rows=batch_size, start=start)
            all_datasets.extend(batch)
            logger.info(f"Fetched {len(all_datasets)}/{total} datasets")
        return all_datasets

    def extract_documents(self, dataset: CkanDataset) -> list[ImportedDocument]:
        """Extract downloadable document references from a dataset."""
        docs = []
        for resource in dataset.resources:
            fmt = (resource.get("format") or "").upper()
            url = resource.get("url", "")
            if not url:
                continue
            if fmt in ("PDF", "DOCX", "DOC", "JPEG", "JPG", "PNG"):
                docs.append(ImportedDocument(
                    source_type="ckan",
                    source_id=dataset.id,
                    title=resource.get("name") or dataset.title,
                    file_url=url,
                    file_format=fmt.lower(),
                    file_size=resource.get("size"),
                    metadata={
                        "dataset_title": dataset.title,
                        "dataset_notes": dataset.notes,
                        "resource_id": resource.get("id"),
                        "tags": [t.get("name", "") for t in dataset.tags],
                    },
                ))
        return docs
