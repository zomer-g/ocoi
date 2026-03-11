"""Client for the gov.il DynamicCollector API (ministers' conflict of interest)."""

import httpx
from ocoi_common.config import settings
from ocoi_common.logging import setup_logging
from ocoi_common.models import GovilRecord, ImportedDocument

logger = setup_logging("ocoi.importer.govil")

# The gov.il DynamicCollector uses a POST-based API
GOVIL_COLLECTOR_ID = "ministers_conflict"

# Browser-like headers to avoid Cloudflare 403
_BROWSER_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://www.gov.il",
    "Referer": "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict",
}


class GovilClient:
    """Fetches ministers' conflict of interest agreements from gov.il."""

    def __init__(self, base_url: str | None = None):
        self.base_url = base_url or settings.govil_collector_url

    async def fetch_page(self, skip: int = 0, limit: int = 20) -> dict:
        """Fetch a single page from the DynamicCollector API."""
        payload = {
            "DynamicTemplateID": GOVIL_COLLECTOR_ID,
            "QueryFilters": {"skip": skip, "limit": limit},
            "From": skip,
            "Size": limit,
        }
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.post(self.base_url, json=payload, headers=_BROWSER_HEADERS)
            resp.raise_for_status()
            return resp.json()

    async def fetch_all_records(self, per_page: int = 20) -> list[GovilRecord]:
        """Fetch all records by paginating through all pages."""
        records = []
        skip = 0

        # First page to get total
        first_page = await self.fetch_page(skip=0, limit=per_page)
        total = first_page.get("TotalResults", first_page.get("totalResults", 345))
        items = self._extract_items(first_page)
        records.extend(items)
        logger.info(f"Gov.il: {total} total records, fetched first {len(items)}")

        skip = per_page
        while skip < total:
            page_data = await self.fetch_page(skip=skip, limit=per_page)
            items = self._extract_items(page_data)
            if not items:
                break
            records.extend(items)
            logger.info(f"Fetched {len(records)}/{total} gov.il records")
            skip += per_page

        return records

    def _extract_items(self, page_data: dict) -> list[GovilRecord]:
        """Extract GovilRecord objects from API response."""
        items = page_data.get("Results", page_data.get("results", []))
        records = []
        for item in items:
            data = item.get("Data", item) if isinstance(item, dict) else item
            record = GovilRecord(
                name=self._get_field(data, "שם בעל התפקיד", "name"),
                position_type=self._get_field(data, "סוג התפקיד", "positionType"),
                ministry=self._get_field(data, "משרד", "ministry"),
                date=self._get_field(data, "תאריך", "date"),
                pdf_url=self._extract_pdf_url(data),
                raw_data=data if isinstance(data, dict) else {},
            )
            records.append(record)
        return records

    def _get_field(self, data: dict, hebrew_key: str, english_key: str) -> str | None:
        if not isinstance(data, dict):
            return None
        return data.get(hebrew_key) or data.get(english_key)

    def _extract_pdf_url(self, data: dict) -> str | None:
        """Try to extract PDF URL from various possible field names."""
        if not isinstance(data, dict):
            return None
        for key in ("FileUrl", "fileUrl", "file_url", "Link", "link", "url"):
            if url := data.get(key):
                if isinstance(url, str) and url.endswith(".pdf"):
                    return url
                if isinstance(url, str) and "gov.il" in url:
                    return url
        # Check nested fields
        for key, val in data.items():
            if isinstance(val, str) and val.endswith(".pdf"):
                return val
        return None

    def record_to_document(self, record: GovilRecord) -> ImportedDocument | None:
        """Convert a GovilRecord to an ImportedDocument."""
        if not record.pdf_url:
            return None
        return ImportedDocument(
            source_type="govil",
            source_id=f"govil_{record.name}_{record.date or 'unknown'}",
            title=f"הסדר ניגוד עניינים - {record.name}",
            file_url=record.pdf_url,
            file_format="pdf",
            metadata={
                "name": record.name,
                "position_type": record.position_type,
                "ministry": record.ministry,
                "date": record.date,
            },
        )
