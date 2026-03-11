"""Client for the gov.il DynamicCollector API (ministers' conflict of interest).

Uses curl_cffi to impersonate Chrome's TLS fingerprint, bypassing Cloudflare.
"""

import asyncio
from curl_cffi import requests as cffi_requests
from ocoi_common.logging import setup_logging
from ocoi_common.models import GovilRecord, ImportedDocument

logger = setup_logging("ocoi.importer.govil")

GOVIL_API_URL = "https://www.gov.il/he/api/DynamicCollector"
GOVIL_TEMPLATE_ID = "c6e0f53e-02c0-4db1-ae89-76590f0f502e"
GOVIL_BLOB_BASE = "https://www.gov.il/BlobFolder/dynamiccollectorresultitem"

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=utf-8",
    "Origin": "https://www.gov.il",
    "Referer": "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict",
}


class GovilClient:
    """Fetches ministers' conflict of interest agreements from gov.il."""

    def __init__(self, api_url: str | None = None):
        self.api_url = api_url or GOVIL_API_URL

    def _fetch_page_sync(self, skip: int = 0) -> dict:
        """Fetch a single page (synchronous, uses curl_cffi with Chrome impersonation)."""
        payload = {
            "DynamicTemplateID": GOVIL_TEMPLATE_ID,
            "QueryFilters": {"skip": {"Query": skip}},
            "From": skip,
        }
        resp = cffi_requests.post(
            self.api_url,
            json=payload,
            headers=_HEADERS,
            impersonate="chrome",
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    async def fetch_page(self, skip: int = 0) -> dict:
        """Fetch a single page (async wrapper)."""
        return await asyncio.to_thread(self._fetch_page_sync, skip)

    async def fetch_all_records(self, per_page: int = 20) -> list[GovilRecord]:
        """Fetch all records by paginating through all pages."""
        records: list[GovilRecord] = []

        # First page to get total
        first_page = await self.fetch_page(skip=0)
        total = first_page.get("TotalResults", 0)
        items = self._extract_items(first_page)
        records.extend(items)
        logger.info(f"Gov.il: {total} total records, fetched first {len(items)}")

        # Remaining pages
        skip = per_page
        while skip < total:
            page_data = await self.fetch_page(skip=skip)
            items = self._extract_items(page_data)
            if not items:
                break
            records.extend(items)
            logger.info(f"Fetched {len(records)}/{total} gov.il records")
            skip += per_page

        return records

    def _extract_items(self, page_data: dict) -> list[GovilRecord]:
        """Extract GovilRecord objects from API response."""
        items = page_data.get("Results", [])
        records = []
        for item in items:
            if not isinstance(item, dict):
                continue
            data = item.get("Data", {})
            url_name = item.get("UrlName", "")

            # Extract PDF info from the 'file' array
            files = data.get("file", [])
            pdf_file = files[0] if files else {}
            pdf_filename = pdf_file.get("FileName", "")
            pdf_display = pdf_file.get("DisplayName", "")
            pdf_size = int(pdf_file.get("FileSize", 0) or 0)

            # Build full PDF URL
            pdf_url = None
            if pdf_filename and url_name:
                pdf_url = f"{GOVIL_BLOB_BASE}/{url_name}/he/{pdf_filename}"

            # Map list IDs to position types
            position_ids = data.get("list", [])
            position_type = self._map_position_type(position_ids[0] if position_ids else "")

            ministry_ids = data.get("government_ministry", [])

            record = GovilRecord(
                name=data.get("function", ""),
                position_type=position_type,
                ministry=ministry_ids[0] if ministry_ids else None,
                date=data.get("date"),
                pdf_url=pdf_url,
                raw_data={
                    "url_name": url_name,
                    "pdf_display": pdf_display,
                    "pdf_size": pdf_size,
                    "position_type_id": position_ids[0] if position_ids else None,
                    "ministry_id": ministry_ids[0] if ministry_ids else None,
                },
            )
            records.append(record)
        return records

    @staticmethod
    def _map_position_type(type_id: str) -> str:
        """Map position type ID to Hebrew label."""
        return {"1": "שר", "2": "סגן שר"}.get(str(type_id), str(type_id))

    def record_to_document(self, record: GovilRecord) -> ImportedDocument | None:
        """Convert a GovilRecord to an ImportedDocument."""
        if not record.pdf_url:
            return None

        pdf_display = record.raw_data.get("pdf_display", "")
        title = pdf_display or f"הסדר ניגוד עניינים - {record.name}"

        return ImportedDocument(
            source_type="govil",
            source_id=f"govil_{record.name}_{record.date or 'unknown'}",
            title=title,
            file_url=record.pdf_url,
            file_format="pdf",
            file_size=record.raw_data.get("pdf_size"),
            metadata={
                "name": record.name,
                "position_type": record.position_type,
                "ministry": record.ministry,
                "date": record.date,
                "pdf_display": pdf_display,
            },
        )
