"""OpenCorporates API client for matching Israeli companies."""

import httpx
from ocoi_common.config import settings
from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.matcher.opencorporates")

BASE_URL = "https://api.opencorporates.com/v0.4"


class OpenCorporatesClient:
    """Search for Israeli companies on OpenCorporates."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or settings.opencorporates_api_key

    async def search_company(
        self,
        name: str,
        jurisdiction: str = "il",
    ) -> list[dict]:
        """Search for a company by name in the Israeli jurisdiction."""
        params = {
            "q": name,
            "jurisdiction_code": jurisdiction,
            "per_page": 5,
        }
        if self.api_key:
            params["api_token"] = self.api_key

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{BASE_URL}/companies/search", params=params)
                resp.raise_for_status()
                data = resp.json()
                companies = data.get("results", {}).get("companies", [])
                return [c.get("company", c) for c in companies]
        except Exception as e:
            logger.warning(f"OpenCorporates search failed for '{name}': {e}")
            return []

    async def get_company(self, jurisdiction: str, company_number: str) -> dict | None:
        """Get company details by jurisdiction and number."""
        try:
            params = {}
            if self.api_key:
                params["api_token"] = self.api_key
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{BASE_URL}/companies/{jurisdiction}/{company_number}",
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", {}).get("company")
        except Exception:
            return None
