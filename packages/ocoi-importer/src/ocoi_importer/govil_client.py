"""Client for the gov.il DynamicCollector API (ministers' conflict of interest).

Multi-strategy approach:
  1. Direct httpx POST to the API endpoint (fast, works if API isn't CF-protected)
  2. Playwright stealth browser with response interception (handles CF challenges)
"""

import asyncio
import httpx
from ocoi_common.logging import setup_logging
from ocoi_common.models import GovilRecord, ImportedDocument

logger = setup_logging("ocoi.importer.govil")

GOVIL_PAGE_URL = "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict"
GOVIL_API_URL = "https://www.gov.il/he/api/DynamicCollector"
GOVIL_TEMPLATE_ID = "c6e0f53e-02c0-4db1-ae89-76590f0f502e"
GOVIL_BLOB_BASE = "https://www.gov.il/BlobFolder/dynamiccollectorresultitem"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Stealth init script — removes common headless browser fingerprints
_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
delete navigator.__proto__.webdriver;
window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const p = [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
            { name: 'Native Client', filename: 'internal-nacl-plugin' },
        ];
        p.length = 3;
        return p;
    }
});
Object.defineProperty(navigator, 'languages', {
    get: () => ['he-IL', 'he', 'en-US', 'en']
});
if (navigator.permissions) {
    const oq = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : oq(p);
}
const gp = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(p) {
    if (p === 37445) return 'Intel Inc.';
    if (p === 37446) return 'Intel Iris OpenGL Engine';
    return gp.call(this, p);
};
"""


class GovilClient:
    """Fetches ministers' conflict of interest agreements from gov.il."""

    async def fetch_all_records(self, per_page: int = 20) -> list[GovilRecord]:
        """Fetch all records, trying multiple strategies."""
        # Strategy 1: Direct API call (fast — no browser needed)
        try:
            logger.info("Strategy 1: Trying direct API access...")
            return await self._fetch_direct(per_page)
        except Exception as e:
            logger.warning(f"Direct API failed: {e}")

        # Strategy 2: Playwright stealth browser with retries
        last_error = None
        for attempt in range(1, 3):
            try:
                logger.info(f"Strategy 2: Playwright attempt {attempt}/2...")
                return await self._fetch_with_browser(per_page)
            except Exception as e:
                last_error = e
                logger.warning(f"Playwright attempt {attempt} failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(8)

        raise RuntimeError(
            f"Gov.il fetch failed (all strategies exhausted). Last error: {last_error}"
        )

    # ── Strategy 1: Direct httpx ──────────────────────────────────────────

    async def _fetch_direct(self, per_page: int) -> list[GovilRecord]:
        """Fetch via direct httpx POST to the DynamicCollector API."""
        # Gov.il API may cap results per request at ~20, so use small page size
        # and paginate through all results step by step
        page_size = 20
        headers = {
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://www.gov.il",
            "Referer": GOVIL_PAGE_URL,
            "User-Agent": _UA,
            "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }

        async with httpx.AsyncClient(
            timeout=30, follow_redirects=True, headers=headers
        ) as client:
            first = await self._httpx_fetch(client, 0, quantity=page_size)
            total_reported = first.get("TotalResults", 0)
            all_items = list(first.get("Results", []))
            logger.info(
                f"Direct API: TotalResults={total_reported}, "
                f"first page returned {len(all_items)} items"
            )

            # Paginate: always probe next pages regardless of TotalResults
            skip = len(all_items)
            empty_streak = 0
            while empty_streak < 3:  # stop after 3 consecutive empty pages
                if skip >= 10000:  # safety cap
                    break
                data = await self._httpx_fetch(client, skip, quantity=page_size)
                results = data.get("Results", [])
                if not results:
                    empty_streak += 1
                    skip += page_size
                    logger.info(f"Direct API: empty page at skip={skip} (streak={empty_streak})")
                    continue
                empty_streak = 0
                all_items.extend(results)
                skip += len(results)
                logger.info(f"Direct API: fetched {len(all_items)} records total (skip={skip})")

        records = [r for item in all_items if (r := self._parse_item(item))]
        logger.info(f"Direct API: parsed {len(records)} records total")
        return records

    async def _httpx_fetch(self, client: httpx.AsyncClient, skip: int, quantity: int = 20) -> dict:
        # Gov.il pagination uses skip inside QueryFilters (matches ?skip=N in URL)
        query_filters = {"skip": {"Query": str(skip)}} if skip > 0 else {}
        resp = await client.post(
            GOVIL_API_URL,
            json={
                "DynamicTemplateID": GOVIL_TEMPLATE_ID,
                "QueryFilters": query_filters,
                "From": skip,
                "Quantity": quantity,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or "Results" not in data:
            raise ValueError(f"Unexpected API response: {str(data)[:200]}")
        return data

    # ── Strategy 2: Playwright stealth browser ────────────────────────────

    async def _fetch_with_browser(self, per_page: int) -> list[GovilRecord]:
        """Fetch via stealth Playwright browser — bypasses Cloudflare."""
        from playwright.async_api import async_playwright

        captured_api_data: list[dict] = []

        async with async_playwright() as p:
            logger.info("Launching stealth Chromium...")
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--window-size=1920,1080",
                ],
            )
            try:
                context = await browser.new_context(
                    locale="he-IL",
                    user_agent=_UA,
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )
                await context.add_init_script(_STEALTH_SCRIPT)
                page = await context.new_page()

                # Intercept DynamicCollector responses from page's own loading
                async def on_response(response):
                    if "DynamicCollector" in response.url and response.status == 200:
                        try:
                            body = await response.json()
                            captured_api_data.append(body)
                        except Exception:
                            pass

                page.on("response", on_response)

                # Navigate and handle Cloudflare
                logger.info("Navigating to Gov.il page...")
                await page.goto(GOVIL_PAGE_URL, wait_until="domcontentloaded", timeout=90000)
                title = await page.title()
                logger.info(f"Page title: '{title}'")

                if self._is_cloudflare_challenge(title):
                    logger.info("Cloudflare challenge detected, waiting up to 45s...")
                    try:
                        await page.wait_for_function(
                            """() => {
                                const t = document.title.toLowerCase();
                                return !t.includes('just a moment')
                                    && !t.includes('checking')
                                    && !t.includes('attention required')
                                    && t.length > 0;
                            }""",
                            timeout=45000,
                        )
                        logger.info(f"Challenge resolved! Title: '{await page.title()}'")
                    except Exception:
                        # Challenge didn't resolve via title, but check if we
                        # captured any API data anyway (some CF pages redirect)
                        if not captured_api_data:
                            raise RuntimeError(
                                "Cloudflare challenge could not be resolved. "
                                "The server IP is likely blocked."
                            )
                        logger.info("Challenge title persists but API data was captured")

                # Wait for network to settle
                try:
                    await page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    pass
                await asyncio.sleep(2)

                # Use captured data or make direct API calls
                if captured_api_data:
                    logger.info(f"Captured {len(captured_api_data)} API response(s)")
                    all_items = self._collect_from_captured(captured_api_data, page, per_page)
                else:
                    all_items = await self._paginate_via_browser(page, per_page)

                if isinstance(all_items, list):
                    records = [r for item in all_items if (r := self._parse_item(item))]
                else:
                    # all_items is a coroutine from _paginate_via_browser
                    items = await all_items
                    records = [r for item in items if (r := self._parse_item(item))]

                logger.info(f"Browser: parsed {len(records)} records")
                return records
            finally:
                await browser.close()

    async def _collect_from_captured(
        self, captured: list[dict], page, per_page: int
    ) -> list[dict]:
        """Collect all items: use captured first page + fetch remaining via browser."""
        first = captured[0]
        total = first.get("TotalResults", 0)
        all_items = list(first.get("Results", []))
        logger.info(f"Gov.il: {total} total records, first page has {len(all_items)}")

        skip = per_page
        while skip < total:
            try:
                data = await self._browser_fetch(page, skip)
                results = data.get("Results", [])
                if not results:
                    break
                all_items.extend(results)
                logger.info(f"Fetched {len(all_items)}/{total}")
                skip += per_page
            except Exception as e:
                logger.warning(f"Browser fetch at skip={skip} failed: {e}")
                break

        return all_items

    async def _paginate_via_browser(self, page, per_page: int) -> list[dict]:
        """Fallback: make API calls directly through the browser context."""
        logger.info("Making direct API calls through browser...")
        first = await self._browser_fetch(page, 0)
        total = first.get("TotalResults", 0)
        all_items = list(first.get("Results", []))
        logger.info(f"Gov.il: {total} total records, first page has {len(all_items)}")

        skip = per_page
        while skip < total:
            data = await self._browser_fetch(page, skip)
            results = data.get("Results", [])
            if not results:
                break
            all_items.extend(results)
            logger.info(f"Fetched {len(all_items)}/{total}")
            skip += per_page

        return all_items

    @staticmethod
    def _is_cloudflare_challenge(title: str) -> bool:
        t = title.lower()
        return any(
            s in t
            for s in ["just a moment", "checking", "attention required", "cloudflare"]
        )

    async def _browser_fetch(self, page, skip: int, quantity: int = 20) -> dict:
        """Make the DynamicCollector API call from within the browser context."""
        return await page.evaluate(
            """async (params) => {
                const qf = params.skip > 0
                    ? {skip: {Query: String(params.skip)}}
                    : {};
                const resp = await fetch('/he/api/DynamicCollector', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json;charset=utf-8'},
                    body: JSON.stringify({
                        DynamicTemplateID: params.templateId,
                        QueryFilters: qf,
                        From: params.skip,
                        Quantity: params.quantity
                    })
                });
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                return resp.json();
            }""",
            {"templateId": GOVIL_TEMPLATE_ID, "skip": skip, "quantity": quantity},
        )

    # ── Parsing ───────────────────────────────────────────────────────────

    def _parse_item(self, item: dict) -> GovilRecord | None:
        if not isinstance(item, dict):
            return None
        data = item.get("Data", {})
        url_name = item.get("UrlName", "")

        files = data.get("file", [])
        pdf_file = files[0] if files else {}
        pdf_filename = pdf_file.get("FileName", "")
        pdf_display = pdf_file.get("DisplayName", "")
        pdf_size = int(pdf_file.get("FileSize", 0) or 0)

        pdf_url = None
        if pdf_filename and url_name:
            pdf_url = f"{GOVIL_BLOB_BASE}/{url_name}/he/{pdf_filename}"

        position_ids = data.get("list", [])
        position_type = self._map_position_type(position_ids[0] if position_ids else "")
        ministry_ids = data.get("government_ministry", [])

        return GovilRecord(
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

    @staticmethod
    def _map_position_type(type_id: str) -> str:
        return {"1": "שר", "2": "סגן שר"}.get(str(type_id), str(type_id))

    def record_to_document(self, record: GovilRecord) -> ImportedDocument | None:
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
