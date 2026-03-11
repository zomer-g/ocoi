"""Client for the gov.il DynamicCollector API (ministers' conflict of interest).

Uses Playwright headless browser with stealth anti-detection patches to
bypass Cloudflare protection. Intercepts the page's own API responses
as the primary data source, with direct API calls as fallback.
"""

import asyncio
from ocoi_common.logging import setup_logging
from ocoi_common.models import GovilRecord, ImportedDocument

logger = setup_logging("ocoi.importer.govil")

GOVIL_PAGE_URL = "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict"
GOVIL_TEMPLATE_ID = "c6e0f53e-02c0-4db1-ae89-76590f0f502e"
GOVIL_BLOB_BASE = "https://www.gov.il/BlobFolder/dynamiccollectorresultitem"

MAX_RETRIES = 3
RETRY_DELAY = 8  # seconds between retries

# Stealth init script — removes common headless browser fingerprints
_STEALTH_SCRIPT = """
// Remove webdriver flag (main Cloudflare detection signal)
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
delete navigator.__proto__.webdriver;

// Chrome runtime object (missing in headless)
window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };

// Realistic plugins array
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const plugins = [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
            { name: 'Native Client', filename: 'internal-nacl-plugin' },
        ];
        plugins.length = 3;
        return plugins;
    }
});

// Languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['he-IL', 'he', 'en-US', 'en']
});

// Permissions API override
if (navigator.permissions) {
    const origQuery = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (params) =>
        params.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : origQuery(params);
}

// WebGL vendor/renderer (headless gives "Google SwiftShader")
const getParameter = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(param) {
    if (param === 37445) return 'Intel Inc.';
    if (param === 37446) return 'Intel Iris OpenGL Engine';
    return getParameter.call(this, param);
};
"""


class GovilClient:
    """Fetches ministers' conflict of interest agreements from gov.il."""

    async def fetch_all_records(self, per_page: int = 20) -> list[GovilRecord]:
        """Fetch all records using headless browser. Retries on failure."""
        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return await self._fetch_with_browser(per_page)
            except Exception as e:
                last_error = e
                logger.warning(f"Gov.il fetch attempt {attempt}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES:
                    logger.info(f"Retrying in {RETRY_DELAY}s...")
                    await asyncio.sleep(RETRY_DELAY)
        raise RuntimeError(f"Gov.il fetch failed after {MAX_RETRIES} attempts: {last_error}")

    async def _fetch_with_browser(self, per_page: int) -> list[GovilRecord]:
        """Single attempt to fetch all records via stealth Playwright browser."""
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
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                )

                # Apply stealth patches before any page loads
                await context.add_init_script(_STEALTH_SCRIPT)
                page = await context.new_page()

                # Intercept DynamicCollector API responses from the page itself
                async def on_response(response):
                    if "DynamicCollector" in response.url and response.status == 200:
                        try:
                            body = await response.json()
                            captured_api_data.append(body)
                        except Exception:
                            pass

                page.on("response", on_response)

                # Navigate to the page
                logger.info("Navigating to Gov.il page...")
                await page.goto(GOVIL_PAGE_URL, wait_until="domcontentloaded", timeout=90000)

                # Check for Cloudflare challenge
                title = await page.title()
                logger.info(f"Page title after load: '{title}'")

                if self._is_cloudflare_challenge(title):
                    logger.info("Cloudflare challenge detected, waiting for resolution...")
                    try:
                        await page.wait_for_function(
                            """() => {
                                const t = document.title.toLowerCase();
                                return !t.includes('just a moment')
                                    && !t.includes('checking')
                                    && !t.includes('attention required');
                            }""",
                            timeout=30000,
                        )
                        title = await page.title()
                        logger.info(f"Challenge resolved! Title: '{title}'")
                    except Exception:
                        raise RuntimeError(
                            f"Cloudflare challenge not resolved (title: '{await page.title()}'). "
                            "The server IP may be blocked by Cloudflare."
                        )

                # Wait for network to settle and page data to load
                await page.wait_for_load_state("networkidle", timeout=30000)
                # Brief pause for any late API responses
                await asyncio.sleep(2)

                # Strategy 1: Use intercepted API responses from the page's own loading
                if captured_api_data:
                    logger.info(f"Captured {len(captured_api_data)} API response(s) from page load")
                    first = captured_api_data[0]
                    total = first.get("TotalResults", 0)
                    all_items = list(first.get("Results", []))
                    logger.info(f"Gov.il: {total} total records, first page has {len(all_items)}")

                    # Fetch remaining pages via direct API calls (reusing browser cookies)
                    skip = per_page
                    while skip < total:
                        data = await self._browser_fetch(page, skip)
                        results = data.get("Results", [])
                        if not results:
                            break
                        all_items.extend(results)
                        logger.info(f"Fetched {len(all_items)}/{total} gov.il records")
                        skip += per_page

                # Strategy 2: Fallback — make our own API calls
                else:
                    logger.info("No intercepted responses, making direct API calls...")
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
                        logger.info(f"Fetched {len(all_items)}/{total} gov.il records")
                        skip += per_page

                records = [r for item in all_items if (r := self._parse_item(item))]
                logger.info(f"Parsed {len(records)} valid records from {len(all_items)} items")
                return records
            finally:
                await browser.close()

    @staticmethod
    def _is_cloudflare_challenge(title: str) -> bool:
        """Check if the page title indicates a Cloudflare challenge."""
        t = title.lower()
        return any(s in t for s in ["just a moment", "checking", "attention required", "cloudflare"])

    async def _browser_fetch(self, page, skip: int) -> dict:
        """Make the DynamicCollector API call from within the browser context."""
        return await page.evaluate(
            """async (params) => {
                const resp = await fetch('/he/api/DynamicCollector', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json;charset=utf-8'},
                    body: JSON.stringify({
                        DynamicTemplateID: params.templateId,
                        QueryFilters: {skip: {Query: params.skip}},
                        From: params.skip
                    })
                });
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                return resp.json();
            }""",
            {"templateId": GOVIL_TEMPLATE_ID, "skip": skip},
        )

    def _parse_item(self, item: dict) -> GovilRecord | None:
        """Parse a raw API result item into a GovilRecord."""
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
