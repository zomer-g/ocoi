"""Generic Gov.il scraper — based on github.com/zomer-g/govil-scraper.

Supports:
  - DynamicCollector pages (POST API with GUID from page HTML)
  - Traditional Collector pages (GET API with discovered CollectorTypes)
  - Custom API endpoints (some DynamicCollectors have their own URLs)

Multi-strategy session management:
  1. cloudscraper (solves Cloudflare JS challenges automatically)
  2. Playwright headless browser (fallback for tougher challenges)
"""

import asyncio
import html
import re
import time
from urllib.parse import urlparse, parse_qs

import cloudscraper
import httpx

from ocoi_common.logging import setup_logging
from ocoi_common.models import GovilRecord, ImportedDocument

logger = setup_logging("ocoi.importer.govil")

GOVIL_BASE = "https://www.gov.il"
GOVIL_API_URL = f"{GOVIL_BASE}/he/api/DynamicCollector"
GOVIL_BLOB_BASE = f"{GOVIL_BASE}/BlobFolder/dynamiccollectorresultitem"

# Default for ministers' conflict of interest (backwards compat)
DEFAULT_URL = "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict"
DEFAULT_TEMPLATE_ID = "c6e0f53e-02c0-4db1-ae89-76590f0f502e"

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_COMMON_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": f"{GOVIL_BASE}/",
    "Origin": GOVIL_BASE,
}


class PageType:
    DYNAMIC_COLLECTOR = "dynamic"
    TRADITIONAL_COLLECTOR = "traditional"


class PageConfig:
    """Configuration extracted from a Gov.il collector page."""

    def __init__(self):
        self.page_type: str = PageType.DYNAMIC_COLLECTOR
        self.collector_name: str = ""
        self.template_id: str | None = None  # GUID for DynamicCollector
        self.custom_api_url: str | None = None
        self.x_client_id: str | None = None
        self.items_per_page: int = 20
        self.office_id: str | None = None
        self.collector_types: list[str] = []  # For traditional collectors
        self.page_url: str = ""


class GovILSession:
    """Session manager that handles Cloudflare challenges."""

    def __init__(self):
        self._scraper = None
        self._warmed = False

    def _init_cloudscraper(self):
        self._scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True},
            delay=10,
        )
        self._scraper.headers.update({
            "User-Agent": _UA,
            **_COMMON_HEADERS,
        })

    async def warm(self) -> bool:
        """Warm the session by visiting gov.il to get Cloudflare cookies."""
        if self._warmed:
            return True

        self._init_cloudscraper()

        # Try cloudscraper first
        try:
            resp = await asyncio.to_thread(self._scraper.get, f"{GOVIL_BASE}/he")
            if resp.status_code == 200 and len(resp.text) > 1000:
                logger.info("Session warmed via cloudscraper")
                self._warmed = True
                return True
            logger.warning(f"Cloudscraper warm: status={resp.status_code}, len={len(resp.text)}")
        except Exception as e:
            logger.warning(f"Cloudscraper warm failed: {e}")

        # Fallback to Playwright for cookies
        try:
            await self._warm_with_playwright()
            return True
        except Exception as e:
            logger.error(f"Playwright warm failed: {e}")
            return False

    async def _warm_with_playwright(self):
        """Use Playwright to solve Cloudflare challenge and extract cookies."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError("Playwright not installed — Gov.il scraping unavailable in this environment")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--window-size=1920,1080",
                ],
            )
            try:
                context = await browser.new_context(
                    locale="he-IL",
                    user_agent=_UA,
                    viewport={"width": 1920, "height": 1080},
                )
                page = await context.new_page()
                await page.goto(f"{GOVIL_BASE}/he", wait_until="domcontentloaded", timeout=60000)

                # Wait for Cloudflare challenge to resolve
                for _ in range(30):
                    title = await page.title()
                    if not _is_cloudflare_challenge(title):
                        break
                    await asyncio.sleep(1.5)

                # Extract cookies and inject into cloudscraper session
                cookies = await context.cookies()
                for c in cookies:
                    self._scraper.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

                self._warmed = True
                logger.info(f"Session warmed via Playwright ({len(cookies)} cookies)")
            finally:
                await browser.close()

    async def request(
        self, method: str, url: str, retries: int = 3, **kwargs
    ) -> dict:
        """Make an HTTP request with retries, session re-warming on 403."""
        if not self._warmed:
            await self.warm()

        last_error = None
        for attempt in range(retries):
            try:
                resp = await asyncio.to_thread(
                    getattr(self._scraper, method.lower()), url, **kwargs
                )
                if resp.status_code == 403:
                    logger.warning(f"403 on attempt {attempt + 1}, re-warming session...")
                    self._warmed = False
                    await self.warm()
                    continue
                if resp.status_code == 429:
                    delay = 2 ** (attempt + 1)
                    logger.warning(f"429 rate limited, waiting {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_error = e
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)

        # Last resort: try Playwright fallback
        try:
            logger.info("All retries exhausted, trying Playwright fallback...")
            self._warmed = False
            await self._warm_with_playwright()
            resp = await asyncio.to_thread(
                getattr(self._scraper, method.lower()), url, **kwargs
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            pass

        raise RuntimeError(f"Request failed after {retries} attempts: {last_error}")


def _is_cloudflare_challenge(title: str) -> bool:
    t = title.lower()
    return any(s in t for s in ["just a moment", "checking", "attention required", "cloudflare"])


# ── URL Parsing ──────────────────────────────────────────────────────────


def parse_gov_url(url: str) -> PageConfig:
    """Parse a Gov.il collector URL to determine page type and extract parameters."""
    config = PageConfig()
    config.page_url = url
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    qs = parse_qs(parsed.query)

    # DynamicCollector: /he/departments/dynamiccollectors/{name}
    m = re.search(r"/departments/dynamiccollectors?/([^/?#]+)", path, re.IGNORECASE)
    if m:
        config.page_type = PageType.DYNAMIC_COLLECTOR
        config.collector_name = m.group(1)
        config.office_id = qs.get("officeId", [None])[0]
        return config

    # Traditional Collector: /he/collectors/{name}
    m = re.search(r"/collectors?/([^/?#]+)", path, re.IGNORECASE)
    if m:
        config.page_type = PageType.TRADITIONAL_COLLECTOR
        config.collector_name = m.group(1)
        config.office_id = qs.get("officeId", [None])[0]
        return config

    raise ValueError(f"Could not parse Gov.il URL: {url}")


# ── HTML Config Extraction ───────────────────────────────────────────────


async def extract_dynamic_page_config(session: GovILSession, config: PageConfig) -> PageConfig:
    """Fetch the HTML page and extract DynamicCollector config from ng-init attributes."""
    try:
        resp = await asyncio.to_thread(session._scraper.get, config.page_url)
        resp.raise_for_status()
        page_html = resp.text
    except Exception as e:
        logger.warning(f"Failed to fetch page HTML: {e}")
        return config

    # Look for ng-init="dynamicCtrl.Events.initCtrl(...)"
    match = re.search(
        r'ng-init\s*=\s*"dynamicCtrl\.Events\.initCtrl\(([^"]+)\)"',
        page_html,
    )
    if not match:
        match = re.search(
            r"ng-init\s*=\s*'dynamicCtrl\.Events\.initCtrl\(([^']+)\)'",
            page_html,
        )

    if match:
        raw = html.unescape(match.group(1))
        _parse_init_ctrl_args(raw, config)
    else:
        # Fallback: look near "initCtrl" text
        idx = page_html.find("initCtrl")
        if idx >= 0:
            chunk = page_html[max(0, idx - 500): idx + 1500]
            raw = html.unescape(chunk)
            _parse_init_ctrl_args(raw, config)

    return config


def _parse_init_ctrl_args(raw: str, config: PageConfig):
    """Parse the arguments of initCtrl() to extract GUID, custom API URL, etc."""
    # Extract GUIDs (UUID format)
    guids = re.findall(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", raw, re.I)

    # Extract custom API URLs
    urls = re.findall(r"https?://[^\s'\"\\,]+", raw)
    api_urls = [u for u in urls if "api" in u.lower() or "dynamiccollector" not in u.lower()]

    # Extract items per page (number after URL or GUID)
    numbers = re.findall(r"(?:,\s*)(\d+)(?:\s*[,)])", raw)
    per_page = None
    for n in numbers:
        val = int(n)
        if 5 <= val <= 100:
            per_page = val
            break

    if guids:
        config.template_id = guids[0]
        logger.info(f"Extracted template GUID: {config.template_id}")

    if api_urls:
        config.custom_api_url = api_urls[0]
        if len(guids) > 1:
            config.x_client_id = guids[1]
        logger.info(f"Found custom API URL: {config.custom_api_url}")

    if per_page:
        config.items_per_page = per_page
        logger.info(f"Items per page: {per_page}")


async def discover_collector_types(session: GovILSession, config: PageConfig) -> list[str]:
    """For Traditional Collectors, discover the CollectorType values."""
    url = (
        f"{GOVIL_BASE}/CollectorsWebApi/api/DataCollector/GetLayoutCollectorModel"
        f"?collectorId={config.collector_name}&culture=he"
    )
    try:
        data = await session.request("get", url)
        text = str(data)
        types = re.findall(r"collectionTypes=([^&\"']+)", text)
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for t in types:
            if t not in seen:
                seen.add(t)
                unique.append(t)
        if unique:
            logger.info(f"Discovered collector types: {unique}")
            return unique
    except Exception as e:
        logger.warning(f"Failed to discover collector types: {e}")

    return [config.collector_name]


# ── Main GovilClient ─────────────────────────────────────────────────────


class GovilClient:
    """Fetches records from any Gov.il collector page."""

    def __init__(self, url: str = DEFAULT_URL):
        self.url = url
        self.session = GovILSession()
        self._config: PageConfig | None = None

    async def fetch_all_records(self, per_page: int = 20) -> list[GovilRecord]:
        """Fetch all records from the configured Gov.il page."""
        # Parse URL
        config = parse_gov_url(self.url)
        logger.info(f"Scraping: {config.collector_name} (type: {config.page_type})")

        # Warm session
        if not await self.session.warm():
            raise RuntimeError("Could not establish Gov.il session")

        # Extract page config from HTML
        if config.page_type == PageType.DYNAMIC_COLLECTOR:
            await extract_dynamic_page_config(self.session, config)
            if not config.template_id:
                # Fallback to default if this is the default URL
                if "ministers_conflict" in self.url:
                    config.template_id = DEFAULT_TEMPLATE_ID
                else:
                    raise RuntimeError(
                        f"Could not extract template GUID from page: {self.url}"
                    )
        elif config.page_type == PageType.TRADITIONAL_COLLECTOR:
            config.collector_types = await discover_collector_types(self.session, config)

        self._config = config

        # Fetch all pages
        if config.page_type == PageType.DYNAMIC_COLLECTOR:
            return await self._fetch_dynamic(config, per_page)
        else:
            return await self._fetch_traditional(config, per_page)

    # ── DynamicCollector Fetching ────────────────────────────────────────

    async def _fetch_dynamic(self, config: PageConfig, per_page: int) -> list[GovilRecord]:
        """Fetch all items from a DynamicCollector API."""
        all_items = []
        skip = 0
        total = None
        page_size = config.items_per_page or per_page

        while True:
            try:
                if config.custom_api_url:
                    data = await self._fetch_custom_api(config, skip)
                else:
                    data = await self._fetch_standard_api(config, skip, page_size)

                results = data.get("Results", [])
                if total is None:
                    total = data.get("TotalResults", 0)
                    logger.info(f"Total records on website: {total}")

                if not results:
                    break

                all_items.extend(results)
                skip += len(results)
                logger.info(f"Fetched {len(all_items)}/{total} records")

                if total and len(all_items) >= total:
                    break

                # Rate limit: slower for large datasets
                delay = 1.0 if (total or 0) > 500 else 0.5
                await asyncio.sleep(delay)

            except Exception as e:
                logger.warning(f"Fetch error at skip={skip}: {e}")
                if all_items:
                    break
                raise

        records = [r for item in all_items if (r := self._parse_item(item))]
        logger.info(f"Parsed {len(records)} records from {len(all_items)} items")
        return records

    async def _fetch_standard_api(self, config: PageConfig, skip: int, page_size: int) -> dict:
        """Standard DynamicCollector POST request."""
        return await self.session.request(
            "post",
            GOVIL_API_URL,
            json={
                "DynamicTemplateID": config.template_id,
                "QueryFilters": {"skip": {"Query": skip}},
                "From": skip,
            },
        )

    async def _fetch_custom_api(self, config: PageConfig, skip: int) -> dict:
        """Custom API endpoint for some DynamicCollectors."""
        headers = {}
        if config.x_client_id:
            headers["x-client-id"] = config.x_client_id
        return await self.session.request(
            "post",
            config.custom_api_url,
            json={"skip": skip},
            headers=headers,
        )

    # ── Traditional Collector Fetching ───────────────────────────────────

    async def _fetch_traditional(self, config: PageConfig, per_page: int) -> list[GovilRecord]:
        """Fetch all items from a Traditional Collector GET API."""
        all_items = []
        skip = 0
        total = None
        page_size = per_page

        while True:
            try:
                # Build query params
                params = {
                    "culture": "he",
                    "skip": str(skip),
                    "limit": str(page_size),
                }
                if config.office_id:
                    params["officeId"] = config.office_id

                url = f"{GOVIL_BASE}/CollectorsWebApi/api/DataCollector/GetResults"
                # Add CollectorType params (may be multiple)
                ct_params = "&".join(f"CollectorType={t}" for t in config.collector_types)
                full_url = f"{url}?{ct_params}&" + "&".join(f"{k}={v}" for k, v in params.items())

                data = await self.session.request("get", full_url)

                results = data.get("results", [])
                if total is None:
                    total = data.get("total", 0)
                    logger.info(f"Total records: {total}")

                if not results:
                    break

                all_items.extend(results)
                skip += len(results)
                logger.info(f"Fetched {len(all_items)}/{total}")

                if total and len(all_items) >= total:
                    break

                delay = 1.0 if (total or 0) > 500 else 0.5
                await asyncio.sleep(delay)

            except Exception as e:
                logger.warning(f"Traditional fetch error at skip={skip}: {e}")
                if all_items:
                    break
                raise

        records = [r for item in all_items if (r := self._parse_traditional_item(item))]
        logger.info(f"Parsed {len(records)} records")
        return records

    # ── Parsing ──────────────────────────────────────────────────────────

    def _parse_item(self, item: dict) -> GovilRecord | None:
        """Parse a DynamicCollector API result item into a GovilRecord."""
        if not isinstance(item, dict):
            return None
        data = item.get("Data", {})
        url_name = item.get("UrlName", "")

        # Find file attachments
        files = self._extract_files(data)
        pdf_file = files[0] if files else {}
        pdf_filename = pdf_file.get("FileName", "")
        pdf_display = pdf_file.get("DisplayName", "")
        pdf_size = int(pdf_file.get("FileSize", 0) or 0)

        pdf_url = None
        if pdf_filename and url_name:
            # Check if FileName is already a full URL
            if pdf_filename.startswith("http"):
                pdf_url = pdf_filename
            else:
                pdf_url = f"{GOVIL_BLOB_BASE}/{url_name}/he/{pdf_filename}"

        # Parse common fields
        position_ids = data.get("list", [])
        position_type = self._map_position_type(position_ids[0] if position_ids else "")
        ministry_ids = data.get("government_ministry", [])
        name = data.get("function", "") or data.get("title", "") or item.get("Description", "")

        return GovilRecord(
            name=name,
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

    def _parse_traditional_item(self, item: dict) -> GovilRecord | None:
        """Parse a Traditional Collector result item."""
        if not isinstance(item, dict):
            return None

        title = item.get("title", "")
        description = item.get("description", "")
        url = item.get("url", "")

        # Try to find PDF URL in the item
        pdf_url = None
        if url and url.endswith(".pdf"):
            pdf_url = f"{GOVIL_BASE}{url}" if url.startswith("/") else url

        # Check tags.metaData for nested document links
        tags = item.get("tags", {})
        metadata = tags.get("metaData", {})

        return GovilRecord(
            name=title or description[:100],
            position_type=None,
            ministry=None,
            date=item.get("publishDate"),
            pdf_url=pdf_url,
            raw_data={
                "url": url,
                "description": description,
                "metadata": metadata,
            },
        )

    @staticmethod
    def _extract_files(data: dict) -> list[dict]:
        """Extract file attachment dicts from various possible locations."""
        for key in ("file", "File", "files", "fileData", "Document", "Files", "Attachments"):
            val = data.get(key)
            if isinstance(val, list) and val:
                return val
        return []

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
