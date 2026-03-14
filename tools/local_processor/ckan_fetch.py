"""Import phase — search CKAN and download PDFs locally."""

import hashlib
from pathlib import Path

import httpx

from .config import LocalConfig
from . import state as st


SUPPORTED_FORMATS = {"PDF", "DOCX", "DOC"}


async def search_ckan(cfg: LocalConfig) -> list[dict]:
    """Search CKAN for conflict-of-interest datasets and extract document URLs."""
    search_url = f"{cfg.ckan_base_url}/api/3/action/package_search"
    all_docs: list[dict] = []

    async with httpx.AsyncClient(timeout=30) as client:
        # Get total count first
        resp = await client.get(search_url, params={"q": cfg.ckan_query, "rows": 0})
        resp.raise_for_status()
        total = resp.json()["result"]["count"]
        print(f"  Found {total} CKAN datasets")

        # Fetch all datasets
        batch_size = 100
        for start in range(0, total, batch_size):
            resp = await client.get(
                search_url,
                params={"q": cfg.ckan_query, "rows": batch_size, "start": start},
            )
            resp.raise_for_status()
            datasets = resp.json()["result"]["results"]

            for ds in datasets:
                for resource in ds.get("resources", []):
                    fmt = (resource.get("format") or "").upper()
                    url = resource.get("url", "")
                    if not url or fmt not in SUPPORTED_FORMATS:
                        continue

                    all_docs.append({
                        "file_url": url,
                        "title": resource.get("name") or ds.get("title", ""),
                        "file_format": fmt.lower(),
                        "file_size": resource.get("size"),
                        "source_type": "ckan",
                        "source_id": ds.get("id", ""),
                        "source_title": ds.get("title", ""),
                        "source_url": f"{cfg.ckan_base_url}/dataset/{ds.get('id', '')}",
                    })

            print(f"  Fetched {min(start + batch_size, total)}/{total} datasets")

    return all_docs


async def check_server_duplicates(
    cfg: LocalConfig, urls: list[str]
) -> set[str]:
    """Ask the server which URLs already exist."""
    if not urls:
        return set()

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(
                f"{cfg.server_url}/api/v1/push/check-duplicates",
                json={"urls": urls},
                headers={"X-Push-Key": cfg.push_api_key},
            )
            resp.raise_for_status()
            return set(resp.json().get("existing_urls", []))
        except Exception as e:
            print(f"  Warning: could not check server duplicates: {e}")
            return set()


async def run_import(cfg: LocalConfig, limit: int | None = None, query: str | None = None) -> int:
    """Import phase: search CKAN, filter duplicates, download PDFs.

    Returns the number of newly downloaded documents.
    """
    print("\n=== Import Phase ===")

    # Override query if provided
    if query:
        cfg.ckan_query = query

    # Search CKAN
    docs = await search_ckan(cfg)
    print(f"  Total document URLs found: {len(docs)}")

    # Load local state
    local_state = st.load_state()
    already_local = set(local_state.keys())

    # Filter out locally known URLs
    new_docs = [d for d in docs if d["file_url"] not in already_local]
    print(f"  New (not in local state): {len(new_docs)}")

    # Check server for duplicates
    new_urls = [d["file_url"] for d in new_docs]
    server_existing = await check_server_duplicates(cfg, new_urls)
    if server_existing:
        new_docs = [d for d in new_docs if d["file_url"] not in server_existing]
        print(f"  After server dedup: {len(new_docs)}")

    # Apply limit
    if limit and len(new_docs) > limit:
        new_docs = new_docs[:limit]
        print(f"  Limited to: {limit}")

    if not new_docs:
        print("  Nothing to download.")
        return 0

    # Ensure cache dir exists
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)

    # Download PDFs
    downloaded = 0
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for i, doc in enumerate(new_docs, 1):
            url = doc["file_url"]
            try:
                print(f"  [{i}/{len(new_docs)}] Downloading: {doc['title'][:60]}...")
                resp = await client.get(url)
                resp.raise_for_status()
                pdf_bytes = resp.content

                # Validate PDF header
                if not pdf_bytes[:5].startswith(b"%PDF"):
                    print(f"    Skipped — not a valid PDF")
                    st.mark(local_state, url, "failed", title=doc["title"], error="not_pdf")
                    continue

                # Compute hash and save
                content_hash = hashlib.sha256(pdf_bytes).hexdigest()
                local_path = cfg.cache_dir / f"{content_hash}.pdf"
                local_path.write_bytes(pdf_bytes)

                st.mark(
                    local_state, url, "downloaded",
                    title=doc["title"],
                    content_hash=content_hash,
                    local_path=str(local_path),
                    file_format=doc["file_format"],
                    file_size=len(pdf_bytes),
                    source_type=doc["source_type"],
                    source_id=doc["source_id"],
                    source_title=doc["source_title"],
                    source_url=doc["source_url"],
                )
                downloaded += 1
                print(f"    OK ({len(pdf_bytes):,} bytes)")

            except Exception as e:
                print(f"    Failed: {e}")
                st.mark(local_state, url, "failed", title=doc["title"], error=str(e)[:200])

    print(f"\n  Downloaded: {downloaded}/{len(new_docs)}")
    return downloaded
