"""Push phase — send processed documents to the server."""

import asyncio
import base64
import json
from pathlib import Path

import httpx

from .config import LocalConfig
from . import state as st


async def push_single(
    client: httpx.AsyncClient,
    cfg: LocalConfig,
    url: str,
    info: dict,
    skip_extract: bool = False,
) -> str:
    """Push a single document to the server. Returns status string."""
    # Build payload
    payload: dict = {
        "title": info.get("title", ""),
        "file_url": url,
        "file_format": info.get("file_format", "pdf"),
        "file_size": info.get("file_size"),
        "content_hash": info.get("content_hash"),
        "source_type": info.get("source_type", "ckan"),
        "source_id": info.get("source_id", ""),
        "source_title": info.get("source_title", ""),
        "source_url": info.get("source_url", ""),
    }

    # Add markdown content
    md_path = info.get("markdown_path")
    if md_path and Path(md_path).exists():
        payload["markdown_content"] = Path(md_path).read_text(encoding="utf-8")

    # Add extraction JSON
    if not skip_extract:
        extraction_path = info.get("extraction_path")
        if extraction_path and Path(extraction_path).exists():
            payload["extraction_json"] = json.loads(
                Path(extraction_path).read_text(encoding="utf-8")
            )

    # Add PDF bytes (base64-encoded)
    local_path = info.get("local_path")
    if local_path and Path(local_path).exists():
        pdf_bytes = Path(local_path).read_bytes()
        payload["pdf_base64"] = base64.b64encode(pdf_bytes).decode("ascii")

    # Send to server
    resp = await client.post(
        f"{cfg.server_url}/api/v1/push/documents",
        json=payload,
        headers={"X-Push-Key": cfg.push_api_key},
        timeout=120,  # Large payload with PDF
    )
    resp.raise_for_status()
    result = resp.json()
    return result.get("status", "unknown")


async def run_push(
    cfg: LocalConfig,
    skip_extract: bool = False,
    limit: int | None = None,
) -> int:
    """Push all processed documents to the server.

    Returns the number of successfully pushed documents.
    """
    print("\n=== Push Phase ===")

    local_state = st.load_state()

    # Collect pushable docs
    target_status = "converted" if skip_extract else "extracted"
    to_push = st.get_by_status(local_state, target_status)

    if limit:
        to_push = to_push[:limit]

    if not to_push:
        print("  Nothing to push.")
        return 0

    errors = cfg.validate()
    if "PUSH_API_KEY" in " ".join(errors):
        print("  ERROR: PUSH_API_KEY not set.")
        return 0

    print(f"  Documents to push: {len(to_push)}")
    pushed = 0
    skipped = 0
    failed = 0

    async with httpx.AsyncClient() as client:
        for i, url in enumerate(to_push, 1):
            info = local_state[url]
            title = info.get("title", url)[:60]

            for attempt in range(3):
                try:
                    print(f"  [{i}/{len(to_push)}] Pushing: {title}...")
                    status = await push_single(client, cfg, url, info, skip_extract)

                    if status == "created":
                        st.mark(local_state, url, "pushed")
                        pushed += 1
                        print(f"    Created")
                    elif status == "skipped":
                        st.mark(local_state, url, "pushed")
                        skipped += 1
                        print(f"    Skipped (duplicate)")
                    else:
                        print(f"    Server returned: {status}")
                        failed += 1
                    break

                except httpx.HTTPStatusError as e:
                    if e.response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                        delay = 5 * (attempt + 1)
                        print(f"    Retry ({e.response.status_code}) in {delay}s...")
                        await asyncio.sleep(delay)
                    else:
                        print(f"    Failed: HTTP {e.response.status_code}")
                        st.mark(local_state, url, "failed", error=str(e)[:200])
                        failed += 1
                        break

                except Exception as e:
                    if attempt < 2:
                        delay = 5 * (attempt + 1)
                        print(f"    Retry in {delay}s: {e}")
                        await asyncio.sleep(delay)
                    else:
                        print(f"    Failed: {e}")
                        st.mark(local_state, url, "failed", error=str(e)[:200])
                        failed += 1
                        break

    print(f"\n  Pushed: {pushed}, Skipped: {skipped}, Failed: {failed}")
    return pushed
