"""Import service — CKAN search + selective import, Gov.il bulk import."""

import hashlib
import logging
from datetime import datetime
from pathlib import Path

import httpx

from ocoi_common.timezone import now_israel, now_israel_naive

from sqlalchemy import select
from ocoi_common.config import settings
from ocoi_common.models import ImportedDocument
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.crud import get_or_create_source, create_document
from ocoi_db.models import Document, IgnoredResource

logger = logging.getLogger("ocoi.api.import")

# Module-level state for Gov.il bulk import progress polling
_import_state: dict = {
    "running": False,
    "source": None,
    "total_on_website": 0,
    "already_in_db": 0,
    "new_to_import": 0,
    "total": 0,
    "imported": 0,
    "skipped": 0,
    "errors": 0,
    "error_messages": [],
    "started_at": None,
    "finished_at": None,
}


# Render's free-tier Postgres caps at 1 GB. Past ~90% Render emails a warning; past
# 100% it suspends the database. We refuse to start a bulk import past this threshold
# so a half-full import doesn't push us over and trigger a suspend.
_DB_STORAGE_LIMIT_BYTES = 900 * 1024 * 1024  # 900 MB


class DBStoragePressureError(RuntimeError):
    """Raised by _check_db_storage_pressure when DB size exceeds the soft limit."""


async def _check_db_storage_pressure() -> None:
    """Abort if Postgres database size exceeds _DB_STORAGE_LIMIT_BYTES.

    No-op for SQLite (the size function doesn't exist there). Any error during the
    check is logged but not raised — we'd rather let the import attempt than block
    on a flaky size query.
    """
    if "postgresql" not in settings.database_url and "postgres" not in settings.database_url:
        return
    from sqlalchemy import text as _sa_text
    try:
        async with async_session_factory() as session:
            result = await session.execute(_sa_text("SELECT pg_database_size(current_database())"))
            size = int(result.scalar() or 0)
    except Exception as e:
        logger.warning(f"DB size pre-check failed, proceeding anyway: {e}")
        return
    if size >= _DB_STORAGE_LIMIT_BYTES:
        mb = size // (1024 * 1024)
        raise DBStoragePressureError(
            f"DB size {mb} MB exceeds {_DB_STORAGE_LIMIT_BYTES // (1024*1024)} MB soft limit; "
            "free space before importing more (see plan)."
        )


def get_import_status() -> dict:
    """Return a snapshot of the current import state."""
    return dict(_import_state)


def reset_import_state() -> None:
    """Force-reset import state (useful when import gets stuck)."""
    global _import_state
    _import_state.update({
        "running": False,
        "source": None,
        "total_on_website": 0,
        "already_in_db": 0,
        "new_to_import": 0,
        "total": 0,
        "imported": 0,
        "skipped": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": None,
        "finished_at": None,
    })


def _load_cached_govil_records() -> list[dict] | None:
    """Try to load pre-fetched Gov.il records from data/govil_records.json."""
    import json
    for path in [
        Path("/app/data/govil_records.json"),           # Docker
        settings.data_dir / "govil_records.json",       # Local dev
        Path(__file__).resolve().parents[5] / "data" / "govil_records.json",
    ]:
        if path.exists():
            try:
                records = json.loads(path.read_text(encoding="utf-8"))
                logger.info(f"Loaded {len(records)} cached Gov.il records from {path}")
                return records
            except Exception as e:
                logger.warning(f"Failed to load cached records from {path}: {e}")
    return None


# ── Shared: download PDF + convert ────────────────────────────────────────


async def download_pdf(file_url: str, doc_id: str) -> tuple[bytes | None, str | None]:
    """Download a PDF from URL. Returns (pdf_bytes, error_message).

    Does NOT convert — just downloads and validates the %PDF header.
    """
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
            resp = await http.get(file_url)
            resp.raise_for_status()
            pdf_bytes = resp.content

        if not pdf_bytes[:5].startswith(b"%PDF"):
            logger.warning(f"Downloaded non-PDF for {doc_id}: starts={pdf_bytes[:40]!r} url={file_url[:100]}")
            return None, "Downloaded file is not a valid PDF"

        logger.info(f"Downloaded PDF for {doc_id}: {len(pdf_bytes)} bytes")
        return pdf_bytes, None

    except Exception as e:
        logger.error(f"PDF download failed for {file_url}: {e}")
        return None, str(e)


def _compute_content_hash(pdf_bytes: bytes) -> str:
    """Compute SHA-256 hash of PDF bytes for duplicate detection."""
    return hashlib.sha256(pdf_bytes).hexdigest()


async def _check_duplicate_hash(session, content_hash: str) -> Document | None:
    """Check if a document with this content hash already exists."""
    result = await session.execute(
        select(Document).where(Document.content_hash == content_hash).limit(1)
    )
    return result.scalars().first()


async def check_duplicate(
    session,
    file_url: str | None = None,
    content_hash: str | None = None,
    title: str | None = None,
) -> Document | None:
    """Unified duplicate detection — used by all import paths.

    Checks in order: file_url → content_hash → title.
    Returns existing Document or None.
    """
    if file_url:
        result = await session.execute(
            select(Document).where(Document.file_url == file_url).limit(1)
        )
        if doc := result.scalars().first():
            return doc
    if content_hash:
        result = await session.execute(
            select(Document).where(Document.content_hash == content_hash).limit(1)
        )
        if doc := result.scalars().first():
            return doc
    if title:
        result = await session.execute(
            select(Document).where(Document.title == title).limit(1)
        )
        if doc := result.scalars().first():
            return doc
    return None


# ── CKAN: Search + selective import ──────────────────────────────────────


async def search_ckan(query: str, rows: int = 20, start: int = 0) -> dict:
    """Search CKAN datasets and return results with resource-level details."""
    from ocoi_importer.ckan_client import CkanClient

    client = CkanClient()
    datasets = await client.search_datasets(query=query, rows=rows, start=start)
    total = await client.get_total_count(query=query)

    # Check which resources are already imported or ignored
    async with async_session_factory() as session:
        results = []
        for ds in datasets:
            docs = client.extract_documents(ds)

            # Build resource-level info with import/ignore status
            resources = []
            already_imported = 0
            for doc in docs:
                existing = await session.execute(
                    select(Document).where(Document.file_url == doc.file_url)
                )
                is_imported = existing.scalars().first() is not None
                ignored = await session.execute(
                    select(IgnoredResource).where(IgnoredResource.file_url == doc.file_url)
                )
                is_ignored = ignored.scalars().first() is not None
                if is_imported:
                    already_imported += 1
                resources.append({
                    "url": doc.file_url,
                    "title": doc.title,
                    "format": doc.file_format,
                    "size": doc.file_size,
                    "resource_id": doc.metadata.get("resource_id"),
                    "already_imported": is_imported,
                    "ignored": is_ignored,
                })

            results.append({
                "id": ds.id,
                "title": ds.title,
                "notes": ds.notes,
                "metadata_created": ds.metadata_created,
                "metadata_modified": ds.metadata_modified,
                "tags": [t.get("name", "") for t in ds.tags],
                "num_resources": len(ds.resources),
                "num_documents": len(docs),
                "already_imported": already_imported,
                "resources": resources,
            })

    return {
        "total": total,
        "start": start,
        "rows": rows,
        "results": results,
    }


async def import_ckan_datasets(dataset_ids: list[str]) -> dict:
    """Import ALL resources from specific CKAN datasets by their IDs."""
    from ocoi_importer.ckan_client import CkanClient

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"imported": 0, "skipped": 0, "errors": 1, "error_messages": [str(e)]}

    client = CkanClient()
    imported_at = now_israel().isoformat()
    stats = {"imported": 0, "skipped": 0, "errors": 0, "error_messages": []}

    async with async_session_factory() as session:
        for ds_id in dataset_ids:
            try:
                datasets = await client.search_datasets(query=f"id:{ds_id}", rows=1)
                if not datasets:
                    stats["errors"] += 1
                    stats["error_messages"].append(f"Dataset {ds_id} not found")
                    continue

                ds = datasets[0]
                docs = client.extract_documents(ds)

                for doc in docs:
                    await _import_single_ckan_doc(session, doc, ds, imported_at, stats)

            except Exception as e:
                stats["errors"] += 1
                if len(stats["error_messages"]) < 20:
                    stats["error_messages"].append(f"Dataset {ds_id}: {e}")

        await session.commit()

    return stats


async def import_ckan_resources(resource_urls: list[dict]) -> dict:
    """Import specific CKAN resources by their URLs.

    Each item in resource_urls should have: dataset_id, url, title, format, size.
    """
    from ocoi_importer.ckan_client import CkanClient

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"imported": 0, "skipped": 0, "errors": 1, "error_messages": [str(e)]}

    client = CkanClient()
    imported_at = now_israel().isoformat()
    stats = {"imported": 0, "skipped": 0, "errors": 0, "error_messages": []}

    # Group by dataset_id to minimize API calls
    by_dataset: dict[str, list[dict]] = {}
    for r in resource_urls:
        ds_id = r.get("dataset_id", "")
        if ds_id not in by_dataset:
            by_dataset[ds_id] = []
        by_dataset[ds_id].append(r)

    async with async_session_factory() as session:
        for ds_id, resources in by_dataset.items():
            try:
                datasets = await client.search_datasets(query=f"id:{ds_id}", rows=1)
                ds = datasets[0] if datasets else None

                for res in resources:
                    url = res.get("url", "")
                    if not url:
                        continue

                    doc = ImportedDocument(
                        source_type="ckan",
                        source_id=ds_id,
                        title=res.get("title", ""),
                        file_url=url,
                        file_format=res.get("format", "pdf"),
                        file_size=res.get("size"),
                        metadata={
                            "dataset_title": ds.title if ds else "",
                            "dataset_notes": ds.notes if ds else None,
                            "resource_id": res.get("resource_id"),
                            "tags": [t.get("name", "") for t in ds.tags] if ds else [],
                        },
                    )
                    await _import_single_ckan_doc(session, doc, ds, imported_at, stats)

            except Exception as e:
                stats["errors"] += 1
                if len(stats["error_messages"]) < 20:
                    stats["error_messages"].append(f"Dataset {ds_id}: {e}")

        await session.commit()

    return stats


async def _import_single_ckan_doc(session, doc, ds, imported_at: str, stats: dict, *, skip_conversion: bool = False):
    """Import a single CKAN document: download PDF, convert to markdown, save to DB.

    ALWAYS saves the document record and PDF bytes, even if conversion fails.
    If skip_conversion=True, marks as pending and defers conversion to reconvert-all
    (used by bulk import to reduce memory pressure on 512MB Render).
    User can reconvert later with OCR.
    """
    # PDF-only gate (defense-in-depth; client-side filter should catch most)
    fmt = (doc.file_format or "").lower()
    url_lower = (doc.file_url or "").lower().split("?")[0]
    if fmt != "pdf" and not url_lower.endswith(".pdf"):
        logger.info(f"Skipping non-PDF resource [{fmt}]: {doc.title[:60]}")
        stats["skipped"] += 1
        return

    # Check duplicate by URL
    dup = await check_duplicate(session, file_url=doc.file_url)
    if dup:
        stats["skipped"] += 1
        return

    # Check if this URL is already in the ignore list (prevents re-showing it in search)
    already_ignored_q = await session.execute(
        select(IgnoredResource).where(IgnoredResource.file_url == doc.file_url)
    )
    if already_ignored_q.scalars().first():
        stats["skipped"] += 1
        return

    # Download PDF
    pdf_bytes, download_error = await download_pdf(doc.file_url, doc.title[:50])

    # Check duplicate by content hash (different URL pointing to same content)
    content_hash = None
    if pdf_bytes:
        content_hash = _compute_content_hash(pdf_bytes)
        dup = await check_duplicate(session, content_hash=content_hash)
        if dup:
            logger.info(f"Duplicate content hash for '{doc.title[:50]}' — matches doc {dup.id}")
            # Add this duplicate URL to IgnoredResource so future searches hide it.
            # CKAN often exposes the same PDF under multiple datasets with different URLs;
            # without this, the user keeps seeing "already imported content" in search results.
            try:
                session.add(IgnoredResource(
                    file_url=doc.file_url,
                    title=doc.title[:200] if doc.title else None,
                ))
                # No need to commit here — caller commits the session
            except Exception as ignore_err:
                logger.debug(f"Failed to add hash-dup URL to ignore list: {ignore_err}")
            stats["skipped"] += 1
            return

    # Convert to markdown (skipped in bulk mode to save memory on 512MB Render)
    md_text = None
    if pdf_bytes and not skip_conversion:
        from ocoi_api.services.pdf_converter import convert_pdf_bytes
        md_text = convert_pdf_bytes(pdf_bytes, doc.title[:50])

    # ALWAYS create the document record (even if conversion failed)
    metadata = dict(doc.metadata)
    metadata["imported_at"] = imported_at
    if ds:
        metadata["metadata_created"] = ds.metadata_created
        metadata["metadata_modified"] = ds.metadata_modified

    src = await get_or_create_source(
        session,
        source_type="ckan",
        source_id=doc.source_id,
        title=metadata.get("dataset_title", doc.title),
        url=doc.file_url,
        metadata_json=metadata,
    )
    db_doc = await create_document(
        session,
        source_id=src.id,
        title=doc.title,
        file_url=doc.file_url,
        file_format=doc.file_format,
        file_size=doc.file_size,
    )

    # External-source imports: keep metadata only; PDF is re-fetchable from file_url.
    # Storing the blob inline blew past Render's 1 GB Postgres limit.
    if pdf_bytes:
        db_doc.content_hash = content_hash
        db_doc.file_size = len(pdf_bytes)

    # Store markdown if available
    if md_text:
        db_doc.markdown_content = md_text
        db_doc.conversion_status = "converted"
        db_doc.converted_at = now_israel_naive()
    elif pdf_bytes:
        # PDF stored but not yet converted — either skip_conversion=True
        # (bulk mode), or conversion produced no text (needs OCR)
        db_doc.conversion_status = "pending" if skip_conversion else "no_text"
    else:
        db_doc.conversion_status = "failed"  # Download failed

    stats["imported"] += 1


# ── Bulk CKAN import (all results for a query) ───────────────────────────


async def run_bulk_ckan_import(query: str) -> dict:
    """Import ALL CKAN resources matching a query. Background task with progress via _import_state."""
    import gc
    from ocoi_importer.ckan_client import CkanClient

    global _import_state
    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"status": "error", "message": str(e)}

    _import_state.update({
        "running": True,
        "source": "ckan-bulk",
        "total_on_website": 0,
        "already_in_db": 0,
        "new_to_import": 0,
        "total": 0,
        "imported": 0,
        "skipped": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": now_israel().isoformat(),
        "finished_at": None,
    })

    try:
        client = CkanClient()
        total_datasets = await client.get_total_count(query=query)
        _import_state["total_on_website"] = total_datasets

        page_size = 20
        offset = 0
        imported_at = now_israel().isoformat()

        while offset < total_datasets:
            if not _import_state["running"]:
                logger.info("Bulk import stopped externally")
                break
            try:
                datasets = await client.search_datasets(query=query, rows=page_size, start=offset)
                if not datasets:
                    break

                # Commit per-document (not per-page) so crashes mid-page
                # don't lose already-imported docs. Each doc gets its own session.
                for ds in datasets:
                    docs = client.extract_documents(ds)
                    _import_state["total"] += len(docs)

                    for doc in docs:
                        try:
                            async with bg_session_factory() as session:
                                # Check if already imported
                                existing = await session.execute(
                                    select(Document).where(Document.file_url == doc.file_url)
                                )
                                if existing.scalars().first():
                                    _import_state["skipped"] += 1
                                    _import_state["already_in_db"] += 1
                                    continue

                                # Check if ignored
                                ignored = await session.execute(
                                    select(IgnoredResource).where(IgnoredResource.file_url == doc.file_url)
                                )
                                if ignored.scalars().first():
                                    _import_state["skipped"] += 1
                                    continue

                                _import_state["new_to_import"] += 1
                                stats_tmp = {"imported": 0, "skipped": 0, "errors": 0, "error_messages": []}
                                # skip_conversion=True: bulk imports thousands; running pdftotext
                                # per doc adds memory + CPU pressure. User triggers reconvert-all
                                # (which processes one at a time with asyncio.to_thread) afterward.
                                await _import_single_ckan_doc(session, doc, ds, imported_at, stats_tmp, skip_conversion=True)
                                _import_state["imported"] += stats_tmp["imported"]
                                _import_state["skipped"] += stats_tmp["skipped"]
                                _import_state["errors"] += stats_tmp["errors"]
                                for msg in stats_tmp["error_messages"]:
                                    if len(_import_state["error_messages"]) < 50:
                                        _import_state["error_messages"].append(msg)

                                # Commit this doc — if server crashes here, only this doc is lost
                                await session.commit()

                        except Exception as e:
                            _import_state["errors"] += 1
                            if len(_import_state["error_messages"]) < 50:
                                _import_state["error_messages"].append(f"{doc.title[:40]}: {e}")

            except Exception as e:
                _import_state["errors"] += 1
                if len(_import_state["error_messages"]) < 50:
                    _import_state["error_messages"].append(f"Page {offset}: {e}")

            offset += page_size
            gc.collect()

    except Exception as e:
        _import_state["errors"] += 1
        _import_state["error_messages"].append(f"Fatal: {e}")
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = now_israel().isoformat()

    return get_import_status()


# ── Gov.il: Automated bulk import ────────────────────────────────────────


async def run_govil_import(limit: int = 0, url: str = "") -> dict:
    """Bulk import from Gov.il. Updates _import_state for progress polling."""
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"status": "error", "message": str(e)}

    _import_state.update({
        "running": True,
        "source": "govil",
        "total_on_website": 0,
        "already_in_db": 0,
        "new_to_import": 0,
        "total": 0,
        "imported": 0,
        "skipped": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": now_israel().isoformat(),
        "finished_at": None,
    })

    try:
        await _import_govil(limit, url=url)
    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = now_israel().isoformat()

    return get_import_status()


async def run_govil_with_records(raw_items: list[dict]) -> dict:
    """Process pre-fetched Gov.il API items (sent from user's browser). Updates _import_state."""
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"status": "error", "message": str(e)}

    _import_state.update({
        "running": True,
        "source": "govil",
        "total_on_website": len(raw_items),
        "already_in_db": 0,
        "new_to_import": 0,
        "total": 0,
        "imported": 0,
        "skipped": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": now_israel().isoformat(),
        "finished_at": None,
    })

    try:
        from ocoi_importer.govil_client import GovilClient
        client = GovilClient()

        # Parse raw API items into GovilRecord objects
        records = [r for item in raw_items if (r := client._parse_item(item))]
        _import_state["total_on_website"] = len(records)

        # Check which are already in DB
        new_records = []
        async with async_session_factory() as session:
            for record in records:
                doc_info = client.record_to_document(record)
                if not doc_info:
                    _import_state["skipped"] += 1
                    continue
                existing = await session.execute(
                    select(Document).where(Document.file_url == doc_info.file_url)
                )
                if existing.scalars().first():
                    _import_state["already_in_db"] += 1
                else:
                    new_records.append(record)

        _import_state["new_to_import"] = len(new_records)
        _import_state["total"] = len(new_records)

        # Import new records
        await _process_new_records(client, new_records)

    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = now_israel().isoformat()

    return get_import_status()


async def _import_govil(limit: int, url: str = ""):
    """Import documents from Gov.il — fetch metadata, download PDFs, convert to markdown."""
    from ocoi_importer.govil_client import GovilClient

    client = GovilClient(url=url) if url else GovilClient()

    # Phase 1: Fetch all records from website (with fallback to cached file)
    try:
        records = await client.fetch_all_records()
    except Exception as e:
        logger.warning(f"Live Gov.il scraping failed: {e}, trying cached records...")
        cached = _load_cached_govil_records()
        if cached:
            records = [r for item in cached if (r := client._parse_item(item))]
            logger.info(f"Loaded {len(records)} records from cached file")
        else:
            raise
    _import_state["total_on_website"] = len(records)

    if limit > 0:
        records = records[:limit]

    # Phase 2: Check which records are already in DB
    new_records = []
    async with async_session_factory() as session:
        for record in records:
            doc_info = client.record_to_document(record)
            if not doc_info:
                _import_state["skipped"] += 1
                continue
            existing = await session.execute(
                select(Document).where(Document.file_url == doc_info.file_url)
            )
            if existing.scalars().first():
                _import_state["already_in_db"] += 1
            else:
                new_records.append(record)

    _import_state["new_to_import"] = len(new_records)
    _import_state["total"] = len(new_records)

    # Phase 3: Import new records
    await _process_new_records(client, new_records)


async def _process_new_records(client, new_records: list) -> None:
    """Download PDFs, convert to markdown, save to DB.

    ALWAYS saves documents even if conversion fails — user can reconvert later.
    Uses per-document sessions with bg_session_factory to prevent memory accumulation.
    """
    import gc
    imported_at = now_israel().isoformat()

    for record in new_records:
        try:
            doc_info = client.record_to_document(record)
            if not doc_info:
                _import_state["skipped"] += 1
                continue

            # Download PDF
            pdf_bytes, download_error = await download_pdf(doc_info.file_url, doc_info.title[:50])

            # Check duplicate by content hash
            content_hash = None
            if pdf_bytes:
                content_hash = _compute_content_hash(pdf_bytes)
                async with bg_session_factory() as dup_session:
                    dup = await check_duplicate(dup_session, content_hash=content_hash)
                if dup:
                    _import_state["skipped"] += 1
                    continue

            # Try conversion (no OCR during bulk import)
            md_text = None
            if pdf_bytes:
                from ocoi_api.services.pdf_converter import convert_pdf_bytes
                md_text = convert_pdf_bytes(pdf_bytes, doc_info.title[:50])

            # ALWAYS save to DB — each doc in its own session
            async with bg_session_factory() as session:
                metadata = dict(doc_info.metadata)
                metadata["imported_at"] = imported_at
                metadata.update(record.raw_data)

                src = await get_or_create_source(
                    session,
                    source_type="govil",
                    source_id=doc_info.source_id,
                    title=doc_info.title,
                    url=doc_info.file_url,
                    metadata_json=metadata,
                )
                db_doc = await create_document(
                    session,
                    source_id=src.id,
                    title=doc_info.title,
                    file_url=doc_info.file_url,
                    file_format="pdf",
                    file_size=doc_info.file_size,
                )

                if pdf_bytes:
                    # Metadata only — PDF is re-fetchable from file_url; see import_ckan_resources.
                    db_doc.content_hash = content_hash
                    db_doc.file_size = len(pdf_bytes)

                if md_text:
                    db_doc.markdown_content = md_text
                    db_doc.conversion_status = "converted"
                    db_doc.converted_at = now_israel_naive()
                elif pdf_bytes:
                    db_doc.conversion_status = "no_text"
                else:
                    db_doc.conversion_status = "failed"

                _import_state["imported"] += 1
                await session.commit()

        except Exception as e:
            _import_state["errors"] += 1
            if len(_import_state["error_messages"]) < 20:
                _import_state["error_messages"].append(
                    f"Gov.il '{record.name[:50]}': {e}"
                )
        gc.collect()
