"""Import service — CKAN search + selective import, odata.org.il bulk import."""

import hashlib
import logging

import httpx

from ocoi_common.timezone import now_israel, now_israel_naive

from sqlalchemy import select
from ocoi_common.config import settings
from ocoi_common.models import ImportedDocument, OdataRecord
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.crud import get_or_create_source, create_document
from ocoi_db.models import (
    Company, Document, EntityRelationship, IgnoredResource, Person, Source,
)

logger = logging.getLogger("ocoi.api.import")

# Module-level state for bulk import progress polling
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


# Refuse to start a bulk import once the Postgres data directory is near the plan's
# storage cap — Render auto-suspends the DB once the cap is exceeded, which is how the
# original outage happened. Tune via DB_STORAGE_LIMIT_MB (default 4500 MB ≈ 90% of the
# current 5 GB Basic plan; bump in env when you resize the plan).
import os as _os
_DB_STORAGE_LIMIT_BYTES = int(_os.environ.get("DB_STORAGE_LIMIT_MB", "4500")) * 1024 * 1024


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


# ── odata.org.il: snapshot bulk import ───────────────────────────────────


async def run_odata_import() -> dict:
    """Pull all conflict-of-interest declarations from the odata.org.il
    snapshot dataset (3 ZIPs ≈ 210 MB ≈ 346 PDFs), persist each PDF's bytes
    inside Document.pdf_content, and convert to markdown along the way.

    Idempotent: re-running skips documents whose SHA-256 content hash is
    already in the DB.
    """
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"status": "error", "message": str(e)}

    _import_state.update({
        "running": True,
        "source": "odata",
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
        await _run_odata_import()
    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = now_israel().isoformat()

    return get_import_status()


async def _run_odata_import() -> None:
    """Stream records from the odata ZIPs and persist each one. Frees the
    PDF bytes immediately after the DB row is committed so peak memory
    stays bounded."""
    import gc
    from ocoi_importer.odata_client import iter_records, gov_il_listing_url, ODATA_DATASET_PAGE

    imported_at = now_israel().isoformat()

    def _on_progress(stage: str, done: int, total: int) -> None:
        # Reflect ZIP download / parse progress in the status payload so the
        # admin UI shows movement before any PDF lands in the DB.
        if stage.startswith("download:"):
            _import_state["error_messages"][:] = _import_state["error_messages"][:0]  # no-op, cheap
        # No need to mutate counters here — the caller updates them per record.
        return

    async for rec in iter_records(progress=_on_progress):
        _import_state["total"] += 1
        _import_state["total_on_website"] = _import_state["total"]
        try:
            await _persist_odata_record(rec, imported_at, ODATA_DATASET_PAGE, gov_il_listing_url)
        except Exception as e:
            _import_state["errors"] += 1
            if len(_import_state["error_messages"]) < 20:
                _import_state["error_messages"].append(
                    f"odata '{rec.name[:50]}': {e}"
                )
        finally:
            # Always drop the bytes from the in-memory record once we're done.
            rec.pdf_bytes = b""
        gc.collect()


async def _persist_odata_record(
    rec: OdataRecord,
    imported_at: str,
    dataset_page_url: str,
    listing_url_fn,
) -> None:
    """Insert one OdataRecord into the DB. Skips on content-hash duplicates.

    Stores the PDF bytes inline in `Document.pdf_content` so the file is
    available across container restarts (the snapshot ZIP is the only
    upstream source — we don't want to re-download it just to render a
    document detail page later).
    """
    from ocoi_api.services.pdf_converter import convert_pdf_bytes

    pdf_bytes = rec.pdf_bytes
    if not pdf_bytes:
        _import_state["skipped"] += 1
        return

    content_hash = _compute_content_hash(pdf_bytes)

    # ── Dedup step 1: byte-level (catches identical PDFs from any source) ──
    async with bg_session_factory() as dup_session:
        dup = await check_duplicate(dup_session, content_hash=content_hash)
    if dup is not None:
        _import_state["skipped"] += 1
        _import_state["already_in_db"] += 1
        return

    # ── Dedup step 2: cross-source metadata dedup ─────────────────────────
    # The legacy gov.il importer keyed each Source row by
    # f"govil_{name}_{date or 'unknown'}". The odata snapshot is just a
    # mirror of those same documents — if the bytes happen to differ
    # (the snapshot tool may have re-encoded the PDF) the byte-level check
    # above will miss them, but the (name, date) tuple is identical. Treat
    # any matching legacy row as a duplicate so we never end up with two
    # records for the same person+date across the two importers.
    legacy_source_id = f"govil_{rec.name}_{rec.date or 'unknown'}"
    async with bg_session_factory() as legacy_session:
        legacy_q = await legacy_session.execute(
            select(Source.id).where(
                Source.source_type == "govil",
                Source.source_id == legacy_source_id,
            ).limit(1)
        )
        if legacy_q.scalar_one_or_none() is not None:
            _import_state["skipped"] += 1
            _import_state["already_in_db"] += 1
            return

    title = rec.pdf_filename.removesuffix(".pdf") if rec.pdf_filename else rec.name
    listing_url = listing_url_fn(rec.url_name) if rec.url_name else dataset_page_url

    md_text = convert_pdf_bytes(pdf_bytes, title[:50])

    async with bg_session_factory() as session:
        metadata = {
            "name": rec.name,
            "position": rec.position,
            "ministry": rec.ministry,
            "date": rec.date,
            "url_name": rec.url_name,
            "pdf_filename": rec.pdf_filename,
            "imported_at": imported_at,
            **rec.raw_data,
        }
        src = await get_or_create_source(
            session,
            source_type="odata",
            source_id=rec.source_id,
            title=title,
            url=listing_url,
            metadata_json=metadata,
        )
        db_doc = await create_document(
            session,
            source_id=src.id,
            title=title,
            file_url=listing_url,
            file_format="pdf",
            file_size=len(pdf_bytes),
        )

        # Store the actual PDF bytes — required because the upstream ZIP is
        # the only place these PDFs live, and we don't want to re-download
        # 70 MB to view a single document.
        db_doc.pdf_content = pdf_bytes
        db_doc.content_hash = content_hash
        db_doc.file_size = len(pdf_bytes)

        if md_text:
            db_doc.markdown_content = md_text
            db_doc.conversion_status = "converted"
            db_doc.converted_at = now_israel_naive()
        else:
            db_doc.conversion_status = "no_text"

        _import_state["imported"] += 1
        _import_state["new_to_import"] = _import_state["imported"]
        await session.commit()


# ── MK constituent-outreach expenses (Excel upload) ─────────────────────


async def run_mk_expenses_import(file_bytes: bytes, filename: str) -> dict:
    """Parse a Knesset MK-expenses Excel and create EntityRelationships
    aggregated per (MK, supplier) pair.

    Idempotent: re-uploading the same file is detected via
    Document.content_hash and the unique compound index on
    entity_relationships, so no duplicate rows are written.
    """
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    try:
        await _check_db_storage_pressure()
    except DBStoragePressureError as e:
        return {"status": "error", "message": str(e)}

    _import_state.update({
        "running": True,
        "source": "mk_expenses",
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
        await _run_mk_expenses_import(file_bytes, filename)
    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = now_israel().isoformat()

    return get_import_status()


async def _run_mk_expenses_import(file_bytes: bytes, filename: str) -> None:
    from ocoi_importer.mk_expenses_client import (
        CANONICAL_KNESSET_NAME,
        canonical_supplier_name,
        iter_rows,
    )

    file_hash = hashlib.sha256(file_bytes).hexdigest()

    # ── Persist the Excel as a Document so the audit trail / CMS works ──
    # Use async_session_factory (expire_on_commit=False) so we can read
    # ORM attributes after commit without triggering greenlet IO.
    async with async_session_factory() as session:
        # Was the same file already ingested?
        existing_doc_q = await session.execute(
            select(Document).where(Document.content_hash == file_hash).limit(1)
        )
        existing_doc = existing_doc_q.scalars().first()

        if existing_doc is None:
            src = await get_or_create_source(
                session,
                source_type="mk_expenses",
                source_id=f"mk_expenses_{file_hash[:16]}",
                title=filename,
                url=f"upload://{filename}",
                metadata_json={
                    "filename": filename,
                    "imported_at": now_israel().isoformat(),
                    "kind": "mk_expenses",
                },
            )
            doc = await create_document(
                session,
                source_id=src.id,
                title=filename,
                file_url=f"upload://{filename}",
                file_format="xlsx",
                file_size=len(file_bytes),
            )
            doc.pdf_content = file_bytes  # the column is generic binary
            doc.content_hash = file_hash
            doc.conversion_status = "stored"
            # Capture ids BEFORE commit so we don't depend on lazy-load.
            doc_id = str(doc.id)
            await session.commit()
        else:
            _import_state["error_messages"].append(
                f"קובץ {filename} כבר יובא בעבר; מתחיל ייבוא קשרים מחדש (idempotent)."
            )
            doc_id = str(existing_doc.id)

    # ── Pass 1: stream rows, aggregate per (mk, supplier) ─────────────────
    aggregated: dict[tuple[str, str], dict] = {}
    skipped = 0
    total_rows = 0

    for row in iter_rows(file_bytes):
        total_rows += 1
        supplier = canonical_supplier_name(row.raw_supplier_name)
        if not supplier or not row.mk_name:
            skipped += 1
            continue
        key = (row.mk_name, supplier)
        bucket = aggregated.get(key)
        if bucket is None:
            bucket = {
                "count": 0,
                "total_amount": 0.0,
                "min_date": None,
                "max_date": None,
                "categories": set(),
                "is_internal": supplier == CANONICAL_KNESSET_NAME,
            }
            aggregated[key] = bucket
        bucket["count"] += 1
        bucket["total_amount"] += row.amount
        if row.date:
            if bucket["min_date"] is None or row.date < bucket["min_date"]:
                bucket["min_date"] = row.date
            if bucket["max_date"] is None or row.date > bucket["max_date"]:
                bucket["max_date"] = row.date
        if row.expense_category:
            bucket["categories"].add(row.expense_category)

        # Surface progress to the polling admin UI every 500 rows so the
        # user can see Pass 1 making progress (Excel parse alone can take
        # several seconds on a 24K-row workbook).
        if total_rows % 500 == 0:
            _import_state["total"] = total_rows
            _import_state["total_on_website"] = total_rows
            _import_state["skipped"] = skipped

    _import_state["total"] = total_rows
    _import_state["total_on_website"] = total_rows
    _import_state["skipped"] = skipped

    # ── Pass 2: bulk upsert entities + bulk insert relationships ─────────
    # The naive approach (per-pair upsert + dedup query) is O(N) DB
    # round-trips per pair, which on free-tier Postgres balloons to many
    # minutes for the 5K+ unique pairs in a single Excel. Instead we
    # pre-load all matches in a couple of IN-list queries, batch-insert
    # any missing entities, then batch-insert relationships skipping any
    # that already exist (idempotent re-runs).
    all_mk_names = sorted({k[0] for k in aggregated.keys()})
    all_supplier_names = sorted({k[1] for k in aggregated.keys()})
    person_cache: dict[str, str] = {}
    company_cache: dict[str, str] = {}

    async with async_session_factory() as session:
        # ── 2a. Persons: pre-load existing, bulk-insert missing ──
        for chunk in _chunked(all_mk_names, 500):
            rows = await session.execute(
                select(Person.id, Person.name_hebrew).where(
                    Person.name_hebrew.in_(chunk)
                )
            )
            for pid, name in rows.fetchall():
                person_cache[name] = str(pid)

        new_persons = [
            Person(name_hebrew=name)
            for name in all_mk_names if name not in person_cache
        ]
        if new_persons:
            session.add_all(new_persons)
            await session.flush()
            for p in new_persons:
                person_cache[p.name_hebrew] = str(p.id)

        # ── 2b. Companies: same pattern ──
        for chunk in _chunked(all_supplier_names, 500):
            rows = await session.execute(
                select(Company.id, Company.name_hebrew).where(
                    Company.name_hebrew.in_(chunk)
                )
            )
            for cid, name in rows.fetchall():
                company_cache[name] = str(cid)

        new_companies = [
            Company(name_hebrew=name)
            for name in all_supplier_names if name not in company_cache
        ]
        if new_companies:
            session.add_all(new_companies)
            await session.flush()
            for c in new_companies:
                company_cache[c.name_hebrew] = str(c.id)

        await session.commit()

        # ── 2c. Relationships: dedup against existing for THIS document ──
        existing_pair_q = await session.execute(
            select(
                EntityRelationship.source_entity_id,
                EntityRelationship.target_entity_id,
            ).where(
                EntityRelationship.document_id == doc_id,
                EntityRelationship.relationship_type == "mk_expense_payment",
            )
        )
        existing_pairs: set[tuple[str, str]] = {
            (str(s), str(t)) for s, t in existing_pair_q.fetchall()
        }

        # Build the EntityRelationship rows in memory, skipping pairs we
        # already wrote (so re-running is a no-op at relationship level).
        to_insert: list[EntityRelationship] = []
        already_skipped = 0
        for (mk_name, supplier_name), bucket in aggregated.items():
            pid = person_cache.get(mk_name)
            cid = company_cache.get(supplier_name)
            if not pid or not cid:
                continue
            if (pid, cid) in existing_pairs:
                already_skipped += 1
                continue
            to_insert.append(EntityRelationship(
                source_entity_type="person",
                source_entity_id=pid,
                target_entity_type="company",
                target_entity_id=cid,
                relationship_type="mk_expense_payment",
                document_id=doc_id,
                details=_format_mk_expense_details(bucket),
                confidence=1.0,
                origin_kind="mk_expense",
            ))

        # Bulk-insert in chunks; commit per chunk so the polling UI
        # surfaces progress and the WAL stays bounded.
        BATCH_SIZE = 500
        inserted = 0
        for chunk in _chunked(to_insert, BATCH_SIZE):
            try:
                session.add_all(chunk)
                await session.flush()
                await session.commit()
                inserted += len(chunk)
                _import_state["imported"] = inserted
                _import_state["new_to_import"] = inserted
            except Exception as e:
                # Rollback the partial chunk and surface the error; we
                # keep going so a single bad row doesn't lose the rest.
                _import_state["errors"] += len(chunk)
                if len(_import_state["error_messages"]) < 30:
                    _import_state["error_messages"].append(f"batch insert failed: {e}")
                try:
                    await session.rollback()
                except Exception:
                    pass

        _import_state["already_in_db"] += already_skipped


def _chunked(seq, size: int):
    """Yield ``size``-sized chunks from a sequence — used for IN-list and
    bulk-insert batching above."""
    if not seq:
        return
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _format_mk_expense_details(bucket: dict) -> str:
    """Build a Hebrew one-liner describing one (MK, supplier) aggregate."""
    parts: list[str] = []
    parts.append("תקציב קשר עם הציבור")
    parts.append(f"{bucket['count']} חיובים")
    if bucket["min_date"] and bucket["max_date"]:
        if bucket["min_date"] == bucket["max_date"]:
            parts.append(bucket["min_date"])
        else:
            parts.append(f"{bucket['min_date']} – {bucket['max_date']}")
    total = bucket["total_amount"]
    parts.append(f"סה\"כ {total:,.2f} ₪")
    if bucket["categories"]:
        cats = sorted(bucket["categories"])
        parts.append("קטגוריות: " + ", ".join(cats[:4]))
    return " | ".join(parts)
