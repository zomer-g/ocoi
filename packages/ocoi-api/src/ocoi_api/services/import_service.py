"""Import service — CKAN search + selective import, Gov.il bulk import."""

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import httpx

from sqlalchemy import select
from ocoi_common.config import settings
from ocoi_db.engine import async_session_factory
from ocoi_db.crud import get_or_create_source, create_document
from ocoi_db.models import Document

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


def get_import_status() -> dict:
    """Return a snapshot of the current import state."""
    return dict(_import_state)


# ── CKAN: Search + selective import ──────────────────────────────────────


async def search_ckan(query: str, rows: int = 20, start: int = 0) -> dict:
    """Search CKAN datasets and return results with existing-in-DB status."""
    from ocoi_importer.ckan_client import CkanClient

    client = CkanClient()
    datasets = await client.search_datasets(query=query, rows=rows, start=start)
    total = await client.get_total_count(query=query)

    # Check which datasets already have documents imported
    async with async_session_factory() as session:
        results = []
        for ds in datasets:
            docs = client.extract_documents(ds)
            # Check how many of these doc URLs already exist
            existing_count = 0
            for doc in docs:
                existing = await session.execute(
                    select(Document).where(Document.file_url == doc.file_url)
                )
                if existing.scalar_one_or_none():
                    existing_count += 1

            results.append({
                "id": ds.id,
                "title": ds.title,
                "notes": ds.notes,
                "metadata_created": ds.metadata_created,
                "metadata_modified": ds.metadata_modified,
                "tags": [t.get("name", "") for t in ds.tags],
                "num_resources": len(ds.resources),
                "num_documents": len(docs),
                "already_imported": existing_count,
            })

    return {
        "total": total,
        "start": start,
        "rows": rows,
        "results": results,
    }


async def import_ckan_datasets(dataset_ids: list[str]) -> dict:
    """Import specific CKAN datasets by their IDs."""
    from ocoi_importer.ckan_client import CkanClient

    client = CkanClient()
    imported_at = datetime.now(timezone.utc).isoformat()
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
                    existing = await session.execute(
                        select(Document).where(Document.file_url == doc.file_url)
                    )
                    if existing.scalar_one_or_none():
                        stats["skipped"] += 1
                        continue

                    metadata = dict(doc.metadata)
                    metadata["imported_at"] = imported_at
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
                    await create_document(
                        session,
                        source_id=src.id,
                        title=doc.title,
                        file_url=doc.file_url,
                        file_format=doc.file_format,
                        file_size=doc.file_size,
                    )
                    stats["imported"] += 1

            except Exception as e:
                stats["errors"] += 1
                if len(stats["error_messages"]) < 20:
                    stats["error_messages"].append(f"Dataset {ds_id}: {e}")

        await session.commit()

    return stats


# ── Gov.il: Automated bulk import ────────────────────────────────────────


async def run_govil_import(limit: int = 0, url: str = "") -> dict:
    """Bulk import from Gov.il. Updates _import_state for progress polling."""
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

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
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    })

    try:
        await _import_govil(limit, url=url)
    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = datetime.now(timezone.utc).isoformat()

    return get_import_status()


async def run_govil_with_records(raw_items: list[dict]) -> dict:
    """Process pre-fetched Gov.il API items (sent from user's browser). Updates _import_state."""
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

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
        "started_at": datetime.now(timezone.utc).isoformat(),
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
                if existing.scalar_one_or_none():
                    _import_state["already_in_db"] += 1
                else:
                    new_records.append(record)

        _import_state["new_to_import"] = len(new_records)
        _import_state["total"] = len(new_records)

        # Import new records (same logic as _import_govil phase 3)
        await _process_new_records(client, new_records)

    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = datetime.now(timezone.utc).isoformat()

    return get_import_status()


async def _import_govil(limit: int, url: str = ""):
    """Import documents from Gov.il — fetch metadata, download PDFs, convert to markdown."""
    from ocoi_importer.govil_client import GovilClient

    client = GovilClient(url=url) if url else GovilClient()

    # Phase 1: Fetch all records from website
    records = await client.fetch_all_records()
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
            if existing.scalar_one_or_none():
                _import_state["already_in_db"] += 1
            else:
                new_records.append(record)

    _import_state["new_to_import"] = len(new_records)
    _import_state["total"] = len(new_records)

    # Phase 3: Import new records
    await _process_new_records(client, new_records)


async def _process_new_records(client, new_records: list) -> None:
    """Download PDFs, convert to markdown, save to DB. Only saves docs with actual content."""
    imported_at = datetime.now(timezone.utc).isoformat()
    async with async_session_factory() as session:
        for record in new_records:
            try:
                doc_info = client.record_to_document(record)
                if not doc_info:
                    _import_state["skipped"] += 1
                    continue

                # Download PDF and convert to markdown FIRST — don't save metadata-only
                temp_id = hashlib.md5(doc_info.file_url.encode()).hexdigest()
                md_text = await _download_and_convert_pdf(
                    file_url=doc_info.file_url,
                    doc_id=temp_id,
                )
                if not md_text:
                    logger.warning(f"Skipping '{doc_info.title}' — PDF download/conversion failed")
                    _import_state["skipped"] += 1
                    continue

                # PDF content obtained — now save to DB
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

                # Rename temp files to use actual DB id
                actual_id = str(db_doc.id)
                temp_pdf = settings.pdf_dir / f"{temp_id}.pdf"
                temp_md = settings.markdown_dir / f"{temp_id}.md"
                actual_pdf = settings.pdf_dir / f"{actual_id}.pdf"
                actual_md = settings.markdown_dir / f"{actual_id}.md"
                if temp_pdf.exists():
                    temp_pdf.rename(actual_pdf)
                if temp_md.exists():
                    temp_md.rename(actual_md)

                db_doc.markdown_content = md_text
                db_doc.conversion_status = "converted"
                db_doc.file_path = str(actual_pdf)

                _import_state["imported"] += 1

            except Exception as e:
                _import_state["errors"] += 1
                if len(_import_state["error_messages"]) < 20:
                    _import_state["error_messages"].append(
                        f"Gov.il '{record.name[:50]}': {e}"
                    )

        await session.commit()


def convert_pdf_to_markdown(pdf_path: Path, doc_id: str) -> str | None:
    """Extract text from a local PDF file using pdfplumber. Returns markdown or None."""
    import pdfplumber

    pages = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text:
                pages.append(f"## עמוד {i + 1}\n\n{text}")

    md_text = "\n\n---\n\n".join(pages)
    if md_text and len(md_text.strip()) > 50:
        md_path = settings.markdown_dir / f"{doc_id}.md"
        md_path.write_text(md_text, encoding="utf-8")
        logger.info(f"Converted to markdown: {md_path.name} ({len(md_text)} chars)")
        return md_text
    else:
        logger.warning(f"PDF conversion produced empty/short text for {doc_id}")
        return None


async def _download_and_convert_pdf(file_url: str, doc_id: str) -> str | None:
    """Download a PDF from URL, save to disk, extract text with pdfplumber."""
    try:
        pdf_path = settings.pdf_dir / f"{doc_id}.pdf"
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
            resp = await http.get(file_url)
            resp.raise_for_status()
            pdf_path.write_bytes(resp.content)

        logger.info(f"Downloaded PDF: {pdf_path.name} ({len(resp.content)} bytes)")
        return convert_pdf_to_markdown(pdf_path, doc_id)

    except Exception as e:
        logger.error(f"PDF download/convert failed for {file_url}: {e}")
        return None
