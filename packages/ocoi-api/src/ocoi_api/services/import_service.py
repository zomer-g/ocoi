"""Import service — CKAN search + selective import, Gov.il bulk import."""

from datetime import datetime, timezone

from sqlalchemy import select
from ocoi_db.engine import async_session_factory
from ocoi_db.crud import get_or_create_source, create_document
from ocoi_db.models import Document

# Module-level state for Gov.il bulk import progress polling
_import_state: dict = {
    "running": False,
    "source": None,
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
                # Fetch the specific dataset
                datasets = await client.search_datasets(query=f"id:{ds_id}", rows=1)
                if not datasets:
                    stats["errors"] += 1
                    stats["error_messages"].append(f"Dataset {ds_id} not found")
                    continue

                ds = datasets[0]
                docs = client.extract_documents(ds)

                for doc in docs:
                    # Check if already exists
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


# ── Gov.il: Browser-extracted import ─────────────────────────────────────


async def import_govil_records(records: list[dict]) -> dict:
    """Import Gov.il records that were extracted client-side from the browser.

    Each record should have: name, date, pdf_url, pdf_display, position_type_id, ministry_id
    """
    imported_at = datetime.now(timezone.utc).isoformat()
    stats = {"imported": 0, "skipped": 0, "errors": 0, "error_messages": []}

    async with async_session_factory() as session:
        for rec in records:
            try:
                pdf_url = rec.get("pdf_url", "")
                name = rec.get("name", "")
                if not pdf_url:
                    stats["skipped"] += 1
                    continue

                # Check if already exists
                existing = await session.execute(
                    select(Document).where(Document.file_url == pdf_url)
                )
                if existing.scalar_one_or_none():
                    stats["skipped"] += 1
                    continue

                title = rec.get("pdf_display") or f"הסדר ניגוד עניינים - {name}"
                source_id = f"govil_{name}_{rec.get('date', 'unknown')}"

                metadata = {
                    "name": name,
                    "position_type_id": rec.get("position_type_id"),
                    "ministry_id": rec.get("ministry_id"),
                    "date": rec.get("date"),
                    "pdf_display": rec.get("pdf_display"),
                    "pdf_size": rec.get("pdf_size"),
                    "imported_at": imported_at,
                }

                src = await get_or_create_source(
                    session,
                    source_type="govil",
                    source_id=source_id,
                    title=title,
                    url=pdf_url,
                    metadata_json=metadata,
                )
                await create_document(
                    session,
                    source_id=src.id,
                    title=title,
                    file_url=pdf_url,
                    file_format="pdf",
                    file_size=rec.get("pdf_size"),
                )
                stats["imported"] += 1

            except Exception as e:
                stats["errors"] += 1
                if len(stats["error_messages"]) < 20:
                    stats["error_messages"].append(f"Gov.il '{name[:50]}': {e}")

        await session.commit()

    return stats
