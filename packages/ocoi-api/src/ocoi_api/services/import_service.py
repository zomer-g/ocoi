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


# ── Gov.il: Bulk import ──────────────────────────────────────────────────


async def run_govil_import(limit: int = 0) -> dict:
    """Bulk import from Gov.il. Updates _import_state for progress polling."""
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    _import_state.update({
        "running": True,
        "source": "govil",
        "total": 0,
        "imported": 0,
        "skipped": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    })

    try:
        await _import_govil(limit)
    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = datetime.now(timezone.utc).isoformat()

    return get_import_status()


async def _import_govil(limit: int):
    """Import documents from Gov.il DynamicCollector."""
    from ocoi_importer.govil_client import GovilClient

    client = GovilClient()

    records = await client.fetch_all_records()
    if limit > 0:
        records = records[:limit]

    _import_state["total"] = len(records)
    imported_at = datetime.now(timezone.utc).isoformat()

    async with async_session_factory() as session:
        for record in records:
            try:
                doc_info = client.record_to_document(record)
                if not doc_info:
                    _import_state["skipped"] += 1
                    continue

                # Check if document already exists
                existing = await session.execute(
                    select(Document).where(Document.file_url == doc_info.file_url)
                )
                if existing.scalar_one_or_none():
                    _import_state["skipped"] += 1
                    continue

                metadata = dict(doc_info.metadata)
                metadata["imported_at"] = imported_at
                metadata["raw_record"] = record.raw_data

                src = await get_or_create_source(
                    session,
                    source_type="govil",
                    source_id=doc_info.source_id,
                    title=doc_info.title,
                    url=doc_info.file_url,
                    metadata_json=metadata,
                )
                await create_document(
                    session,
                    source_id=src.id,
                    title=doc_info.title,
                    file_url=doc_info.file_url,
                    file_format="pdf",
                )
                _import_state["imported"] += 1

            except Exception as e:
                _import_state["errors"] += 1
                if len(_import_state["error_messages"]) < 20:
                    _import_state["error_messages"].append(f"Gov.il '{record.name[:50]}': {e}")

        await session.commit()
