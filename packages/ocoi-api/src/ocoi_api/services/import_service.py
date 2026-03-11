"""Import service — orchestrates CKAN and Gov.il document imports with progress tracking."""

from datetime import datetime, timezone

from ocoi_db.engine import async_session_factory
from ocoi_db.crud import get_or_create_source, create_document

# Module-level state for import progress polling
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


async def run_import(source: str = "all", limit: int = 0) -> dict:
    """Run import from the specified source(s). Updates _import_state for progress polling.

    Args:
        source: "ckan", "govil", or "all"
        limit: max documents to import per source (0 = all)
    """
    global _import_state

    if _import_state["running"]:
        return {"status": "error", "message": "Import already running"}

    _import_state.update({
        "running": True,
        "source": source,
        "total": 0,
        "imported": 0,
        "skipped": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    })

    try:
        if source in ("ckan", "all"):
            await _import_ckan(limit)
        if source in ("govil", "all"):
            await _import_govil(limit)
    except Exception as e:
        _import_state["error_messages"].append(f"Fatal: {str(e)}")
        _import_state["errors"] += 1
    finally:
        _import_state["running"] = False
        _import_state["finished_at"] = datetime.now(timezone.utc).isoformat()

    return get_import_status()


async def _import_ckan(limit: int):
    """Import documents from CKAN (odata.org.il)."""
    from ocoi_importer.ckan_client import CkanClient

    client = CkanClient()

    try:
        total = await client.get_total_count()
    except Exception as e:
        _import_state["error_messages"].append(f"CKAN: Failed to get count: {e}")
        _import_state["errors"] += 1
        return

    if limit > 0:
        datasets = await client.search_datasets(rows=limit)
    else:
        datasets = await client.fetch_all_datasets()

    all_docs = []
    for ds in datasets:
        docs = client.extract_documents(ds)
        all_docs.extend(docs)

    _import_state["total"] += len(all_docs)

    imported_at = datetime.now(timezone.utc).isoformat()

    async with async_session_factory() as session:
        for doc in all_docs:
            try:
                # Enrich metadata with import timestamp
                metadata = dict(doc.metadata)
                metadata["imported_at"] = imported_at
                metadata["metadata_created"] = None
                metadata["metadata_modified"] = None

                # Try to get dataset timestamps from the original dataset
                for ds in datasets:
                    if ds.id == doc.source_id:
                        metadata["metadata_created"] = ds.metadata_created
                        metadata["metadata_modified"] = ds.metadata_modified
                        break

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

                if db_doc.created_at is None:
                    # Newly created (not a duplicate)
                    _import_state["imported"] += 1
                else:
                    _import_state["skipped"] += 1

            except Exception as e:
                _import_state["errors"] += 1
                if len(_import_state["error_messages"]) < 20:
                    _import_state["error_messages"].append(f"CKAN doc '{doc.title[:50]}': {e}")

        await session.commit()


async def _import_govil(limit: int):
    """Import documents from Gov.il DynamicCollector."""
    from ocoi_importer.govil_client import GovilClient

    client = GovilClient()

    try:
        records = await client.fetch_all_records()
    except Exception as e:
        _import_state["error_messages"].append(f"Gov.il: Failed to fetch records: {e}")
        _import_state["errors"] += 1
        return

    if limit > 0:
        records = records[:limit]

    _import_state["total"] += len(records)

    imported_at = datetime.now(timezone.utc).isoformat()

    async with async_session_factory() as session:
        for record in records:
            try:
                doc_info = client.record_to_document(record)
                if not doc_info:
                    _import_state["skipped"] += 1
                    continue

                # Enrich metadata with import timestamp and raw record data
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
                db_doc = await create_document(
                    session,
                    source_id=src.id,
                    title=doc_info.title,
                    file_url=doc_info.file_url,
                    file_format="pdf",
                )

                if db_doc.created_at is None:
                    _import_state["imported"] += 1
                else:
                    _import_state["skipped"] += 1

            except Exception as e:
                _import_state["errors"] += 1
                if len(_import_state["error_messages"]) < 20:
                    _import_state["error_messages"].append(f"Gov.il '{record.name[:50]}': {e}")

        await session.commit()
