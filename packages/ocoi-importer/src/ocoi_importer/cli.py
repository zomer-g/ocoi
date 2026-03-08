"""CLI entry point for the importer package."""

import asyncio

import click

from ocoi_common.config import settings
from ocoi_common.logging import setup_logging
from ocoi_db.engine import async_session_factory
from ocoi_db.crud import get_or_create_source, create_document

logger = setup_logging("ocoi.importer")


@click.group()
def cli():
    """Import conflict of interest data from CKAN and gov.il."""
    settings.ensure_dirs()


@cli.command()
@click.option("--source", type=click.Choice(["ckan", "govil", "all"]), default="all")
@click.option("--limit", type=int, default=0, help="Max datasets to import (0=all)")
@click.option("--download/--no-download", default=True, help="Download PDFs after import")
def import_data(source: str, limit: int, download: bool):
    """Import metadata and optionally download PDFs from sources."""
    asyncio.run(_import(source, limit, download))


async def _import(source: str, limit: int, download: bool):
    if source in ("ckan", "all"):
        await _import_ckan(limit)
    if source in ("govil", "all"):
        await _import_govil(limit)


async def _import_ckan(limit: int):
    from ocoi_importer.ckan_client import CkanClient
    from ocoi_importer.downloader import Downloader

    client = CkanClient()
    downloader = Downloader()

    total = await client.get_total_count()
    logger.info(f"CKAN: {total} datasets found")

    if limit > 0:
        datasets = await client.search_datasets(rows=limit)
    else:
        datasets = await client.fetch_all_datasets()

    all_docs = []
    for ds in datasets:
        docs = client.extract_documents(ds)
        all_docs.extend(docs)

    logger.info(f"CKAN: {len(all_docs)} documents extracted from {len(datasets)} datasets")

    # Save to database
    async with async_session_factory() as session:
        for doc in all_docs:
            src = await get_or_create_source(
                session,
                source_type="ckan",
                source_id=doc.source_id,
                title=doc.metadata.get("dataset_title", doc.title),
                url=doc.file_url,
                metadata_json=doc.metadata,
            )
            db_doc = await create_document(
                session,
                source_id=src.id,
                title=doc.title,
                file_url=doc.file_url,
                file_format=doc.file_format,
                file_size=doc.file_size,
            )

            # Download PDF
            local_path = await downloader.download(doc.file_url)
            if local_path:
                db_doc.file_path = str(local_path)

        await session.commit()
    logger.info(f"CKAN import complete: {len(all_docs)} documents saved")


async def _import_govil(limit: int):
    from ocoi_importer.govil_client import GovilClient
    from ocoi_importer.downloader import Downloader

    client = GovilClient()
    downloader = Downloader()

    records = await client.fetch_all_records()
    if limit > 0:
        records = records[:limit]
    logger.info(f"Gov.il: {len(records)} records fetched")

    async with async_session_factory() as session:
        imported = 0
        for record in records:
            doc_info = client.record_to_document(record)
            if not doc_info:
                continue

            src = await get_or_create_source(
                session,
                source_type="govil",
                source_id=doc_info.source_id,
                title=doc_info.title,
                url=doc_info.file_url,
                metadata_json=doc_info.metadata,
            )
            db_doc = await create_document(
                session,
                source_id=src.id,
                title=doc_info.title,
                file_url=doc_info.file_url,
                file_format="pdf",
            )

            if doc_info.file_url:
                local_path = await downloader.download(doc_info.file_url)
                if local_path:
                    db_doc.file_path = str(local_path)
                    imported += 1

        await session.commit()
    logger.info(f"Gov.il import complete: {imported} documents downloaded")


if __name__ == "__main__":
    cli()
