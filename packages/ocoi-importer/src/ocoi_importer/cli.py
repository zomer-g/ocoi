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
    """Import conflict-of-interest data from CKAN and odata.org.il snapshots."""
    settings.ensure_dirs()


@cli.command()
@click.option("--source", type=click.Choice(["ckan", "odata", "all"]), default="all")
@click.option("--limit", type=int, default=0, help="Max datasets to import (0=all)")
@click.option("--download/--no-download", default=True, help="Download PDFs after import")
def import_data(source: str, limit: int, download: bool):
    """Import metadata and optionally download PDFs from sources."""
    asyncio.run(_import(source, limit, download))


async def _import(source: str, limit: int, download: bool):
    if source in ("ckan", "all"):
        await _import_ckan(limit)
    if source in ("odata", "all"):
        await _import_odata(limit)


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


async def _import_odata(limit: int):
    """Import all conflict-of-interest declarations from the odata.org.il snapshot ZIPs.

    Stores PDF bytes inline in Document.pdf_content (the ZIP is the only
    upstream source) and converts each PDF to markdown along the way.
    """
    import hashlib
    from ocoi_importer.odata_client import iter_records, gov_il_listing_url, ODATA_DATASET_PAGE
    from ocoi_db.models import Document
    from sqlalchemy import select

    count = 0
    async for rec in iter_records():
        if limit and count >= limit:
            break

        async with async_session_factory() as session:
            # Skip duplicates by content hash
            content_hash = hashlib.sha256(rec.pdf_bytes).hexdigest()
            existing = await session.execute(
                select(Document).where(Document.content_hash == content_hash).limit(1)
            )
            if existing.scalars().first():
                continue

            listing_url = gov_il_listing_url(rec.url_name) if rec.url_name else ODATA_DATASET_PAGE
            metadata = {
                "name": rec.name,
                "position": rec.position,
                "ministry": rec.ministry,
                "date": rec.date,
                "url_name": rec.url_name,
                "pdf_filename": rec.pdf_filename,
                **rec.raw_data,
            }
            src = await get_or_create_source(
                session,
                source_type="odata",
                source_id=rec.source_id,
                title=rec.pdf_filename.removesuffix(".pdf") or rec.name,
                url=listing_url,
                metadata_json=metadata,
            )
            db_doc = await create_document(
                session,
                source_id=src.id,
                title=rec.pdf_filename.removesuffix(".pdf") or rec.name,
                file_url=listing_url,
                file_format="pdf",
                file_size=len(rec.pdf_bytes),
            )
            db_doc.pdf_content = rec.pdf_bytes
            db_doc.content_hash = content_hash
            await session.commit()
        count += 1
        # Drop bytes to keep peak memory bounded.
        rec.pdf_bytes = b""

    logger.info(f"odata import complete: {count} documents saved")


if __name__ == "__main__":
    cli()
