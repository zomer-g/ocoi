"""CLI entry point for the converter package."""

import asyncio
from pathlib import Path

import click

from ocoi_common.config import settings
from ocoi_common.logging import setup_logging
from ocoi_db.engine import async_session_factory
from ocoi_db.crud import get_documents_by_status, update_document_markdown

logger = setup_logging("ocoi.converter")


def convert_pdf(pdf_path: Path) -> str:
    """Convert a PDF file to Markdown, auto-selecting the best method."""
    from ocoi_converter.pymupdf_converter import has_embedded_text, convert_with_pymupdf

    # Try fast path first for digital PDFs
    if has_embedded_text(pdf_path):
        try:
            result = convert_with_pymupdf(pdf_path)
            if len(result.strip()) > 100:
                return result
        except Exception as e:
            logger.warning(f"PyMuPDF failed, falling back to Marker: {e}")

    # Fall back to Marker (OCR) for scanned PDFs
    from ocoi_converter.marker_converter import convert_with_marker
    return convert_with_marker(pdf_path)


@click.group()
def cli():
    """Convert PDF documents to Markdown."""
    pass


@cli.command()
@click.option("--input", "input_path", type=click.Path(exists=True), help="Path to a PDF file")
@click.option("--output", "output_path", type=click.Path(), help="Output markdown file path")
def convert(input_path: str, output_path: str | None):
    """Convert a single PDF file to Markdown."""
    pdf_path = Path(input_path)
    markdown = convert_pdf(pdf_path)

    if output_path:
        out = Path(output_path)
    else:
        out = pdf_path.with_suffix(".md")

    out.write_text(markdown, encoding="utf-8")
    click.echo(f"Converted: {out} ({len(markdown)} chars)")


@cli.command()
@click.option("--limit", type=int, default=100, help="Max documents to convert")
def convert_pending(limit: int):
    """Convert all pending documents in the database."""
    asyncio.run(_convert_pending(limit))


async def _convert_pending(limit: int):
    async with async_session_factory() as session:
        docs = await get_documents_by_status(session, "conversion_status", "pending", limit)
        logger.info(f"Found {len(docs)} pending documents to convert")

        converted = 0
        for doc in docs:
            if not doc.file_path:
                logger.warning(f"No file path for document {doc.id}, skipping")
                continue

            pdf_path = Path(doc.file_path)
            if not pdf_path.exists():
                logger.warning(f"File not found: {pdf_path}, skipping")
                continue

            try:
                markdown = convert_pdf(pdf_path)
                md_path = settings.markdown_dir / f"{doc.id}.md"
                md_path.write_text(markdown, encoding="utf-8")

                await update_document_markdown(session, doc.id, markdown, str(md_path))
                converted += 1
                logger.info(f"Converted [{converted}/{len(docs)}]: {doc.title}")
            except Exception as e:
                logger.error(f"Failed to convert {doc.title}: {e}")
                doc.conversion_status = "failed"

        await session.commit()
    logger.info(f"Conversion complete: {converted}/{len(docs)} documents")


if __name__ == "__main__":
    cli()
