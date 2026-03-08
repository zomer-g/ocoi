"""Fast PDF to Markdown conversion using PyMuPDF4LLM.

Best for digital PDFs with embedded text (not scanned).
Much faster than Marker (~100x) but no OCR capability.
"""

from pathlib import Path

from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.converter.pymupdf")


def has_embedded_text(pdf_path: Path) -> bool:
    """Check if a PDF has embedded text (not scanned)."""
    import pymupdf

    doc = pymupdf.open(str(pdf_path))
    text_pages = 0
    for page in doc:
        text = page.get_text().strip()
        if len(text) > 50:
            text_pages += 1
    doc.close()
    return text_pages > 0


def convert_with_pymupdf(pdf_path: Path) -> str:
    """Convert a digital PDF to Markdown using PyMuPDF4LLM."""
    import pymupdf4llm

    logger.info(f"Converting with PyMuPDF: {pdf_path.name}")
    markdown = pymupdf4llm.to_markdown(str(pdf_path))
    logger.info(f"Converted {pdf_path.name}: {len(markdown)} chars")
    return markdown
