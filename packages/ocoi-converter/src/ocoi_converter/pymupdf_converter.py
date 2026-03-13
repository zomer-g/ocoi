"""Fast PDF to text conversion using PyMuPDF.

Best for digital PDFs with embedded text (not scanned).
Uses pymupdf blocks extraction for correct RTL Hebrew text handling.
"""

import re
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
    """Convert a digital PDF to text using PyMuPDF (RTL-safe).

    Uses block-level extraction instead of pymupdf4llm.to_markdown()
    to avoid reversed Hebrew text in RTL documents.
    """
    import pymupdf

    logger.info(f"Converting with PyMuPDF: {pdf_path.name}")
    doc = pymupdf.open(str(pdf_path))
    pages = []
    for i, page in enumerate(doc):
        blocks = page.get_text("blocks")
        paragraphs = []
        for b in blocks:
            if b[6] == 0:  # text block (not image)
                text = b[4].strip()
                if text:
                    # Replace RTL/LTR marks with space (they act as word separators)
                    text = re.sub(r"[\u200f\u200e]+", " ", text)
                    # Join fragmented lines within a block
                    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
                    # Clean up multiple spaces
                    text = re.sub(r" +", " ", text)
                    paragraphs.append(text)
        if paragraphs:
            pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))
    doc.close()
    result = "\n\n".join(pages)
    logger.info(f"Converted {pdf_path.name}: {len(result)} chars")
    return result
