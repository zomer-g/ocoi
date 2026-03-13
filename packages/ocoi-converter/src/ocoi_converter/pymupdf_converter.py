"""PDF to text conversion using PyMuPDF.

Supports both digital PDFs (embedded text) and scanned PDFs (via Tesseract OCR).
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


def _extract_blocks(page) -> list[str]:
    """Extract text blocks from a page (RTL-safe)."""
    blocks = page.get_text("blocks")
    paragraphs = []
    for b in blocks:
        if b[6] == 0:  # text block (not image)
            text = b[4].strip()
            if text:
                text = re.sub(r"[\u200f\u200e]+", " ", text)
                text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
                text = re.sub(r" +", " ", text)
                paragraphs.append(text)
    return paragraphs


def _ocr_blocks(page) -> list[str]:
    """OCR a scanned page using Tesseract Hebrew."""
    try:
        tp = page.get_textpage_ocr(language="heb", dpi=150, full=True)
        blocks = page.get_text("blocks", textpage=tp)
        paragraphs = []
        for b in blocks:
            if b[6] == 0:
                text = b[4].strip()
                if text:
                    text = re.sub(r"[\u200f\u200e]+", " ", text)
                    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
                    text = re.sub(r" +", " ", text)
                    paragraphs.append(text)
        return paragraphs
    except Exception as e:
        logger.warning(f"OCR failed for page: {e}")
        return []


def convert_with_pymupdf(pdf_path: Path) -> str:
    """Convert a PDF to text using PyMuPDF (RTL-safe).

    First tries direct text extraction for digital PDFs.
    Falls back to Tesseract OCR for scanned PDFs.
    """
    import pymupdf

    logger.info(f"Converting with PyMuPDF: {pdf_path.name}")
    doc = pymupdf.open(str(pdf_path))

    # First try: direct text extraction
    pages = []
    for i, page in enumerate(doc):
        paragraphs = _extract_blocks(page)
        if paragraphs:
            pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))

    result = "\n\n".join(pages)

    # Fallback: OCR for scanned PDFs
    if len(result.strip()) <= 50:
        logger.info(f"No embedded text in {pdf_path.name}, trying OCR...")
        pages = []
        for i, page in enumerate(doc):
            paragraphs = _ocr_blocks(page)
            if paragraphs:
                pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))
        result = "\n\n".join(pages)
        if result:
            logger.info(f"OCR succeeded for {pdf_path.name}: {len(result)} chars")

    doc.close()
    logger.info(f"Converted {pdf_path.name}: {len(result)} chars")
    return result
