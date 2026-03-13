"""Centralized PDF-to-Markdown conversion with Hebrew OCR support.

This is the SINGLE place for all PDF text extraction logic.
Supports both digital PDFs (embedded text) and scanned PDFs (Tesseract OCR).
"""

import logging
import os
import re
import tempfile
from pathlib import Path

logger = logging.getLogger("ocoi.pdf_converter")

# ── Tessdata path detection (lazy singleton) ──────────────────────────────

_TESSDATA_DIR: str | None = None


def _find_tessdata() -> str | None:
    """Find the tessdata directory containing heb.traineddata."""
    # Check env var first
    env_path = os.environ.get("TESSDATA_PREFIX")
    if env_path and Path(env_path, "heb.traineddata").exists():
        return env_path

    # Common locations on Debian/Ubuntu
    for candidate in [
        "/usr/share/tesseract-ocr/5/tessdata",
        "/usr/share/tesseract-ocr/4.00/tessdata",
        "/usr/share/tessdata",
        "/usr/local/share/tessdata",
    ]:
        if Path(candidate, "heb.traineddata").exists():
            return candidate

    logger.warning("Could not find heb.traineddata in any standard location!")
    return None


def get_tessdata() -> str | None:
    """Get tessdata directory path (cached after first call)."""
    global _TESSDATA_DIR
    if _TESSDATA_DIR is None:
        _TESSDATA_DIR = _find_tessdata() or ""
        if _TESSDATA_DIR:
            logger.info(f"Tesseract Hebrew data found at: {_TESSDATA_DIR}")
        else:
            logger.warning("No Hebrew tessdata found — OCR will use wrong language")
    return _TESSDATA_DIR or None


# ── Text extraction helpers ───────────────────────────────────────────────


def _extract_text_blocks(page) -> list[str]:
    """Extract text from a PDF page using block-level extraction (RTL-safe)."""
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


def _ocr_page(page) -> list[str]:
    """OCR a scanned PDF page using Tesseract (Hebrew)."""
    try:
        ocr_kwargs = {"language": "heb", "dpi": 200, "full": True}
        tessdata = get_tessdata()
        if tessdata:
            ocr_kwargs["tessdata"] = tessdata
        tp = page.get_textpage_ocr(**ocr_kwargs)
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


# ── Public API ────────────────────────────────────────────────────────────


def convert_pdf(pdf_path: Path, doc_id: str, *, use_ocr: bool = False) -> str | None:
    """Convert a local PDF file to markdown text.

    Args:
        pdf_path: Path to the PDF file on disk.
        doc_id: Document ID for logging.
        use_ocr: If True, falls back to Tesseract OCR for scanned PDFs.

    Returns:
        Markdown text or None if extraction failed.
    """
    import pymupdf

    # Validate PDF header
    file_size = pdf_path.stat().st_size
    with open(pdf_path, "rb") as f:
        header = f.read(8)
    if not header.startswith(b"%PDF"):
        logger.warning(f"Not a valid PDF ({doc_id}): starts with {header[:20]!r}, size={file_size}")
        return None

    doc = pymupdf.open(str(pdf_path))
    page_count = len(doc)

    # Phase 1: Direct text extraction (fast, for digital PDFs)
    pages = []
    for i, page in enumerate(doc):
        paragraphs = _extract_text_blocks(page)
        if paragraphs:
            pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))

    md_text = "\n\n".join(pages)

    # Phase 2: OCR fallback (slow, for scanned PDFs)
    if use_ocr and len(md_text.strip()) <= 50:
        logger.info(f"No embedded text in {doc_id} ({page_count} pages), trying OCR...")
        pages = []
        for i, page in enumerate(doc):
            paragraphs = _ocr_page(page)
            if paragraphs:
                pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))
        md_text = "\n\n".join(pages)
        if md_text and len(md_text.strip()) > 50:
            logger.info(f"OCR succeeded for {doc_id}: {len(md_text)} chars")
    elif len(md_text.strip()) <= 50 and not use_ocr:
        logger.info(f"No embedded text in {doc_id} ({page_count} pages), OCR not enabled")

    doc.close()

    if md_text and len(md_text.strip()) > 50:
        logger.info(f"Converted {doc_id}: {len(md_text)} chars ({page_count} pages)")
        return md_text

    logger.warning(
        f"PDF conversion produced empty/short text for {doc_id} "
        f"(pages={page_count}, file_size={file_size}, text_len={len(md_text)})"
    )
    return None


def convert_pdf_bytes(pdf_bytes: bytes, doc_id: str, *, use_ocr: bool = False) -> str | None:
    """Convert PDF bytes to markdown text.

    Writes bytes to a temp file, converts, and cleans up.
    """
    if not pdf_bytes or not pdf_bytes[:5].startswith(b"%PDF"):
        starts = repr(pdf_bytes[:20]) if pdf_bytes else "empty"
        logger.warning(f"Invalid PDF bytes for {doc_id}: starts={starts}")
        return None

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = Path(tmp.name)

    try:
        return convert_pdf(tmp_path, doc_id, use_ocr=use_ocr)
    finally:
        tmp_path.unlink(missing_ok=True)
