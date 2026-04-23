"""Centralized PDF-to-Markdown conversion with Hebrew OCR support.

Uses lightweight CLI tools (pdftotext from poppler, tesseract) instead of
pymupdf to stay under 512MB on Render free tier.
  - pdftotext: ~5MB RSS (vs pymupdf ~150MB)
  - tesseract: ~30MB RSS per invocation, exits after each page
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger("ocoi.pdf_converter")


def _has_tool(name: str) -> bool:
    """Check if a CLI tool is available on PATH."""
    return shutil.which(name) is not None


def _pdftotext_extract(pdf_path: Path) -> str | None:
    """Extract text from PDF using poppler's pdftotext (digital PDFs)."""
    if not _has_tool("pdftotext"):
        logger.warning("pdftotext not installed — cannot extract text from digital PDFs")
        return None

    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            logger.warning(f"pdftotext failed: {result.stderr[:200]}")
            return None
        return result.stdout
    except subprocess.TimeoutExpired:
        logger.warning(f"pdftotext timed out for {pdf_path}")
        return None
    except Exception as e:
        logger.warning(f"pdftotext error: {e}")
        return None


def _get_page_count(pdf_path: Path) -> int:
    """Get number of pages using pdfinfo."""
    if not _has_tool("pdfinfo"):
        return 0
    try:
        result = subprocess.run(
            ["pdfinfo", str(pdf_path)],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.splitlines():
            if line.startswith("Pages:"):
                return int(line.split(":")[1].strip())
    except Exception:
        pass
    return 0


def _ocr_pdf(pdf_path: Path, *, max_pages: int = 4) -> str | None:
    """OCR a scanned PDF using pdftoppm + tesseract (page by page to save memory).

    max_pages limits how many pages to OCR — kept at 4 to fit inside 512MB Render
    when extraction may run concurrently with imports.
    """
    if not _has_tool("pdftoppm") or not _has_tool("tesseract"):
        logger.warning("pdftoppm or tesseract not installed — cannot OCR scanned PDFs")
        return None

    page_count = _get_page_count(pdf_path) or 20  # guess max if pdfinfo unavailable
    ocr_pages = min(page_count, max_pages)
    if page_count > max_pages:
        logger.info(f"PDF has {page_count} pages, OCR limited to first {max_pages}")
    pages_text = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for page_num in range(1, ocr_pages + 1):
            try:
                # Convert single page to grayscale PGM (much smaller than color PPM)
                ppm_prefix = os.path.join(tmpdir, f"page")
                result = subprocess.run(
                    ["pdftoppm", "-f", str(page_num), "-l", str(page_num),
                     "-r", "120", "-gray", str(pdf_path), ppm_prefix],
                    capture_output=True, timeout=30,
                )
                if result.returncode != 0:
                    break  # Past last page or error

                # Find the generated image (PGM for grayscale, PPM for color)
                ppm_files = list(Path(tmpdir).glob("page-*.*"))
                if not ppm_files:
                    break

                ppm_file = ppm_files[0]

                # OCR the image with tesseract
                ocr_result = subprocess.run(
                    ["tesseract", str(ppm_file), "stdout", "-l", "heb"],
                    capture_output=True, text=True, timeout=60,
                )

                # Clean up image immediately to save disk
                ppm_file.unlink(missing_ok=True)

                if ocr_result.returncode == 0 and ocr_result.stdout.strip():
                    text = ocr_result.stdout.strip()
                    text = re.sub(r" +", " ", text)
                    pages_text.append(f"--- עמוד {page_num} ---\n{text}")

            except subprocess.TimeoutExpired:
                logger.warning(f"OCR timed out on page {page_num}")
                # Clean up any leftover ppm files
                for f in Path(tmpdir).glob("page-*.ppm"):
                    f.unlink(missing_ok=True)
                continue
            except Exception as e:
                logger.warning(f"OCR error on page {page_num}: {e}")
                continue

    if pages_text:
        return "\n\n".join(pages_text)
    return None


def _format_extracted_text(raw_text: str, page_count: int) -> str:
    """Format pdftotext output with page markers."""
    if not raw_text or not raw_text.strip():
        return ""

    # pdftotext with -layout uses form feeds (\f) as page separators
    pages = raw_text.split("\f")
    formatted = []
    for i, page_text in enumerate(pages):
        text = page_text.strip()
        if text:
            # Clean up whitespace
            text = re.sub(r"[\u200f\u200e]+", " ", text)
            text = re.sub(r" +", " ", text)
            formatted.append(f"--- עמוד {i + 1} ---\n{text}")

    return "\n\n".join(formatted)


# ── Public API ────────────────────────────────────────────────────────────


def convert_pdf(pdf_path: Path, doc_id: str, *, use_ocr: bool = False) -> str | None:
    """Convert a local PDF file to markdown text.

    Uses pdftotext (poppler) for digital PDFs, tesseract for scanned PDFs.
    All tools are lightweight CLI binaries — no Python PDF libraries loaded.
    """
    # Validate PDF header
    file_size = pdf_path.stat().st_size
    with open(pdf_path, "rb") as f:
        header = f.read(8)
    if not header.startswith(b"%PDF"):
        logger.warning(f"Not a valid PDF ({doc_id}): starts with {header[:20]!r}, size={file_size}")
        return None

    page_count = _get_page_count(pdf_path)

    # Phase 1: Direct text extraction (fast, ~5MB memory)
    raw_text = _pdftotext_extract(pdf_path)
    md_text = _format_extracted_text(raw_text or "", page_count)

    # Phase 2: OCR fallback for scanned PDFs (slower, ~30MB per page but exits)
    if use_ocr and len(md_text.strip()) <= 50:
        logger.info(f"No embedded text in {doc_id} ({page_count} pages), trying OCR...")
        ocr_text = _ocr_pdf(pdf_path)
        if ocr_text and len(ocr_text.strip()) > 50:
            md_text = ocr_text
            logger.info(f"OCR succeeded for {doc_id}: {len(md_text)} chars")
    elif len(md_text.strip()) <= 50 and not use_ocr:
        logger.info(f"No embedded text in {doc_id} ({page_count} pages), OCR not enabled")

    if md_text and len(md_text.strip()) > 50:
        logger.info(f"Converted {doc_id}: {len(md_text)} chars ({page_count} pages)")
        return md_text

    logger.warning(
        f"PDF conversion produced empty/short text for {doc_id} "
        f"(pages={page_count}, file_size={file_size}, text_len={len(md_text)})"
    )
    return None


def convert_pdf_bytes(pdf_bytes: bytes, doc_id: str, *, use_ocr: bool = False) -> str | None:
    """Convert PDF bytes to markdown text."""
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
