"""Conversion phase — PDF to markdown using PyMuPDF (Windows-compatible)."""

import re
from pathlib import Path

from . import state as st


def convert_pdf(pdf_path: Path) -> str:
    """Convert a PDF to text using PyMuPDF.

    Tries direct text extraction first (digital PDFs).
    Falls back to OCR via Tesseract if < 50 chars extracted (scanned PDFs).
    """
    import pymupdf

    doc = pymupdf.open(str(pdf_path))

    # Phase 1: Direct text extraction
    pages = []
    for i, page in enumerate(doc):
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
        if paragraphs:
            pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))

    result = "\n\n".join(pages)

    # Phase 2: OCR fallback for scanned PDFs
    if len(result.strip()) <= 50:
        try:
            pages = []
            for i, page in enumerate(doc):
                tp = page.get_textpage_ocr(language="heb", dpi=200, full=True)
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
                if paragraphs:
                    pages.append(f"--- עמוד {i + 1} ---\n" + "\n".join(paragraphs))
            result = "\n\n".join(pages)
        except Exception as e:
            # Tesseract not installed on Windows — graceful fallback
            print(f"    OCR unavailable: {e}")

    doc.close()
    return result


def run_convert(limit: int | None = None) -> int:
    """Convert all downloaded PDFs to markdown.

    Returns the number of newly converted documents.
    """
    print("\n=== Convert Phase ===")

    local_state = st.load_state()
    to_convert = st.get_by_status(local_state, "downloaded")

    if limit:
        to_convert = to_convert[:limit]

    if not to_convert:
        print("  Nothing to convert.")
        return 0

    print(f"  Documents to convert: {len(to_convert)}")
    converted = 0

    for i, url in enumerate(to_convert, 1):
        info = local_state[url]
        local_path = Path(info.get("local_path", ""))
        title = info.get("title", url)[:60]

        if not local_path.exists():
            print(f"  [{i}/{len(to_convert)}] {title} — PDF not found, skipping")
            st.mark(local_state, url, "failed", error="pdf_not_found")
            continue

        try:
            print(f"  [{i}/{len(to_convert)}] Converting: {title}...")
            markdown = convert_pdf(local_path)

            if len(markdown.strip()) > 50:
                # Save markdown next to PDF
                md_path = local_path.with_suffix(".md")
                md_path.write_text(markdown, encoding="utf-8")

                st.mark(
                    local_state, url, "converted",
                    markdown_path=str(md_path),
                    markdown_chars=len(markdown),
                )
                converted += 1
                print(f"    OK ({len(markdown):,} chars)")
            else:
                st.mark(local_state, url, "converted", markdown_chars=0)
                converted += 1
                print(f"    No text extracted (scanned/empty)")

        except Exception as e:
            print(f"    Failed: {e}")
            st.mark(local_state, url, "failed", error=str(e)[:200])

    print(f"\n  Converted: {converted}/{len(to_convert)}")
    return converted
