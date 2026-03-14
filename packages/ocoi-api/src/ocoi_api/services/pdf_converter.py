"""Centralized PDF-to-Markdown conversion with Hebrew OCR support.

Runs pymupdf in a SUBPROCESS to avoid OOM on 512MB Render instances.
All pymupdf memory is freed when the subprocess exits.
"""

import json
import logging
import sys
import tempfile
from pathlib import Path

logger = logging.getLogger("ocoi.pdf_converter")


# ── Subprocess worker script (runs pymupdf in isolation) ──────────────

_WORKER_SCRIPT = '''
import json, os, re, sys
from pathlib import Path

def _find_tessdata():
    env_path = os.environ.get("TESSDATA_PREFIX")
    if env_path and Path(env_path, "heb.traineddata").exists():
        return env_path
    for candidate in [
        "/usr/share/tesseract-ocr/5/tessdata",
        "/usr/share/tesseract-ocr/4.00/tessdata",
        "/usr/share/tessdata",
        "/usr/local/share/tessdata",
    ]:
        if Path(candidate, "heb.traineddata").exists():
            return candidate
    return None

def _extract_text_blocks(page):
    blocks = page.get_text("blocks")
    paragraphs = []
    for b in blocks:
        if b[6] == 0:
            text = b[4].strip()
            if text:
                text = re.sub(r"[\\u200f\\u200e]+", " ", text)
                text = re.sub(r"(?<!\\n)\\n(?!\\n)", " ", text)
                text = re.sub(r" +", " ", text)
                paragraphs.append(text)
    return paragraphs

def _ocr_page(page, tessdata):
    try:
        ocr_kwargs = {"language": "heb", "dpi": 200, "full": True}
        if tessdata:
            ocr_kwargs["tessdata"] = tessdata
        tp = page.get_textpage_ocr(**ocr_kwargs)
        blocks = page.get_text("blocks", textpage=tp)
        paragraphs = []
        for b in blocks:
            if b[6] == 0:
                text = b[4].strip()
                if text:
                    text = re.sub(r"[\\u200f\\u200e]+", " ", text)
                    text = re.sub(r"(?<!\\n)\\n(?!\\n)", " ", text)
                    text = re.sub(r" +", " ", text)
                    paragraphs.append(text)
        return paragraphs
    except Exception:
        return []

def main():
    import pymupdf
    args = json.loads(sys.argv[1])
    pdf_path = args["pdf_path"]
    use_ocr = args.get("use_ocr", False)

    with open(pdf_path, "rb") as f:
        header = f.read(8)
    if not header.startswith(b"%PDF"):
        json.dump({"error": "not_pdf"}, sys.stdout)
        return

    doc = pymupdf.open(pdf_path)
    page_count = len(doc)

    pages = []
    for i, page in enumerate(doc):
        paragraphs = _extract_text_blocks(page)
        if paragraphs:
            pages.append(f"--- עמוד {i + 1} ---\\n" + "\\n".join(paragraphs))

    md_text = "\\n\\n".join(pages)

    if use_ocr and len(md_text.strip()) <= 50:
        tessdata = _find_tessdata()
        pages = []
        for i, page in enumerate(doc):
            paragraphs = _ocr_page(page, tessdata)
            if paragraphs:
                pages.append(f"--- עמוד {i + 1} ---\\n" + "\\n".join(paragraphs))
        md_text = "\\n\\n".join(pages)

    doc.close()

    if md_text and len(md_text.strip()) > 50:
        json.dump({"text": md_text, "pages": page_count}, sys.stdout)
    else:
        json.dump({"error": "no_text", "pages": page_count}, sys.stdout)

main()
'''


# ── Public API ────────────────────────────────────────────────────────────


def convert_pdf(pdf_path: Path, doc_id: str, *, use_ocr: bool = False) -> str | None:
    """Convert a local PDF file to markdown text.

    Runs pymupdf in a subprocess to keep main process memory low.
    """
    import subprocess

    args_json = json.dumps({"pdf_path": str(pdf_path), "use_ocr": use_ocr})

    try:
        result = subprocess.run(
            [sys.executable, "-c", _WORKER_SCRIPT, args_json],
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout per document
        )
    except subprocess.TimeoutExpired:
        logger.warning(f"PDF conversion timed out for {doc_id}")
        return None

    if result.returncode != 0:
        logger.warning(f"PDF converter subprocess failed for {doc_id}: {result.stderr[:500]}")
        return None

    try:
        output = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning(f"PDF converter returned invalid JSON for {doc_id}: {result.stdout[:200]}")
        return None

    if "error" in output:
        if output["error"] == "not_pdf":
            logger.warning(f"Not a valid PDF ({doc_id})")
        else:
            logger.warning(f"PDF conversion produced no text for {doc_id} ({output.get('pages', '?')} pages)")
        return None

    md_text = output.get("text", "")
    pages = output.get("pages", 0)
    logger.info(f"Converted {doc_id}: {len(md_text)} chars ({pages} pages)")
    return md_text


def convert_pdf_bytes(pdf_bytes: bytes, doc_id: str, *, use_ocr: bool = False) -> str | None:
    """Convert PDF bytes to markdown text.

    Writes bytes to a temp file, converts in subprocess, and cleans up.
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
