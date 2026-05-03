"""Importer for the odata.org.il snapshot of gov.il conflict-of-interest declarations.

The dataset publishes the same documents that used to be scraped live from
gov.il, bundled into three ZIP files plus a metadata CSV. Each ZIP contains:
  - the same metadata CSV at "איתור הסדרי ניגוד עניינים/איתור הסדרי ניגוד עניינים.csv"
  - a slice of the PDFs under "איתור הסדרי ניגוד עניינים/attachments/"

Total: 346 PDFs across the three ZIPs (~210 MB).

This module:
  1. Streams each ZIP into a SpooledTemporaryFile (no 200 MB blob in RAM).
  2. Reads the embedded CSV once (it is identical across all three parts).
  3. Walks attachments/ in each ZIP and, for every PDF, parses the filename
     and pairs it with the next un-consumed CSV row that has the same
     person name. Yields one OdataRecord per PDF — including the PDF bytes.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import logging
import re
import tempfile
import zipfile
from collections.abc import Awaitable, Callable
from typing import Any  # noqa: F401  # kept for future ProgressCb refinements

import httpx

from ocoi_common.models import OdataRecord

logger = logging.getLogger("ocoi.odata")

ODATA_DATASET_PAGE = (
    "https://www.odata.org.il/dataset/gov-versions-scraper-ministers_conflict-274fd0fe"
)

ODATA_ZIP_URLS: list[str] = [
    "https://www.odata.org.il/dataset/f480b912-8858-4ee5-ab57-87be2a39821f/resource/928959a7-129a-4bcc-af6e-0f3fc68638ad/download/-part-1.zip",
    "https://www.odata.org.il/dataset/f480b912-8858-4ee5-ab57-87be2a39821f/resource/2dbd1ba6-d76f-4015-bfbb-983abed4426d/download/-part-2.zip",
    "https://www.odata.org.il/dataset/f480b912-8858-4ee5-ab57-87be2a39821f/resource/482b62a0-831f-48c6-989a-1a34722b46b5/download/-part-3.zip",
]

INNER_CSV_NAME = "איתור הסדרי ניגוד עניינים/איתור הסדרי ניגוד עניינים.csv"
ATTACHMENTS_PREFIX = "איתור הסדרי ניגוד עניינים/attachments/"

# Optional progress callback signature: ProgressCb(stage, done, total).
ProgressCb = Callable[[str, int, int], Any]


# ── Filename parsing ─────────────────────────────────────────────────────


# Some filenames in the ZIP encode the gershayim '"' as '_' (filesystem-safe).
# E.g. `מנכ_ל` should be displayed as `מנכ"ל`.
_UNDERSCORE_FOR_GERSHAYIM = re.compile(r"(?<=[֐-׿])_(?=[֐-׿])")


def _restore_gershayim(s: str) -> str:
    return _UNDERSCORE_FOR_GERSHAYIM.sub('"', s)


def parse_pdf_filename(filename: str) -> tuple[str, str | None, str | None]:
    """Split `name - position - ministry.pdf` into its three parts.

    The dataset is inconsistent about spacing around dashes (`a - b - c.pdf`,
    `a- b- c.pdf`, `a -b -c.pdf`, …). We normalise by collapsing repeated
    spaces around any '-' before splitting.
    """
    stem = filename
    if stem.lower().endswith(".pdf"):
        stem = stem[:-4]
    stem = _restore_gershayim(stem)
    # Normalise spaces around dashes so split picks up all variants.
    normalised = re.sub(r"\s*-\s*", " - ", stem)
    parts = [p.strip() for p in normalised.split(" - ") if p.strip()]
    if not parts:
        return ("", None, None)
    if len(parts) == 1:
        return (parts[0], None, None)
    if len(parts) == 2:
        return (parts[0], parts[1], None)
    # 3+ parts: the LAST part is the ministry (sometimes the position itself
    # contains a dash, e.g. "ראש הממשלה - תפקיד נוסף", which we keep as the
    # position).
    name = parts[0]
    ministry = parts[-1]
    position = " - ".join(parts[1:-1])
    return (name, position, ministry)


def _make_source_id(name: str, position: str | None, ministry: str | None) -> str:
    raw = f"{name}|{position or ''}|{ministry or ''}".encode("utf-8")
    return "odata_" + hashlib.sha256(raw).hexdigest()[:16]


# ── ZIP download + parse ─────────────────────────────────────────────────


async def _download_zip_to_tempfile(
    client: httpx.AsyncClient,
    url: str,
    progress: ProgressCb | None,
    label: str,
) -> tempfile.SpooledTemporaryFile:
    """Stream a ZIP into a SpooledTemporaryFile (~50 MB in-memory threshold)."""
    tmp = tempfile.SpooledTemporaryFile(max_size=50 * 1024 * 1024)
    received = 0
    async with client.stream("GET", url, timeout=180) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
            tmp.write(chunk)
            received += len(chunk)
            if progress and total:
                progress(f"download:{label}", received, total)
    tmp.seek(0)
    return tmp


def _read_inner_csv(zf: zipfile.ZipFile) -> list[dict[str, str]]:
    """Return the inner CSV as a list of dicts. Empty list if missing."""
    if INNER_CSV_NAME not in zf.namelist():
        return []
    with zf.open(INNER_CSV_NAME) as f:
        text = f.read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def _attachment_entries(zf: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    """Sorted list of PDF attachments in the ZIP (matches CSV order best-effort)."""
    items = [
        zi
        for zi in zf.infolist()
        if zi.filename.startswith(ATTACHMENTS_PREFIX) and not zi.is_dir()
    ]
    return items


def _zip_to_records(
    zf: zipfile.ZipFile,
    csv_rows: list[dict[str, str]],
    consumed_csv_indices: set[int],
    progress: ProgressCb | None,
    label: str,
) -> list[OdataRecord]:
    """Walk one ZIP's attachments and turn each PDF into an OdataRecord.

    `consumed_csv_indices` is shared across ZIPs so we never reuse the same
    CSV row twice when matching by name.
    """
    out: list[OdataRecord] = []
    entries = _attachment_entries(zf)
    total = len(entries)
    for idx, zi in enumerate(entries, 1):
        try:
            with zf.open(zi) as f:
                pdf_bytes = f.read()
        except Exception as e:
            logger.warning(f"Failed to read {zi.filename}: {e}")
            continue

        original_filename = _restore_gershayim(zi.filename.split("/")[-1])
        name, position, ministry = parse_pdf_filename(original_filename)
        if not name:
            logger.warning(f"Could not parse name from {original_filename}")
            continue

        # Find the next un-consumed CSV row with this name.
        date: str | None = None
        url_name: str | None = None
        for ri, row in enumerate(csv_rows):
            if ri in consumed_csv_indices:
                continue
            if (row.get("Data.function") or "").strip() == name.strip():
                date = (row.get("Data.date") or "").strip() or None
                url_name = (row.get("UrlName") or "").strip() or None
                consumed_csv_indices.add(ri)
                break

        record = OdataRecord(
            name=name.strip(),
            position=position,
            ministry=ministry,
            date=date,
            url_name=url_name,
            pdf_filename=original_filename,
            pdf_bytes=pdf_bytes,
            source_id=_make_source_id(name, position, ministry),
            raw_data={
                "pdf_size": len(pdf_bytes),
                "zip_part": label,
            },
        )
        out.append(record)

        if progress:
            progress(f"parse:{label}", idx, total)
    return out


async def iter_records(
    progress: ProgressCb | None = None,
    zip_urls: list[str] | None = None,
):
    """Async iterator over every PDF declaration in the snapshot.

    Yields one OdataRecord at a time so callers can persist incrementally and
    let the bytes go out of scope — peak memory stays bounded to roughly one
    ZIP (~70 MB) plus the current record. The CSV is parsed once from the
    first ZIP and reused thereafter.
    """
    urls = zip_urls or ODATA_ZIP_URLS
    csv_rows: list[dict[str, str]] = []
    consumed: set[int] = set()

    timeout = httpx.Timeout(300.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for i, url in enumerate(urls, 1):
            label = f"part-{i}/{len(urls)}"
            logger.info(f"Downloading {label} from {url}")
            tmp = await _download_zip_to_tempfile(client, url, progress, label)
            try:
                with zipfile.ZipFile(tmp) as zf:
                    if not csv_rows:
                        csv_rows = _read_inner_csv(zf)
                        logger.info(f"Inner CSV has {len(csv_rows)} rows")
                    # _zip_to_records is CPU-bound (PDF decompression); offload
                    # to a worker thread, then yield one record at a time.
                    part_records = await asyncio.to_thread(
                        _zip_to_records, zf, csv_rows, consumed, progress, label
                    )
                    logger.info(f"{label}: parsed {len(part_records)} PDFs")
                    for record in part_records:
                        yield record
                    # Drop the part_records list before the next ZIP is loaded.
                    part_records.clear()
            finally:
                tmp.close()


async def fetch_all_records(
    progress: ProgressCb | None = None,
    zip_urls: list[str] | None = None,
) -> list[OdataRecord]:
    """Eager wrapper around `iter_records` — useful for tests / scripts.

    Production import paths should use `iter_records` directly to avoid
    holding ~200 MB of PDF bytes in memory at once.
    """
    out: list[OdataRecord] = []
    async for rec in iter_records(progress=progress, zip_urls=zip_urls):
        out.append(rec)
    if progress:
        progress("done", len(out), len(out))
    return out


def gov_il_listing_url(url_name: str) -> str:
    """Construct the public gov.il listing URL for a given declaration slug."""
    return (
        "https://www.gov.il/he/departments/dynamiccollectors/"
        f"ministers_conflict?{url_name}"
    )


__all__ = [
    "ODATA_DATASET_PAGE",
    "ODATA_ZIP_URLS",
    "OdataRecord",
    "fetch_all_records",
    "gov_il_listing_url",
    "iter_records",
    "parse_pdf_filename",
]
