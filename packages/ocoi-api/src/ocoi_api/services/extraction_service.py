"""Extraction service — PDF→text→DeepSeek→entities, with configurable prompts."""

import asyncio
import json
from datetime import datetime
from pathlib import Path

import httpx

from ocoi_common.timezone import now_israel, now_israel_naive
from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.orm import undefer

from ocoi_common.config import settings
from ocoi_common.logging import setup_logging
from ocoi_common.models import (
    EntityType, RelationshipType, RestrictionType,
    ExtractionResult, ExtractedPerson, ExtractedCompany,
    ExtractedAssociation, ExtractedDomain, ExtractedRelationship,
)
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.models import Document
from ocoi_db.crud import (
    upsert_person, upsert_company, upsert_association, upsert_domain,
    create_relationship, create_extraction_run,
)

logger = setup_logging("ocoi.api.extraction")

PROMPT_FILE = Path(settings.data_dir) / "extraction_prompt.json"

# Default prompts
_DEFAULT_SYSTEM_PROMPT = """אתה מומחה בניתוח הסדרים למניעת ניגוד עניינים בישראל.

כללי זהב:
1. חלץ אך ורק מידע שכתוב במפורש במסמך. אסור להמציא או לנחש שמות, חברות או עובדות.
2. אם שם לא מופיע במפורש — כתוב null. לעולם אל תנחש.
3. שמות חברות, עמותות, מפלגות, קרנות, מועצות, משרדי עו"ד — העתק בדיוק כפי שמופיע במסמך.
4. **עדיפות קריטית לשמות ספציפיים על פני תחומים מופשטים**. אם המסמך מזכיר בשם ספציפי — זו חברה/עמותה, לא תחום.
5. החזר JSON תקין בלבד."""

_DEFAULT_USER_PROMPT = """נתח את מסמך ניגוד העניינים הבא.

═══════════════════════════════════════════════════════
שלב 1 — זהה את הנושא המרכזי (בעל התפקיד):
═══════════════════════════════════════════════════════
מסמכים אלה יכולים להיות מכמה סוגים:
- חוות דעת למניעת ניגוד עניינים — נשלחת אל בעל התפקיד. חפש את שמו בשורת "לכבוד" (למשל: "לכבוד חה"כ אבי דיכטר") או בשורת "הנדון".
- פראפרזה/סיכום הסדר — השם מופיע בשורת "הנדון".
- הצהרת ניגוד עניינים עצמית — בעל התפקיד הוא הכותב. חפש "אני הח"מ <שם>" או "אני <שם>, ת.ז.".
- הסדר למניעת ניגוד עניינים — השם מופיע בשורת "אל:", "לכבוד" או "הנדון".

חשוב: חלץ את השם המלא (שם פרטי + שם משפחה), לא רק תואר. למשל: "יעקב פרי" ולא "שר".
אם הטקסט במסמך לא קריא (OCR של כתב יד או שאלון ריק) אבל **הכותרת של המסמך מכילה שם ברור** — השתמש בשם מהכותרת.

═══════════════════════════════════════════════════════
שלב 2 — זהה כל חברה/עמותה/גוף המוזכר בשם:
═══════════════════════════════════════════════════════
⚠️ זהו השלב הקריטי ביותר. תת-חילוץ פה הוא הבעיה המרכזית במערכת.

**חוק מרכזי: כל גוף שמוזכר בשם מפורש חייב להיחלץ בנפרד, בשמו המלא.**

דוגמאות למה שחייב להיחלץ כ-company/association (לא כ-domain!):
✅ "משרד עורכי הדין שיבולת ושות'" → company (שם: "משרד עורכי הדין שיבולת ושות'")
✅ "חברת שפיר" → company (שם: "חברת שפיר")
✅ "חברת וי מדיה פרסום ונדל"ן" → company
✅ "מועצה דתית מרחבים" → association (שם: "מועצה דתית מרחבים")
✅ "קרן קריית מלאכי לפיתוח הספורט" → association
✅ "מפלגת הליכוד" → association
✅ "מפלגת יש עתיד" → association
✅ "עמותת חמישים פלוס מינוס" → association
✅ "עמותת ירעים" → association
✅ "הסתדרות הציונית העולמית" → association
✅ "קרן היסוד" → association
✅ "הקונגרס היהודי העולמי" → association
✅ "מכללת קיי באר שבע" → association (מוסד חינוך = עמותה)
✅ "בנק הפועלים", "בנק לאומי" → company (type: בנק)
✅ "חברת איתוראן" → company

❌ שגיאות נפוצות (אל תעשה!):
- טקסט אומר "חברת שפיר" → ❌ לא תחום "תחזוקה"! ✅ חברה בשם "חברת שפיר"
- טקסט אומר "משרד עורכי דין שיבולת" → ❌ לא תחום "עורכי דין"! ✅ חברה/משרד בשם זה
- טקסט אומר "מועצה דתית מרחבים" → ❌ לא תחום "דת"! ✅ עמותה בשם זה
- טקסט אומר "מפלגת הליכוד" → ❌ לא תחום "פוליטיקה"! ✅ עמותה בשם "מפלגת הליכוד"

**איך למצוא כל הישויות:**
חפש בטקסט הסדרי התחייבויות כמו "להימנע מלטפל בנושאים הקשורים ל: X, Y, Z" או "לא לעסוק בענייני: X, Y, Z".
כל פריט ברשימה (X, Y, Z) חייב להיחלץ כישות נפרדת בשמו המלא.

לכל חברה/ארגון, ציין את סוג הקשר:
- owns = בעלות/החזקת מניות
- manages = ניהול
- board_member = חבר דירקטוריון / יו"ר דירקטוריון
- employed_by = מועסק / עובד שם
- related_to = חבר, קרוב משפחה של עובד שם, או קשר אחר

═══════════════════════════════════════════════════════
שלב 3 — זהה מגבלות/הגבלות:
═══════════════════════════════════════════════════════
לכל מגבלה, ציין:
- תיאור ההגבלה (מה בעל התפקיד מנוע מלעשות)
- שם/שמות החברות/גופים שלגביהם חלה ההגבלה (בדיוק כפי שמופיעים)
- סוג ההגבלה: full (מלאה), partial (חלקית/מותנית), cooling_off (תקופת צינון)

═══════════════════════════════════════════════════════
שלב 4 — בני משפחה עם קשרים עסקיים:
═══════════════════════════════════════════════════════
רק אם מוזכרים במפורש עם קשר עסקי (למשל: "בת זוגי עובדת בחברת X").
אם השם מושחר/מצונזר — כתוב "מצונזר" בשדה השם.

═══════════════════════════════════════════════════════
שלב 5 — domains (תחומים) — רק כ-FALLBACK:
═══════════════════════════════════════════════════════
**domains הם לא החלופה לחברות/עמותות בעלות שם.**
השתמש ב-domains רק כאשר:
- המסמך מזכיר תחום כללי ללא שם ספציפי (למשל: "כל חברה בתחום הבנקאות")
- אין ישות ספציפית בשם שאפשר לחלץ

אם חברה מוזכרת בשם (למשל "חברת וי מדיה"), היא הולכת ל-companies, **לא** ל-domains.

═══════════════════════════════════════════════════════

החזר JSON במבנה הבא:

{{
  "office_holder": {{
    "name_hebrew": "השם המלא כפי שמופיע במסמך או בכותרת",
    "name_english": null,
    "title": "שר / סגן שר / מנכ\"ל / יו\"ר / חבר ועדה / וכד'",
    "position": "התפקיד המדויק",
    "ministry": "שם המשרד או הגוף (למשל: משרד החקלאות, הכנסת, ועדה מקומית X)"
  }},
  "restrictions": [
    {{
      "description": "תיאור ההגבלה כפי שמופיע במסמך",
      "related_entities": ["שם מדויק של חברה/גוף"],
      "related_domains": ["תחום עסקי אם צוין בלי שם ספציפי"],
      "restriction_type": "full|partial|cooling_off",
      "end_date": "YYYY-MM-DD or null",
      "details": "פרטים נוספים"
    }}
  ],
  "companies": [
    {{
      "name_hebrew": "שם החברה/בנק/משרד עו\"ד בדיוק כפי שמופיע",
      "name_english": null,
      "company_type": "פרטית|ציבורית|ממשלתית|בנק|חל\"צ|קבוצה|null",
      "relationship_to_holder": "owns|manages|employed_by|board_member|related_to"
    }}
  ],
  "associations": [
    {{
      "name_hebrew": "שם העמותה/מפלגה/מוסד/קרן/מועצה/מכללה בדיוק כפי שמופיע",
      "relationship_to_holder": "manages|board_member|related_to"
    }}
  ],
  "family_members": [
    {{
      "name": "שם או מצונזר",
      "relation": "חבר קרוב / בן זוג / ילד / הורה / אח",
      "related_companies": ["שם חברה אם רלוונטי"]
    }}
  ],
  "domains": ["תחום עסקי ספציפי — רק כשאין שם של גוף ספציפי לחלץ. למשל: בנקאות, נדל\"ן, ביטחון"]
}}

כללים אחרונים:
- **אם בחרת בדומיין במקום בשם ספציפי — עצור וחזור, זו כנראה טעות.**
- אל תכלול תחומים גנריים כמו "החזקת מניות", "דיונים פרלמנטריים", "החלטות ממשלה".
- אם אין חברות במסמך — החזר רשימה ריקה.
- אם אין מגבלות — החזר רשימה ריקה.
- אם המסמך קובע שלא נדרשות הגבלות — ציין זאת ב-restrictions עם restriction_type: null.

טקסט המסמך:
{document_text}"""


# ── Prompt management ────────────────────────────────────────────────────


def get_extraction_prompt() -> dict:
    """Read the extraction prompt from disk, or return defaults."""
    if PROMPT_FILE.exists():
        try:
            data = json.loads(PROMPT_FILE.read_text(encoding="utf-8"))
            return {
                "system_prompt": data.get("system_prompt", _DEFAULT_SYSTEM_PROMPT),
                "user_prompt": data.get("user_prompt", _DEFAULT_USER_PROMPT),
            }
        except Exception:
            pass
    return {
        "system_prompt": _DEFAULT_SYSTEM_PROMPT,
        "user_prompt": _DEFAULT_USER_PROMPT,
    }


def set_extraction_prompt(system_prompt: str, user_prompt: str) -> None:
    """Save the extraction prompt to disk."""
    PROMPT_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROMPT_FILE.write_text(
        json.dumps({"system_prompt": system_prompt, "user_prompt": user_prompt}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── Text preprocessing helpers (smart truncation, bundle splitting, title fallback) ──

# Regex patterns for COI arrangement starts — tolerant to OCR noise (extra periods, spaces)
# Hebrew OCR often inserts stray periods/commas between words, e.g. "הצהרה. והתחייבות"
import re as _re_markers

_ARRANGEMENT_START_PATTERNS = [
    # "הסדר למניעת ניגוד עניינים" (with optional OCR punct)
    _re_markers.compile(r"הסדר\s*[.,]?\s*למניעת\s*ניגוד\s*עניינים"),
    # "הצהרה והתחייבות למניעת ניגוד עניינים" (with optional OCR punct between words)
    _re_markers.compile(r"הצהרה\s*[.,]?\s*ו?התחייבות\s*למניעת\s*ניגוד\s*עניינים"),
    # "חוות דעת למניעת ניגוד עניינים"
    _re_markers.compile(r"חוות\s*[.,]?\s*דעת\s*למניעת\s*ניגוד\s*עניינים"),
]


def _find_arrangement_positions(text: str) -> list[int]:
    """Find all positions where a new COI arrangement section starts.

    Uses regex patterns tolerant to OCR noise (stray punctuation between words).
    """
    positions = set()
    for pat in _ARRANGEMENT_START_PATTERNS:
        for m in pat.finditer(text):
            positions.add(m.start())
    return sorted(positions)


def _split_bundle_arrangements(text: str, min_section_chars: int = 400) -> list[str]:
    """Split a bundled document containing multiple COI arrangements into sections.

    Each section starts at a "הסדר למניעת ניגוד עניינים" (or similar) marker.
    If only one marker (or zero) — returns [text] unchanged.
    Tiny sections (<min_section_chars) are merged into the next section.
    """
    positions = _find_arrangement_positions(text)
    if len(positions) <= 1:
        return [text]

    # Build sections: from each position to the next (or end)
    sections = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        section = text[start:end]
        if section.strip():
            sections.append(section)

    # Merge tiny sections (likely table-of-contents or stray markers) into the next one
    merged = []
    buffer = ""
    for s in sections:
        if buffer:
            s = buffer + "\n\n" + s
            buffer = ""
        if len(s) < min_section_chars:
            buffer = s
        else:
            merged.append(s)
    if buffer:
        # Leftover small section — append to last if exists, else keep as its own
        if merged:
            merged[-1] = merged[-1] + "\n\n" + buffer
        else:
            merged.append(buffer)

    return merged if merged else [text]


def _smart_truncate(text: str, max_chars: int = 15000) -> str:
    """Truncate text to max_chars, preferring the typed arrangement section.

    Many COI PDFs have a scanned questionnaire at the start (mostly illegible OCR)
    followed by a typed arrangement at the end. Naive truncation keeps the useless
    scanned part. Instead, if an arrangement marker appears past the midpoint,
    start the window at the EARLIEST such marker (for max runway).
    """
    if len(text) <= max_chars:
        return text

    positions = _find_arrangement_positions(text)
    # Pick the EARLIEST marker past the midpoint — gives maximum runway after it.
    # (Earliest marker still past the midpoint skips the scanned questionnaire.)
    for pos in positions:
        if pos > max_chars // 2:
            return text[pos:pos + max_chars]

    # No good marker found — default to first max_chars
    return text[:max_chars]


# Hebrew name detection for title fallback
import re as _re

_HEBREW_CHAR = r"\u0590-\u05FF"
# Noise words commonly found in COI filenames (to strip before looking for names)
_TITLE_NOISE_WORDS = {
    # COI-related noise
    "הסדר", "הסכם", "הסדרי", "הסכמי", "למניעת", "מניעת", "ניגוד", "עניינים", "עיניינים",
    "הצהרה", "התחייבות", "חוות", "דעת", "פראפרזה", "סיכום",
    "מושחר", "מצונזר", "עיבוד", "OCR", "לאחר",
    # Titles / roles (not names)
    "יועץ", "יועצת", "שר", "של", "שרה", "סגן", "סגנית", "מנכל", "מנכ\"ל", "מנכייל",
    "עוזר", "עוזרת", "ראש", "מטה", "דובר", "דוברת", "עובד", "עובדת", "עובדי",
    "הכנסת", "ממשלה", "משרד", "משרדי", "ועדה", "וועדה", "מועצה",
    "נציג", "נציגה", "חבר", "חברת", "בקשה", "תרומות", "שדי",
    # Common descriptive words that appear near names but aren't names
    "חדש", "חדשה", "במליאה", "במועצה", "בחברה",
    "מסמך", "לפי", "עבור", "עם", "את", "הוא", "היא",
    # File extensions
    "pdf", "docx", "doc",
}


def _extract_name_from_title(title: str) -> str | None:
    """Best-effort: pull a Hebrew person-name from a document filename.

    Returns a cleaned 2-4 word Hebrew name if found, else None.
    Used as a fallback when extraction from text yields no person.
    """
    if not title:
        return None
    # Strip file extension
    s = title.rsplit(".", 1)[0]
    # Keep only Hebrew letters, spaces, dashes
    s = _re.sub(rf"[^{_HEBREW_CHAR}\s\-]", " ", s)
    # Split on whitespace/dashes
    tokens = [t.strip() for t in _re.split(r"[\s\-_]+", s) if t.strip()]
    # Keep only pure-Hebrew tokens that aren't noise
    name_tokens = [
        t for t in tokens
        if _re.fullmatch(rf"[{_HEBREW_CHAR}]+", t)
        and t not in _TITLE_NOISE_WORDS
        and len(t) >= 2
    ]
    if 2 <= len(name_tokens) <= 4:
        return " ".join(name_tokens)
    # If exactly 1 token remains, it's probably not a full name
    return None


# ── Extraction state (same polling pattern as import) ────────────────────

_extraction_stop = False  # Set True on SIGTERM to stop extraction gracefully

_extraction_state: dict = {
    "running": False,
    "total": 0,
    "processed": 0,
    "entities_found": 0,
    "relationships_found": 0,
    "errors": 0,
    "error_messages": [],
    "started_at": None,
    "finished_at": None,
}


def stop_extraction():
    """Signal extraction to stop gracefully."""
    global _extraction_stop
    _extraction_stop = True


def get_extraction_status() -> dict:
    return dict(_extraction_state)


# ── Main extraction pipeline ─────────────────────────────────────────────


async def run_extraction(document_ids: list[str] | None = None) -> dict:
    """Run extraction on documents. Updates _extraction_state for polling."""
    global _extraction_state

    if _extraction_state["running"]:
        return {"status": "error", "message": "Extraction already running"}

    global _extraction_stop
    _extraction_stop = False

    _extraction_state.update({
        "running": True,
        "total": 0,
        "processed": 0,
        "entities_found": 0,
        "relationships_found": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": now_israel().isoformat(),
        "finished_at": None,
    })

    try:
        await _run_extraction(document_ids)
    except Exception as e:
        _extraction_state["error_messages"].append(f"Fatal: {str(e)}")
        _extraction_state["errors"] += 1
    finally:
        _extraction_state["running"] = False
        _extraction_state["finished_at"] = now_israel().isoformat()

    return get_extraction_status()


async def _run_extraction(document_ids: list[str] | None):
    """Internal extraction loop — processes docs one at a time with per-doc commits."""
    import gc
    prompt_config = get_extraction_prompt()
    client = AsyncOpenAI(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
    )

    # Phase 1: Get list of document IDs to process
    async with async_session_factory() as session:
        if document_ids:
            result = await session.execute(
                select(Document.id).where(Document.id.in_(document_ids))
            )
        else:
            # Auto-retry: reset failed extractions that have a chance of succeeding
            # (conversion didn't permanently fail — OOM/crash may have caused the failure)
            retry_result = await session.execute(
                select(Document.id).where(
                    Document.extraction_status == "failed",
                    Document.conversion_status != "failed",
                )
            )
            retry_ids = [row[0] for row in retry_result.all()]
            if retry_ids:
                from sqlalchemy import update
                await session.execute(
                    update(Document)
                    .where(Document.id.in_(retry_ids))
                    .values(extraction_status="pending")
                )
                await session.commit()
                logger.info(f"Auto-reset {len(retry_ids)} failed extractions for retry")

            # Select pending docs, skip those with conversion_status == "failed"
            # (no PDF available at all — retrying won't help)
            result = await session.execute(
                select(Document.id).where(
                    Document.extraction_status == "pending",
                    Document.conversion_status != "failed",
                )
            )
        doc_ids = [row[0] for row in result.all()]

    _extraction_state["total"] = len(doc_ids)
    logger.info(f"Starting extraction on {len(doc_ids)} documents")

    # Phase 2: Process each doc in its own session (commit after each)
    for doc_id in doc_ids:
        if _extraction_stop:
            logger.info("Extraction stopped by shutdown signal")
            break
        try:
            async with bg_session_factory() as session:
                # Only load markdown_content first — pdf_content is large and rarely needed
                result = await session.execute(
                    select(Document).options(
                        undefer(Document.markdown_content),
                    ).where(Document.id == doc_id)
                )
                doc = result.scalars().first()
                if not doc:
                    _extraction_state["processed"] += 1
                    continue

                # Cache scalar fields before any commit (bg_session expires on commit)
                doc_title = doc.title or ""
                doc_file_id = doc.id
                doc_file_url = doc.file_url or ""

                # Step 1: Get text content (convert PDF if needed)
                text = doc.markdown_content
                if not text:
                    text, had_pdf = await _download_and_convert(session, doc_file_id, doc_file_url)
                    if text:
                        doc.markdown_content = text
                        doc.conversion_status = "converted"
                    else:
                        # Distinguish: no PDF source vs OCR produced nothing
                        if had_pdf:
                            doc.conversion_status = "no_text"  # PDF exists, OCR failed — retryable
                            error_msg = f"OCR produced no text: {doc_title[:60]}"
                        else:
                            doc.conversion_status = "failed"  # no PDF at all — permanent
                            error_msg = f"No PDF source: {doc_title[:60]}"
                        doc.extraction_status = "failed"
                        await session.commit()
                        _extraction_state["errors"] += 1
                        _extraction_state["error_messages"].append(error_msg)
                        _extraction_state["processed"] += 1
                        continue

                # Step 2: Send to DeepSeek (with bundle splitting + smart truncation)
                title_prefix = f"כותרת המסמך: {doc_title}\n\n" if doc_title else ""

                # Detect if this is a bundle (multiple arrangements in one doc)
                sections = _split_bundle_arrangements(text)
                if len(sections) > 1:
                    logger.info(f"Doc {doc_file_id}: bundle detected — {len(sections)} arrangement sections")

                # Run DeepSeek on each section and accumulate results
                merged_persons: list = []
                merged_companies: list = []
                merged_associations: list = []
                merged_domains: list = []
                merged_relationships: list = []
                merged_raw_outputs: list = []

                for section_idx, section_text in enumerate(sections):
                    # Smart-truncate each section to 15000 chars
                    truncated_section = _smart_truncate(section_text, max_chars=15000)
                    section_input = title_prefix + truncated_section
                    user_prompt = prompt_config["user_prompt"].format(document_text=section_input)

                    for attempt in range(3):
                        try:
                            response = await client.chat.completions.create(
                                model="deepseek-chat",
                                messages=[
                                    {"role": "system", "content": prompt_config["system_prompt"]},
                                    {"role": "user", "content": user_prompt},
                                ],
                                temperature=0.1,
                                max_tokens=4000,
                                response_format={"type": "json_object"},
                            )
                            break
                        except Exception as api_err:
                            if attempt < 2:
                                wait = 2 ** (attempt + 1)
                                logger.warning(f"DeepSeek API error (attempt {attempt+1}/3), retrying in {wait}s: {api_err}")
                                await asyncio.sleep(wait)
                            else:
                                raise

                    section_content = response.choices[0].message.content
                    section_data = json.loads(section_content)
                    section_extraction = _parse_llm_response(section_data)

                    merged_persons.extend(section_extraction.persons)
                    merged_companies.extend(section_extraction.companies)
                    merged_associations.extend(section_extraction.associations)
                    merged_domains.extend(section_extraction.domains)
                    merged_relationships.extend(section_extraction.relationships)
                    merged_raw_outputs.append(section_data)

                # Deduplicate by Hebrew name (keep first occurrence)
                def _dedupe_by_name(items):
                    seen = set()
                    out = []
                    for it in items:
                        key = getattr(it, "name_hebrew", None)
                        if key is None or key in seen:
                            continue
                        seen.add(key)
                        out.append(it)
                    return out

                # Title fallback: if no persons extracted, try to get one from the title
                if not merged_persons:
                    title_name = _extract_name_from_title(doc_title)
                    if title_name:
                        logger.info(f"Doc {doc_file_id}: no persons extracted — using title fallback '{title_name}'")
                        merged_persons.append(ExtractedPerson(
                            name_hebrew=title_name,
                            name_english=None,
                            title=None,
                            position=None,
                            ministry=None,
                        ))

                # Build combined extraction object
                extraction = ExtractionResult(
                    persons=_dedupe_by_name(merged_persons),
                    companies=_dedupe_by_name(merged_companies),
                    associations=_dedupe_by_name(merged_associations),
                    domains=_dedupe_by_name(merged_domains),
                    relationships=merged_relationships,
                )
                data = {"sections": merged_raw_outputs} if len(merged_raw_outputs) > 1 else merged_raw_outputs[0]

                # Step 3: Save entities and relationships to DB
                entity_id_map = {}

                for person in extraction.persons:
                    db_person = await upsert_person(
                        session,
                        name_hebrew=person.name_hebrew,
                        name_english=person.name_english,
                        title=person.title,
                        position=person.position,
                        ministry=person.ministry,
                    )
                    entity_id_map[("person", person.name_hebrew)] = db_person.id

                for company in extraction.companies:
                    db_company = await upsert_company(
                        session,
                        name_hebrew=company.name_hebrew,
                        name_english=company.name_english,
                        company_type=company.company_type,
                    )
                    entity_id_map[("company", company.name_hebrew)] = db_company.id
                    # Try matching against external registry
                    if not db_company.registration_number:
                        try:
                            from ocoi_api.services.registry_service import match_entity_against_registry
                            await match_entity_against_registry(
                                session, "company", company.name_hebrew, db_company.id
                            )
                        except Exception as match_err:
                            logger.debug(f"Registry match failed for company '{company.name_hebrew}': {match_err}")

                for assoc in extraction.associations:
                    db_assoc = await upsert_association(
                        session,
                        name_hebrew=assoc.name_hebrew,
                        registration_number=assoc.registration_number,
                    )
                    entity_id_map[("association", assoc.name_hebrew)] = db_assoc.id
                    # Try matching against external registry
                    if not db_assoc.registration_number:
                        try:
                            from ocoi_api.services.registry_service import match_entity_against_registry
                            await match_entity_against_registry(
                                session, "association", assoc.name_hebrew, db_assoc.id
                            )
                        except Exception as match_err:
                            logger.debug(f"Registry match failed for association '{assoc.name_hebrew}': {match_err}")

                for domain in extraction.domains:
                    db_domain = await upsert_domain(
                        session,
                        name_hebrew=domain.name_hebrew,
                    )
                    entity_id_map[("domain", domain.name_hebrew)] = db_domain.id

                rels_saved = 0
                for rel in extraction.relationships:
                    src_id = entity_id_map.get((rel.source_type.value, rel.source_name))
                    tgt_id = entity_id_map.get((rel.target_type.value, rel.target_name))
                    if src_id and tgt_id:
                        await create_relationship(
                            session,
                            source_entity_type=rel.source_type.value,
                            source_entity_id=src_id,
                            target_entity_type=rel.target_type.value,
                            target_entity_id=tgt_id,
                            relationship_type=rel.relationship_type.value,
                            document_id=doc_file_id,
                            details=rel.details,
                            restriction_type=rel.restriction_type.value if rel.restriction_type else None,
                            confidence=rel.confidence,
                        )
                        rels_saved += 1

                # Step 4: Create extraction run record
                entities_count = len(extraction.persons) + len(extraction.companies) + len(extraction.associations) + len(extraction.domains)
                await create_extraction_run(
                    session,
                    document_id=doc_file_id,
                    extractor_type="llm",
                    model_version="deepseek-chat",
                    entities_found=entities_count,
                    relationships_found=rels_saved,
                    raw_output_json=data,
                )

                doc.extraction_status = "extracted"
                doc.extracted_at = now_israel_naive()
                await session.commit()

                _extraction_state["entities_found"] += entities_count
                _extraction_state["relationships_found"] += rels_saved

                logger.info(
                    f"Extracted: {doc_title[:50]} → "
                    f"{len(extraction.persons)}P, {len(extraction.companies)}C, {rels_saved}R"
                )

        except Exception as e:
            logger.error(f"Extraction failed for doc {doc_id}: {e}", exc_info=True)
            # Mark as failed in a fresh session
            try:
                async with bg_session_factory() as err_session:
                    result = await err_session.execute(
                        select(Document).where(Document.id == doc_id)
                    )
                    doc = result.scalars().first()
                    if doc:
                        doc.extraction_status = "failed"
                        await err_session.commit()
            except Exception:
                pass
            _extraction_state["errors"] += 1
            if len(_extraction_state["error_messages"]) < 20:
                _extraction_state["error_messages"].append(
                    f"Doc {doc_id}: {e}"
                )

        _extraction_state["processed"] += 1
        gc.collect()


# ── PDF download + text extraction ───────────────────────────────────────


async def _download_and_convert(session, doc_id: str, file_url: str) -> tuple[str | None, bool]:
    """Get PDF bytes and convert to markdown with OCR.

    Priority order: DB blob (most reliable on Render) → disk → URL download.
    Runs CPU-intensive conversion in a thread to avoid blocking the event loop.

    Returns (text, had_pdf): text is the converted markdown, had_pdf indicates
    whether a PDF was found at all (to distinguish 'no PDF' from 'OCR failed').
    """
    import tempfile
    from ocoi_api.services.pdf_converter import convert_pdf

    pdf_bytes = None

    # 1. Load from DB (primary source — Render has no persistent disk)
    try:
        result = await session.execute(
            select(Document.pdf_content).where(Document.id == doc_id)
        )
        pdf_bytes = result.scalar()
        if pdf_bytes:
            logger.info(f"Loaded PDF from DB for {doc_id} ({len(pdf_bytes)} bytes)")
    except Exception as e:
        logger.warning(f"DB pdf_content load failed for {doc_id}: {e}")

    # 2. Try disk cache
    if not pdf_bytes:
        pdf_path = Path(settings.pdf_dir) / f"{doc_id}.pdf"
        if pdf_path.exists():
            try:
                pdf_bytes = await asyncio.to_thread(pdf_path.read_bytes)
                logger.info(f"Loaded PDF from disk for {doc_id}")
            except Exception as e:
                logger.warning(f"Disk read failed for {doc_id}: {e}")

    # 3. Download from URL
    if not pdf_bytes and file_url and not file_url.startswith("upload://"):
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
                resp = await http.get(file_url)
                resp.raise_for_status()
                pdf_bytes = resp.content
                logger.info(f"Downloaded PDF for {doc_id} from {file_url[:80]} ({len(pdf_bytes)} bytes)")
        except Exception as e:
            logger.warning(f"PDF download failed for {doc_id} from {file_url[:80]}: {e}")

    if not pdf_bytes:
        logger.error(f"No PDF source available for {doc_id} (url={file_url[:80] if file_url else 'none'})")
        return None, False

    # Validate PDF header
    if not pdf_bytes[:5].startswith(b"%PDF"):
        logger.error(f"Invalid PDF for {doc_id}: starts with {pdf_bytes[:20]!r}")
        return None, False

    # Write to temp file and convert with OCR in a thread
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = Path(tmp.name)

        # Free PDF bytes from memory before OCR (saves ~1MB+ on 512MB Render)
        del pdf_bytes

        md_text = await asyncio.to_thread(convert_pdf, tmp_path, str(doc_id), use_ocr=True)

        # Clean up temp file
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

        if md_text:
            logger.info(f"Converted PDF for {doc_id}: {len(md_text)} chars")
        else:
            logger.warning(f"PDF conversion produced no text for {doc_id}")

        return md_text, True  # had_pdf=True, even if OCR produced nothing

    except Exception as e:
        logger.error(f"PDF conversion failed for {doc_id}: {e}")
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, True  # had_pdf=True (PDF existed but conversion crashed)


# ── LLM response parsing (adapted from ocoi-extractor/llm_extractor.py) ─


def _parse_llm_response(data: dict) -> ExtractionResult:
    """Parse DeepSeek JSON response into ExtractionResult."""
    result = ExtractionResult()

    # Extract office holder as person
    if holder := data.get("office_holder"):
        if name := holder.get("name_hebrew"):
            result.persons.append(ExtractedPerson(
                name_hebrew=name,
                name_english=holder.get("name_english"),
                title=holder.get("title"),
                position=holder.get("position"),
                ministry=holder.get("ministry"),
            ))

    # Extract companies
    for comp in data.get("companies", []):
        if name := comp.get("name_hebrew"):
            result.companies.append(ExtractedCompany(
                name_hebrew=name,
                name_english=comp.get("name_english"),
                company_type=comp.get("company_type"),
            ))
            if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                rel_type = _map_relationship(comp.get("relationship_to_holder"))
                result.relationships.append(ExtractedRelationship(
                    source_type=EntityType.PERSON,
                    source_name=holder_name,
                    target_type=EntityType.COMPANY,
                    target_name=name,
                    relationship_type=rel_type,
                    confidence=0.8,
                ))

    # Extract associations
    for assoc in data.get("associations", []):
        if name := assoc.get("name_hebrew"):
            result.associations.append(ExtractedAssociation(name_hebrew=name))
            if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                rel_type = _map_relationship(assoc.get("relationship_to_holder"))
                result.relationships.append(ExtractedRelationship(
                    source_type=EntityType.PERSON,
                    source_name=holder_name,
                    target_type=EntityType.ASSOCIATION,
                    target_name=name,
                    relationship_type=rel_type,
                    confidence=0.8,
                ))

    # Extract domains
    for domain_name in data.get("domains", []):
        result.domains.append(ExtractedDomain(name_hebrew=domain_name))
        if holder_name := data.get("office_holder", {}).get("name_hebrew"):
            result.relationships.append(ExtractedRelationship(
                source_type=EntityType.PERSON,
                source_name=holder_name,
                target_type=EntityType.DOMAIN,
                target_name=domain_name,
                relationship_type=RelationshipType.OPERATES_IN,
                confidence=0.7,
            ))

    # Extract restrictions as relationships
    for restriction in data.get("restrictions", []):
        holder_name = data.get("office_holder", {}).get("name_hebrew", "")
        for entity_name in restriction.get("related_entities", []):
            result.relationships.append(ExtractedRelationship(
                source_type=EntityType.PERSON,
                source_name=holder_name,
                target_type=EntityType.COMPANY,
                target_name=entity_name,
                relationship_type=RelationshipType.RESTRICTED_FROM,
                details=restriction.get("description"),
                restriction_type=_map_restriction(restriction.get("restriction_type")),
                confidence=0.9,
            ))

    # Extract family members (skip redacted/censored names)
    for member in data.get("family_members", []):
        if name := member.get("name"):
            if name in ("מצונזר", "********", "*****", "לא צוין"):
                continue
            result.persons.append(ExtractedPerson(name_hebrew=name))
            if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                result.relationships.append(ExtractedRelationship(
                    source_type=EntityType.PERSON,
                    source_name=holder_name,
                    target_type=EntityType.PERSON,
                    target_name=name,
                    relationship_type=RelationshipType.FAMILY_MEMBER,
                    details=member.get("relation"),
                    confidence=0.9,
                ))

    return result


def _map_relationship(rel: str | None) -> RelationshipType:
    mapping = {
        "owns": RelationshipType.OWNS,
        "manages": RelationshipType.MANAGES,
        "employed_by": RelationshipType.EMPLOYED_BY,
        "board_member": RelationshipType.BOARD_MEMBER,
        "related_to": RelationshipType.RELATED_TO,
    }
    return mapping.get(rel or "", RelationshipType.RELATED_TO)


def _map_restriction(rtype: str | None) -> RestrictionType | None:
    mapping = {
        "full": RestrictionType.FULL,
        "partial": RestrictionType.PARTIAL,
        "cooling_off": RestrictionType.COOLING_OFF,
    }
    return mapping.get(rtype or "")
