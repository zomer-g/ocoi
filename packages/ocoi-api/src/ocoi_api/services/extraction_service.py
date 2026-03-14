"""Extraction service — PDF→text→DeepSeek→entities, with configurable prompts."""

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
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

כללים קריטיים:
1. חלץ אך ורק מידע שכתוב במפורש במסמך. אסור להמציא או לנחש שמות, חברות או עובדות.
2. אם שם לא מופיע במפורש — כתוב null. לעולם אל תנחש.
3. שמות חברות וארגונים — העתק בדיוק כפי שמופיע במסמך.
4. החזר JSON תקין בלבד."""

_DEFAULT_USER_PROMPT = """נתח את מסמך ניגוד העניינים הבא.

שלב 1 — זהה את הנושא המרכזי (בעל התפקיד):
מסמכים אלה יכולים להיות מכמה סוגים:
- חוות דעת למניעת ניגוד עניינים — נשלחת אל בעל התפקיד. חפש את שמו בשורת "לכבוד" (למשל: "לכבוד חה"כ אבי דיכטר") או בשורת "הנדון".
- פראפרזה/סיכום הסדר — השם מופיע בשורת "הנדון" (למשל: "הנדון: פראפרזה אודות ההסדר... של שר המדע... יעקב פרי").
- הצהרת ניגוד עניינים עצמית — בעל התפקיד הוא הכותב, לא הנמען. חפש את שם החותם בתחתית המסמך.
- הסדר למניעת ניגוד עניינים — השם מופיע בשורת "אל:" או "הנדון".

חשוב: חלץ את השם המלא (שם פרטי + שם משפחה), לא רק תואר. למשל: "יעקב פרי" ולא "שר", "אבי דיכטר" ולא "השר".
אם הכותרת של המסמך מכילה שם — זה בדרך כלל שם בעל התפקיד.

שלב 2 — זהה חברות, בנקים וארגונים:
חלץ את השם המדויק של כל חברה/בנק/ארגון כפי שמופיע במסמך.
לכל חברה, ציין את סוג הקשר לבעל התפקיד:
- owns = בעלות/החזקת מניות
- manages = ניהול
- board_member = חבר דירקטוריון / יו"ר דירקטוריון
- employed_by = מועסק / עובד
- related_to = קשר אחר (עסקי, משפחתי של קרוב, וכד')

שלב 3 — זהה מגבלות/הגבלות:
לכל מגבלה, ציין:
- תיאור ההגבלה (מה בעל התפקיד מנוע מלעשות)
- שם החברה/גוף שלגביו חלה ההגבלה
- סוג ההגבלה: full (מלאה), partial (חלקית/מותנית בהתייעצות), cooling_off (תקופת צינון)

שלב 4 — בני משפחה עם קשרים עסקיים:
רק אם מוזכרים במפורש עם קשר עסקי.
אם השם מושחר/מצונזר (כוכביות ********) — כתוב "מצונזר" בשדה השם.

החזר JSON במבנה הבא:

{{
  "office_holder": {{
    "name_hebrew": "השם המלא כפי שמופיע במסמך",
    "name_english": null,
    "title": "שר / סגן שר / מנכ\"ל / יו\"ר / עיתונאי / וכד'",
    "position": "התפקיד המדויק",
    "ministry": "שם המשרד או הגוף (למשל: משרד החקלאות ופיתוח הכפר)"
  }},
  "restrictions": [
    {{
      "description": "תיאור ההגבלה כפי שמופיע במסמך",
      "related_entities": ["שם מדויק של חברה/גוף"],
      "related_domains": ["תחום עסקי אם צוין"],
      "restriction_type": "full|partial|cooling_off",
      "end_date": "YYYY-MM-DD or null",
      "details": "פרטים נוספים"
    }}
  ],
  "companies": [
    {{
      "name_hebrew": "שם החברה/בנק בדיוק כפי שמופיע במסמך",
      "name_english": null,
      "company_type": "פרטית|ציבורית|ממשלתית|בנק|חל\"צ|קבוצה|null",
      "relationship_to_holder": "owns|manages|employed_by|board_member|related_to"
    }}
  ],
  "associations": [
    {{
      "name_hebrew": "שם העמותה בדיוק כפי שמופיע",
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
  "domains": ["תחום עסקי ספציפי בלבד — למשל: בנקאות, חקלאות, תקשורת, נדל\"ן, ביטחון"]
}}

חשוב:
- אל תכלול תחומים גנריים כמו "החזקת מניות", "מכירת מניות", "דיונים פרלמנטריים", "החלטות ממשלה". כלול רק מגזרים עסקיים ספציפיים.
- אם אין חברות במסמך — החזר רשימה ריקה.
- אם אין מגבלות — החזר רשימה ריקה.
- אם המסמך קובע שלא נדרשות הגבלות — ציין זאת ב-restrictions עם description מתאים ו-restriction_type: null.

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


# ── Extraction state (same polling pattern as import) ────────────────────

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


def get_extraction_status() -> dict:
    return dict(_extraction_state)


# ── Main extraction pipeline ─────────────────────────────────────────────


async def run_extraction(document_ids: list[str] | None = None) -> dict:
    """Run extraction on documents. Updates _extraction_state for polling."""
    global _extraction_state

    if _extraction_state["running"]:
        return {"status": "error", "message": "Extraction already running"}

    _extraction_state.update({
        "running": True,
        "total": 0,
        "processed": 0,
        "entities_found": 0,
        "relationships_found": 0,
        "errors": 0,
        "error_messages": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
    })

    try:
        await _run_extraction(document_ids)
    except Exception as e:
        _extraction_state["error_messages"].append(f"Fatal: {str(e)}")
        _extraction_state["errors"] += 1
    finally:
        _extraction_state["running"] = False
        _extraction_state["finished_at"] = datetime.now(timezone.utc).isoformat()

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
            result = await session.execute(
                select(Document.id).where(Document.extraction_status == "pending")
            )
        doc_ids = [row[0] for row in result.all()]

    _extraction_state["total"] = len(doc_ids)
    logger.info(f"Starting extraction on {len(doc_ids)} documents")

    # Phase 2: Process each doc in its own session (commit after each)
    for doc_id in doc_ids:
        try:
            async with bg_session_factory() as session:
                result = await session.execute(
                    select(Document).options(
                        undefer(Document.markdown_content),
                        undefer(Document.pdf_content),
                    ).where(Document.id == doc_id)
                )
                doc = result.scalars().first()
                if not doc:
                    _extraction_state["processed"] += 1
                    continue

                # Step 1: Get text content (convert PDF if needed)
                text = doc.markdown_content
                if not text:
                    text = await _download_and_convert(doc)
                    if text:
                        doc.markdown_content = text
                        doc.conversion_status = "converted"
                    else:
                        doc.conversion_status = "failed"
                        doc.extraction_status = "failed"
                        await session.commit()
                        _extraction_state["errors"] += 1
                        _extraction_state["error_messages"].append(
                            f"Failed to extract text: {doc.title[:60]}"
                        )
                        _extraction_state["processed"] += 1
                        continue

                # Step 2: Send to DeepSeek
                title_prefix = f"כותרת המסמך: {doc.title}\n\n" if doc.title else ""
                truncated = title_prefix + text[:15000]
                user_prompt = prompt_config["user_prompt"].format(document_text=truncated)

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

                content = response.choices[0].message.content
                data = json.loads(content)
                extraction = _parse_llm_response(data)

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
                            document_id=doc.id,
                            details=rel.details,
                            restriction_type=rel.restriction_type.value if rel.restriction_type else None,
                            confidence=rel.confidence,
                        )
                        rels_saved += 1

                # Step 4: Create extraction run record
                entities_count = len(extraction.persons) + len(extraction.companies) + len(extraction.associations) + len(extraction.domains)
                await create_extraction_run(
                    session,
                    document_id=doc.id,
                    extractor_type="llm",
                    model_version="deepseek-chat",
                    entities_found=entities_count,
                    relationships_found=rels_saved,
                    raw_output_json=data,
                )

                doc.extraction_status = "extracted"
                doc.extracted_at = datetime.now(timezone.utc)
                await session.commit()

                _extraction_state["entities_found"] += entities_count
                _extraction_state["relationships_found"] += rels_saved

                logger.info(
                    f"Extracted: {doc.title[:50]} → "
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


async def _download_and_convert(doc: Document) -> str | None:
    """Get PDF bytes (from DB, disk, or URL) and convert to markdown.

    Uses the centralized pdf_converter module for all conversion logic.
    """
    from ocoi_api.services.pdf_converter import convert_pdf_bytes

    try:
        # Get PDF bytes: DB → disk → download
        pdf_bytes = None
        if doc.pdf_content:
            pdf_bytes = doc.pdf_content
        else:
            pdf_path = Path(settings.pdf_dir) / f"{doc.id}.pdf"
            if pdf_path.exists():
                pdf_bytes = pdf_path.read_bytes()

        if not pdf_bytes and doc.file_url and not doc.file_url.startswith("upload://"):
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
                resp = await http.get(doc.file_url)
                resp.raise_for_status()
                pdf_bytes = resp.content
                # Store in DB for next time
                doc.pdf_content = pdf_bytes

        if not pdf_bytes:
            return None

        return convert_pdf_bytes(pdf_bytes, doc.id, use_ocr=True)

    except Exception as e:
        logger.error(f"PDF download/convert failed for {doc.file_url}: {e}")
        return None


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
