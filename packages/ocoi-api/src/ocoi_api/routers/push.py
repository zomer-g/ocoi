"""Push router — receives processed documents from the local processor CLI."""

import base64
import hashlib
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_api.schemas import (
    PushDocumentItem,
    PushDocumentResponse,
    CheckDuplicatesRequest,
    CheckDuplicatesResponse,
)
from ocoi_common.config import settings
from ocoi_common.timezone import now_israel_naive
from ocoi_db.models import Document, Source
from ocoi_db.crud import (
    get_or_create_source,
    create_document,
    upsert_person,
    upsert_company,
    upsert_association,
    upsert_domain,
    create_relationship,
    create_extraction_run,
)

logger = logging.getLogger("ocoi.api.push")

router = APIRouter(prefix="/push", tags=["push"])


# --- Auth dependency ---


async def verify_push_key(request: Request):
    """Verify the X-Push-Key header matches the configured push API key."""
    if not settings.push_api_key:
        raise HTTPException(503, "Push API key not configured on server")
    key = request.headers.get("X-Push-Key", "")
    if not key or key != settings.push_api_key:
        raise HTTPException(401, "Invalid push key")


# --- Endpoints ---


@router.post(
    "/documents",
    response_model=PushDocumentResponse,
    dependencies=[Depends(verify_push_key)],
)
async def push_document(item: PushDocumentItem, db: AsyncSession = Depends(get_db)):
    """Receive a single processed document from the local processor.

    Accepts metadata, markdown content, PDF bytes (base64), and optional
    extraction JSON. Creates Source, Document, and (if extraction provided)
    entities + relationships.
    """
    try:
        # --- Dedup by file_url ---
        existing = await db.execute(
            select(Document).where(Document.file_url == item.file_url).limit(1)
        )
        if existing.scalars().first():
            return PushDocumentResponse(status="skipped", error="duplicate file_url")

        # --- Dedup by content_hash ---
        if item.content_hash:
            existing = await db.execute(
                select(Document).where(Document.content_hash == item.content_hash).limit(1)
            )
            if existing.scalars().first():
                return PushDocumentResponse(status="skipped", error="duplicate content_hash")

        # --- Create source ---
        source = await get_or_create_source(
            db,
            source_type=item.source_type,
            source_id=item.source_id or item.file_url,
            title=item.source_title or item.title,
            url=item.source_url or item.file_url,
        )

        # --- Create document ---
        doc = await create_document(
            db,
            source_id=source.id,
            title=item.title,
            file_url=item.file_url,
            file_format=item.file_format,
            file_size=item.file_size,
        )

        # Set markdown content
        if item.markdown_content:
            doc.markdown_content = item.markdown_content
            doc.conversion_status = "converted"
            doc.converted_at = now_israel_naive()
        else:
            doc.conversion_status = "no_text"

        # Set content hash
        if item.content_hash:
            doc.content_hash = item.content_hash

        # Store PDF bytes
        if item.pdf_base64:
            try:
                doc.pdf_content = base64.b64decode(item.pdf_base64)
                if not item.content_hash:
                    doc.content_hash = hashlib.sha256(doc.pdf_content).hexdigest()
            except Exception as e:
                logger.warning(f"Failed to decode PDF base64 for {item.file_url}: {e}")

        # --- Extract entities if extraction_json provided ---
        extracted = False
        if item.extraction_json:
            try:
                extracted = await _process_extraction(db, doc, item.extraction_json)
            except Exception as e:
                logger.error(f"Extraction processing failed for {item.title}: {e}")
                doc.extraction_status = "failed"

        await db.commit()

        logger.info(
            f"Pushed document: {item.title[:60]} "
            f"(extracted={extracted})"
        )
        return PushDocumentResponse(
            status="created",
            document_id=str(doc.id),
            extracted=extracted,
        )

    except Exception as e:
        logger.error(f"Push failed for {item.file_url}: {e}", exc_info=True)
        await db.rollback()
        return PushDocumentResponse(status="error", error=str(e)[:500])


@router.post(
    "/check-duplicates",
    response_model=CheckDuplicatesResponse,
    dependencies=[Depends(verify_push_key)],
)
async def check_duplicates(
    req: CheckDuplicatesRequest,
    db: AsyncSession = Depends(get_db),
):
    """Check which file URLs already exist in the database."""
    if not req.urls:
        return CheckDuplicatesResponse(existing_urls=[])

    result = await db.execute(
        select(Document.file_url).where(Document.file_url.in_(req.urls))
    )
    existing = [row[0] for row in result.all()]
    return CheckDuplicatesResponse(existing_urls=existing)


# --- Extraction helper ---


async def _process_extraction(
    session: AsyncSession,
    doc: Document,
    extraction_json: dict,
) -> bool:
    """Parse extraction JSON and create entities + relationships.

    Reuses the same parsing logic as extraction_service._parse_llm_response.
    """
    from ocoi_api.services.extraction_service import _parse_llm_response

    extraction = _parse_llm_response(extraction_json)
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

    for assoc in extraction.associations:
        db_assoc = await upsert_association(
            session,
            name_hebrew=assoc.name_hebrew,
            registration_number=getattr(assoc, "registration_number", None),
        )
        entity_id_map[("association", assoc.name_hebrew)] = db_assoc.id

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

    entities_count = (
        len(extraction.persons)
        + len(extraction.companies)
        + len(extraction.associations)
        + len(extraction.domains)
    )
    await create_extraction_run(
        session,
        document_id=doc.id,
        extractor_type="llm",
        model_version="deepseek-chat",
        entities_found=entities_count,
        relationships_found=rels_saved,
        raw_output_json=extraction_json,
    )

    doc.extraction_status = "extracted"
    doc.extracted_at = now_israel_naive()
    return True
