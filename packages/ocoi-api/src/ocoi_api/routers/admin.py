"""Admin CRUD routes — protected with Google OAuth JWT."""

import json
import logging
import uuid
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, Depends, Query, HTTPException, Request, UploadFile, File

logger = logging.getLogger("ocoi.api.admin")
from sqlalchemy import select, func, delete, update, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.auth import get_current_admin
from ocoi_api.dependencies import get_db
from ocoi_api.schemas import (
    PersonCreate, PersonUpdate,
    CompanyCreate, CompanyUpdate,
    AssociationCreate, AssociationUpdate,
    DomainCreate, DomainUpdate,
    RelationshipCreate,
)
from ocoi_common.config import settings
from ocoi_common.timezone import now_israel, now_israel_naive
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.crud import _add_alias, _get_aliases
from ocoi_db.models import (
    Person, Company, Association, Domain,
    EntityRelationship, Document, Source, ExtractionRun, IgnoredResource,
    SiteContent, Suggestion, User, EntityMatchProposal,
    BillingAccount, OAuthClient, UsageEvent,
)

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(get_current_admin)],
)


# ── Memory monitoring ─────────────────────────────────────────────────────

@router.get("/memory")
async def memory_info():
    """Return current process memory usage for debugging OOM issues."""
    import os
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        mem = proc.memory_info()
        return {
            "status": "ok",
            "data": {
                "rss_mb": round(mem.rss / 1024 / 1024, 1),
                "vms_mb": round(mem.vms / 1024 / 1024, 1),
            },
        }
    except ImportError:
        # Fallback: read from /proc on Linux
        try:
            with open(f"/proc/{os.getpid()}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_kb = int(line.split()[1])
                        return {"status": "ok", "data": {"rss_mb": round(rss_kb / 1024, 1)}}
        except Exception:
            pass
        return {"status": "ok", "data": {"rss_mb": None, "message": "psutil not installed"}}


# ── Dashboard stats ───────────────────────────────────────────────────────

@router.get("/stats")
async def admin_stats(db: AsyncSession = Depends(get_db)):
    counts = {}
    for model, key in [
        (Person, "persons"), (Company, "companies"),
        (Association, "associations"), (Domain, "domains"),
        (Document, "documents"), (EntityRelationship, "relationships"),
        (Source, "sources"),
    ]:
        result = await db.execute(select(func.count()).select_from(model))
        counts[key] = result.scalar()
    return {"status": "ok", "data": counts}


# ── Persons CRUD ──────────────────────────────────────────────────────────

@router.post("/persons")
async def create_person(body: PersonCreate, db: AsyncSession = Depends(get_db)):
    person = Person(**body.model_dump())
    db.add(person)
    await db.commit()
    await db.refresh(person)
    return {"status": "ok", "data": {"id": str(person.id)}}


@router.put("/persons/{person_id}")
async def update_person(
    person_id: uuid.UUID, body: PersonUpdate,
    keep_alias: bool = Query(False, description="Store old name as alias when renaming"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Person).where(Person.id == person_id))
    person = result.scalars().first()
    if not person:
        raise HTTPException(404, "Person not found")
    updates = body.model_dump(exclude_unset=True)
    # Only store old name as alias if explicitly requested (e.g. real alias like nickname)
    if keep_alias and "name_hebrew" in updates and updates["name_hebrew"] and updates["name_hebrew"] != person.name_hebrew:
        _add_alias(person, person.name_hebrew)
    # Serialize aliases list to JSON string for storage
    if "aliases" in updates:
        updates["aliases"] = json.dumps(updates["aliases"] or [], ensure_ascii=False)
    for field, value in updates.items():
        setattr(person, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/persons/{person_id}")
async def delete_person(person_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Person).where(Person.id == person_id))
    if not result.scalars().first():
        raise HTTPException(404, "Person not found")
    await db.execute(
        delete(EntityRelationship).where(
            ((EntityRelationship.source_entity_type == "person") & (EntityRelationship.source_entity_id == person_id))
            | ((EntityRelationship.target_entity_type == "person") & (EntityRelationship.target_entity_id == person_id))
        )
    )
    await db.execute(delete(Person).where(Person.id == person_id))
    await db.commit()
    return {"status": "ok"}


# ── Companies CRUD ────────────────────────────────────────────────────────

@router.post("/companies")
async def create_company(body: CompanyCreate, db: AsyncSession = Depends(get_db)):
    company = Company(**body.model_dump())
    db.add(company)
    await db.commit()
    await db.refresh(company)
    return {"status": "ok", "data": {"id": str(company.id)}}


@router.put("/companies/{company_id}")
async def update_company(
    company_id: uuid.UUID, body: CompanyUpdate,
    keep_alias: bool = Query(False, description="Store old name as alias when renaming"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalars().first()
    if not company:
        raise HTTPException(404, "Company not found")
    updates = body.model_dump(exclude_unset=True)
    if keep_alias and "name_hebrew" in updates and updates["name_hebrew"] and updates["name_hebrew"] != company.name_hebrew:
        _add_alias(company, company.name_hebrew)
    if "aliases" in updates:
        updates["aliases"] = json.dumps(updates["aliases"] or [], ensure_ascii=False)
    for field, value in updates.items():
        setattr(company, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/companies/{company_id}")
async def delete_company(company_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Company).where(Company.id == company_id))
    if not result.scalars().first():
        raise HTTPException(404, "Company not found")
    await db.execute(
        delete(EntityRelationship).where(
            ((EntityRelationship.source_entity_type == "company") & (EntityRelationship.source_entity_id == company_id))
            | ((EntityRelationship.target_entity_type == "company") & (EntityRelationship.target_entity_id == company_id))
        )
    )
    await db.execute(delete(Company).where(Company.id == company_id))
    await db.commit()
    return {"status": "ok"}


# ── Associations CRUD ─────────────────────────────────────────────────────

@router.post("/associations")
async def create_association(body: AssociationCreate, db: AsyncSession = Depends(get_db)):
    assoc = Association(**body.model_dump())
    db.add(assoc)
    await db.commit()
    await db.refresh(assoc)
    return {"status": "ok", "data": {"id": str(assoc.id)}}


@router.put("/associations/{assoc_id}")
async def update_association(
    assoc_id: uuid.UUID, body: AssociationUpdate,
    keep_alias: bool = Query(False, description="Store old name as alias when renaming"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Association).where(Association.id == assoc_id))
    assoc = result.scalars().first()
    if not assoc:
        raise HTTPException(404, "Association not found")
    updates = body.model_dump(exclude_unset=True)
    if keep_alias and "name_hebrew" in updates and updates["name_hebrew"] and updates["name_hebrew"] != assoc.name_hebrew:
        _add_alias(assoc, assoc.name_hebrew)
    if "aliases" in updates:
        updates["aliases"] = json.dumps(updates["aliases"] or [], ensure_ascii=False)
    for field, value in updates.items():
        setattr(assoc, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/associations/{assoc_id}")
async def delete_association(assoc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Association).where(Association.id == assoc_id))
    if not result.scalars().first():
        raise HTTPException(404, "Association not found")
    await db.execute(
        delete(EntityRelationship).where(
            ((EntityRelationship.source_entity_type == "association") & (EntityRelationship.source_entity_id == assoc_id))
            | ((EntityRelationship.target_entity_type == "association") & (EntityRelationship.target_entity_id == assoc_id))
        )
    )
    await db.execute(delete(Association).where(Association.id == assoc_id))
    await db.commit()
    return {"status": "ok"}


# ── Domains CRUD ──────────────────────────────────────────────────────────

@router.post("/domains")
async def create_domain(body: DomainCreate, db: AsyncSession = Depends(get_db)):
    domain = Domain(**body.model_dump())
    db.add(domain)
    await db.commit()
    await db.refresh(domain)
    return {"status": "ok", "data": {"id": str(domain.id)}}


@router.put("/domains/{domain_id}")
async def update_domain(
    domain_id: uuid.UUID, body: DomainUpdate,
    keep_alias: bool = Query(False, description="Store old name as alias when renaming"),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Domain).where(Domain.id == domain_id))
    domain = result.scalars().first()
    if not domain:
        raise HTTPException(404, "Domain not found")
    updates = body.model_dump(exclude_unset=True)
    if keep_alias and "name_hebrew" in updates and updates["name_hebrew"] and updates["name_hebrew"] != domain.name_hebrew:
        _add_alias(domain, domain.name_hebrew)
    if "aliases" in updates:
        updates["aliases"] = json.dumps(updates["aliases"] or [], ensure_ascii=False)
    for field, value in updates.items():
        setattr(domain, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/domains/{domain_id}")
async def delete_domain(domain_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Domain).where(Domain.id == domain_id))
    if not result.scalars().first():
        raise HTTPException(404, "Domain not found")
    await db.execute(delete(Domain).where(Domain.id == domain_id))
    await db.commit()
    return {"status": "ok"}


# ── Relationships CRUD ────────────────────────────────────────────────────

_ENTITY_TABLE = {"person": Person, "company": Company, "association": Association, "domain": Domain}


async def _resolve_entity_name(db: AsyncSession, entity_type: str, entity_id: str) -> str:
    """Resolve entity UUID to its Hebrew name."""
    model = _ENTITY_TABLE.get(entity_type.lower())
    if not model:
        return entity_type
    result = await db.execute(select(model).where(model.id == entity_id))
    entity = result.scalars().first()
    if entity:
        return getattr(entity, "name_hebrew", "") or str(entity_id)[:8]
    return str(entity_id)[:8]


@router.get("/relationships")
async def list_relationships(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    q: str = Query("", description="Search by entity name or relationship type"),
    origin_kind: str = Query("", description="Filter by origin_kind (e.g. 'coi_declaration', 'mk_expense')"),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit

    # If search query, filter by relationship_type or resolve entity names
    filters = []
    if q.strip():
        search = f"%{q.strip()}%"
        filters.append(EntityRelationship.relationship_type.ilike(search))
    if origin_kind.strip():
        filters.append(EntityRelationship.origin_kind == origin_kind.strip())

    count_q = select(func.count()).select_from(EntityRelationship)
    for f in filters:
        count_q = count_q.where(f)
    total = (await db.execute(count_q)).scalar()

    query = select(EntityRelationship).order_by(EntityRelationship.created_at.desc())
    for f in filters:
        query = query.where(f)
    result = await db.execute(query.offset(offset).limit(limit))
    rels = result.scalars().all()

    # Resolve entity names and document info
    data = []
    for r in rels:
        source_name = await _resolve_entity_name(db, r.source_entity_type, str(r.source_entity_id))
        target_name = await _resolve_entity_name(db, r.target_entity_type, str(r.target_entity_id))

        # Get document and source info
        doc_title = ""
        source_title = ""
        source_date = None
        doc_result = await db.execute(
            select(Document).where(Document.id == r.document_id)
        )
        doc = doc_result.scalars().first()
        if doc:
            doc_title = doc.title or ""
            src_result = await db.execute(
                select(Source).where(Source.id == doc.source_id)
            )
            src = src_result.scalars().first()
            if src:
                source_title = src.title or ""
                source_date = (src.metadata_json or {}).get("date") if src.metadata_json else None

        data.append({
            "id": str(r.id),
            "entity1_name": source_name,
            "entity1_type": r.source_entity_type,
            "entity2_name": target_name,
            "entity2_type": r.target_entity_type,
            "relationship_type": r.relationship_type,
            "details": r.details,
            "confidence": r.confidence,
            "origin_kind": r.origin_kind,
            "document_id": str(r.document_id),
            "document_title": doc_title,
            "source_name": source_title,
            "source_date": source_date,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        })
    return {"status": "ok", "data": data, "meta": {"total": total, "page": page, "limit": limit}}


@router.post("/relationships")
async def create_relationship(body: RelationshipCreate, db: AsyncSession = Depends(get_db)):
    rel = EntityRelationship(**body.model_dump())
    db.add(rel)
    await db.commit()
    await db.refresh(rel)
    return {"status": "ok", "data": {"id": str(rel.id)}}


@router.delete("/relationships/{rel_id}")
async def delete_relationship_single(rel_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EntityRelationship).where(EntityRelationship.id == rel_id))
    if not result.scalars().first():
        raise HTTPException(404, "Relationship not found")
    await db.execute(delete(EntityRelationship).where(EntityRelationship.id == rel_id))
    await db.commit()
    return {"status": "ok"}


@router.post("/relationships/bulk-delete")
async def delete_relationships_bulk(body: dict, db: AsyncSession = Depends(get_db)):
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(400, "No ids provided")
    uuids = [uuid.UUID(i) for i in ids]
    await db.execute(delete(EntityRelationship).where(EntityRelationship.id.in_(uuids)))
    await db.commit()
    return {"status": "ok", "deleted": len(uuids)}


@router.post("/relationships/replace-entity")
async def replace_entity_in_relationships(body: dict, db: AsyncSession = Depends(get_db)):
    """Replace one entity with another across all relationships in a document.

    Body: {old_entity_type, old_entity_id, new_entity_type, new_entity_id, document_id}
    Updates all relationships where the old entity appears (as source or target).
    """
    required = ["old_entity_type", "old_entity_id", "new_entity_type", "new_entity_id", "document_id"]
    for field in required:
        if field not in body:
            raise HTTPException(400, f"Missing field: {field}")

    doc_id = uuid.UUID(body["document_id"])
    old_type = body["old_entity_type"]
    old_id = body["old_entity_id"]
    new_type = body["new_entity_type"]
    new_id = body["new_entity_id"]

    # Update source side
    source_result = await db.execute(
        update(EntityRelationship)
        .where(
            and_(
                EntityRelationship.document_id == doc_id,
                EntityRelationship.source_entity_type == old_type,
                EntityRelationship.source_entity_id == old_id,
            )
        )
        .values(source_entity_type=new_type, source_entity_id=new_id)
    )

    # Update target side
    target_result = await db.execute(
        update(EntityRelationship)
        .where(
            and_(
                EntityRelationship.document_id == doc_id,
                EntityRelationship.target_entity_type == old_type,
                EntityRelationship.target_entity_id == old_id,
            )
        )
        .values(target_entity_type=new_type, target_entity_id=new_id)
    )

    await db.commit()
    updated = source_result.rowcount + target_result.rowcount
    return {"status": "ok", "updated": updated}


def formatSize(size: int | None) -> str:
    """Format file size for display."""
    if not size:
        return ""
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size // 1024} KB"
    return f"{size / (1024 * 1024):.1f} MB"


# ── Documents management ──────────────────────────────────────────────────

@router.get("/documents")
async def list_documents(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = None,
    conversion: str | None = None,
    source_type: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    q: str = Query("", alias="search", description="Search by title"),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit

    # Select only lightweight columns — skip pdf_content and markdown_content BLOBs
    from sqlalchemy import case, literal
    light_cols = [
        Document.id, Document.title, Document.source_id,
        Document.conversion_status, Document.extraction_status,
        Document.file_url, Document.file_size,
        Document.created_at, Document.converted_at, Document.extracted_at,
        case((Document.markdown_content.isnot(None), literal(True)), else_=literal(False)).label("has_content"),
        case((Document.pdf_content.isnot(None), literal(True)), else_=literal(False)).label("has_pdf"),
        Source.source_type.label("src_type"),
    ]
    query = select(*light_cols).join(Source, Document.source_id == Source.id)
    count_q = select(func.count()).select_from(Document).join(Source, Document.source_id == Source.id)

    if status:
        query = query.where(Document.extraction_status == status)
        count_q = count_q.where(Document.extraction_status == status)
    if conversion:
        query = query.where(Document.conversion_status == conversion)
        count_q = count_q.where(Document.conversion_status == conversion)
    if source_type:
        query = query.where(Source.source_type == source_type)
        count_q = count_q.where(Source.source_type == source_type)
    if date_from:
        query = query.where(Document.created_at >= date_from)
        count_q = count_q.where(Document.created_at >= date_from)
    if date_to:
        query = query.where(Document.created_at <= date_to)
        count_q = count_q.where(Document.created_at <= date_to)
    if q.strip():
        search_filter = Document.title.ilike(f"%{q.strip()}%")
        query = query.where(search_filter)
        count_q = count_q.where(search_filter)

    total = (await db.execute(count_q)).scalar()
    result = await db.execute(query.order_by(Document.created_at.desc()).offset(offset).limit(limit))
    rows = result.all()
    data = []
    for r in rows:
        data.append({
            "id": str(r.id),
            "title": r.title,
            "source_type": r.src_type,
            "conversion_status": r.conversion_status,
            "extraction_status": r.extraction_status,
            "file_url": r.file_url,
            "file_size": r.file_size,
            "has_content": bool(r.has_content),
            "has_pdf": bool(r.has_pdf),
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "converted_at": r.converted_at.isoformat() if r.converted_at else None,
            "extracted_at": r.extracted_at.isoformat() if r.extracted_at else None,
        })
    return {"status": "ok", "data": data, "meta": {"total": total, "page": page, "limit": limit}}


@router.get("/documents/{doc_id}")
async def get_document_detail(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Full document detail: info, extraction runs, entities and relationships."""
    # Load document with markdown content (pdf_content stays deferred by default)
    from sqlalchemy.orm import undefer
    result = await db.execute(
        select(Document).options(undefer(Document.markdown_content)).where(Document.id == doc_id)
    )
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")

    # Check has_pdf without loading the blob
    pdf_check = await db.execute(
        select(Document.pdf_content.isnot(None)).where(Document.id == doc_id)
    )
    has_pdf = bool(pdf_check.scalar())
    pdf_size = doc.file_size

    # Source info
    source = await db.get(Source, doc.source_id) if doc.source_id else None

    # Extraction runs
    runs_result = await db.execute(
        select(ExtractionRun).where(ExtractionRun.document_id == doc_id).order_by(ExtractionRun.created_at.desc())
    )
    runs = runs_result.scalars().all()
    extraction_runs = []
    for run in runs:
        extraction_runs.append({
            "id": str(run.id),
            "extractor_type": run.extractor_type,
            "model_version": run.model_version,
            "entities_found": run.entities_found,
            "relationships_found": run.relationships_found,
            "raw_output": run.raw_output_json,
            "created_at": run.created_at.isoformat() if run.created_at else None,
        })

    # Relationships for this document
    rels_result = await db.execute(
        select(EntityRelationship).where(EntityRelationship.document_id == doc_id)
    )
    rels = rels_result.scalars().all()
    relationships = []
    for r in rels:
        source_name = await _resolve_entity_name(db, r.source_entity_type, str(r.source_entity_id))
        target_name = await _resolve_entity_name(db, r.target_entity_type, str(r.target_entity_id))
        relationships.append({
            "id": str(r.id),
            "entity1_name": source_name,
            "entity1_type": r.source_entity_type,
            "entity2_name": target_name,
            "entity2_type": r.target_entity_type,
            "relationship_type": r.relationship_type,
            "details": r.details,
            "confidence": r.confidence,
        })

    # Collect unique entities from relationships
    entity_ids_seen = set()
    entities = []
    for r in rels:
        for etype, eid in [(r.source_entity_type, str(r.source_entity_id)), (r.target_entity_type, str(r.target_entity_id))]:
            key = f"{etype}:{eid}"
            if key not in entity_ids_seen:
                entity_ids_seen.add(key)
                name = await _resolve_entity_name(db, etype, eid)
                entities.append({"id": eid, "type": etype, "name": name})

    # Build processing log
    md_len = len(doc.markdown_content) if doc.markdown_content else 0
    total_entities = len(entities)
    total_rels = len(relationships)

    processing_log = [
        {
            "step": "import",
            "label": "ייבוא",
            "status": source.source_type if source else "unknown",
            "timestamp": doc.created_at.isoformat() if doc.created_at else None,
            "details": f"{source.source_type or '—'}" + (f" — {source.title[:40]}" if source and source.title else ""),
        },
        {
            "step": "storage",
            "label": "אחסון PDF",
            "status": "stored" if has_pdf else "missing",
            "timestamp": doc.created_at.isoformat() if doc.created_at and has_pdf else None,
            "details": formatSize(pdf_size) if pdf_size else "חסר",
        },
        {
            "step": "conversion",
            "label": "המרה ל-MD",
            "status": doc.conversion_status,
            "timestamp": doc.converted_at.isoformat() if doc.converted_at else None,
            "details": f"{md_len:,} תווים" if md_len else ("ללא טקסט" if doc.conversion_status == "no_text" else "ממתין"),
        },
        {
            "step": "extraction",
            "label": "חילוץ ישויות",
            "status": doc.extraction_status,
            "timestamp": doc.extracted_at.isoformat() if doc.extracted_at else None,
            "details": f"{total_entities} ישויות, {total_rels} קשרים" if doc.extraction_status == "extracted" else ("ממתין" if doc.extraction_status == "pending" else "נכשל"),
        },
    ]

    # Resolve reviewer name for the verified panel
    verified_by_name = None
    if getattr(doc, "verified_by", None):
        reviewer = await db.get(User, doc.verified_by)
        if reviewer is not None:
            verified_by_name = reviewer.name or reviewer.email

    return {
        "status": "ok",
        "data": {
            "id": str(doc.id),
            "title": doc.title,
            "source_type": source.source_type if source else None,
            "source_title": source.title if source else None,
            "file_format": doc.file_format,
            "file_url": doc.file_url,
            "file_size": doc.file_size,
            "file_path": doc.file_path,
            "has_pdf": has_pdf,
            "pdf_size": pdf_size,
            "conversion_status": doc.conversion_status,
            "extraction_status": doc.extraction_status,
            "markdown_content": doc.markdown_content or "",
            "markdown_length": md_len,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "converted_at": doc.converted_at.isoformat() if doc.converted_at else None,
            "extracted_at": doc.extracted_at.isoformat() if doc.extracted_at else None,
            "processing_log": processing_log,
            "extraction_runs": extraction_runs,
            "relationships": relationships,
            "entities": entities,
            "verified": bool(getattr(doc, "verified", False)),
            "verified_at": doc.verified_at.isoformat() if getattr(doc, "verified_at", None) else None,
            "verified_by": str(doc.verified_by) if getattr(doc, "verified_by", None) else None,
            "verified_by_name": verified_by_name,
        },
    }


@router.get("/documents/{doc_id}/pdf")
async def serve_document_pdf(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Serve the PDF file for a document (from disk or DB)."""
    from pathlib import Path
    from fastapi.responses import FileResponse as FR, Response
    from sqlalchemy.orm import undefer

    # First try to serve from disk without loading pdf_content
    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")

    filename = f"{doc.title or doc.id}.pdf"

    # Try file_path first, then look in pdf_dir
    pdf_path = None
    if doc.file_path and Path(doc.file_path).is_file():
        pdf_path = Path(doc.file_path)
    else:
        candidate = settings.pdf_dir / f"{doc.id}.pdf"
        if candidate.is_file():
            pdf_path = candidate

    if pdf_path:
        return FR(pdf_path, media_type="application/pdf", filename=filename)

    # Only load pdf_content from DB if disk file not found
    result2 = await db.execute(
        select(Document).options(undefer(Document.pdf_content)).where(Document.id == doc_id)
    )
    doc = result2.scalars().first()
    if doc.pdf_content:
        return Response(
            content=doc.pdf_content,
            media_type="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{filename}"'},
        )

    raise HTTPException(404, "PDF file not found")


async def _resolve_pdf_path(doc: Document, httpx_mod, db: AsyncSession | None = None) -> "Path | None":
    """Get local PDF path for a document. Checks disk, DB, then downloads from URL.
    Validates %PDF header on ALL sources to catch cached HTML error pages."""
    import logging
    _log = logging.getLogger("ocoi.api")
    from pathlib import Path as _Path

    def _is_valid_pdf(path: "Path") -> bool:
        """Check if file starts with %PDF header."""
        try:
            with open(path, "rb") as f:
                header = f.read(5)
            if header.startswith(b"%PDF"):
                return True
            _log.warning(
                f"Invalid cached file for '{doc.title[:50]}': "
                f"starts={header!r} size={path.stat().st_size} path={path}"
            )
            path.unlink(missing_ok=True)  # Delete invalid cached file
            return False
        except Exception:
            return False

    # Try local file first (validate it's actually a PDF)
    pdf_path = settings.pdf_dir / f"{doc.id}.pdf"
    if pdf_path.exists():
        if _is_valid_pdf(pdf_path):
            return pdf_path
        # Invalid file was deleted, continue to other sources

    # Try file_path field
    if doc.file_path:
        fp = _Path(doc.file_path)
        if fp.exists() and _is_valid_pdf(fp):
            return fp

    # Try PDF content from database
    if doc.pdf_content:
        if doc.pdf_content[:5].startswith(b"%PDF"):
            settings.pdf_dir.mkdir(parents=True, exist_ok=True)
            pdf_path.write_bytes(doc.pdf_content)
            return pdf_path
        else:
            _log.warning(f"DB pdf_content is not a PDF for '{doc.title[:50]}': starts={doc.pdf_content[:20]!r}")
            doc.pdf_content = None  # Clear invalid DB content

    # Download from URL
    url = doc.file_url
    if not url or url.startswith("upload://"):
        return None

    try:
        async with httpx_mod.AsyncClient(timeout=60, follow_redirects=True) as http:
            resp = await http.get(url)
            resp.raise_for_status()
        pdf_bytes = resp.content

        # Validate it's actually a PDF
        if not pdf_bytes[:5].startswith(b"%PDF"):
            _log.warning(
                f"Downloaded non-PDF for '{doc.title[:50]}': "
                f"starts={pdf_bytes[:40]!r} size={len(pdf_bytes)} url={url[:100]}"
            )
            return None

        settings.pdf_dir.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(pdf_bytes)

        # External-source PDF: keep file_size metadata only; the blob is re-fetchable from
        # file_url, and storing it in Postgres exhausts the Render 1 GB limit.
        if db:
            doc.file_size = len(pdf_bytes)

        return pdf_path
    except Exception as exc:
        _log.warning(f"Download failed for '{doc.title[:50]}': {exc}")
        return None


_reconvert_state: dict = {
    "running": False,
    "total": 0,
    "processed": 0,
    "updated": 0,
    "skipped": 0,
    "errors": [],
}


@router.get("/documents/reconvert-all/status")
async def reconvert_all_status():
    """Poll reconvert-all progress."""
    return {"status": "ok", "data": dict(_reconvert_state)}


@router.post("/documents/reconvert-all")
async def reconvert_all_documents(background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """Re-extract markdown from all PDFs with OCR fallback. Runs as background task in batches."""
    global _reconvert_state
    if _reconvert_state["running"]:
        raise HTTPException(409, "Reconvert already running")

    result = await db.execute(select(func.count()).select_from(Document))
    total = result.scalar() or 0

    _reconvert_state.update({
        "running": True,
        "total": total,
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "errors": [],
    })

    background_tasks.add_task(_reconvert_all_bg)
    return {"status": "ok", "message": f"Reconvert started for {total} documents"}


async def _reconvert_all_bg():
    """Background worker: reconvert all documents one at a time with per-doc sessions."""
    import asyncio
    import gc
    import tempfile
    import httpx as _httpx
    from ocoi_api.services.pdf_converter import convert_pdf

    global _reconvert_state

    try:
        # Phase 1: Get IDs only
        async with bg_session_factory() as db:
            id_result = await db.execute(select(Document.id))
            doc_ids = [r[0] for r in id_result.all()]
        _reconvert_state["total"] = len(doc_ids)

        # Phase 2: Process each doc in its own session
        for i, doc_id in enumerate(doc_ids):
            try:
                async with bg_session_factory() as db:
                    # Don't load pdf_content eagerly — fetch it separately to control memory
                    doc_result = await db.execute(
                        select(Document).where(Document.id == doc_id)
                    )
                    doc = doc_result.scalars().first()
                    if not doc:
                        _reconvert_state["processed"] += 1
                        continue

                    # Try to get a PDF file: disk → DB blob → URL download
                    pdf_path = settings.pdf_dir / f"{doc.id}.pdf"
                    tmp_path = None

                    if pdf_path.exists():
                        # Validate it's a real PDF
                        with open(pdf_path, "rb") as f:
                            if not f.read(5).startswith(b"%PDF"):
                                pdf_path.unlink(missing_ok=True)
                                pdf_path = None
                    else:
                        pdf_path = None

                    if not pdf_path:
                        # Load blob from DB (separate query, only when needed)
                        blob_result = await db.execute(
                            select(Document.pdf_content).where(Document.id == doc_id)
                        )
                        pdf_bytes = blob_result.scalar()

                        if pdf_bytes and pdf_bytes[:5].startswith(b"%PDF"):
                            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                                tmp.write(pdf_bytes)
                                tmp_path = Path(tmp.name)
                            del pdf_bytes  # Free memory before OCR
                            pdf_path = tmp_path
                        elif not pdf_bytes:
                            # Try URL download
                            url = doc.file_url
                            if url and not url.startswith("upload://"):
                                try:
                                    async with _httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
                                        resp = await http.get(url)
                                        resp.raise_for_status()
                                    if resp.content[:5].startswith(b"%PDF"):
                                        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                                            tmp.write(resp.content)
                                            tmp_path = Path(tmp.name)
                                        pdf_path = tmp_path
                                        # External-source: metadata only, blob is re-fetchable
                                        # (storing it filled Render's 1 GB Postgres).
                                        doc.file_size = len(resp.content)
                                except Exception as exc:
                                    logger.warning(f"Download failed for reconvert '{doc.title[:50]}': {exc}")

                    if not pdf_path:
                        _reconvert_state["skipped"] += 1
                        _reconvert_state["processed"] += 1
                        continue

                    # Run conversion in thread to keep event loop responsive
                    md_text = await asyncio.to_thread(convert_pdf, pdf_path, str(doc.id), use_ocr=True)

                    # Clean up temp file
                    if tmp_path:
                        try:
                            tmp_path.unlink(missing_ok=True)
                        except Exception:
                            pass

                    if md_text:
                        doc.markdown_content = md_text
                        doc.conversion_status = "converted"
                        doc.converted_at = now_israel_naive()
                        _reconvert_state["updated"] += 1
                    else:
                        doc.conversion_status = "no_text"
                        _reconvert_state["skipped"] += 1

                    await db.commit()

            except Exception as e:
                if len(_reconvert_state["errors"]) < 20:
                    _reconvert_state["errors"].append(f"doc {doc_id}: {e}")
                _reconvert_state["skipped"] += 1
                # Clean up temp file on error
                if tmp_path:
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass

            _reconvert_state["processed"] += 1
            gc.collect()

    except Exception as e:
        _reconvert_state["errors"].append(f"Fatal: {e}")
    finally:
        _reconvert_state["running"] = False


@router.post("/documents/backfill-pdf")
async def backfill_pdf():
    """Disabled: storing PDF blobs in Postgres exhausted Render's 1 GB DB limit and
    suspended the database. PDFs are re-fetched on demand from file_url instead."""
    raise HTTPException(
        410,
        "PDF backfill is disabled — pdf_content is no longer stored for external sources. "
        "PDFs are fetched on demand from file_url.",
    )


@router.delete("/documents/purge/metadata-only")
async def purge_metadata_only_documents(db: AsyncSession = Depends(get_db)):
    """Delete all documents that have no actual content (no markdown, just URL metadata)."""
    # Find docs without content
    result = await db.execute(
        select(Document).where(
            (Document.markdown_content.is_(None)) | (Document.markdown_content == "")
        )
    )
    docs = result.scalars().all()
    count = len(docs)
    for d in docs:
        await db.execute(delete(ExtractionRun).where(ExtractionRun.document_id == d.id))
        await db.execute(delete(EntityRelationship).where(EntityRelationship.document_id == d.id))
        await db.execute(delete(Document).where(Document.id == d.id))
    # Also clean orphaned sources
    await db.commit()
    return {"status": "ok", "data": {"deleted": count}}


@router.delete("/documents/purge/non-pdf")
async def purge_non_pdf_documents(db: AsyncSession = Depends(get_db)):
    """Delete all non-PDF documents (DOCX, DOC, JPEG, PNG, etc.) that were imported
    before the PDF-only filter was added. Only PDFs should be in the corpus.

    A document is considered non-PDF if:
    - file_format is not 'pdf' (case-insensitive), AND
    - the URL doesn't end in .pdf (fallback check for missing/wrong format field)
    """
    # Select candidates. SQLite-safe: do filtering in Python.
    result = await db.execute(select(Document))
    all_docs = result.scalars().all()

    to_delete = []
    format_breakdown: dict[str, int] = {}
    for d in all_docs:
        fmt = (d.file_format or "").lower()
        url = (d.file_url or "").lower().split("?")[0]
        is_pdf = fmt == "pdf" or url.endswith(".pdf")
        if not is_pdf:
            to_delete.append(d)
            format_breakdown[fmt or "(empty)"] = format_breakdown.get(fmt or "(empty)", 0) + 1

    count = len(to_delete)
    for d in to_delete:
        await db.execute(delete(ExtractionRun).where(ExtractionRun.document_id == d.id))
        await db.execute(delete(EntityRelationship).where(EntityRelationship.document_id == d.id))
        await db.execute(delete(Document).where(Document.id == d.id))

    await db.commit()
    return {
        "status": "ok",
        "data": {
            "deleted": count,
            "format_breakdown": format_breakdown,
        },
    }


@router.post("/documents/upload")
async def upload_document(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a PDF file, convert to markdown, and create a document record."""
    import hashlib
    import traceback as tb
    from ocoi_db.crud import get_or_create_source, create_document

    try:
        # Validate file type
        filename = file.filename or "document.pdf"
        if not filename.lower().endswith(".pdf"):
            raise HTTPException(400, "רק קבצי PDF נתמכים")

        # Read and validate size (20MB limit)
        content = await file.read()
        logger.info(f"Upload received: {filename}, {len(content)} bytes")
        if len(content) > 20 * 1024 * 1024:
            raise HTTPException(400, "הקובץ גדול מדי (מקסימום 20MB)")
        if len(content) == 0:
            raise HTTPException(400, "הקובץ ריק")

        # Check for duplicate using unified detection
        from ocoi_api.services.import_service import check_duplicate
        content_hash = hashlib.sha256(content).hexdigest()
        title_to_check = filename.rsplit(".", 1)[0]
        dup = await check_duplicate(db, content_hash=content_hash, title=title_to_check)
        if dup:
            if dup.content_hash == content_hash:
                raise HTTPException(409, "מסמך זהה כבר קיים במערכת (תוכן זהה)")
            raise HTTPException(409, f"מסמך בשם '{title_to_check}' כבר קיים במערכת")

        # Convert PDF bytes to markdown (no disk needed)
        from ocoi_api.services.pdf_converter import convert_pdf_bytes
        try:
            md_text = convert_pdf_bytes(content, title_to_check)
        except Exception as e:
            logger.warning(f"PDF conversion error for {filename}: {e}")
            md_text = None
        is_scanned = not md_text
        logger.info(f"Upload conversion: {filename} -> {'scanned' if is_scanned else f'{len(md_text)} chars'}")

        # Create source and document
        doc_url = f"upload://{uuid.uuid4()}"
        src = await get_or_create_source(
            db,
            source_type="upload",
            source_id=filename,
            title=filename,
            url=doc_url,
        )
        db_doc = await create_document(
            db,
            source_id=src.id,
            title=title_to_check,
            file_url=doc_url,
            file_format="pdf",
            file_size=len(content),
        )
        logger.info(f"Upload DB record created: {db_doc.id}")

        if md_text:
            db_doc.markdown_content = md_text
            db_doc.conversion_status = "converted"
            db_doc.converted_at = now_israel_naive()
        else:
            db_doc.conversion_status = "no_text"
        db_doc.pdf_content = content
        db_doc.content_hash = content_hash
        db_doc.file_size = len(content)

        await db.commit()
        logger.info(f"Upload committed: {db_doc.id}")

        return {
            "status": "ok",
            "data": {
                "id": str(db_doc.id),
                "title": db_doc.title,
                "file_size": len(content),
                "markdown_length": len(md_text) if md_text else 0,
                "scanned": is_scanned,
            },
        }
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is (400, 409, etc.)
    except Exception as e:
        error_details = tb.format_exc()
        logger.error(f"Upload failed for '{file.filename}': {error_details}")
        raise HTTPException(500, detail=f"שגיאה בהעלאת מסמך: {type(e).__name__}: {e}")


@router.delete("/documents/{doc_id}")
async def delete_document(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Document).where(Document.id == doc_id))
    if not result.scalars().first():
        raise HTTPException(404, "Document not found")
    await db.execute(delete(ExtractionRun).where(ExtractionRun.document_id == doc_id))
    await db.execute(delete(EntityRelationship).where(EntityRelationship.document_id == doc_id))
    await db.execute(delete(Document).where(Document.id == doc_id))
    await db.commit()
    return {"status": "ok"}


# ── Batch operations (MUST be before {doc_id} routes to avoid FastAPI path capture) ──

@router.post("/documents/batch/reconvert")
async def batch_reconvert(body: dict, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """Batch reconvert documents — by IDs or by filter."""
    document_ids = body.get("document_ids", [])
    filter_type = body.get("filter")

    if filter_type == "no_text":
        result = await db.execute(
            select(Document.id).where(Document.conversion_status == "no_text")
        )
        document_ids = [str(row[0]) for row in result.all()]
    elif not document_ids:
        raise HTTPException(400, "Provide document_ids or filter")

    if not document_ids:
        return {"status": "ok", "message": "אין מסמכים להמרה מחדש", "count": 0}

    background_tasks.add_task(_batch_reconvert_bg, document_ids)
    return {"status": "ok", "message": f"המרה מחדש הופעלה ל-{len(document_ids)} מסמכים", "count": len(document_ids)}


async def _batch_reconvert_bg(document_ids: list[str]):
    """Background worker for batch reconvert."""
    import gc
    import httpx as _httpx

    from ocoi_api.services.pdf_converter import convert_pdf
    from sqlalchemy.orm import undefer

    for doc_id in document_ids:
        try:
            async with bg_session_factory() as db:
                result = await db.execute(
                    select(Document).options(undefer(Document.pdf_content)).where(Document.id == doc_id)
                )
                doc = result.scalars().first()
                if not doc:
                    continue

                pdf_path = await _resolve_pdf_path(doc, _httpx, db)
                if not pdf_path:
                    continue

                md_text = convert_pdf(pdf_path, str(doc.id), use_ocr=True)
                if md_text:
                    doc.markdown_content = md_text
                    doc.conversion_status = "converted"
                    doc.converted_at = now_israel_naive()
                    # Only persist the blob for user uploads (no external file_url to
                    # re-fetch from). External-source rows leave pdf_content NULL to
                    # keep Postgres storage flat.
                    if (
                        not doc.pdf_content
                        and pdf_path.exists()
                        and (doc.file_url or "").startswith("upload://")
                    ):
                        doc.pdf_content = pdf_path.read_bytes()
                else:
                    doc.conversion_status = "no_text"

                await db.commit()
        except Exception as e:
            logger.warning(f"Batch reconvert failed for {doc_id}: {e}")
        gc.collect()


@router.post("/documents/batch/extract")
async def batch_extract(body: dict, db: AsyncSession = Depends(get_db)):
    """Batch extract entities — by IDs or by filter."""
    import asyncio
    from ocoi_api.services.extraction_service import get_extraction_status, run_extraction

    document_ids = body.get("document_ids", [])
    filter_type = body.get("filter")

    if filter_type == "pending":
        result = await db.execute(
            select(Document.id).where(
                Document.extraction_status == "pending",
                Document.conversion_status == "converted",
            )
        )
        document_ids = [str(row[0]) for row in result.all()]
    elif not document_ids:
        raise HTTPException(400, "Provide document_ids or filter")

    if not document_ids:
        return {"status": "ok", "message": "אין מסמכים לחילוץ", "count": 0}

    status = get_extraction_status()
    if status["running"]:
        raise HTTPException(409, "חילוץ כבר רץ — נסה שוב אחרי שיסתיים")

    asyncio.create_task(run_extraction(document_ids))
    return {"status": "ok", "message": f"חילוץ הופעל ל-{len(document_ids)} מסמכים", "count": len(document_ids)}


@router.post("/documents/batch/reset-status")
async def batch_reset_status(body: dict, db: AsyncSession = Depends(get_db)):
    """Reset conversion_status or extraction_status for selected documents."""
    document_ids = body.get("document_ids", [])
    field = body.get("field", "extraction_status")
    value = body.get("value", "pending")

    if not document_ids:
        raise HTTPException(400, "Provide document_ids")
    if field not in ("conversion_status", "extraction_status"):
        raise HTTPException(400, "field must be conversion_status or extraction_status")

    for doc_id in document_ids:
        result = await db.execute(select(Document).where(Document.id == doc_id))
        doc = result.scalars().first()
        if doc:
            setattr(doc, field, value)

    await db.commit()
    return {"status": "ok", "message": f"אופס {len(document_ids)} מסמכים", "count": len(document_ids)}


# ── Single-document operations (AFTER batch to avoid {doc_id} capturing "batch") ──

@router.post("/documents/{doc_id}/reconvert")
async def reconvert_document(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Re-extract markdown from a single document's PDF (download if needed) using RTL-safe pymupdf."""
    import httpx as _httpx
    from ocoi_api.services.pdf_converter import convert_pdf
    from sqlalchemy.orm import undefer

    result = await db.execute(
        select(Document).options(undefer(Document.pdf_content)).where(Document.id == doc_id)
    )
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")

    pdf_path = await _resolve_pdf_path(doc, _httpx, db)
    if not pdf_path:
        raise HTTPException(404, "לא ניתן למצוא או להוריד את ה-PDF")

    md_text = convert_pdf(pdf_path, str(doc.id), use_ocr=True)
    if not md_text:
        doc.conversion_status = "no_text"
        await db.commit()
        raise HTTPException(500, "המרה נכשלה — לא הופק טקסט מה-PDF")


    doc.markdown_content = md_text
    doc.conversion_status = "converted"
    doc.converted_at = now_israel_naive()
    # Only persist the blob for user uploads — external sources are re-fetchable.
    if (
        not doc.pdf_content
        and pdf_path.exists()
        and (doc.file_url or "").startswith("upload://")
    ):
        doc.pdf_content = pdf_path.read_bytes()
    await db.commit()

    return {
        "status": "ok",
        "data": {
            "id": str(doc.id),
            "markdown_length": len(md_text),
        },
    }


@router.post("/documents/{doc_id}/reextract")
async def reextract_document(
    doc_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Delete existing extraction data for a document and re-run LLM extraction."""
    import asyncio
    from ocoi_api.services.extraction_service import get_extraction_status, run_extraction

    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")

    # Delete old extraction data for this document
    await db.execute(delete(ExtractionRun).where(ExtractionRun.document_id == doc_id))
    await db.execute(delete(EntityRelationship).where(EntityRelationship.document_id == doc_id))
    doc.extraction_status = "pending"
    await db.commit()

    # Trigger extraction for just this document
    status = get_extraction_status()
    if status["running"]:
        raise HTTPException(409, "חילוץ כבר רץ — נסה שוב אחרי שיסתיים")

    asyncio.create_task(run_extraction([str(doc_id)]))
    return {"status": "ok", "message": "חילוץ מחדש הופעל"}


# ── Human verification ────────────────────────────────────────────────────


@router.patch("/documents/{doc_id}/verify")
async def verify_document(
    doc_id: uuid.UUID,
    body: dict,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Flip the human-verified flag on a document and cascade the change
    to every relationship extracted from it.

    Body: ``{"verified": true/false}`` (defaults to ``true`` if omitted).
    Permission: ``manage_documents`` (admins included).
    """
    verified = bool(body.get("verified", True))

    doc = await db.get(Document, doc_id)
    if doc is None:
        raise HTTPException(404, "Document not found")

    doc.verified = verified
    if verified:
        doc.verified_at = now_israel_naive()
        # `current` is the User ORM row returned by get_current_admin.
        doc.verified_by = str(getattr(current, "id", None)) if getattr(current, "id", None) else None
    else:
        doc.verified_at = None
        doc.verified_by = None

    # Cascade so the public graph can render verified edges distinctly.
    await db.execute(
        update(EntityRelationship)
        .where(EntityRelationship.document_id == str(doc_id))
        .values(verified=verified)
    )
    await db.commit()

    # Look up reviewer name for the response so the UI doesn't need
    # another round-trip.
    reviewer_name = None
    if doc.verified_by:
        reviewer = await db.get(User, doc.verified_by)
        reviewer_name = getattr(reviewer, "name", None) or getattr(reviewer, "email", None)

    return {
        "status": "ok",
        "data": {
            "id": str(doc.id),
            "verified": doc.verified,
            "verified_at": doc.verified_at.isoformat() if doc.verified_at else None,
            "verified_by": str(doc.verified_by) if doc.verified_by else None,
            "verified_by_name": reviewer_name,
        },
    }


# ── CKAN: search + selective import ───────────────────────────────────────

@router.get("/import/ckan/search")
async def ckan_search(
    q: str = Query(..., min_length=1),
    rows: int = Query(20, ge=1, le=100),
    start: int = Query(0, ge=0),
):
    from ocoi_api.services.import_service import search_ckan
    data = await search_ckan(query=q, rows=rows, start=start)
    return {"status": "ok", "data": data}


@router.post("/import/ckan/import")
async def ckan_import(body: dict):
    from ocoi_api.services.import_service import import_ckan_datasets, import_ckan_resources

    # Resource-level import (new)
    resources = body.get("resources", [])
    if resources:
        stats = await import_ckan_resources(resources)
        return {"status": "ok", "data": stats}

    # Dataset-level import (legacy)
    dataset_ids = body.get("dataset_ids", [])
    if not dataset_ids:
        raise HTTPException(400, "No dataset_ids or resources provided")
    stats = await import_ckan_datasets(dataset_ids)
    return {"status": "ok", "data": stats}


@router.post("/import/ckan/bulk")
async def ckan_bulk_import(body: dict, background_tasks: BackgroundTasks):
    """Import ALL CKAN resources matching a query. Runs as background task."""
    from ocoi_api.services.import_service import run_bulk_ckan_import, get_import_status
    from ocoi_api.services.extraction_service import get_extraction_status

    query = body.get("query", "")
    if not query:
        raise HTTPException(400, "query is required")

    status = get_import_status()
    if status["running"]:
        raise HTTPException(409, "ייבוא כבר רץ — נסה שוב אחרי שיסתיים")

    # Prevent running alongside extraction — both together push past 512MB on Render
    ext_status = get_extraction_status()
    if ext_status.get("running"):
        raise HTTPException(409, "חילוץ ישויות רץ כרגע — חכה שיסתיים לפני התחלת ייבוא חדש")

    background_tasks.add_task(run_bulk_ckan_import, query)
    return {"status": "ok", "message": f"ייבוא מתחיל עבור חיפוש: {query}"}


# ── Ignored resources ─────────────────────────────────────────────────────

@router.post("/import/ignore")
async def ignore_resources(body: dict, db: AsyncSession = Depends(get_db)):
    """Mark resource URLs as ignored so they don't appear in search results."""
    resources = body.get("resources", [])
    if not resources:
        raise HTTPException(400, "No resources provided")
    added = 0
    for res in resources:
        url = res.get("url", "")
        if not url:
            continue
        existing = await db.execute(select(IgnoredResource).where(IgnoredResource.file_url == url))
        if existing.scalars().first():
            continue
        db.add(IgnoredResource(
            file_url=url,
            title=res.get("title", ""),
            source_type=res.get("source_type", "ckan"),
        ))
        added += 1
    await db.commit()
    return {"status": "ok", "added": added}


@router.post("/import/unignore")
async def unignore_resources(body: dict, db: AsyncSession = Depends(get_db)):
    """Remove URLs from the ignore list."""
    urls = body.get("urls", [])
    if not urls:
        raise HTTPException(400, "No urls provided")
    await db.execute(delete(IgnoredResource).where(IgnoredResource.file_url.in_(urls)))
    await db.commit()
    return {"status": "ok"}


# ── odata.org.il: snapshot bulk import ───────────────────────────────────


@router.post("/import/odata/trigger")
async def odata_trigger(background_tasks: BackgroundTasks):
    """Kick off the odata.org.il snapshot import (3 ZIPs ≈ 346 PDFs).

    The job runs in the background; clients poll `/import/status` for
    progress. PDFs are stored inline in Document.pdf_content because the
    snapshot ZIP is the only upstream source.
    """
    from ocoi_api.services.import_service import run_odata_import, try_claim_import

    # Atomic claim — closes the race between the HTTP response and the
    # background task actually starting. If two clients click submit in
    # quick succession, only the first wins; the second gets 409.
    if not try_claim_import("odata"):
        raise HTTPException(409, "Import already running")

    background_tasks.add_task(run_odata_import)
    return {"status": "ok", "message": "odata.org.il import started"}


# ── MK constituent-outreach expenses (Excel upload) ──────────────────────


@router.post("/import/mk-expenses/upload")
async def mk_expenses_upload(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """Accept a Knesset MK-expenses .xlsx and start a background import.

    The file's bytes are streamed into Document.pdf_content (the column is
    used as generic binary), and aggregated EntityRelationship rows are
    created with `origin_kind='mk_expense'`. Progress is exposed through
    the existing `/import/status` endpoint.
    """
    from ocoi_api.services.import_service import (
        run_mk_expenses_import,
        try_claim_import,
    )

    # Validate the upload BEFORE claiming the lock so a bad request
    # doesn't leave us in a stuck running=True state.
    if not (file.filename or "").lower().endswith(".xlsx"):
        raise HTTPException(400, "Expected an .xlsx file")
    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Empty upload")

    # Atomic claim — see odata_trigger above.
    if not try_claim_import("mk_expenses"):
        raise HTTPException(409, "Import already running")

    background_tasks.add_task(run_mk_expenses_import, contents, file.filename or "mk_expenses.xlsx")
    return {"status": "ok", "message": "MK expenses import started"}


@router.get("/import/status")
async def import_status():
    from ocoi_api.services.import_service import get_import_status
    return {"status": "ok", "data": get_import_status()}


@router.post("/import/reset")
async def import_reset():
    from ocoi_api.services.import_service import reset_import_state
    reset_import_state()
    return {"status": "ok", "message": "Import state reset"}


# ── Entity extraction (DeepSeek LLM) ─────────────────────────────────────

@router.post("/extraction/reset")
async def reset_extraction(db: AsyncSession = Depends(get_db)):
    """Delete ALL entities, relationships, and extraction runs. Reset document statuses to pending."""
    # Count before deletion
    rel_count = (await db.execute(select(func.count()).select_from(EntityRelationship))).scalar()
    run_count = (await db.execute(select(func.count()).select_from(ExtractionRun))).scalar()
    person_count = (await db.execute(select(func.count()).select_from(Person))).scalar()
    company_count = (await db.execute(select(func.count()).select_from(Company))).scalar()
    assoc_count = (await db.execute(select(func.count()).select_from(Association))).scalar()
    domain_count = (await db.execute(select(func.count()).select_from(Domain))).scalar()

    # Delete in order (relationships first due to FK constraints)
    await db.execute(delete(EntityRelationship))
    await db.execute(delete(ExtractionRun))
    await db.execute(delete(Person))
    await db.execute(delete(Company))
    await db.execute(delete(Association))
    await db.execute(delete(Domain))

    # Reset all document extraction statuses to pending
    from sqlalchemy import update
    await db.execute(
        update(Document).where(Document.extraction_status != "pending").values(extraction_status="pending")
    )

    # Also reset saved prompt to defaults
    from ocoi_api.services.extraction_service import PROMPT_FILE
    if PROMPT_FILE.exists():
        PROMPT_FILE.unlink()

    await db.commit()
    return {
        "status": "ok",
        "deleted": {
            "relationships": rel_count,
            "extraction_runs": run_count,
            "persons": person_count,
            "companies": company_count,
            "associations": assoc_count,
            "domains": domain_count,
        },
    }


@router.get("/extraction/prompt")
async def get_prompt():
    from ocoi_api.services.extraction_service import get_extraction_prompt
    return {"status": "ok", "data": get_extraction_prompt()}


@router.put("/extraction/prompt")
async def update_prompt(body: dict):
    from ocoi_api.services.extraction_service import set_extraction_prompt
    system_prompt = body.get("system_prompt", "")
    user_prompt = body.get("user_prompt", "")
    if not system_prompt or not user_prompt:
        raise HTTPException(400, "Both system_prompt and user_prompt required")
    set_extraction_prompt(system_prompt, user_prompt)
    return {"status": "ok"}


@router.post("/extraction/trigger")
async def trigger_extraction(body: dict = {}):
    import asyncio
    from ocoi_api.services.extraction_service import get_extraction_status, run_extraction
    from ocoi_api.services.import_service import get_import_status
    status = get_extraction_status()
    if status["running"]:
        raise HTTPException(409, "Extraction already running")
    # Prevent running alongside bulk import — both together push past 512MB on Render
    imp_status = get_import_status()
    if imp_status.get("running"):
        raise HTTPException(409, "ייבוא רץ כרגע — חכה שיסתיים לפני התחלת חילוץ")
    document_ids = body.get("document_ids")
    asyncio.create_task(run_extraction(document_ids))
    return {"status": "ok", "message": "Extraction started"}


@router.get("/extraction/status")
async def extraction_status():
    from ocoi_api.services.extraction_service import get_extraction_status
    return {"status": "ok", "data": get_extraction_status()}


# ── External entity registry ──────────────────────────────────────────────

@router.get("/registry/sources")
async def registry_sources(db: AsyncSession = Depends(get_db)):
    """List all registry sources with their sync status."""
    from ocoi_api.services.registry_service import REGISTRY_SOURCES
    from ocoi_db.models import RegistrySyncStatus

    result = await db.execute(select(RegistrySyncStatus))
    sync_rows = {r.source_type: r for r in result.scalars().all()}

    sources = []
    for key, config in REGISTRY_SOURCES.items():
        sync = sync_rows.get(key)
        sources.append({
            "key": key,
            "label": config["label"],
            "entity_type": config["entity_type"],
            "last_synced_at": sync.last_synced_at.isoformat() if sync and sync.last_synced_at else None,
            "record_count": sync.record_count if sync else 0,
            "sync_status": sync.sync_status if sync else "never",
            "error_message": sync.error_message if sync else None,
        })
    return {"status": "ok", "data": sources}


@router.get("/registry/sync/status")
async def registry_sync_status():
    """Get current sync progress (for polling)."""
    from ocoi_api.services.registry_service import get_registry_sync_state
    return {"status": "ok", "data": get_registry_sync_state()}


@router.post("/registry/sync-all")
async def registry_sync_all(background_tasks: BackgroundTasks):
    """Trigger sync for all registry sources sequentially."""
    from ocoi_api.services.registry_service import get_registry_sync_state, run_all_registry_syncs
    state = get_registry_sync_state()
    if state["running"]:
        raise HTTPException(409, "סנכרון כבר רץ — נסה שוב אחרי שיסתיים")
    background_tasks.add_task(run_all_registry_syncs)
    return {"status": "ok", "message": "סנכרון כל המרשמים הופעל"}


@router.post("/registry/sync/{source}")
async def registry_sync(source: str, background_tasks: BackgroundTasks):
    """Trigger sync for a specific registry source."""
    from ocoi_api.services.registry_service import REGISTRY_SOURCES, get_registry_sync_state, run_registry_sync
    if source not in REGISTRY_SOURCES:
        raise HTTPException(400, f"Unknown source: {source}")
    state = get_registry_sync_state()
    if state["running"]:
        raise HTTPException(409, "סנכרון כבר רץ — נסה שוב אחרי שיסתיים")
    background_tasks.add_task(run_registry_sync, source)
    return {"status": "ok", "message": f"סנכרון {REGISTRY_SOURCES[source]['label']} הופעל"}


@router.get("/registry/records")
async def registry_records(
    source: str | None = Query(None),
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Browse registry records with optional source and search filter."""
    from ocoi_db.models import RegistryRecord

    query = select(RegistryRecord)
    if source:
        query = query.where(RegistryRecord.source_type == source)
    if search:
        query = query.where(RegistryRecord.name.ilike(f"%{search}%"))

    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    records = (await db.execute(
        query.order_by(RegistryRecord.name).offset((page - 1) * limit).limit(limit)
    )).scalars().all()

    return {
        "status": "ok",
        "data": [
            {
                "id": r.id,
                "name": r.name,
                "registration_number": r.registration_number,
                "source_type": r.source_type,
                "status": r.status,
            }
            for r in records
        ],
        "meta": {"total": total, "page": page, "limit": limit},
    }


@router.post("/registry/match-all")
async def registry_match_all(background_tasks: BackgroundTasks):
    """Trigger matching all unmatched entities against the registry."""
    from ocoi_api.services.registry_service import get_registry_match_state, match_all_unmatched
    state = get_registry_match_state()
    if state["running"]:
        raise HTTPException(409, "התאמה כבר רצה — נסה שוב אחרי שתסתיים")
    background_tasks.add_task(match_all_unmatched)
    return {"status": "ok", "message": "התאמת ישויות הופעלה"}


@router.get("/registry/match/status")
async def registry_match_status():
    """Get current match-all progress (for polling)."""
    from ocoi_api.services.registry_service import get_registry_match_state
    return {"status": "ok", "data": get_registry_match_state()}


# NOTE: The previous read-only `/users` endpoint that returned
# `sorted(settings.admin_email_set)` was removed when the full user-CRUD
# block was added further below. Two `@router.get("/users")` definitions
# would have FastAPI dispatch to whichever was registered first — the
# old one — and the new UI would receive a list of bare email strings
# instead of user dicts, crashing the page with "u.id is undefined".


# ── Site Content CMS ─────────────────────────────────────────────────────

ALLOWED_CONTENT_KEYS = {"header_links", "footer_text", "about_content"}

@router.get("/site-content/{key}")
async def get_site_content(key: str, db: AsyncSession = Depends(get_db)):
    if key not in ALLOWED_CONTENT_KEYS:
        raise HTTPException(404, f"Unknown content key: {key}")
    row = await db.get(SiteContent, key)
    return {"status": "ok", "data": {"key": key, "value": row.value if row else ""}}

@router.put("/site-content/{key}")
async def update_site_content(key: str, body: dict, db: AsyncSession = Depends(get_db)):
    if key not in ALLOWED_CONTENT_KEYS:
        raise HTTPException(404, f"Unknown content key: {key}")
    value = body.get("value", "")
    row = await db.get(SiteContent, key)
    if row:
        row.value = value
    else:
        db.add(SiteContent(key=key, value=value))
    await db.commit()
    return {"status": "ok"}


# ── User-submitted Suggestions (review queue) ─────────────────────────────

_SUGGESTION_STATUSES = {"pending", "approved", "rejected"}


@router.get("/suggestions")
async def list_suggestions(
    status: str | None = None,
    target_kind: str | None = None,
    page: int = 1,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    """List submitted suggestions with optional filters. Newest first."""
    page = max(1, page)
    limit = max(1, min(200, limit))
    offset = (page - 1) * limit

    q = select(Suggestion)
    cq = select(func.count()).select_from(Suggestion)
    if status and status in _SUGGESTION_STATUSES:
        q = q.where(Suggestion.status == status)
        cq = cq.where(Suggestion.status == status)
    if target_kind:
        q = q.where(Suggestion.target_kind == target_kind)
        cq = cq.where(Suggestion.target_kind == target_kind)

    total = (await db.execute(cq)).scalar() or 0
    rows = (await db.execute(
        q.order_by(Suggestion.created_at.desc()).limit(limit).offset(offset)
    )).scalars().all()

    return {
        "status": "ok",
        "data": [
            {
                "id": r.id,
                "target_kind": r.target_kind,
                "target_id": r.target_id,
                "field_name": r.field_name,
                "current_value": r.current_value,
                "proposed_value": r.proposed_value,
                "comment": r.comment,
                "submitter_email": r.submitter_email,
                "document_id": r.document_id,
                "status": r.status,
                "admin_notes": r.admin_notes,
                "resolved_at": r.resolved_at.isoformat() if r.resolved_at else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
        "meta": {
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit if total else 0,
        },
    }


@router.patch("/suggestions/{suggestion_id}")
async def update_suggestion(
    suggestion_id: str,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Update status / admin_notes for a suggestion."""
    from datetime import datetime
    row = await db.get(Suggestion, suggestion_id)
    if row is None:
        raise HTTPException(404, "Suggestion not found")

    new_status = body.get("status")
    if new_status is not None:
        if new_status not in _SUGGESTION_STATUSES:
            raise HTTPException(400, f"status must be one of {sorted(_SUGGESTION_STATUSES)}")
        row.status = new_status
        if new_status in ("approved", "rejected"):
            row.resolved_at = datetime.utcnow()
        else:
            row.resolved_at = None

    if "admin_notes" in body:
        row.admin_notes = (body.get("admin_notes") or None)

    await db.commit()
    return {"status": "ok"}


@router.delete("/suggestions/{suggestion_id}")
async def delete_suggestion(
    suggestion_id: str,
    db: AsyncSession = Depends(get_db),
):
    row = await db.get(Suggestion, suggestion_id)
    if row is None:
        raise HTTPException(404, "Suggestion not found")
    await db.delete(row)
    await db.commit()
    return {"status": "ok"}


# ── User management (admin-only via SECTION_PERMISSIONS map) ─────────────

def _user_to_dict(u: User) -> dict:
    """Serialise a User row for the admin UI. Permissions arrive as a
    parsed list, never the raw JSON string."""
    try:
        perms = json.loads(u.permissions) if u.permissions else []
    except Exception:
        perms = []
    if not isinstance(perms, list):
        perms = []
    return {
        "id": str(u.id),
        "email": u.email,
        "name": u.name or "",
        "role": u.role,
        "permissions": [p for p in perms if isinstance(p, str)],
        "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None,
        "created_at": u.created_at.isoformat() if u.created_at else None,
    }


@router.get("/users")
async def list_admin_users(db: AsyncSession = Depends(get_db)):
    """List every user that can access the admin panel."""
    rows = (await db.execute(
        select(User).order_by(User.role.desc(), User.email)
    )).scalars().all()
    return {"status": "ok", "data": [_user_to_dict(u) for u in rows]}


@router.post("/users")
async def create_admin_user(body: dict, db: AsyncSession = Depends(get_db)):
    """Add a new content_manager (or admin) row. Email is the unique key —
    the new user signs in with Google and must hit the same address."""
    from ocoi_common.permissions import (
        ALL_PERMISSIONS,
        DEFAULT_CONTENT_MANAGER_PERMISSIONS,
    )

    email = (body.get("email") or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email")
    name = (body.get("name") or email).strip()
    role = body.get("role") or "content_manager"
    if role not in ("admin", "content_manager"):
        raise HTTPException(400, "Invalid role")
    raw_perms = body.get("permissions")
    if raw_perms is None:
        # Sensible default when the caller didn't say.
        raw_perms = list(DEFAULT_CONTENT_MANAGER_PERMISSIONS)
    perms = [p for p in raw_perms if isinstance(p, str) and p in ALL_PERMISSIONS]

    existing = (await db.execute(
        select(User).where(User.email == email)
    )).scalars().first()
    if existing is not None:
        raise HTTPException(409, "User already exists")

    user = User(
        email=email,
        name=name,
        role=role,
        permissions=json.dumps(perms),
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return {"status": "ok", "data": _user_to_dict(user)}


@router.patch("/users/{user_id}")
async def update_admin_user(
    user_id: uuid.UUID,
    body: dict,
    request: Request,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update a user's name, role, or permissions list. Self-demotion is
    blocked so an admin can't accidentally lock the entire org out."""
    from ocoi_common.permissions import ALL_PERMISSIONS

    user = await db.get(User, user_id)
    if user is None:
        raise HTTPException(404, "User not found")

    if "name" in body:
        user.name = (body.get("name") or "").strip() or user.name
    if "role" in body:
        new_role = body.get("role")
        if new_role not in ("admin", "content_manager"):
            raise HTTPException(400, "Invalid role")
        # Don't let the current admin demote themselves.
        if str(user.id) == str(current.id) and new_role != "admin":
            raise HTTPException(400, "אי אפשר לשנות תפקיד של עצמך")
        user.role = new_role
    if "permissions" in body:
        raw = body.get("permissions") or []
        perms = [p for p in raw if isinstance(p, str) and p in ALL_PERMISSIONS]
        user.permissions = json.dumps(perms)

    await db.commit()
    await db.refresh(user)
    return {"status": "ok", "data": _user_to_dict(user)}


@router.delete("/users/{user_id}")
async def delete_admin_user(
    user_id: uuid.UUID,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    user = await db.get(User, user_id)
    if user is None:
        raise HTTPException(404, "User not found")
    if str(user.id) == str(current.id):
        raise HTTPException(400, "אי אפשר למחוק את עצמך")
    await db.delete(user)
    await db.commit()
    return {"status": "ok"}


# ── Permissions catalogue (read-only, admin-only via section map) ───────


@router.get("/permissions/catalog")
async def admin_permissions_catalog():
    """Return the canonical list of permission keys + Hebrew labels so the
    admin UI can render the same checkbox set the backend enforces."""
    from ocoi_common.permissions import (
        DEFAULT_CONTENT_MANAGER_PERMISSIONS,
        PERMISSION_LABELS_HE,
    )
    return {
        "status": "ok",
        "data": {
            "permissions": [
                {"key": k, "label": v} for k, v in PERMISSION_LABELS_HE.items()
            ],
            "default_content_manager": list(DEFAULT_CONTENT_MANAGER_PERMISSIONS),
        },
    }


# ── MCP server administration ───────────────────────────────────────────
# All endpoints are admin-only via SECTION_PERMISSIONS ('/api/v1/admin/mcp').


@router.get("/mcp/users")
async def list_mcp_users(db: AsyncSession = Depends(get_db)):
    """List every user enrolled in the MCP surface (= has a BillingAccount
    row, created on invite). Includes 30-day usage totals.

    We filter by BillingAccount, not by ``role == 'mcp_user'``, because
    admins (ADMIN_EMAILS) and content_managers can also be invited to
    use the MCP surface without losing their primary role. The invite
    endpoint always creates a BillingAccount, so its presence is the
    canonical "this user has MCP access" signal.
    """
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(days=30)

    # Step 1: every user with a BillingAccount row, joined to their billing.
    enrolled = (await db.execute(
        select(User, BillingAccount)
        .join(BillingAccount, BillingAccount.user_id == User.id)
        .order_by(User.created_at.desc())
    )).all()

    if not enrolled:
        return {"status": "ok", "data": []}

    user_ids = [str(u.id) for u, _ in enrolled]

    # Step 2: aggregated usage for those users (one row per user).
    usage_rows = (await db.execute(
        select(
            UsageEvent.user_id,
            func.count(UsageEvent.id).label("calls_30d"),
            func.coalesce(func.sum(UsageEvent.bytes_out), 0).label("bytes_out_30d"),
        )
        .where(UsageEvent.user_id.in_(user_ids))
        .where(UsageEvent.started_at >= cutoff)
        .group_by(UsageEvent.user_id)
    )).all()
    usage_by_user = {r.user_id: r for r in usage_rows}

    return {
        "status": "ok",
        "data": [
            {
                "id": str(u.id),
                "email": u.email,
                "name": u.name,
                "role": u.role,
                "last_login_at": u.last_login_at.isoformat() if u.last_login_at else None,
                "calls_30d": int(usage_by_user[str(u.id)].calls_30d) if str(u.id) in usage_by_user else 0,
                "bytes_out_30d": int(usage_by_user[str(u.id)].bytes_out_30d) if str(u.id) in usage_by_user else 0,
                "plan": b.plan,
                "monthly_quota": b.monthly_quota,
                "stripe_customer_id": b.stripe_customer_id,
            }
            for u, b in enrolled
        ],
    }


@router.get("/mcp/users/{user_id}/events")
async def list_mcp_user_events(
    user_id: uuid.UUID,
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Last N usage events for one MCP user — drill-down for the admin UI."""
    rows = (await db.execute(
        select(UsageEvent)
        .where(UsageEvent.user_id == str(user_id))
        .order_by(UsageEvent.started_at.desc())
        .limit(limit)
    )).scalars().all()
    return {
        "status": "ok",
        "data": [
            {
                "id": str(ev.id),
                "tool": ev.tool,
                "client_id": ev.client_id,
                "started_at": ev.started_at.isoformat() if ev.started_at else None,
                "duration_ms": ev.duration_ms,
                "bytes_in": ev.bytes_in,
                "bytes_out": ev.bytes_out,
                "status": ev.status,
                "error_message": ev.error_message,
                "stripe_pushed_at": ev.stripe_pushed_at.isoformat() if ev.stripe_pushed_at else None,
            }
            for ev in rows
        ],
    }


@router.patch("/mcp/users/{user_id}")
async def update_mcp_user_billing(
    user_id: uuid.UUID,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Change plan ('free' | 'metered') or monthly quota. Flipping to
    'metered' lazily provisions the Stripe customer + subscription item
    so the next batcher tick can push usage."""
    user = await db.get(User, user_id)
    if user is None:
        raise HTTPException(404, "User not found")
    row = await db.get(BillingAccount, str(user_id))
    if row is None:
        row = BillingAccount(user_id=str(user_id), plan="free")
        db.add(row)
        await db.flush()

    if "plan" in body:
        new_plan = body["plan"]
        if new_plan not in ("free", "metered"):
            raise HTTPException(400, "plan must be 'free' or 'metered'")
        row.plan = new_plan
        if new_plan == "metered":
            from ocoi_api.mcp.billing import ensure_stripe_customer
            try:
                await ensure_stripe_customer(str(user_id), user.email)
            except Exception as e:
                logger.exception("Stripe provisioning failed for user %s", user_id)
                raise HTTPException(502, f"Stripe error: {e}")
    if "monthly_quota" in body:
        q = body["monthly_quota"]
        if q is not None and (not isinstance(q, int) or q < 0):
            raise HTTPException(400, "monthly_quota must be a non-negative integer or null")
        row.monthly_quota = q

    await db.commit()
    await db.refresh(row)
    return {
        "status": "ok",
        "data": {
            "user_id": str(user_id),
            "plan": row.plan,
            "monthly_quota": row.monthly_quota,
            "stripe_customer_id": row.stripe_customer_id,
            "stripe_subscription_item_id": row.stripe_subscription_item_id,
        },
    }


@router.get("/mcp/clients")
async def list_mcp_clients(db: AsyncSession = Depends(get_db)):
    """All registered OAuth clients (Claude Desktop instances, Cursor, etc)."""
    rows = (await db.execute(
        select(OAuthClient).order_by(OAuthClient.created_at.desc())
    )).scalars().all()
    return {
        "status": "ok",
        "data": [
            {
                "id": str(c.id),
                "client_id": c.client_id,
                "client_name": c.client_name,
                "is_public": c.is_public,
                "redirect_uris": json.loads(c.redirect_uris) if c.redirect_uris else [],
                "created_at": c.created_at.isoformat() if c.created_at else None,
                "revoked_at": c.revoked_at.isoformat() if c.revoked_at else None,
            }
            for c in rows
        ],
    }


@router.delete("/mcp/clients/{client_pk}")
async def revoke_mcp_client(client_pk: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Revoke a registered client. Existing access tokens (JWTs) keep
    working until they expire (15 min); refresh tokens for the client
    will be rejected immediately at /oauth/token."""
    from datetime import datetime
    row = await db.get(OAuthClient, client_pk)
    if row is None:
        raise HTTPException(404, "Client not found")
    if row.revoked_at is None:
        row.revoked_at = datetime.utcnow()
        await db.commit()
    return {"status": "ok"}


@router.post("/mcp/invites")
async def invite_mcp_user(body: dict, db: AsyncSession = Depends(get_db)):
    """Pre-create a User row with role='mcp_user' so the email can
    complete Google OAuth on the MCP surface. With MCP_INVITE_ONLY=true
    (the default) this is the ONLY way to grant MCP access — emails
    without a row get the "invite required" error page."""
    email = (body.get("email") or "").strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email")
    name = (body.get("name") or email).strip()
    quota_raw = body.get("monthly_quota")
    if quota_raw is not None and (not isinstance(quota_raw, int) or quota_raw < 0):
        raise HTTPException(400, "monthly_quota must be a non-negative integer or null")

    existing = (await db.execute(
        select(User).where(User.email == email)
    )).scalars().first()
    if existing is not None:
        # Reactivate / promote silently. Admin already has admin_users
        # management for admin↔content_manager swaps; mcp_user is its
        # own surface and treating "invite existing email" as idempotent
        # avoids breaking onboarding when the row already exists.
        if existing.role not in ("admin", "content_manager", "mcp_user"):
            existing.role = "mcp_user"
        await db.commit()
        await db.refresh(existing)
        user = existing
    else:
        user = User(email=email, name=name, role="mcp_user", permissions=None)
        db.add(user)
        await db.flush()

    billing = await db.get(BillingAccount, str(user.id))
    if billing is None:
        billing = BillingAccount(
            user_id=str(user.id), plan="free", monthly_quota=quota_raw,
        )
        db.add(billing)
    elif "monthly_quota" in body:
        billing.monthly_quota = quota_raw
    await db.commit()
    return {
        "status": "ok",
        "data": {
            "id": str(user.id),
            "email": user.email,
            "name": user.name,
            "role": user.role,
            "monthly_quota": billing.monthly_quota,
            "plan": billing.plan,
        },
    }


@router.delete("/mcp/users/{user_id}")
async def remove_mcp_user(user_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Revoke a user's MCP access.

    For ``role='mcp_user'`` rows (MCP-only users) we delete the User
    entirely. For admins / content_managers we only drop the
    BillingAccount — their admin login keeps working, they just lose
    the MCP surface. This matches the rule that /admin/users is the
    only place that touches admin/content_manager User rows.
    """
    user = await db.get(User, user_id)
    if user is None:
        raise HTTPException(404, "User not found")
    if user.role == "mcp_user":
        await db.delete(user)  # cascades to BillingAccount + UsageEvent
    else:
        billing = await db.get(BillingAccount, str(user_id))
        if billing is None:
            raise HTTPException(404, "User has no MCP access to revoke")
        await db.delete(billing)
    await db.commit()
    return {"status": "ok"}


# ── Duplicate-entity detection: scan + merge + review queue ────────────


@router.post("/entities/merge")
async def merge_two_entities(
    body: dict,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Manual entity merge — point all relationships from ``merge_id``
    at ``keep_id``, fold the merge entity's name + aliases into the
    keep entity, and delete the merge row. Used both by the proposal
    approval flow and by an admin doing it directly from the entity UI.

    Body: ``{entity_type, keep_id, merge_id}``. Permission: manage_entities.
    """
    from ocoi_api.services.match_service import merge_entities

    entity_type = (body.get("entity_type") or "").strip()
    keep_id = (body.get("keep_id") or "").strip()
    merge_id = (body.get("merge_id") or "").strip()
    if not entity_type or not keep_id or not merge_id:
        raise HTTPException(400, "entity_type, keep_id, merge_id required")

    try:
        summary = await merge_entities(db, entity_type, keep_id, merge_id)
        await db.commit()
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "ok", "data": summary}


@router.post("/matches/scan-duplicates")
async def matches_scan_duplicates(
    background_tasks: BackgroundTasks,
    body: dict | None = None,
):
    """Kick off the background duplicate-scan job. By default scans
    person + company + association; the body may pass
    ``{"kinds": ["person"]}`` to narrow it down."""
    from ocoi_api.services.match_service import (
        get_match_status,
        run_duplicate_scan,
        try_claim_scan,
    )

    if not try_claim_scan():
        raise HTTPException(409, "Scan already running")

    kinds = None
    if body and isinstance(body.get("kinds"), list):
        kinds = [str(k) for k in body["kinds"] if isinstance(k, str)]
    if not kinds:
        kinds = ["person", "company", "association"]

    background_tasks.add_task(run_duplicate_scan, tuple(kinds))
    return {"status": "ok", "message": "סריקת כפילויות החלה ברקע", "scanning": kinds, "state": get_match_status()}


@router.get("/matches/scan-status")
async def matches_scan_status():
    from ocoi_api.services.match_service import get_match_status
    return {"status": "ok", "data": get_match_status()}


@router.post("/matches/scan-reset")
async def matches_scan_reset():
    """Force-reset the scan slot. Use if a previous run got stuck and
    the trigger endpoint keeps returning 409."""
    from ocoi_api.services.match_service import reset_match_state
    reset_match_state()
    return {"status": "ok"}


@router.get("/matches")
async def matches_list(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    proposal_kind: str | None = Query(None, description="'duplicate' / 'registry_match'"),
    status: str | None = Query("pending", description="pending / approved / rejected / dismissed / all"),
    entity_type: str | None = Query(None),
    min_score: float | None = Query(None, ge=0.0, le=1.0),
    db: AsyncSession = Depends(get_db),
):
    """List EntityMatchProposal rows with optional filters, newest /
    highest-score first. Each row is enriched with display names for
    both sides so the UI doesn't need a second round-trip."""
    from ocoi_api.services.match_service import proposal_to_dict

    q = select(EntityMatchProposal)
    cq = select(func.count()).select_from(EntityMatchProposal)
    filters = []
    if proposal_kind:
        filters.append(EntityMatchProposal.proposal_kind == proposal_kind)
    if status and status != "all":
        filters.append(EntityMatchProposal.status == status)
    if entity_type:
        filters.append(EntityMatchProposal.entity_type == entity_type)
    if min_score is not None:
        filters.append(EntityMatchProposal.score >= min_score)
    for f in filters:
        q = q.where(f)
        cq = cq.where(f)

    total = (await db.execute(cq)).scalar() or 0
    rows = (await db.execute(
        q.order_by(
            EntityMatchProposal.status.asc(),  # pending first
            EntityMatchProposal.score.desc(),
            EntityMatchProposal.created_at.desc(),
        )
        .offset((page - 1) * limit)
        .limit(limit)
    )).scalars().all()

    data = [await proposal_to_dict(db, r) for r in rows]
    return {
        "status": "ok",
        "data": data,
        "meta": {
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit if total else 0,
        },
    }


@router.post("/matches/{proposal_id}/approve")
async def matches_approve(
    proposal_id: uuid.UUID,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Approve a proposal. For 'duplicate' proposals this runs the
    actual merge — entity_id is kept, target_id is folded in."""
    from ocoi_api.services.match_service import merge_entities

    proposal = await db.get(EntityMatchProposal, proposal_id)
    if not proposal:
        raise HTTPException(404, "Proposal not found")
    if proposal.status != "pending":
        raise HTTPException(409, f"Proposal already {proposal.status}")

    if proposal.proposal_kind == "duplicate":
        try:
            summary = await merge_entities(
                db,
                proposal.entity_type,
                proposal.entity_id,
                proposal.target_id,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        # Mark every other open proposal that references the merged-away
        # id as dismissed — they're no longer actionable.
        await db.execute(
            update(EntityMatchProposal)
            .where(
                EntityMatchProposal.id != proposal.id,
                EntityMatchProposal.status == "pending",
                or_(
                    and_(
                        EntityMatchProposal.entity_type == proposal.target_type,
                        EntityMatchProposal.entity_id == proposal.target_id,
                    ),
                    and_(
                        EntityMatchProposal.target_type == proposal.target_type,
                        EntityMatchProposal.target_id == proposal.target_id,
                    ),
                ),
            )
            .values(status="dismissed", reviewed_at=datetime.utcnow())
        )
    else:
        summary = {"proposal_kind": proposal.proposal_kind}

    proposal.status = "approved"
    proposal.reviewed_by = str(getattr(current, "id", None)) if getattr(current, "id", None) else None
    proposal.reviewed_at = datetime.utcnow()
    await db.commit()
    return {"status": "ok", "data": summary}


@router.get("/audit/orphans-and-garbage")
async def audit_orphans_and_garbage(
    db: AsyncSession = Depends(get_db),
):
    """One-shot data-quality audit:

    1. **Garbage names** — entity rows whose ``name_hebrew`` is a
       placeholder ("null", "***", "----", empty after trim, etc.).
       The LLM extractor occasionally emits these and the importer
       persists them as real entities, polluting the graph.
    2. **Orphan relationships** — rows in ``entity_relationships`` whose
       ``source_entity_id`` or ``target_entity_id`` doesn't exist in
       the corresponding entity table. Caused by mid-merge deletes or
       relationship-level inserts that ran before the entity insert.
       The graph endpoint renders these as nameless circles labelled
       only with the UUID prefix.

    Returns counts + sample IDs per category so the admin can decide
    whether to mark hidden / delete / repair.
    """
    from sqlalchemy import text as sa_text

    # Garbage names — same predicate everywhere; built as a SQL fragment
    # so we don't have to fetch every row into Python.
    GARBAGE_SQL = (
        "TRIM(name_hebrew) IS NULL "
        "OR TRIM(name_hebrew) = '' "
        "OR LOWER(TRIM(name_hebrew)) IN ('null', 'none', 'n/a') "
        "OR TRIM(name_hebrew) ~ '^[\\*_\\-–—=\\.,\\s]+$'"
    )
    garbage: dict[str, list[dict]] = {}
    for tbl, etype in (("persons", "person"), ("companies", "company"),
                       ("associations", "association"), ("domains", "domain")):
        rows = (await db.execute(sa_text(
            f"SELECT id::text AS id, name_hebrew FROM {tbl} WHERE {GARBAGE_SQL}"
        ))).fetchall()
        garbage[etype] = [
            {"id": r[0], "name": r[1]} for r in rows
        ]

    # Orphan relationships — pairs that reference an ID not present in
    # the corresponding entity table. We do this with LEFT JOINs per
    # entity_type to keep each query simple and indexable.
    orphans: dict[str, dict] = {}
    for etype, tbl in (("person", "persons"), ("company", "companies"),
                       ("association", "associations"), ("domain", "domains")):
        # Source side
        src_rows = (await db.execute(sa_text(f"""
            SELECT DISTINCT er.source_entity_id::text AS id
            FROM entity_relationships er
            LEFT JOIN {tbl} t ON t.id = er.source_entity_id
            WHERE er.source_entity_type = :etype AND t.id IS NULL
        """).bindparams(etype=etype))).fetchall()
        tgt_rows = (await db.execute(sa_text(f"""
            SELECT DISTINCT er.target_entity_id::text AS id
            FROM entity_relationships er
            LEFT JOIN {tbl} t ON t.id = er.target_entity_id
            WHERE er.target_entity_type = :etype AND t.id IS NULL
        """).bindparams(etype=etype))).fetchall()
        orphan_ids = sorted({r[0] for r in src_rows} | {r[0] for r in tgt_rows})
        # Count how many relationships reference each orphan id (useful for
        # deciding whether to delete the rels outright or attempt to back-fill
        # the entity row).
        ref_counts = {}
        if orphan_ids:
            count_rows = (await db.execute(sa_text(f"""
                SELECT id, c FROM (
                    SELECT source_entity_id::text AS id, COUNT(*) AS c
                    FROM entity_relationships
                    WHERE source_entity_type = :etype
                      AND source_entity_id::text = ANY(:ids)
                    GROUP BY source_entity_id
                    UNION ALL
                    SELECT target_entity_id::text AS id, COUNT(*) AS c
                    FROM entity_relationships
                    WHERE target_entity_type = :etype
                      AND target_entity_id::text = ANY(:ids)
                    GROUP BY target_entity_id
                ) x
            """).bindparams(etype=etype, ids=orphan_ids))).fetchall()
            for rid, c in count_rows:
                ref_counts[rid] = ref_counts.get(rid, 0) + int(c)
        orphans[etype] = {
            "count": len(orphan_ids),
            "ids": [{"id": i, "ref_count": ref_counts.get(i, 0)} for i in orphan_ids],
        }

    return {
        "status": "ok",
        "data": {
            "garbage_names": {
                k: {"count": len(v), "items": v} for k, v in garbage.items()
            },
            "orphan_references": orphans,
        },
    }


@router.post("/audit/cleanup")
async def audit_cleanup(
    body: dict | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Delete the junk surfaced by ``/audit/orphans-and-garbage``:

    * **Garbage-name entities** — every Person/Company/Association/Domain
      whose ``name_hebrew`` is a pure placeholder (per
      ``ocoi_common.is_placeholder_name``). Their relationships are
      deleted first (FK-safe), then the entity row.
    * **Orphan relationships** — ``entity_relationships`` rows whose
      source/target id has no matching entity row of that type.

    Body (all optional, default all True):
      ``garbage_entities``: bool — remove placeholder-name entities
      ``orphan_relationships``: bool — remove dangling relationship rows
      ``dry_run``: bool — count only, change nothing

    Returns per-category counts of what was (or would be) removed.
    """
    from sqlalchemy import text as sa_text
    from ocoi_common.blocklist import is_placeholder_name

    body = body or {}
    do_garbage = body.get("garbage_entities", True)
    do_orphans = body.get("orphan_relationships", True)
    dry_run = body.get("dry_run", False)

    report: dict = {"dry_run": dry_run, "garbage_entities": {}, "orphan_relationships": {}}

    _TYPE_TABLE = (
        ("person", "persons"), ("company", "companies"),
        ("association", "associations"), ("domain", "domains"),
    )

    # ── 1. Garbage-name entities ──
    if do_garbage:
        for etype, tbl in _TYPE_TABLE:
            rows = (await db.execute(sa_text(
                f"SELECT id::text AS id, name_hebrew FROM {tbl}"
            ))).fetchall()
            bad_ids = [r[0] for r in rows if is_placeholder_name(r[1])]
            rels_deleted = 0
            if bad_ids and not dry_run:
                # Delete relationships touching these entities first.
                rel_res = await db.execute(sa_text(f"""
                    DELETE FROM entity_relationships
                    WHERE (source_entity_type = :etype AND source_entity_id::text = ANY(:ids))
                       OR (target_entity_type = :etype AND target_entity_id::text = ANY(:ids))
                """).bindparams(etype=etype, ids=bad_ids))
                rels_deleted = rel_res.rowcount or 0
                # Then the entity rows.
                await db.execute(sa_text(
                    f"DELETE FROM {tbl} WHERE id::text = ANY(:ids)"
                ).bindparams(ids=bad_ids))
            report["garbage_entities"][etype] = {
                "entities_removed": len(bad_ids),
                "relationships_removed": rels_deleted,
                "ids": bad_ids,
            }

    # ── 2. Orphan relationships ──
    if do_orphans:
        total_orphans = 0
        per_type: dict[str, int] = {}
        for etype, tbl in _TYPE_TABLE:
            # Count first (also serves as the dry-run answer).
            cnt = (await db.execute(sa_text(f"""
                SELECT COUNT(*) FROM entity_relationships er
                WHERE (er.source_entity_type = :etype
                       AND NOT EXISTS (SELECT 1 FROM {tbl} t WHERE t.id = er.source_entity_id))
                   OR (er.target_entity_type = :etype
                       AND NOT EXISTS (SELECT 1 FROM {tbl} t WHERE t.id = er.target_entity_id))
            """).bindparams(etype=etype))).scalar() or 0
            if cnt and not dry_run:
                await db.execute(sa_text(f"""
                    DELETE FROM entity_relationships er
                    WHERE (er.source_entity_type = :etype
                           AND NOT EXISTS (SELECT 1 FROM {tbl} t WHERE t.id = er.source_entity_id))
                       OR (er.target_entity_type = :etype
                           AND NOT EXISTS (SELECT 1 FROM {tbl} t WHERE t.id = er.target_entity_id))
                """).bindparams(etype=etype))
            per_type[etype] = int(cnt)
            total_orphans += int(cnt)
        report["orphan_relationships"] = {"total": total_orphans, "by_type": per_type}

    if not dry_run:
        await db.commit()

    return {"status": "ok", "data": report}


@router.post("/entities/merge-cross-type")
async def entities_merge_cross_type(
    body: dict,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Merge two entities that live in DIFFERENT tables (e.g. one
    classified as a 'company', the other as an 'association') even
    though they're the same legal entity in the real world.

    Body:
      keep_type / keep_id   — the surviving side (kept as-is)
      merge_type / merge_id — folded in and deleted

    All relationships referencing the merge side are rewritten with
    keep's (type, id) pair. Aliases fold in. The merge row is deleted.
    """
    from ocoi_api.services.match_service import merge_entities_cross_type

    keep_type = (body.get("keep_type") or "").strip()
    keep_id = (body.get("keep_id") or "").strip()
    merge_type = (body.get("merge_type") or "").strip()
    merge_id = (body.get("merge_id") or "").strip()
    if not all([keep_type, keep_id, merge_type, merge_id]):
        raise HTTPException(400, "keep_type/keep_id/merge_type/merge_id all required")

    try:
        summary = await merge_entities_cross_type(
            db,
            keep_type=keep_type, keep_id=keep_id,
            merge_type=merge_type, merge_id=merge_id,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    await db.commit()
    return {"status": "ok", "data": summary}


@router.post("/matches/cleanup-pending")
async def matches_cleanup_pending(
    body: dict,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Bulk-delete pending duplicate proposals matching a filter — useful
    after tightening the matcher rules and realising the previous scan
    wrote a flood of false positives (e.g. all "בנימין"-named persons
    chained together by substring matching).

    Body:
      ``entity_type``   — required: 'person' / 'company' / 'association'
      ``reasons_any``   — optional list[str]; delete only proposals whose
                          reasons text contains ANY of these substrings.
                          Pass ``["prefix_match", "substring_match",
                          "token_subset"]`` to wipe matcher-changed rows
                          without touching the strict-token proposals.

    Returns the number of rows deleted."""
    entity_type = (body.get("entity_type") or "").strip()
    if entity_type not in ("person", "company", "association"):
        raise HTTPException(400, "entity_type must be person/company/association")
    reasons_any = body.get("reasons_any") or []
    if not isinstance(reasons_any, list):
        raise HTTPException(400, "reasons_any must be an array of strings")

    where = [
        EntityMatchProposal.proposal_kind == "duplicate",
        EntityMatchProposal.status == "pending",
        EntityMatchProposal.entity_type == entity_type,
    ]
    if reasons_any:
        # The `reasons` column is JSON-encoded text — checking with LIKE
        # is enough; we don't need full JSON parsing for this admin
        # housekeeping path.
        like_filters = [
            EntityMatchProposal.reasons.ilike(f"%{r}%") for r in reasons_any if r
        ]
        if like_filters:
            where.append(or_(*like_filters))

    res = await db.execute(
        EntityMatchProposal.__table__.delete().where(*where)
    )
    await db.commit()
    return {"status": "ok", "data": {"deleted": int(res.rowcount or 0)}}


@router.get("/matches/clusters")
async def matches_clusters(
    entity_type: str | None = Query(None, description="Filter by 'person' / 'company' / 'association'."),
    min_score: float = Query(0.85, ge=0.0, le=1.0),
    limit: int = Query(30, ge=1, le=2000, description="Cap on number of clusters returned (top-N by size). Default 30 keeps the response under 3s on Render free; merge those, refresh, get the next batch."),
    db: AsyncSession = Depends(get_db),
):
    """Group pending duplicate proposals into connected components so the
    admin can collapse a 30-row cluster in one click instead of 435.

    With 1,500+ pending proposals the response would otherwise need to
    hydrate ~3,000 entity rows in one shot — too slow on Render's free
    tier. ``limit`` (default 100) caps the response to the largest N
    clusters first; sweep through, merge those, refresh, repeat. The
    ``meta`` block tells you how many additional clusters are queued."""
    from ocoi_api.services.match_service import build_duplicate_clusters
    timings: dict = {}
    clusters = await build_duplicate_clusters(
        db,
        entity_type=entity_type,
        min_score=min_score,
        limit=limit,
        timings=timings,
    )
    return {
        "status": "ok",
        "data": clusters,
        "meta": {
            "total": len(clusters),
            "limit": limit,
            "limited": len(clusters) >= limit,
            "timings_ms": timings,
        },
    }


@router.post("/matches/clusters/merge")
async def matches_cluster_merge(
    body: dict,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Merge an entire duplicate cluster into a single canonical entity.

    Body:
      ``entity_type`` — 'person' / 'company' / 'association'
      ``canonical_id`` — entity to keep (others fold into it)
      ``member_ids`` — full list of cluster members; canonical_id is
        ignored if present in the list (we never merge an entity into
        itself).
    """
    from ocoi_api.services.match_service import merge_cluster

    entity_type = (body.get("entity_type") or "").strip()
    canonical_id = (body.get("canonical_id") or "").strip()
    raw_members = body.get("member_ids") or []
    if not entity_type:
        raise HTTPException(400, "entity_type required")
    if not canonical_id:
        raise HTTPException(400, "canonical_id required")
    if not isinstance(raw_members, list) or not raw_members:
        raise HTTPException(400, "member_ids must be a non-empty array")

    try:
        summary = await merge_cluster(
            db,
            entity_type=entity_type,
            canonical_id=canonical_id,
            member_ids=[str(m) for m in raw_members],
            reviewer_id=str(getattr(current, "id", None)) if getattr(current, "id", None) else None,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    await db.commit()
    return {"status": "ok", "data": summary}


@router.post("/matches/clusters/merge-batch")
async def matches_cluster_merge_batch(
    body: dict,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Merge several duplicate clusters in a single round-trip.

    Body:
      ``operations`` — array of ``{entity_type, canonical_id, member_ids}``
        objects (same shape as the single-cluster endpoint above).

    Each cluster is processed sequentially with its own commit so that
    a failure on cluster #7 doesn't roll back clusters #1-#6. The
    response carries per-operation status so the UI can show "5
    succeeded, 1 failed" and re-fetch.
    """
    from ocoi_api.services.match_service import merge_cluster

    raw_ops = body.get("operations") or []
    if not isinstance(raw_ops, list) or not raw_ops:
        raise HTTPException(400, "operations must be a non-empty array")

    reviewer_id = (
        str(getattr(current, "id", None)) if getattr(current, "id", None) else None
    )

    results: list[dict] = []
    total_merged = 0
    total_proposals_approved = 0
    succeeded = 0
    failed = 0

    for idx, op in enumerate(raw_ops):
        if not isinstance(op, dict):
            results.append({"index": idx, "status": "error", "error": "operation must be an object"})
            failed += 1
            continue
        entity_type = (op.get("entity_type") or "").strip()
        canonical_id = (op.get("canonical_id") or "").strip()
        raw_members = op.get("member_ids") or []
        if not entity_type or not canonical_id or not isinstance(raw_members, list) or not raw_members:
            results.append({
                "index": idx,
                "status": "error",
                "error": "entity_type, canonical_id, member_ids are required",
                "canonical_id": canonical_id or None,
            })
            failed += 1
            continue
        try:
            summary = await merge_cluster(
                db,
                entity_type=entity_type,
                canonical_id=canonical_id,
                member_ids=[str(m) for m in raw_members],
                reviewer_id=reviewer_id,
            )
            # Commit per cluster so a later failure doesn't wipe out the
            # earlier successes.
            await db.commit()
            total_merged += int(summary.get("merged_count") or 0)
            total_proposals_approved += int(summary.get("proposals_approved") or 0)
            succeeded += 1
            results.append({
                "index": idx,
                "status": "ok",
                "canonical_id": canonical_id,
                "summary": summary,
            })
        except ValueError as e:
            await db.rollback()
            failed += 1
            results.append({
                "index": idx,
                "status": "error",
                "error": str(e),
                "canonical_id": canonical_id,
            })
        except Exception as e:  # noqa: BLE001 — surface unknown failures
            await db.rollback()
            failed += 1
            results.append({
                "index": idx,
                "status": "error",
                "error": f"{type(e).__name__}: {e}",
                "canonical_id": canonical_id,
            })

    return {
        "status": "ok",
        "data": {
            "total_operations": len(raw_ops),
            "succeeded": succeeded,
            "failed": failed,
            "total_entities_merged": total_merged,
            "total_proposals_approved": total_proposals_approved,
            "results": results,
        },
    }


@router.post("/matches/{proposal_id}/reject")
async def matches_reject(
    proposal_id: uuid.UUID,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Reject a proposal — "definitely not the same". Keeps the row so
    a future scan doesn't re-propose the same pair."""
    proposal = await db.get(EntityMatchProposal, proposal_id)
    if not proposal:
        raise HTTPException(404, "Proposal not found")
    proposal.status = "rejected"
    proposal.reviewed_by = str(getattr(current, "id", None)) if getattr(current, "id", None) else None
    proposal.reviewed_at = datetime.utcnow()
    await db.commit()
    return {"status": "ok"}


@router.get("/mcp/stats")
async def mcp_global_stats(db: AsyncSession = Depends(get_db)):
    """Top-line counters for the admin dashboard."""
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(days=30)
    # Counts everyone enrolled in MCP (= has a BillingAccount row), not
    # just role='mcp_user', so admins who use MCP are included.
    total_users = (await db.execute(
        select(func.count(BillingAccount.user_id))
    )).scalar() or 0
    active_users = (await db.execute(
        select(func.count(func.distinct(UsageEvent.user_id)))
        .where(UsageEvent.started_at >= cutoff)
    )).scalar() or 0
    total_calls = (await db.execute(
        select(func.count(UsageEvent.id)).where(UsageEvent.started_at >= cutoff)
    )).scalar() or 0
    metered_users = (await db.execute(
        select(func.count(BillingAccount.user_id))
        .where(BillingAccount.plan == "metered")
    )).scalar() or 0
    return {
        "status": "ok",
        "data": {
            "total_mcp_users": int(total_users),
            "active_users_30d": int(active_users),
            "calls_30d": int(total_calls),
            "metered_users": int(metered_users),
        },
    }


@router.post("/matches/{proposal_id}/dismiss")
async def matches_dismiss(
    proposal_id: uuid.UUID,
    current = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Dismiss a proposal — "skip for now". Behaviour is identical to
    reject for the scanner (won't be re-proposed), but UI can show
    these separately so the admin tells "ignored" from "not a match"."""
    proposal = await db.get(EntityMatchProposal, proposal_id)
    if not proposal:
        raise HTTPException(404, "Proposal not found")
    proposal.status = "dismissed"
    proposal.reviewed_by = str(getattr(current, "id", None)) if getattr(current, "id", None) else None
    proposal.reviewed_at = datetime.utcnow()
    await db.commit()
    return {"status": "ok"}
