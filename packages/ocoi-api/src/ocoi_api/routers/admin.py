"""Admin CRUD routes — protected with Google OAuth JWT."""

import json
import logging
import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, Query, HTTPException, Request, UploadFile, File

logger = logging.getLogger("ocoi.api.admin")
from sqlalchemy import select, func, delete
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
from ocoi_db.engine import async_session_factory, bg_session_factory
from ocoi_db.crud import _add_alias, _get_aliases
from ocoi_db.models import (
    Person, Company, Association, Domain,
    EntityRelationship, Document, Source, ExtractionRun, IgnoredResource,
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
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit

    # If search query, filter by relationship_type or resolve entity names
    base_filter = None
    if q.strip():
        search = f"%{q.strip()}%"
        base_filter = EntityRelationship.relationship_type.ilike(search)

    count_q = select(func.count()).select_from(EntityRelationship)
    if base_filter is not None:
        count_q = count_q.where(base_filter)
    total = (await db.execute(count_q)).scalar()

    query = select(EntityRelationship).order_by(EntityRelationship.created_at.desc())
    if base_filter is not None:
        query = query.where(base_filter)
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

        # Store in DB for persistence
        if db:
            doc.pdf_content = pdf_bytes
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
    import gc
    import httpx as _httpx
    from datetime import datetime, timezone as tz
    from ocoi_api.services.pdf_converter import convert_pdf
    from sqlalchemy.orm import undefer

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
                    doc_result = await db.execute(
                        select(Document).options(undefer(Document.pdf_content)).where(Document.id == doc_id)
                    )
                    doc = doc_result.scalars().first()
                    if not doc:
                        _reconvert_state["processed"] += 1
                        continue

                    pdf_path = await _resolve_pdf_path(doc, _httpx, db)
                    if not pdf_path:
                        _reconvert_state["skipped"] += 1
                        _reconvert_state["processed"] += 1
                        continue

                    md_text = convert_pdf(pdf_path, str(doc.id), use_ocr=True)
                    if md_text:
                        doc.markdown_content = md_text
                        doc.conversion_status = "converted"
                        doc.converted_at = datetime.now(tz.utc)
                        if not doc.pdf_content and pdf_path.exists():
                            doc.pdf_content = pdf_path.read_bytes()
                        _reconvert_state["updated"] += 1
                    else:
                        doc.conversion_status = "no_text"
                        _reconvert_state["skipped"] += 1

                    await db.commit()

            except Exception as e:
                if len(_reconvert_state["errors"]) < 20:
                    _reconvert_state["errors"].append(f"doc {doc_id}: {e}")
                _reconvert_state["skipped"] += 1

            _reconvert_state["processed"] += 1
            gc.collect()

    except Exception as e:
        _reconvert_state["errors"].append(f"Fatal: {e}")
    finally:
        _reconvert_state["running"] = False


@router.post("/documents/backfill-pdf")
async def backfill_pdf(background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """Download and store PDFs for documents that are missing pdf_content."""
    result = await db.execute(
        select(func.count()).select_from(Document).where(
            Document.pdf_content.is_(None),
            Document.file_url.isnot(None),
            ~Document.file_url.startswith("upload://"),
        )
    )
    count = result.scalar() or 0
    if count == 0:
        return {"status": "ok", "message": "No documents missing PDF content"}

    background_tasks.add_task(_backfill_pdf_bg)
    return {"status": "ok", "message": f"Backfilling PDFs for {count} documents"}


async def _backfill_pdf_bg():
    """Download PDFs for documents missing pdf_content, then reconvert."""
    import gc
    import hashlib
    import httpx as _httpx
    from datetime import datetime, timezone as tz
    from ocoi_api.services.pdf_converter import convert_pdf_bytes
    import logging
    _log = logging.getLogger("ocoi.api.backfill")

    # Phase 1: Get IDs only (no BLOBs loaded)
    async with bg_session_factory() as db:
        result = await db.execute(
            select(Document.id).where(
                Document.pdf_content.is_(None),
                Document.file_url.isnot(None),
                ~Document.file_url.startswith("upload://"),
            )
        )
        doc_ids = [r[0] for r in result.all()]
    _log.info(f"Backfilling PDFs for {len(doc_ids)} documents")

    # Phase 2: Process one at a time in fresh sessions
    for i, doc_id in enumerate(doc_ids):
        try:
            async with bg_session_factory() as db:
                result = await db.execute(select(Document).where(Document.id == doc_id))
                doc = result.scalars().first()
                if not doc:
                    continue

                async with _httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
                    resp = await http.get(doc.file_url)
                    resp.raise_for_status()
                pdf_bytes = resp.content

                if not pdf_bytes or not pdf_bytes[:5].startswith(b"%PDF"):
                    _log.warning(f"Not a PDF for '{doc.title[:40]}': {pdf_bytes[:20]!r}")
                    continue

                doc.pdf_content = pdf_bytes
                doc.file_size = len(pdf_bytes)
                doc.content_hash = hashlib.sha256(pdf_bytes).hexdigest()

                # Try conversion
                md_text = convert_pdf_bytes(pdf_bytes, str(doc.id))
                if md_text:
                    doc.markdown_content = md_text
                    doc.conversion_status = "converted"
                    doc.converted_at = datetime.now(tz.utc)
                    _log.info(f"Backfilled + converted '{doc.title[:40]}': {len(md_text)} chars")
                else:
                    doc.conversion_status = "no_text"
                    _log.info(f"Backfilled PDF for '{doc.title[:40]}' (no embedded text)")

                await db.commit()
        except Exception as e:
            _log.warning(f"Backfill failed for doc {doc_id}: {e}")
        gc.collect()

    _log.info(f"Backfill complete: processed {len(doc_ids)} documents")


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

        from datetime import datetime, timezone
        if md_text:
            db_doc.markdown_content = md_text
            db_doc.conversion_status = "converted"
            db_doc.converted_at = datetime.now(timezone.utc)
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
    from datetime import datetime, timezone as tz
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
                    doc.converted_at = datetime.now(tz.utc)
                    if not doc.pdf_content and pdf_path.exists():
                        doc.pdf_content = pdf_path.read_bytes()
                else:
                    doc.conversion_status = "no_text"

                await db.commit()
        except Exception as e:
            logger.warning(f"Batch reconvert failed for {doc_id}: {e}")
        gc.collect()


@router.post("/documents/batch/extract")
async def batch_extract(body: dict, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """Batch extract entities — by IDs or by filter."""
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

    background_tasks.add_task(run_extraction, document_ids)
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

    from datetime import datetime, timezone as tz
    doc.markdown_content = md_text
    doc.conversion_status = "converted"
    doc.converted_at = datetime.now(tz.utc)
    # Store PDF in DB if not already there
    if not doc.pdf_content and pdf_path.exists():
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
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Delete existing extraction data for a document and re-run LLM extraction."""
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

    background_tasks.add_task(run_extraction, [str(doc_id)])
    return {"status": "ok", "message": "חילוץ מחדש הופעל"}


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

    query = body.get("query", "")
    if not query:
        raise HTTPException(400, "query is required")

    status = get_import_status()
    if status["running"]:
        raise HTTPException(409, "ייבוא כבר רץ — נסה שוב אחרי שיסתיים")

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


# ── Gov.il: automated bulk import ────────────────────────────────────────

@router.post("/import/govil/proxy")
async def govil_proxy(request: Request):
    """Proxy a single Gov.il API page request via cloudscraper (Cloudflare bypass)."""
    import asyncio
    import json as json_mod
    import cloudscraper
    body = await request.json()
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True},
        delay=10,
    )
    scraper.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.gov.il",
        "Referer": "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict",
    })
    last_error = None
    for attempt in range(3):
        try:
            if attempt == 0:
                # Warm session by visiting the homepage first
                await asyncio.to_thread(scraper.get, "https://www.gov.il/he")
            elif attempt > 0:
                await asyncio.sleep(2 * attempt)
            resp = await asyncio.to_thread(
                scraper.post,
                "https://www.gov.il/he/api/DynamicCollector",
                json=body,
            )
            if resp.status_code == 200:
                return resp.json()
            last_error = f"Status {resp.status_code}"
        except Exception as e:
            last_error = e
    raise HTTPException(502, f"Gov.il API unavailable after 3 attempts: {last_error}")


@router.get("/import/govil/cached")
async def govil_cached():
    """Return pre-fetched Gov.il records from data/govil_records.json if available."""
    from ocoi_api.services.import_service import _load_cached_govil_records
    records = _load_cached_govil_records()
    if records is None:
        raise HTTPException(404, "No cached Gov.il records found")
    return {"status": "ok", "data": {"records": records, "count": len(records)}}


@router.post("/import/govil/trigger")
async def govil_trigger(
    request: Request,
    background_tasks: BackgroundTasks,
    limit: int = Query(0, ge=0),
):
    from ocoi_api.services.import_service import get_import_status, run_govil_import

    status = get_import_status()
    if status["running"]:
        raise HTTPException(409, "Import already running")

    # Accept optional URL from request body
    url = ""
    try:
        body = await request.json()
        url = body.get("url", "")
    except Exception:
        pass

    background_tasks.add_task(run_govil_import, limit=limit, url=url)
    return {"status": "ok", "message": "Gov.il import started"}


@router.post("/import/govil/submit")
async def govil_submit(
    request: Request,
    background_tasks: BackgroundTasks,
):
    """Accept pre-fetched Gov.il API items from the browser and process them."""
    from ocoi_api.services.import_service import get_import_status, run_govil_with_records

    status = get_import_status()
    if status["running"]:
        raise HTTPException(409, "Import already running")

    body = await request.json()
    records = body.get("records", [])
    if not records:
        raise HTTPException(400, "No records provided")

    background_tasks.add_task(run_govil_with_records, raw_items=records)
    return {"status": "ok", "message": f"Processing {len(records)} records from browser"}


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
async def trigger_extraction(background_tasks: BackgroundTasks, body: dict = {}):
    from ocoi_api.services.extraction_service import get_extraction_status, run_extraction
    status = get_extraction_status()
    if status["running"]:
        raise HTTPException(409, "Extraction already running")
    document_ids = body.get("document_ids")
    background_tasks.add_task(run_extraction, document_ids)
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


# ── Admin users (read-only from env) ──────────────────────────────────────

@router.get("/users")
async def list_admin_users():
    return {
        "status": "ok",
        "data": sorted(settings.admin_email_set),
    }
