"""Admin CRUD routes — protected with Google OAuth JWT."""

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
from ocoi_db.engine import async_session_factory
from ocoi_db.models import (
    Person, Company, Association, Domain,
    EntityRelationship, Document, Source, ExtractionRun, IgnoredResource,
)

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(get_current_admin)],
)


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
async def update_person(person_id: uuid.UUID, body: PersonUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Person).where(Person.id == person_id))
    person = result.scalar_one_or_none()
    if not person:
        raise HTTPException(404, "Person not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(person, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/persons/{person_id}")
async def delete_person(person_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Person).where(Person.id == person_id))
    if not result.scalar_one_or_none():
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
async def update_company(company_id: uuid.UUID, body: CompanyUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalar_one_or_none()
    if not company:
        raise HTTPException(404, "Company not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(company, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/companies/{company_id}")
async def delete_company(company_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Company).where(Company.id == company_id))
    if not result.scalar_one_or_none():
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
async def update_association(assoc_id: uuid.UUID, body: AssociationUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Association).where(Association.id == assoc_id))
    assoc = result.scalar_one_or_none()
    if not assoc:
        raise HTTPException(404, "Association not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(assoc, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/associations/{assoc_id}")
async def delete_association(assoc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Association).where(Association.id == assoc_id))
    if not result.scalar_one_or_none():
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
async def update_domain(domain_id: uuid.UUID, body: DomainUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Domain).where(Domain.id == domain_id))
    domain = result.scalar_one_or_none()
    if not domain:
        raise HTTPException(404, "Domain not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(domain, field, value)
    await db.commit()
    return {"status": "ok"}


@router.delete("/domains/{domain_id}")
async def delete_domain(domain_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Domain).where(Domain.id == domain_id))
    if not result.scalar_one_or_none():
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
    entity = result.scalar_one_or_none()
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
        doc = doc_result.scalar_one_or_none()
        if doc:
            doc_title = doc.title or ""
            src_result = await db.execute(
                select(Source).where(Source.id == doc.source_id)
            )
            src = src_result.scalar_one_or_none()
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
    if not result.scalar_one_or_none():
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


# ── Documents management ──────────────────────────────────────────────────

@router.get("/documents")
async def list_documents(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = None,
    q: str = Query("", alias="search", description="Search by title"),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    query = select(Document).join(Source, Document.source_id == Source.id)
    count_q = select(func.count()).select_from(Document)
    if status:
        query = query.where(Document.extraction_status == status)
        count_q = count_q.where(Document.extraction_status == status)
    if q.strip():
        search_filter = Document.title.ilike(f"%{q.strip()}%")
        query = query.where(search_filter)
        count_q = count_q.where(search_filter)
    total = (await db.execute(count_q)).scalar()
    result = await db.execute(query.order_by(Document.created_at.desc()).offset(offset).limit(limit))
    docs = result.scalars().all()
    data = []
    for d in docs:
        source = await db.get(Source, d.source_id) if d.source_id else None
        data.append({
            "id": str(d.id),
            "title": d.title,
            "source_type": source.source_type if source else None,
            "conversion_status": d.conversion_status,
            "extraction_status": d.extraction_status,
            "file_url": d.file_url,
            "file_size": d.file_size,
            "has_content": bool(d.markdown_content),
            "has_pdf": bool(d.pdf_content),
            "created_at": d.created_at.isoformat() if d.created_at else None,
        })
    return {"status": "ok", "data": data, "meta": {"total": total, "page": page, "limit": limit}}


@router.get("/documents/{doc_id}")
async def get_document_detail(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Full document detail: info, extraction runs, entities and relationships."""
    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(404, "Document not found")

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
            "conversion_status": doc.conversion_status,
            "extraction_status": doc.extraction_status,
            "markdown_content": doc.markdown_content or "",
            "markdown_length": len(doc.markdown_content) if doc.markdown_content else 0,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
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

    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalar_one_or_none()
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

    # Serve from DB if stored there
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
    """Background worker: reconvert all documents in batches of 10."""
    import gc
    import httpx as _httpx
    from ocoi_api.services.pdf_converter import convert_pdf

    global _reconvert_state
    BATCH_SIZE = 10

    try:
        async with async_session_factory() as db:
            result = await db.execute(select(Document))
            docs = result.scalars().all()
            _reconvert_state["total"] = len(docs)

            for i, doc in enumerate(docs):
                try:
                    pdf_path = await _resolve_pdf_path(doc, _httpx, db)
                    if not pdf_path:
                        _reconvert_state["skipped"] += 1
                        _reconvert_state["processed"] += 1
                        continue

                    md_text = convert_pdf(pdf_path, str(doc.id), use_ocr=True)
                    if md_text:
                        doc.markdown_content = md_text
                        doc.conversion_status = "converted"
                        if not doc.pdf_content and pdf_path.exists():
                            doc.pdf_content = pdf_path.read_bytes()
                        _reconvert_state["updated"] += 1
                    else:
                        doc.conversion_status = "no_text"
                        _reconvert_state["skipped"] += 1

                    gc.collect()

                except Exception as e:
                    if len(_reconvert_state["errors"]) < 20:
                        _reconvert_state["errors"].append(f"{doc.title[:40]}: {e}")
                    _reconvert_state["skipped"] += 1

                _reconvert_state["processed"] += 1

                # Commit in batches to avoid long transactions
                if (i + 1) % BATCH_SIZE == 0:
                    await db.commit()

            await db.commit()
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
    import hashlib
    import httpx as _httpx
    from ocoi_api.services.pdf_converter import convert_pdf_bytes

    async with async_session_factory() as db:
        result = await db.execute(
            select(Document).where(
                Document.pdf_content.is_(None),
                Document.file_url.isnot(None),
                ~Document.file_url.startswith("upload://"),
            )
        )
        docs = result.scalars().all()
        import logging
        _log = logging.getLogger("ocoi.api.backfill")
        _log.info(f"Backfilling PDFs for {len(docs)} documents")

        for i, doc in enumerate(docs):
            try:
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
                    _log.info(f"Backfilled + converted '{doc.title[:40]}': {len(md_text)} chars")
                else:
                    doc.conversion_status = "no_text"
                    _log.info(f"Backfilled PDF for '{doc.title[:40]}' (no embedded text)")

            except Exception as e:
                _log.warning(f"Backfill failed for '{doc.title[:40]}': {e}")

            if (i + 1) % 5 == 0:
                await db.commit()

        await db.commit()
        _log.info(f"Backfill complete: processed {len(docs)} documents")


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

        # Check for duplicate by content hash
        content_hash = hashlib.sha256(content).hexdigest()
        existing_hash = await db.execute(
            select(Document).where(Document.content_hash == content_hash)
        )
        if existing_hash.scalar_one_or_none():
            raise HTTPException(409, "מסמך זהה כבר קיים במערכת (תוכן זהה)")

        # Check for duplicate by title
        title_to_check = filename.rsplit(".", 1)[0]
        existing_title = await db.execute(
            select(Document).where(Document.title == title_to_check)
        )
        if existing_title.scalar_one_or_none():
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
    if not result.scalar_one_or_none():
        raise HTTPException(404, "Document not found")
    await db.execute(delete(ExtractionRun).where(ExtractionRun.document_id == doc_id))
    await db.execute(delete(EntityRelationship).where(EntityRelationship.document_id == doc_id))
    await db.execute(delete(Document).where(Document.id == doc_id))
    await db.commit()
    return {"status": "ok"}


@router.post("/documents/{doc_id}/reconvert")
async def reconvert_document(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Re-extract markdown from a single document's PDF (download if needed) using RTL-safe pymupdf."""
    import httpx as _httpx
    from ocoi_api.services.pdf_converter import convert_pdf

    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalar_one_or_none()
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
    doc = result.scalar_one_or_none()
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
        if existing.scalar_one_or_none():
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


# ── Admin users (read-only from env) ──────────────────────────────────────

@router.get("/users")
async def list_admin_users():
    return {
        "status": "ok",
        "data": sorted(settings.admin_email_set),
    }
