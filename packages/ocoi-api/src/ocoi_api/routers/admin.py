"""Admin CRUD routes — protected with Google OAuth JWT."""

import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, Query, HTTPException, Request, UploadFile, File
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
from ocoi_db.models import (
    Person, Company, Association, Domain,
    EntityRelationship, Document, Source, ExtractionRun,
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

@router.get("/relationships")
async def list_relationships(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    total_q = await db.execute(select(func.count()).select_from(EntityRelationship))
    total = total_q.scalar()
    result = await db.execute(select(EntityRelationship).offset(offset).limit(limit))
    rels = result.scalars().all()
    data = [
        {
            "id": str(r.id),
            "source_entity_type": r.source_entity_type,
            "source_entity_id": str(r.source_entity_id),
            "target_entity_type": r.target_entity_type,
            "target_entity_id": str(r.target_entity_id),
            "relationship_type": r.relationship_type,
            "details": r.details,
            "confidence": r.confidence,
        }
        for r in rels
    ]
    return {"status": "ok", "data": data, "meta": {"total": total, "page": page, "limit": limit}}


@router.post("/relationships")
async def create_relationship(body: RelationshipCreate, db: AsyncSession = Depends(get_db)):
    rel = EntityRelationship(**body.model_dump())
    db.add(rel)
    await db.commit()
    await db.refresh(rel)
    return {"status": "ok", "data": {"id": str(rel.id)}}


@router.delete("/relationships/{rel_id}")
async def delete_relationship(rel_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EntityRelationship).where(EntityRelationship.id == rel_id))
    if not result.scalar_one_or_none():
        raise HTTPException(404, "Relationship not found")
    await db.execute(delete(EntityRelationship).where(EntityRelationship.id == rel_id))
    await db.commit()
    return {"status": "ok"}


# ── Documents management ──────────────────────────────────────────────────

@router.get("/documents")
async def list_documents(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    q = select(Document).join(Source, Document.source_id == Source.id)
    count_q = select(func.count()).select_from(Document)
    if status:
        q = q.where(Document.extraction_status == status)
        count_q = count_q.where(Document.extraction_status == status)
    total = (await db.execute(count_q)).scalar()
    result = await db.execute(q.order_by(Document.created_at.desc()).offset(offset).limit(limit))
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
            "created_at": d.created_at.isoformat() if d.created_at else None,
        })
    return {"status": "ok", "data": data, "meta": {"total": total, "page": page, "limit": limit}}


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
    from ocoi_api.services.import_service import convert_pdf_to_markdown
    from ocoi_db.crud import get_or_create_source, create_document

    # Validate file type
    filename = file.filename or "document.pdf"
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(400, "רק קבצי PDF נתמכים")

    # Read and validate size (20MB limit)
    content = await file.read()
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(400, "הקובץ גדול מדי (מקסימום 20MB)")
    if len(content) == 0:
        raise HTTPException(400, "הקובץ ריק")

    # Save PDF to temp file
    temp_id = str(uuid.uuid4())
    pdf_path = settings.pdf_dir / f"{temp_id}.pdf"
    pdf_path.write_bytes(content)

    # Convert to markdown
    md_text = convert_pdf_to_markdown(pdf_path, temp_id)
    if not md_text:
        pdf_path.unlink(missing_ok=True)
        raise HTTPException(422, "לא ניתן לחלץ טקסט מה-PDF")

    # Create source and document
    doc_url = f"upload://{temp_id}"
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
        title=filename.rsplit(".", 1)[0],
        file_url=doc_url,
        file_format="pdf",
        file_size=len(content),
    )

    # Rename temp files to actual DB id
    actual_id = str(db_doc.id)
    actual_pdf = settings.pdf_dir / f"{actual_id}.pdf"
    actual_md = settings.markdown_dir / f"{actual_id}.md"
    temp_md = settings.markdown_dir / f"{temp_id}.md"
    if pdf_path.exists() and str(pdf_path) != str(actual_pdf):
        pdf_path.rename(actual_pdf)
    if temp_md.exists():
        temp_md.rename(actual_md)

    db_doc.markdown_content = md_text
    db_doc.conversion_status = "converted"
    db_doc.file_path = str(actual_pdf)
    await db.commit()

    return {
        "status": "ok",
        "data": {
            "id": str(db_doc.id),
            "title": db_doc.title,
            "file_size": len(content),
            "markdown_length": len(md_text),
        },
    }


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


# ── Gov.il: automated bulk import ────────────────────────────────────────

@router.post("/import/govil/proxy")
async def govil_proxy(request: Request):
    """Proxy a single Gov.il API page request — retries with delays on failure."""
    import asyncio
    import httpx as hx
    body = await request.json()
    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.gov.il",
        "Referer": "https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }
    last_error = None
    async with hx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
        for attempt in range(3):
            try:
                if attempt > 0:
                    await asyncio.sleep(2 * attempt)
                resp = await client.post("https://www.gov.il/he/api/DynamicCollector", json=body)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_error = e
    raise HTTPException(502, f"Gov.il API unavailable after 3 attempts: {last_error}")


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
