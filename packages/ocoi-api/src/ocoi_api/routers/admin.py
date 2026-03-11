"""Admin CRUD routes — protected with Google OAuth JWT."""

import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, Query, HTTPException
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
    q = select(Document)
    count_q = select(func.count()).select_from(Document)
    if status:
        q = q.where(Document.extraction_status == status)
        count_q = count_q.where(Document.extraction_status == status)
    total = (await db.execute(count_q)).scalar()
    result = await db.execute(q.order_by(Document.created_at.desc()).offset(offset).limit(limit))
    docs = result.scalars().all()
    data = [
        {
            "id": str(d.id),
            "title": d.title,
            "conversion_status": d.conversion_status,
            "extraction_status": d.extraction_status,
            "file_url": d.file_url,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }
        for d in docs
    ]
    return {"status": "ok", "data": data, "meta": {"total": total, "page": page, "limit": limit}}


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
    from ocoi_api.services.import_service import import_ckan_datasets
    dataset_ids = body.get("dataset_ids", [])
    if not dataset_ids:
        raise HTTPException(400, "No dataset_ids provided")
    stats = await import_ckan_datasets(dataset_ids)
    return {"status": "ok", "data": stats}


# ── Gov.il: automated bulk import ────────────────────────────────────────

@router.post("/import/govil/trigger")
async def govil_trigger(
    background_tasks: BackgroundTasks,
    limit: int = Query(0, ge=0),
):
    from ocoi_api.services.import_service import get_import_status, run_govil_import

    status = get_import_status()
    if status["running"]:
        raise HTTPException(409, "Import already running")

    background_tasks.add_task(run_govil_import, limit=limit)
    return {"status": "ok", "message": "Gov.il import started"}


@router.get("/import/status")
async def import_status():
    from ocoi_api.services.import_service import get_import_status
    return {"status": "ok", "data": get_import_status()}


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
