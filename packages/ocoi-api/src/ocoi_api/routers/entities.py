"""Entity CRUD endpoints (persons, companies, associations, domains)."""

import uuid
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.models import Person, Company, Association, Domain, EntityRelationship, Document

router = APIRouter(tags=["entities"])


def _paginate(page: int, limit: int):
    return (page - 1) * limit


def _entity_to_dict(entity, extra_fields: list[str] | None = None) -> dict:
    d = {"id": str(entity.id), "name_hebrew": entity.name_hebrew}
    for field in (extra_fields or []):
        d[field] = getattr(entity, field, None)
    return d


# --- Persons ---

@router.get("/persons")
async def list_persons(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    q: str = Query("", description="Search by name"),
    db: AsyncSession = Depends(get_db),
):
    offset = _paginate(page, limit)
    base = select(Person)
    count_base = select(func.count()).select_from(Person)
    if q.strip():
        filt = Person.name_hebrew.ilike(f"%{q.strip()}%")
        base = base.where(filt)
        count_base = count_base.where(filt)
    total = (await db.execute(count_base)).scalar()
    result = await db.execute(base.offset(offset).limit(limit).order_by(Person.name_hebrew))
    persons = result.scalars().all()
    return {
        "status": "ok",
        "data": [_entity_to_dict(p, ["title", "position", "ministry"]) for p in persons],
        "meta": {"total": total, "page": page, "limit": limit, "pages": (total + limit - 1) // limit},
    }


@router.get("/persons/{person_id}")
async def get_person(person_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Person).where(Person.id == person_id))
    person = result.scalars().first()
    if not person:
        raise HTTPException(404, "Person not found")
    return {
        "status": "ok",
        "data": _entity_to_dict(person, ["name_english", "title", "position", "ministry", "aliases"]),
    }


@router.get("/persons/{person_id}/documents")
async def get_person_documents(person_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Document.id, Document.title, Document.file_url)
        .join(EntityRelationship, EntityRelationship.document_id == Document.id)
        .where(
            or_(
                (EntityRelationship.source_entity_type == "person") & (EntityRelationship.source_entity_id == person_id),
                (EntityRelationship.target_entity_type == "person") & (EntityRelationship.target_entity_id == person_id),
            )
        )
        .distinct()
    )
    docs = [{"id": str(r.id), "title": r.title, "file_url": r.file_url} for r in result.fetchall()]
    return {"status": "ok", "data": docs}


# --- Companies ---

@router.get("/companies")
async def list_companies(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    q: str = Query("", description="Search by name"),
    db: AsyncSession = Depends(get_db),
):
    offset = _paginate(page, limit)
    base = select(Company)
    count_base = select(func.count()).select_from(Company)
    if q.strip():
        filt = Company.name_hebrew.ilike(f"%{q.strip()}%")
        base = base.where(filt)
        count_base = count_base.where(filt)
    total = (await db.execute(count_base)).scalar()
    result = await db.execute(base.offset(offset).limit(limit).order_by(Company.name_hebrew))
    companies = result.scalars().all()
    return {
        "status": "ok",
        "data": [_entity_to_dict(c, ["registration_number", "company_type", "status"]) for c in companies],
        "meta": {"total": total, "page": page, "limit": limit, "pages": (total + limit - 1) // limit},
    }


@router.get("/companies/{company_id}")
async def get_company(company_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalars().first()
    if not company:
        raise HTTPException(404, "Company not found")
    return {
        "status": "ok",
        "data": _entity_to_dict(company, [
            "name_english", "registration_number", "company_type", "status", "match_confidence",
        ]),
    }


@router.get("/companies/{company_id}/documents")
async def get_company_documents(company_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Document.id, Document.title, Document.file_url)
        .join(EntityRelationship, EntityRelationship.document_id == Document.id)
        .where(
            or_(
                (EntityRelationship.source_entity_type == "company") & (EntityRelationship.source_entity_id == company_id),
                (EntityRelationship.target_entity_type == "company") & (EntityRelationship.target_entity_id == company_id),
            )
        )
        .distinct()
    )
    docs = [{"id": str(r.id), "title": r.title, "file_url": r.file_url} for r in result.fetchall()]
    return {"status": "ok", "data": docs}


# --- Associations ---

@router.get("/associations")
async def list_associations(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    q: str = Query("", description="Search by name"),
    db: AsyncSession = Depends(get_db),
):
    offset = _paginate(page, limit)
    base = select(Association)
    count_base = select(func.count()).select_from(Association)
    if q.strip():
        filt = Association.name_hebrew.ilike(f"%{q.strip()}%")
        base = base.where(filt)
        count_base = count_base.where(filt)
    total = (await db.execute(count_base)).scalar()
    result = await db.execute(base.offset(offset).limit(limit))
    assocs = result.scalars().all()
    return {
        "status": "ok",
        "data": [_entity_to_dict(a, ["registration_number"]) for a in assocs],
        "meta": {"total": total, "page": page, "limit": limit, "pages": (total + limit - 1) // limit},
    }


@router.get("/associations/{assoc_id}")
async def get_association(assoc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Association).where(Association.id == assoc_id))
    assoc = result.scalars().first()
    if not assoc:
        raise HTTPException(404, "Association not found")
    return {"status": "ok", "data": _entity_to_dict(assoc, ["name_english", "registration_number"])}


@router.get("/associations/{assoc_id}/documents")
async def get_association_documents(assoc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Document.id, Document.title, Document.file_url)
        .join(EntityRelationship, EntityRelationship.document_id == Document.id)
        .where(
            or_(
                (EntityRelationship.source_entity_type == "association") & (EntityRelationship.source_entity_id == assoc_id),
                (EntityRelationship.target_entity_type == "association") & (EntityRelationship.target_entity_id == assoc_id),
            )
        )
        .distinct()
    )
    docs = [{"id": str(r.id), "title": r.title, "file_url": r.file_url} for r in result.fetchall()]
    return {"status": "ok", "data": docs}


# --- Domains ---

@router.get("/domains")
async def list_domains(
    q: str = Query("", description="Search by name"),
    db: AsyncSession = Depends(get_db),
):
    base = select(Domain).order_by(Domain.name_hebrew)
    if q.strip():
        base = base.where(Domain.name_hebrew.ilike(f"%{q.strip()}%"))
    result = await db.execute(base)
    domains = result.scalars().all()
    return {"status": "ok", "data": [_entity_to_dict(d, ["description"]) for d in domains]}


@router.get("/domains/{domain_id}")
async def get_domain(domain_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Domain).where(Domain.id == domain_id))
    domain = result.scalars().first()
    if not domain:
        raise HTTPException(404, "Domain not found")
    return {"status": "ok", "data": _entity_to_dict(domain, ["description"])}


@router.get("/domains/{domain_id}/documents")
async def get_domain_documents(domain_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Document.id, Document.title, Document.file_url)
        .join(EntityRelationship, EntityRelationship.document_id == Document.id)
        .where(
            or_(
                (EntityRelationship.source_entity_type == "domain") & (EntityRelationship.source_entity_id == domain_id),
                (EntityRelationship.target_entity_type == "domain") & (EntityRelationship.target_entity_id == domain_id),
            )
        )
        .distinct()
    )
    docs = [{"id": str(r.id), "title": r.title, "file_url": r.file_url} for r in result.fetchall()]
    return {"status": "ok", "data": docs}
