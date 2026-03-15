"""Entity CRUD endpoints (persons, companies, associations, domains)."""

import json
import uuid
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, func, or_, and_, union_all, literal_column, cast, String
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.models import Person, Company, Association, Domain, EntityRelationship, Document, RegistryRecord

router = APIRouter(tags=["entities"])


def _paginate(page: int, limit: int):
    return (page - 1) * limit


def _entity_to_dict(entity, extra_fields: list[str] | None = None) -> dict:
    d = {"id": str(entity.id), "name_hebrew": entity.name_hebrew}
    for field in (extra_fields or []):
        val = getattr(entity, field, None)
        # Parse aliases JSON string into a proper list for the API response
        if field == "aliases" and isinstance(val, str):
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                val = []
        d[field] = val
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
            "name_english", "registration_number", "company_type", "status", "match_confidence", "aliases",
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
    return {"status": "ok", "data": _entity_to_dict(assoc, ["name_english", "registration_number", "aliases"])}


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
    return {"status": "ok", "data": _entity_to_dict(domain, ["description", "aliases"])}


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


# --- Top connected entities ---

@router.get("/entities/top-connected")
async def top_connected(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    type: str = Query("", description="Filter: person, company, association"),
    db: AsyncSession = Depends(get_db),
):
    """Return entities ranked by number of connections (descending)."""
    # Build a union of all entity references from both sides of relationships
    source_refs = select(
        EntityRelationship.source_entity_id.label("entity_id"),
        EntityRelationship.source_entity_type.label("entity_type"),
    )
    target_refs = select(
        EntityRelationship.target_entity_id.label("entity_id"),
        EntityRelationship.target_entity_type.label("entity_type"),
    )

    if type:
        source_refs = source_refs.where(EntityRelationship.source_entity_type == type)
        target_refs = target_refs.where(EntityRelationship.target_entity_type == type)

    all_refs = union_all(source_refs, target_refs).subquery("all_refs")

    # Count connections per entity
    counted = (
        select(
            all_refs.c.entity_id,
            all_refs.c.entity_type,
            func.count().label("connection_count"),
        )
        .group_by(all_refs.c.entity_id, all_refs.c.entity_type)
    ).subquery("counted")

    # Get total count
    total = (await db.execute(select(func.count()).select_from(counted))).scalar()

    # Paginate
    offset = _paginate(page, limit)
    rows = (
        await db.execute(
            select(counted)
            .order_by(counted.c.connection_count.desc())
            .offset(offset)
            .limit(limit)
        )
    ).fetchall()

    # Resolve names by looking up each entity
    model_map = {
        "person": Person,
        "company": Company,
        "association": Association,
        "domain": Domain,
    }

    data = []
    for row in rows:
        model = model_map.get(row.entity_type)
        if not model:
            continue
        entity = (
            await db.execute(select(model).where(model.id == row.entity_id))
        ).scalars().first()
        if entity:
            data.append({
                "id": str(row.entity_id),
                "entity_type": row.entity_type,
                "name": entity.name_hebrew,
                "connection_count": row.connection_count,
            })

    return {
        "status": "ok",
        "data": data,
        "meta": {"total": total, "page": page, "limit": limit, "pages": (total + limit - 1) // limit},
    }


# --- Lookup: search entities by registration number or name ---

@router.get("/lookup")
async def lookup_entity(
    q: str = Query("", description="Search by name (partial match)"),
    registration_number: str = Query("", description="Exact registration number"),
    entity_type: str = Query("", description="Filter: person, company, association, domain"),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Unified entity lookup — search across all entity types by name or registration number."""
    results = []

    search_types = [entity_type] if entity_type else ["person", "company", "association", "domain"]

    for etype in search_types:
        if etype == "company":
            stmt = select(Company)
            if registration_number:
                stmt = stmt.where(Company.registration_number == registration_number)
            elif q.strip():
                stmt = stmt.where(Company.name_hebrew.ilike(f"%{q.strip()}%"))
            else:
                continue
            rows = (await db.execute(stmt.limit(limit))).scalars().all()
            for c in rows:
                results.append({
                    "id": str(c.id), "entity_type": "company",
                    "name_hebrew": c.name_hebrew, "registration_number": c.registration_number,
                    "match_confidence": c.match_confidence,
                })

        elif etype == "association":
            stmt = select(Association)
            if registration_number:
                stmt = stmt.where(Association.registration_number == registration_number)
            elif q.strip():
                stmt = stmt.where(Association.name_hebrew.ilike(f"%{q.strip()}%"))
            else:
                continue
            rows = (await db.execute(stmt.limit(limit))).scalars().all()
            for a in rows:
                results.append({
                    "id": str(a.id), "entity_type": "association",
                    "name_hebrew": a.name_hebrew, "registration_number": a.registration_number,
                    "match_confidence": a.match_confidence,
                })

        elif etype == "person":
            if registration_number:
                continue
            if not q.strip():
                continue
            stmt = select(Person).where(Person.name_hebrew.ilike(f"%{q.strip()}%")).limit(limit)
            rows = (await db.execute(stmt)).scalars().all()
            for p in rows:
                results.append({
                    "id": str(p.id), "entity_type": "person",
                    "name_hebrew": p.name_hebrew, "registration_number": None,
                    "match_confidence": None,
                })

        elif etype == "domain":
            if registration_number:
                continue
            if not q.strip():
                continue
            stmt = select(Domain).where(Domain.name_hebrew.ilike(f"%{q.strip()}%")).limit(limit)
            rows = (await db.execute(stmt)).scalars().all()
            for d in rows:
                results.append({
                    "id": str(d.id), "entity_type": "domain",
                    "name_hebrew": d.name_hebrew, "registration_number": None,
                    "match_confidence": None,
                })

    return {"status": "ok", "data": results[:limit]}


# --- Registry lookup: search the full government registry ---

@router.get("/registry/lookup")
async def lookup_registry(
    q: str = Query("", description="Search by name (partial match)"),
    registration_number: str = Query("", description="Exact registration number"),
    source_type: str = Query("", description="Filter: companies, associations, public_benefit, local_authorities, municipal_corporations"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Search the external government registry records (721K+ companies, associations, etc.)."""
    offset = _paginate(page, limit)
    stmt = select(RegistryRecord)
    count_stmt = select(func.count()).select_from(RegistryRecord)
    filters = []

    if source_type:
        filters.append(RegistryRecord.source_type == source_type)
    if registration_number:
        filters.append(RegistryRecord.registration_number == registration_number)
    elif q.strip():
        filters.append(RegistryRecord.name.ilike(f"%{q.strip()}%"))

    if not registration_number and not q.strip():
        return {"status": "ok", "data": [], "meta": {"total": 0, "page": page, "limit": limit}}

    if filters:
        stmt = stmt.where(and_(*filters))
        count_stmt = count_stmt.where(and_(*filters))

    total = (await db.execute(count_stmt)).scalar()
    result = await db.execute(stmt.offset(offset).limit(limit).order_by(RegistryRecord.name))
    records = result.scalars().all()

    return {
        "status": "ok",
        "data": [
            {
                "id": str(r.id),
                "name": r.name,
                "registration_number": r.registration_number,
                "source_type": r.source_type,
                "status": r.status,
            }
            for r in records
        ],
        "meta": {"total": total, "page": page, "limit": limit},
    }
