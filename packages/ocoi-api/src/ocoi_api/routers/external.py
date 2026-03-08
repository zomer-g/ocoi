"""External integration endpoints for querying by registration number, name, ministry."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.models import Company, Person, EntityRelationship
from ocoi_db.graph import get_neighbors

router = APIRouter(prefix="/external", tags=["external"])


@router.get("/by-company")
async def by_company(
    registration_number: str = Query(
        ..., min_length=1, max_length=20,
        description="Company registration number (ח.פ.)",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Look up all connections for a company by registration number.

    Designed for integration with budget transparency sites.
    """
    result = await db.execute(
        select(Company).where(Company.registration_number == registration_number)
    )
    company = result.scalar_one_or_none()
    if not company:
        return {"status": "ok", "data": None, "message": "Company not found"}

    subgraph = await get_neighbors(db, company.id, "company", depth=1)
    return {
        "status": "ok",
        "data": {
            "company": {
                "id": str(company.id),
                "name_hebrew": company.name_hebrew,
                "registration_number": company.registration_number,
                "status": company.status,
            },
            "connections": subgraph.model_dump(),
        },
    }


@router.get("/by-person")
async def by_person(
    name: str = Query(
        ..., min_length=2, max_length=100,
        description="Person name (Hebrew)",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Look up all connections for a person by name. Supports partial matching."""
    result = await db.execute(
        select(Person).where(Person.name_hebrew.like(f"%{name}%"))
    )
    persons = result.scalars().all()
    if not persons:
        return {"status": "ok", "data": [], "message": "No persons found"}

    results = []
    for person in persons[:5]:
        subgraph = await get_neighbors(db, person.id, "person", depth=1)
        results.append({
            "person": {
                "id": str(person.id),
                "name_hebrew": person.name_hebrew,
                "title": person.title,
                "position": person.position,
                "ministry": person.ministry,
            },
            "connections": subgraph.model_dump(),
        })

    return {"status": "ok", "data": results}


@router.get("/by-ministry")
async def by_ministry(
    name: str = Query(
        ..., min_length=2, max_length=100,
        description="Ministry name (Hebrew)",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Get all conflict of interest data for a ministry."""
    result = await db.execute(
        select(Person).where(Person.ministry.like(f"%{name}%"))
    )
    persons = result.scalars().all()

    data = []
    for person in persons:
        rels = await db.execute(
            select(EntityRelationship).where(
                (EntityRelationship.source_entity_type == "person") &
                (EntityRelationship.source_entity_id == person.id)
            )
        )
        relationships = rels.scalars().all()
        data.append({
            "person": {
                "id": str(person.id),
                "name_hebrew": person.name_hebrew,
                "title": person.title,
                "position": person.position,
            },
            "restrictions_count": len([r for r in relationships if r.relationship_type == "restricted_from"]),
            "total_connections": len(relationships),
        })

    return {"status": "ok", "data": data, "meta": {"total": len(data)}}


@router.get("/stats")
async def stats(db: AsyncSession = Depends(get_db)):
    """System statistics."""
    from ocoi_db.crud import count_entities
    counts = await count_entities(db)
    return {"status": "ok", "data": counts}
