"""Full-text search — works on both SQLite and PostgreSQL."""

from sqlalchemy import select, func, union_all, literal, cast, String
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_common.models import EntitySummary, EntityType
from ocoi_db.models import Person, Company, Association, Domain


# Whitelist mapping: entity type → (ORM model, display column)
_ENTITY_MAP = {
    "person": (Person, Person.name_hebrew, "person"),
    "company": (Company, Company.name_hebrew, "company"),
    "association": (Association, Association.name_hebrew, "association"),
    "domain": (Domain, Domain.name_hebrew, "domain"),
}


async def search_entities(
    session: AsyncSession,
    query: str,
    entity_type: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[EntitySummary], int]:
    """Search across all entity types using ORM (no raw SQL interpolation)."""
    search_term = query.strip()
    if not search_term:
        return [], 0

    like_pattern = f"%{search_term}%"

    # Determine which entity types to search
    if entity_type and entity_type in _ENTITY_MAP:
        targets = {entity_type: _ENTITY_MAP[entity_type]}
    else:
        targets = _ENTITY_MAP

    # Count total matches using ORM
    total = 0
    for key, (model, name_col, _) in targets.items():
        count_q = select(func.count()).select_from(model).where(name_col.like(like_pattern))
        result = await session.execute(count_q)
        total += result.scalar() or 0

    # Build UNION ALL using ORM subqueries
    parts = []
    for key, (model, name_col, etype_label) in targets.items():
        part = select(
            cast(model.id, String).label("id"),
            literal(etype_label).label("entity_type"),
            name_col.label("name"),
        ).where(name_col.like(like_pattern))
        parts.append(part)

    if not parts:
        return [], 0

    combined = union_all(*parts).subquery()
    search_q = (
        select(combined.c.id, combined.c.entity_type, combined.c.name)
        .order_by(combined.c.name)
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(search_q)

    entities = [
        EntitySummary(
            id=row.id,
            entity_type=EntityType(row.entity_type),
            name=row.name,
        )
        for row in result.fetchall()
    ]
    return entities, int(total)


async def suggest(
    session: AsyncSession,
    query: str,
    limit: int = 10,
) -> list[EntitySummary]:
    """Autocomplete suggestions for the search bar."""
    entities, _ = await search_entities(session, query, limit=limit)
    return entities
