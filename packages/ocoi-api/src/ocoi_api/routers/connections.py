"""Graph / connection endpoints."""

import uuid
from enum import Enum
from typing import Any
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_common import now_israel
from ocoi_db.graph import get_neighbors, find_path, find_showcase_pair
from ocoi_db.models import Person, Company, Association, Domain

router = APIRouter(tags=["connections"])


class EntityTypeParam(str, Enum):
    person = "person"
    company = "company"
    association = "association"
    domain = "domain"


# Whitelist: entity type → ORM model (prevents any SQL injection via table names)
_MODEL_MAP = {
    "person": Person,
    "company": Company,
    "association": Association,
    "domain": Domain,
}


async def _enrich_subgraph(db: AsyncSession, subgraph):
    """Fill in entity names for nodes and edges by querying via ORM."""
    entity_ids = set()
    for node in subgraph.nodes:
        entity_ids.add((node.entity_type.value, node.id))
    for edge in subgraph.edges:
        entity_ids.add((edge.source_type.value, edge.source_id))
        entity_ids.add((edge.target_type.value, edge.target_id))

    names = {}
    extras = {}
    for etype, eid in entity_ids:
        model = _MODEL_MAP.get(etype)
        if model:
            if etype == "person":
                result = await db.execute(
                    select(model.name_hebrew, model.title, model.position, model.ministry).where(model.id == eid)
                )
                row = result.fetchone()
                if row:
                    names[eid] = row[0]
                    extra = {}
                    if row[1]: extra["title"] = row[1]
                    if row[2]: extra["position"] = row[2]
                    if row[3]: extra["ministry"] = row[3]
                    if extra:
                        extras[eid] = extra
            else:
                result = await db.execute(
                    select(model.name_hebrew).where(model.id == eid)
                )
                row = result.fetchone()
                if row:
                    names[eid] = row[0]

    for node in subgraph.nodes:
        node.name = names.get(node.id, "")
        if node.id in extras:
            node.extra = extras[node.id]
    for edge in subgraph.edges:
        edge.source_name = names.get(edge.source_id, "")
        edge.target_name = names.get(edge.target_id, "")


@router.get("/graph/neighbors/{entity_id}")
async def graph_neighbors(
    entity_id: uuid.UUID,
    type: EntityTypeParam = Query(..., description="Entity type"),
    depth: int = Query(1, ge=1, le=3),
    db: AsyncSession = Depends(get_db),
):
    subgraph = await get_neighbors(db, entity_id, type.value, depth)
    await _enrich_subgraph(db, subgraph)
    return {
        "status": "ok",
        "data": subgraph.model_dump(),
    }


@router.get("/graph/path")
async def graph_path(
    from_id: uuid.UUID = Query(...),
    from_type: EntityTypeParam = Query(...),
    to_id: uuid.UUID = Query(...),
    to_type: EntityTypeParam = Query(...),
    max_hops: int = Query(4, ge=1, le=6),
    db: AsyncSession = Depends(get_db),
):
    subgraph = await find_path(db, from_id, from_type.value, to_id, to_type.value, max_hops)
    if subgraph is None:
        raise HTTPException(404, "No path found between entities")
    await _enrich_subgraph(db, subgraph)
    return {
        "status": "ok",
        "data": subgraph.model_dump(),
    }


# Module-level cache for the showcase result. The pair rotates by date in
# Israel time, so the cache key is just today's date string. A new day →
# new query → new pair. Same day → instant.
_showcase_cache: tuple[str, dict[str, Any] | None] | None = None


def _today_key() -> str:
    """Cache key — today's date in Israel time. Stable for an entire day."""
    return now_israel().date().isoformat()


def _today_seed() -> int:
    """Daily rotation seed — proleptic Gregorian ordinal in Israel time."""
    return now_israel().date().toordinal()


@router.get("/graph/showcase")
async def graph_showcase(db: AsyncSession = Depends(get_db)):
    """Return a "two suns" subgraph illustrating two persons whose declared
    networks overlap. Used by the home page to demonstrate the data shape.
    The chosen pair rotates daily; the result is cached in-process for the
    rest of the calendar day (Israel time)."""
    global _showcase_cache
    today = _today_key()
    if _showcase_cache and _showcase_cache[0] == today:
        return {"status": "ok", "data": _showcase_cache[1], "cached": True}

    subgraph = await find_showcase_pair(db, rotation_seed=_today_seed())
    if subgraph is None:
        _showcase_cache = (today, None)
        return {"status": "ok", "data": None}

    await _enrich_subgraph(db, subgraph)
    payload = subgraph.model_dump()
    _showcase_cache = (today, payload)
    return {"status": "ok", "data": payload}


@router.get("/graph/subgraph")
async def graph_subgraph(
    center: uuid.UUID = Query(...),
    type: EntityTypeParam = Query(...),
    radius: int = Query(2, ge=1, le=3),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    subgraph = await get_neighbors(db, center, type.value, radius)
    # Trim to limit
    if len(subgraph.nodes) > limit:
        subgraph.nodes = subgraph.nodes[:limit]
        node_ids = {n.id for n in subgraph.nodes}
        subgraph.edges = [e for e in subgraph.edges
                         if e.source_id in node_ids and e.target_id in node_ids]
    await _enrich_subgraph(db, subgraph)
    return {
        "status": "ok",
        "data": subgraph.model_dump(),
    }
