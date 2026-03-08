"""Search endpoints."""

from enum import Enum
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.search import search_entities, suggest

router = APIRouter(tags=["search"])


class SearchEntityType(str, Enum):
    person = "person"
    company = "company"
    association = "association"
    domain = "domain"


@router.get("/search")
async def search(
    q: str = Query(..., min_length=1, max_length=200, description="Search query"),
    type: Optional[SearchEntityType] = Query(None, description="Entity type filter"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    entity_type = type.value if type else None
    results, total = await search_entities(db, q, entity_type=entity_type, limit=limit, offset=offset)
    pages = (total + limit - 1) // limit if total > 0 else 0
    return {
        "status": "ok",
        "data": [r.model_dump() for r in results],
        "meta": {"total": total, "page": page, "limit": limit, "pages": pages},
    }


@router.get("/search/suggest")
async def search_suggest(
    q: str = Query(..., min_length=1),
    db: AsyncSession = Depends(get_db),
):
    results = await suggest(db, q, limit=10)
    return {
        "status": "ok",
        "data": [{"text": r.name, "type": r.entity_type, "id": r.id} for r in results],
    }
