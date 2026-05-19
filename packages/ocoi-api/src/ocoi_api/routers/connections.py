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


# ── origin_kind filter helpers ──────────────────────────────────────────
# Most public surfaces (home page + entity page) accept an `exclude_origins`
# query string: comma-separated origin_kind values to drop from the result.
# The frontend uses this to hide `mk_expense` (Knesset constituent-outreach
# payments) by default since those edges visually overwhelm the COI graph.

# Only origin_kinds we know about are accepted; anything else is silently
# ignored so a malformed query string can't widen the result set.
_KNOWN_ORIGINS = {"coi_declaration", "mk_expense"}


def _parse_exclude_origins(raw: str | None) -> tuple[str, ...]:
    """Normalise the query string into a stable tuple suitable for
    caching. Sorted + lower-cased + filtered to known values."""
    if not raw:
        return ()
    parts = {p.strip().lower() for p in raw.split(",") if p.strip()}
    return tuple(sorted(p for p in parts if p in _KNOWN_ORIGINS))


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
    """Fill in entity names + drop hidden entities (and any edge that
    touches them) so the public graph never uses a generic placeholder
    like "עניינים אישיים" as a connector."""
    entity_ids = set()
    for node in subgraph.nodes:
        entity_ids.add((node.entity_type.value, node.id))
    for edge in subgraph.edges:
        entity_ids.add((edge.source_type.value, edge.source_id))
        entity_ids.add((edge.target_type.value, edge.target_id))

    names: dict[str, str] = {}
    extras: dict[str, dict] = {}
    hidden: set[str] = set()
    for etype, eid in entity_ids:
        model = _MODEL_MAP.get(etype)
        if not model:
            continue
        if etype == "person":
            result = await db.execute(
                select(model.name_hebrew, model.title, model.position, model.ministry, model.hidden).where(model.id == eid)
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
                if row[4]:
                    hidden.add(eid)
        else:
            result = await db.execute(
                select(model.name_hebrew, model.hidden).where(model.id == eid)
            )
            row = result.fetchone()
            if row:
                names[eid] = row[0]
                if row[1]:
                    hidden.add(eid)

    # Prune hidden nodes and any edge touching one of them.
    if hidden:
        subgraph.nodes = [n for n in subgraph.nodes if n.id not in hidden]
        subgraph.edges = [
            e for e in subgraph.edges
            if e.source_id not in hidden and e.target_id not in hidden
        ]

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
    exclude_origins: str | None = Query(
        None,
        description="Comma-separated origin_kind values to drop "
                    "(e.g. 'mk_expense'). Unknown values are ignored.",
    ),
    db: AsyncSession = Depends(get_db),
):
    excluded = _parse_exclude_origins(exclude_origins)
    subgraph = await get_neighbors(
        db, entity_id, type.value, depth,
        excluded_origins=excluded or None,
    )
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


# In-process cache for the showcase result. Key is (today_date, exclude_origins)
# so the toggle-on and toggle-off variants live side by side.
_showcase_cache: dict[tuple[str, tuple[str, ...]], dict[str, Any] | None] = {}


def _today_key() -> str:
    """Cache key — today's date in Israel time. Stable for an entire day."""
    return now_israel().date().isoformat()


def _today_seed() -> int:
    """Daily rotation seed — proleptic Gregorian ordinal in Israel time."""
    return now_israel().date().toordinal()


@router.get("/graph/showcase")
async def graph_showcase(
    exclude_origins: str | None = Query(
        None,
        description="Comma-separated origin_kind values to drop. "
                    "Each combination is cached separately for the day.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Return a "two suns" subgraph illustrating two persons whose declared
    networks overlap. Used by the home page to demonstrate the data shape.
    The chosen pair rotates daily; the result is cached in-process for the
    rest of the calendar day (Israel time), keyed by date + excluded
    origins so toggling the MK-expense filter doesn't blow away the cache
    for the other variant."""
    today = _today_key()
    excluded = _parse_exclude_origins(exclude_origins)
    cache_key = (today, excluded)
    if cache_key in _showcase_cache:
        return {"status": "ok", "data": _showcase_cache[cache_key], "cached": True}

    # Prune stale day entries — keeps the dict from growing forever.
    for k in list(_showcase_cache):
        if k[0] != today:
            del _showcase_cache[k]

    subgraph = await find_showcase_pair(
        db,
        rotation_seed=_today_seed(),
        excluded_origins=excluded or None,
    )
    if subgraph is None:
        _showcase_cache[cache_key] = None
        return {"status": "ok", "data": None}

    await _enrich_subgraph(db, subgraph)
    payload = subgraph.model_dump()
    _showcase_cache[cache_key] = payload
    return {"status": "ok", "data": payload}


# ── Pre-warm hook ────────────────────────────────────────────────────────
# Called from main.py's lifespan so the first visitor of the day doesn't
# pay the 1-2s "two suns" computation. Populates BOTH variants: with and
# without MK-expense edges (the only two combinations the UI ever asks
# for today). Failures are swallowed — pre-warming is best-effort, the
# live endpoint will compute on demand if the cache is still cold.


async def _populate_showcase(
    excluded_origins: tuple[str, ...] | None = None,
) -> None:
    """Compute + cache one showcase variant. Opens its own session so it
    can run from a background task (no incoming Request, no Depends)."""
    from ocoi_db.engine import async_session_factory

    excluded = excluded_origins or ()
    today = _today_key()
    cache_key = (today, excluded)
    if cache_key in _showcase_cache:
        return

    async with async_session_factory() as session:
        subgraph = await find_showcase_pair(
            session,
            rotation_seed=_today_seed(),
            excluded_origins=list(excluded) if excluded else None,
        )
        if subgraph is None:
            _showcase_cache[cache_key] = None
            return
        await _enrich_subgraph(session, subgraph)
        _showcase_cache[cache_key] = subgraph.model_dump()


async def prewarm_showcase_cache() -> None:
    """Fire-and-forget — populate both variants for today. The MK-hidden
    variant runs first because it's the public default."""
    import logging
    import time
    log = logging.getLogger("ocoi.api.showcase")
    for variant_label, excluded in (
        ("hidden_mk", ("mk_expense",)),
        ("all_origins", ()),
    ):
        t0 = time.perf_counter()
        try:
            await _populate_showcase(excluded)
            log.info(
                "showcase pre-warm OK (%s) in %.0fms",
                variant_label, (time.perf_counter() - t0) * 1000,
            )
        except Exception as e:
            log.warning("showcase pre-warm failed (%s): %s", variant_label, e)


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
