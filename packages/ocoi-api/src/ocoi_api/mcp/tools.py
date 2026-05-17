"""Tool implementations for the OCOI MCP server.

Each tool is a thin async wrapper around the same DB helpers the public
HTTP API uses (``ocoi_db.search``, ``ocoi_db.graph``, …). We deliberately
do NOT call our own /api/* endpoints — going straight to the session
factory skips a network hop, keeps types narrow, and lets us reuse the
admin DB pool.

All tools are decorated with ``@metered(...)`` so every invocation lands
in ``usage_events``.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select

from ocoi_api.mcp.metering import metered
from ocoi_db.engine import async_session_factory
from ocoi_db.graph import find_path, get_neighbors
from ocoi_db.models import (
    Association,
    Company,
    Document,
    Domain,
    EntityRelationship,
    Person,
    RegistryRecord,
)
from ocoi_db.search import search_entities

# Entity-type → ORM model + small JSON serialiser.
_ENTITY_MODELS = {
    "person": Person,
    "company": Company,
    "association": Association,
    "domain": Domain,
}


def _entity_to_dict(entity, etype: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": str(entity.id),
        "entity_type": etype,
        "name_hebrew": entity.name_hebrew,
        "name_english": getattr(entity, "name_english", None),
    }
    for f in ("title", "position", "ministry"):
        if hasattr(entity, f):
            base[f] = getattr(entity, f)
    for f in ("registration_number", "status", "company_type"):
        if hasattr(entity, f):
            base[f] = getattr(entity, f)
    return base


def _coerce_uuid(value: str) -> str:
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError):
        raise ValueError(f"Invalid entity id (not a UUID): {value}")


# ─── Tool functions ───────────────────────────────────────────────────────


@metered("search")
async def tool_search(
    query: str, entity_type: str | None = None, limit: int = 20,
) -> dict[str, Any]:
    if entity_type and entity_type not in _ENTITY_MODELS:
        raise ValueError(
            f"entity_type must be one of {sorted(_ENTITY_MODELS)} or omitted"
        )
    if not query or not query.strip():
        raise ValueError("query must be non-empty")
    limit = max(1, min(100, int(limit)))

    async with async_session_factory() as session:
        results, total = await search_entities(
            session, query.strip(), entity_type, limit=limit, offset=0,
        )
    return {
        "total": total,
        "results": [
            {"id": r.id, "entity_type": r.entity_type.value, "name": r.name}
            for r in results
        ],
    }


@metered("entity_get")
async def tool_entity_get(entity_type: str, id: str) -> dict[str, Any]:
    if entity_type not in _ENTITY_MODELS:
        raise ValueError(f"entity_type must be one of {sorted(_ENTITY_MODELS)}")
    eid = _coerce_uuid(id)
    model = _ENTITY_MODELS[entity_type]
    async with async_session_factory() as session:
        row = await session.get(model, eid)
        if row is None:
            return {"found": False}
        return {"found": True, "entity": _entity_to_dict(row, entity_type)}


@metered("graph_neighbors")
async def tool_graph_neighbors(
    entity_id: str, entity_type: str, depth: int = 1,
) -> dict[str, Any]:
    if entity_type not in _ENTITY_MODELS:
        raise ValueError(f"entity_type must be one of {sorted(_ENTITY_MODELS)}")
    depth = max(1, min(3, int(depth)))
    eid = _coerce_uuid(entity_id)
    async with async_session_factory() as session:
        subgraph = await get_neighbors(session, eid, entity_type, depth=depth)
    return subgraph.model_dump()


@metered("graph_path")
async def tool_graph_path(
    from_id: str, from_type: str,
    to_id: str, to_type: str,
    max_hops: int = 4,
) -> dict[str, Any]:
    for et in (from_type, to_type):
        if et not in _ENTITY_MODELS:
            raise ValueError(f"entity_type must be one of {sorted(_ENTITY_MODELS)}: got {et}")
    max_hops = max(1, min(6, int(max_hops)))
    async with async_session_factory() as session:
        subgraph = await find_path(
            session,
            _coerce_uuid(from_id), from_type,
            _coerce_uuid(to_id), to_type,
            max_hops=max_hops,
        )
    if subgraph is None:
        return {"found": False}
    return {"found": True, "graph": subgraph.model_dump()}


@metered("document_get")
async def tool_document_get(id: str, include_markdown: bool = False) -> dict[str, Any]:
    did = _coerce_uuid(id)
    async with async_session_factory() as session:
        doc = await session.get(Document, did)
        if doc is None:
            return {"found": False}
        out: dict[str, Any] = {
            "found": True,
            "id": str(doc.id),
            "title": doc.title,
            "file_url": doc.file_url,
            "file_format": doc.file_format,
            "conversion_status": doc.conversion_status,
            "extraction_status": doc.extraction_status,
            "verified": doc.verified,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
        }
        if include_markdown:
            # markdown_content is a deferred column — touching the attr
            # triggers a lazy load. We bound the response so a 10MB doc
            # doesn't get dumped into an MCP tool result.
            md = doc.markdown_content or ""
            out["markdown_content"] = md[:200_000]
            out["markdown_truncated"] = len(md) > 200_000
    return out


@metered("document_entities")
async def tool_document_entities(id: str) -> dict[str, Any]:
    did = _coerce_uuid(id)
    async with async_session_factory() as session:
        rels = (await session.execute(
            select(EntityRelationship).where(EntityRelationship.document_id == did)
        )).scalars().all()
        # Collect distinct (type, id) pairs from both sides of every edge.
        pairs: set[tuple[str, str]] = set()
        for r in rels:
            pairs.add((r.source_entity_type, str(r.source_entity_id)))
            pairs.add((r.target_entity_type, str(r.target_entity_id)))

        entities: list[dict[str, Any]] = []
        for etype, eid in sorted(pairs):
            model = _ENTITY_MODELS.get(etype)
            if model is None:
                continue
            ent = await session.get(model, eid)
            if ent is not None:
                entities.append(_entity_to_dict(ent, etype))
    return {"document_id": str(did), "entities": entities, "relationship_count": len(rels)}


@metered("top_connected")
async def tool_top_connected(
    entity_type: str | None = None, limit: int = 20,
) -> dict[str, Any]:
    if entity_type and entity_type not in _ENTITY_MODELS:
        raise ValueError(f"entity_type must be one of {sorted(_ENTITY_MODELS)}")
    limit = max(1, min(100, int(limit)))
    from sqlalchemy import func, text  # local — keeps the module import light

    # Count edges touching each entity, group by (type, id), sort desc.
    async with async_session_factory() as session:
        clauses: list[str] = []
        if entity_type:
            clauses.append(
                f"((source_entity_type = '{entity_type}') OR (target_entity_type = '{entity_type}'))"
            )
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        sql = text(f"""
            SELECT etype, eid, COUNT(*) AS edges FROM (
                SELECT source_entity_type AS etype, source_entity_id AS eid FROM entity_relationships
                UNION ALL
                SELECT target_entity_type AS etype, target_entity_id AS eid FROM entity_relationships
            ) sub
            {where}
            GROUP BY etype, eid
            ORDER BY edges DESC
            LIMIT :lim
        """)
        rows = (await session.execute(sql, {"lim": limit})).fetchall()

        entries: list[dict[str, Any]] = []
        for etype, eid, edges in rows:
            if entity_type and etype != entity_type:
                continue
            model = _ENTITY_MODELS.get(etype)
            name = None
            if model is not None:
                ent = await session.get(model, str(eid))
                if ent is not None:
                    name = ent.name_hebrew
            entries.append({
                "entity_type": etype,
                "id": str(eid),
                "name_hebrew": name,
                "edge_count": int(edges),
            })
    return {"results": entries}


@metered("by_ministry")
async def tool_by_ministry(ministry: str) -> dict[str, Any]:
    if not ministry or not ministry.strip():
        raise ValueError("ministry must be non-empty")
    async with async_session_factory() as session:
        persons = (await session.execute(
            select(Person).where(Person.ministry.like(f"%{ministry.strip()}%"))
        )).scalars().all()
        data = []
        for p in persons:
            rels = (await session.execute(
                select(EntityRelationship).where(
                    EntityRelationship.source_entity_type == "person",
                    EntityRelationship.source_entity_id == str(p.id),
                )
            )).scalars().all()
            data.append({
                "person": _entity_to_dict(p, "person"),
                "restrictions_count": len([r for r in rels if r.relationship_type == "restricted_from"]),
                "total_connections": len(rels),
            })
    return {"ministry_query": ministry, "results": data, "total": len(data)}


@metered("registry_lookup")
async def tool_registry_lookup(
    registration_number: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    if not registration_number and not name:
        raise ValueError("Provide registration_number or name")
    async with async_session_factory() as session:
        q = select(RegistryRecord)
        if registration_number:
            q = q.where(RegistryRecord.registration_number == registration_number.strip())
        elif name:
            q = q.where(RegistryRecord.name.like(f"%{name.strip()}%"))
        q = q.limit(20)
        rows = (await session.execute(q)).scalars().all()
        return {
            "results": [
                {
                    "id": str(r.id),
                    "source_type": r.source_type,
                    "name": r.name,
                    "registration_number": r.registration_number,
                    "status": r.status,
                }
                for r in rows
            ],
        }


@metered("stats")
async def tool_stats() -> dict[str, Any]:
    from ocoi_db.crud import count_entities
    async with async_session_factory() as session:
        counts = await count_entities(session)
    return {"counts": counts}


# ─── Tool registry ────────────────────────────────────────────────────────
# Each entry: (name, description, function, JSON-schema input). The
# server module turns these into MCP Tool definitions. Descriptions are
# in English (clients pass them to the model) and state that names are
# in Hebrew so the LLM doesn't try to romanise input.


TOOL_DEFS: list[dict[str, Any]] = [
    {
        "name": "search",
        "description": (
            "Search the OCOI Israeli conflict-of-interest dataset for persons, "
            "companies, associations, or domains by name. Entity names are in "
            "Hebrew — pass the query in Hebrew. Returns up to `limit` matches."
        ),
        "fn": tool_search,
        "schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term in Hebrew."},
                "entity_type": {
                    "type": "string",
                    "enum": ["person", "company", "association", "domain"],
                    "description": "Restrict to a single entity type (optional).",
                },
                "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
            },
            "required": ["query"],
        },
    },
    {
        "name": "entity_get",
        "description": "Fetch a single entity by id and type. Returns full row.",
        "fn": tool_entity_get,
        "schema": {
            "type": "object",
            "properties": {
                "entity_type": {
                    "type": "string",
                    "enum": ["person", "company", "association", "domain"],
                },
                "id": {"type": "string", "description": "UUID of the entity."},
            },
            "required": ["entity_type", "id"],
        },
    },
    {
        "name": "graph_neighbors",
        "description": (
            "Get all relationships (edges) connected to an entity up to `depth` hops. "
            "depth=1 = direct neighbours; depth=2..3 expands the subgraph. Returns "
            "nodes + edges as a SubGraph object."
        ),
        "fn": tool_graph_neighbors,
        "schema": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string"},
                "entity_type": {
                    "type": "string",
                    "enum": ["person", "company", "association", "domain"],
                },
                "depth": {"type": "integer", "default": 1, "minimum": 1, "maximum": 3},
            },
            "required": ["entity_id", "entity_type"],
        },
    },
    {
        "name": "graph_path",
        "description": (
            "Find a path between two entities, if one exists, up to `max_hops`. "
            "Useful for explaining how a public official is connected to a "
            "company or another official."
        ),
        "fn": tool_graph_path,
        "schema": {
            "type": "object",
            "properties": {
                "from_id": {"type": "string"},
                "from_type": {
                    "type": "string",
                    "enum": ["person", "company", "association", "domain"],
                },
                "to_id": {"type": "string"},
                "to_type": {
                    "type": "string",
                    "enum": ["person", "company", "association", "domain"],
                },
                "max_hops": {"type": "integer", "default": 4, "minimum": 1, "maximum": 6},
            },
            "required": ["from_id", "from_type", "to_id", "to_type"],
        },
    },
    {
        "name": "document_get",
        "description": (
            "Fetch a source document's metadata. Set include_markdown=true to "
            "also pull the OCR/extracted text (capped at 200k chars)."
        ),
        "fn": tool_document_get,
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "include_markdown": {"type": "boolean", "default": False},
            },
            "required": ["id"],
        },
    },
    {
        "name": "document_entities",
        "description": "List every entity extracted from a single document.",
        "fn": tool_document_entities,
        "schema": {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
        },
    },
    {
        "name": "top_connected",
        "description": (
            "Rank entities by edge count — useful for finding the most "
            "interconnected officials, companies, etc."
        ),
        "fn": tool_top_connected,
        "schema": {
            "type": "object",
            "properties": {
                "entity_type": {
                    "type": "string",
                    "enum": ["person", "company", "association", "domain"],
                },
                "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
            },
        },
    },
    {
        "name": "by_ministry",
        "description": (
            "List all persons attached to a ministry plus their connection "
            "counts. ministry name is a Hebrew substring (partial match)."
        ),
        "fn": tool_by_ministry,
        "schema": {
            "type": "object",
            "properties": {"ministry": {"type": "string"}},
            "required": ["ministry"],
        },
    },
    {
        "name": "registry_lookup",
        "description": (
            "Look up a record in the official Israeli company/association "
            "registries by registration number or name (Hebrew substring)."
        ),
        "fn": tool_registry_lookup,
        "schema": {
            "type": "object",
            "properties": {
                "registration_number": {"type": "string"},
                "name": {"type": "string"},
            },
        },
    },
    {
        "name": "stats",
        "description": "Aggregate counts: persons, companies, associations, documents, relationships.",
        "fn": tool_stats,
        "schema": {"type": "object", "properties": {}},
    },
]
