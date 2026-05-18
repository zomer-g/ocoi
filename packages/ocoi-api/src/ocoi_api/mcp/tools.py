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

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.mcp.metering import metered
from ocoi_common.config import settings
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


# ─── Citation + attribution helpers ───────────────────────────────────────
# Every tool response carries (a) a `data_attribution` blurb the LLM is
# instructed to surface to the user, (b) `ocoi_url` for each entity so
# the user can verify on the public site, and (c) a `sources` list of
# documents wherever the response is about specific entities. Together
# with the server `instructions` field these enforce the project rule:
# every claim must be traceable back to a government PDF.

_OCOI_BASE = settings.mcp_public_url.rstrip("/") if settings.mcp_public_url else ""

DATA_ATTRIBUTION_HE = (
    "מידע מעובד מתוך הצהרות ניגוד עניינים פומביות של בעלי תפקידים בישראל. "
    "מעובד ע\"י ניגוד עניינים לעם (ocoi.org.il) באמצעות OCR + חילוץ ישויות בעזרת LLM. "
    "ייתכנו טעויות חילוץ או נתונים לא מעודכנים — תמיד הפנו את המשתמש לקישור המקור."
)
DATA_ATTRIBUTION_EN = (
    "Processed data from public Israeli conflict-of-interest declarations. "
    "Pipeline: OCR → LLM-based entity extraction by OCOI (ocoi.org.il). "
    "Extraction errors and stale data are possible — always cite the source links."
)


def _entity_url(entity_type: str, entity_id: str) -> str:
    if not _OCOI_BASE:
        return ""
    return f"{_OCOI_BASE}/entity?type={entity_type}&id={entity_id}"


def _document_url(document_id: str) -> str:
    if not _OCOI_BASE:
        return ""
    return f"{_OCOI_BASE}/document?id={document_id}"


def _entity_to_dict(entity, etype: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": str(entity.id),
        "entity_type": etype,
        "name_hebrew": entity.name_hebrew,
        "name_english": getattr(entity, "name_english", None),
        "ocoi_url": _entity_url(etype, str(entity.id)),
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


async def _sources_for_entity(
    session: AsyncSession,
    entity_type: str,
    entity_id: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return every distinct document that mentions the given entity."""
    rows = (await session.execute(
        select(Document.id, Document.title, Document.file_url)
        .join(EntityRelationship, EntityRelationship.document_id == Document.id)
        .where(
            or_(
                and_(
                    EntityRelationship.source_entity_type == entity_type,
                    EntityRelationship.source_entity_id == entity_id,
                ),
                and_(
                    EntityRelationship.target_entity_type == entity_type,
                    EntityRelationship.target_entity_id == entity_id,
                ),
            )
        )
        .distinct()
        .limit(limit)
    )).all()
    return [
        {
            "document_id": str(r.id),
            "title": r.title,
            "original_pdf_url": r.file_url,
            "ocoi_url": _document_url(str(r.id)),
        }
        for r in rows
    ]


async def _sources_for_document_ids(
    session: AsyncSession, document_ids: list[str],
) -> list[dict[str, Any]]:
    """Return citation entries for a known set of document UUIDs.
    Used to flatten the SubGraph edges' document references into a
    single `sources` list at the top of graph responses."""
    if not document_ids:
        return []
    unique = list({d for d in document_ids if d})
    if not unique:
        return []
    rows = (await session.execute(
        select(Document.id, Document.title, Document.file_url)
        .where(Document.id.in_(unique))
    )).all()
    return [
        {
            "document_id": str(r.id),
            "title": r.title,
            "original_pdf_url": r.file_url,
            "ocoi_url": _document_url(str(r.id)),
        }
        for r in rows
    ]


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
            {
                "id": r.id,
                "entity_type": r.entity_type.value,
                "name": r.name,
                "ocoi_url": _entity_url(r.entity_type.value, r.id),
            }
            for r in results
        ],
        "data_attribution": DATA_ATTRIBUTION_HE,
        "next_step_hint": (
            "Call entity_get or graph_neighbors on a result's id to fetch "
            "its source documents (citations the model MUST quote to the user)."
        ),
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
            return {"found": False, "data_attribution": DATA_ATTRIBUTION_HE}
        sources = await _sources_for_entity(session, entity_type, eid)
        return {
            "found": True,
            "entity": _entity_to_dict(row, entity_type),
            "sources": sources,
            "data_attribution": DATA_ATTRIBUTION_HE,
        }


def _enrich_subgraph(graph_dict: dict[str, Any]) -> dict[str, Any]:
    """Attach ocoi_url to every node so the LLM can link entities."""
    for node in graph_dict.get("nodes", []):
        et = node.get("entity_type")
        nid = node.get("id")
        if et and nid:
            node["ocoi_url"] = _entity_url(et, nid)
    return graph_dict


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
        graph = _enrich_subgraph(subgraph.model_dump())
        doc_ids = [e.get("document_id") for e in graph.get("edges", []) if e.get("document_id")]
        sources = await _sources_for_document_ids(session, doc_ids)
    return {
        "graph": graph,
        "sources": sources,
        "data_attribution": DATA_ATTRIBUTION_HE,
    }


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
            return {"found": False, "data_attribution": DATA_ATTRIBUTION_HE}
        graph = _enrich_subgraph(subgraph.model_dump())
        doc_ids = [e.get("document_id") for e in graph.get("edges", []) if e.get("document_id")]
        sources = await _sources_for_document_ids(session, doc_ids)
    return {
        "found": True,
        "graph": graph,
        "sources": sources,
        "data_attribution": DATA_ATTRIBUTION_HE,
    }


@metered("document_get")
async def tool_document_get(id: str, include_markdown: bool = False) -> dict[str, Any]:
    did = _coerce_uuid(id)
    async with async_session_factory() as session:
        doc = await session.get(Document, did)
        if doc is None:
            return {"found": False, "data_attribution": DATA_ATTRIBUTION_HE}
        out: dict[str, Any] = {
            "found": True,
            "id": str(doc.id),
            "title": doc.title,
            "original_pdf_url": doc.file_url,
            "ocoi_url": _document_url(str(doc.id)),
            "file_format": doc.file_format,
            "conversion_status": doc.conversion_status,
            "extraction_status": doc.extraction_status,
            "verified": doc.verified,
            "created_at": doc.created_at.isoformat() if doc.created_at else None,
            "data_attribution": DATA_ATTRIBUTION_HE,
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
        doc = await session.get(Document, did)
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

    source_obj = None
    if doc is not None:
        source_obj = {
            "document_id": str(doc.id),
            "title": doc.title,
            "original_pdf_url": doc.file_url,
            "ocoi_url": _document_url(str(doc.id)),
        }
    return {
        "document_id": str(did),
        "entities": entities,
        "relationship_count": len(rels),
        "sources": [source_obj] if source_obj else [],
        "data_attribution": DATA_ATTRIBUTION_HE,
    }


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
                "ocoi_url": _entity_url(etype, str(eid)),
            })
    return {
        "results": entries,
        "data_attribution": DATA_ATTRIBUTION_HE,
        "next_step_hint": (
            "Call entity_get(entity_type, id) to fetch the source documents "
            "for any entry the user wants to discuss."
        ),
    }


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
    return {
        "ministry_query": ministry,
        "results": data,
        "total": len(data),
        "data_attribution": DATA_ATTRIBUTION_HE,
        "next_step_hint": (
            "Each person carries an ocoi_url. Call graph_neighbors on a "
            "person.id to fetch their source documents."
        ),
    }


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
            "data_attribution": (
                "Records from the Israeli government's official registries "
                "(data.gov.il), mirrored by OCOI for fast lookup. The source_type "
                "field names the originating registry."
            ),
        }


@metered("stats")
async def tool_stats() -> dict[str, Any]:
    from ocoi_db.crud import count_entities
    async with async_session_factory() as session:
        counts = await count_entities(session)
    return {
        "counts": counts,
        "data_attribution": DATA_ATTRIBUTION_HE,
        "source_site": _OCOI_BASE,
    }


# ─── Server-level instructions (MCP InitializeResult.instructions) ───────
# These are prepended to the LLM's context once per session. The hard
# requirements for citations live here so they're never out of view.

SERVER_INSTRUCTIONS = """\
OCOI MCP server — ניגוד עניינים לעם.

This server exposes **processed** conflict-of-interest data extracted
from public Israeli government declarations. The pipeline is:
  PDF → OCR → Hebrew NLP + LLM extraction → entity & relationship graph.

Every relationship is a model's best guess. It is NOT verified legal
fact. Extraction errors, OCR errors, missing context, and stale data
are all common.

THREE RULES WHEN PRESENTING THIS DATA TO THE USER:

1. **Label it as processed data.** Every response that uses information
   from this server must include a sentence like:
   "לפי הנתונים המעובדים של ניגוד עניינים לעם (ocoi.org.il)..."
   or in English:
   "According to OCOI's processed conflict-of-interest data..."
   Never present extracted relationships as if they were authoritative
   government output.

2. **Cite sources.** Every tool response includes a `sources` array (or
   `ocoi_url` per entity). When you state a fact, name the source
   document and link to it. Markdown format:
   `[שם המסמך](https://www.ocoi.org.il/document?id=...)`
   For entities link to their profile page:
   `[שם הישות](https://www.ocoi.org.il/entity?type=...&id=...)`

3. **Encourage verification.** When the user asks something material,
   tell them they can verify against the original PDF
   (`original_pdf_url` field) and the OCOI page (`ocoi_url` field).

These rules are non-negotiable. The whole project's credibility rests on
each claim being traceable back to a government PDF — preserve that.
"""


# ─── Tool registry ────────────────────────────────────────────────────────
# Each entry: (name, description, function, JSON-schema input). The
# server module turns these into MCP Tool definitions. Descriptions are
# in English (clients pass them to the model) and state that names are
# in Hebrew so the LLM doesn't try to romanise input.

_CITE_REMINDER = (
    " Response includes `sources` (and/or `ocoi_url` per entity) — you "
    "MUST cite these to the user with the document title + ocoi_url."
)


TOOL_DEFS: list[dict[str, Any]] = [
    {
        "name": "search",
        "description": (
            "Search the OCOI Israeli conflict-of-interest dataset for persons, "
            "companies, associations, or domains by name. Entity names are in "
            "Hebrew — pass the query in Hebrew. Returns up to `limit` matches, "
            "each with an `ocoi_url` you can show the user. For source documents, "
            "follow up with entity_get or graph_neighbors on a specific id."
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
        "description": (
            "Fetch a single entity by id and type. Response includes the entity, "
            "its OCOI profile URL, and the list of source documents that mention "
            "it — every claim about this entity MUST cite from `sources`."
        ),
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
            "graph.nodes + graph.edges, plus a top-level `sources` list of every "
            "document the edges came from. Each edge in graph.edges also has its "
            "own document_id/document_title/document_url — use those when explaining "
            "a specific relationship to the user."
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
            "company or another official. Response includes `sources` for every "
            "edge in the path — cite the chain of documents, not just the endpoints."
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
