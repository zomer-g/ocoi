"""Document access endpoints."""

import uuid
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import undefer

from ocoi_api.dependencies import get_db
from ocoi_db.models import (
    Document, EntityRelationship,
    Person, Company, Association, Domain,
)

router = APIRouter(tags=["documents"])


_NAME_MODELS = {
    "person": Person,
    "company": Company,
    "association": Association,
    "domain": Domain,
}


@router.get("/documents")
async def list_documents(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, description="Filter by conversion_status"),
    q: str | None = Query(None, description="Substring search on title"),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    query = select(Document)
    count_query = select(func.count()).select_from(Document)

    if status:
        query = query.where(Document.conversion_status == status)
        count_query = count_query.where(Document.conversion_status == status)
    if q:
        like = f"%{q.strip()}%"
        query = query.where(Document.title.like(like))
        count_query = count_query.where(Document.title.like(like))

    total = (await db.execute(count_query)).scalar()
    result = await db.execute(query.offset(offset).limit(limit).order_by(Document.created_at.desc()))
    docs = result.scalars().all()

    return {
        "status": "ok",
        "data": [
            {
                "id": str(d.id),
                "title": d.title,
                "file_format": d.file_format,
                "file_url": d.file_url,
                "conversion_status": d.conversion_status,
                "extraction_status": d.extraction_status,
            }
            for d in docs
        ],
        "meta": {"total": total, "page": page, "limit": limit, "pages": (total + limit - 1) // limit},
    }


@router.get("/documents/{doc_id}")
async def get_document(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")
    return {
        "status": "ok",
        "data": {
            "id": str(doc.id),
            "title": doc.title,
            "file_format": doc.file_format,
            "file_url": doc.file_url,
            "file_size": doc.file_size,
            "conversion_status": doc.conversion_status,
            "extraction_status": doc.extraction_status,
        },
    }


@router.get("/documents/{doc_id}/markdown")
async def get_document_markdown(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Document).options(undefer(Document.markdown_content)).where(Document.id == doc_id)
    )
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")
    if not doc.markdown_content:
        raise HTTPException(404, "Document has not been converted yet")
    return {"status": "ok", "data": {"id": str(doc.id), "markdown": doc.markdown_content}}


@router.get("/documents/{doc_id}/entities")
async def get_document_entities(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(EntityRelationship).where(EntityRelationship.document_id == doc_id)
    )
    rels = result.scalars().all()
    return {
        "status": "ok",
        "data": [
            {
                "source_type": r.source_entity_type,
                "source_id": str(r.source_entity_id),
                "target_type": r.target_entity_type,
                "target_id": str(r.target_entity_id),
                "relationship_type": r.relationship_type,
                "details": r.details,
                "confidence": r.confidence,
            }
            for r in rels
        ],
    }


@router.get("/documents/{doc_id}/graph")
async def get_document_graph(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Return the connection map (nodes + edges) extracted from a single
    document, with names resolved. Shape mirrors the SubGraph used by the
    /graph/* endpoints so the frontend can reuse ConnectionMap."""
    result = await db.execute(
        select(EntityRelationship).where(EntityRelationship.document_id == doc_id)
    )
    rels = list(result.scalars().all())

    # Collect (type, id) pairs for all entities mentioned
    entity_keys: set[tuple[str, str]] = set()
    for r in rels:
        entity_keys.add((r.source_entity_type, str(r.source_entity_id)))
        entity_keys.add((r.target_entity_type, str(r.target_entity_id)))

    # Resolve names per entity-type batch (one query per type at most)
    by_type: dict[str, list[str]] = {}
    for etype, eid in entity_keys:
        by_type.setdefault(etype, []).append(eid)

    names: dict[tuple[str, str], str] = {}
    extras: dict[tuple[str, str], dict] = {}
    for etype, ids in by_type.items():
        model = _NAME_MODELS.get(etype)
        if not model:
            continue
        if etype == "person":
            rows = await db.execute(
                select(model.id, model.name_hebrew, model.title, model.position, model.ministry)
                .where(model.id.in_(ids))
            )
            for row in rows.fetchall():
                eid = str(row[0])
                names[(etype, eid)] = row[1] or ""
                extra = {}
                if row[2]: extra["title"] = row[2]
                if row[3]: extra["position"] = row[3]
                if row[4]: extra["ministry"] = row[4]
                if extra:
                    extras[(etype, eid)] = extra
        else:
            rows = await db.execute(
                select(model.id, model.name_hebrew).where(model.id.in_(ids))
            )
            for row in rows.fetchall():
                eid = str(row[0])
                names[(etype, eid)] = row[1] or ""

    nodes = [
        {
            "id": eid,
            "entity_type": etype,
            "name": names.get((etype, eid), ""),
            "extra": extras.get((etype, eid)),
        }
        for (etype, eid) in entity_keys
    ]

    edges = [
        {
            "source_id": str(r.source_entity_id),
            "source_type": r.source_entity_type,
            "source_name": names.get((r.source_entity_type, str(r.source_entity_id)), ""),
            "target_id": str(r.target_entity_id),
            "target_type": r.target_entity_type,
            "target_name": names.get((r.target_entity_type, str(r.target_entity_id)), ""),
            "relationship_type": r.relationship_type,
            "details": r.details,
            "confidence": r.confidence,
            "document_id": str(r.document_id),
            "document_title": None,
            "document_url": None,
        }
        for r in rels
    ]

    return {"status": "ok", "data": {"nodes": nodes, "edges": edges}}
