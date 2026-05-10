"""Document access endpoints."""

import uuid
from pathlib import Path
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import FileResponse, Response
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import undefer

from ocoi_api.dependencies import get_db
from ocoi_common.config import settings
from ocoi_db.models import (
    Document, EntityRelationship,
    Person, Company, Association, Domain, Source,
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
    source_type: str | None = Query(
        None,
        description="Filter by Source.source_type (e.g. 'odata', 'mk_expenses')",
    ),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    query = (
        select(Document, Source.source_type, Source.title.label("source_title"))
        .join(Source, Document.source_id == Source.id, isouter=True)
    )
    count_query = (
        select(func.count())
        .select_from(Document)
        .join(Source, Document.source_id == Source.id, isouter=True)
    )

    if status:
        query = query.where(Document.conversion_status == status)
        count_query = count_query.where(Document.conversion_status == status)
    if q:
        like = f"%{q.strip()}%"
        query = query.where(Document.title.like(like))
        count_query = count_query.where(Document.title.like(like))
    if source_type:
        query = query.where(Source.source_type == source_type)
        count_query = count_query.where(Source.source_type == source_type)

    total = (await db.execute(count_query)).scalar()
    result = await db.execute(
        query.offset(offset).limit(limit).order_by(Document.created_at.desc())
    )
    rows = result.all()

    # One round-trip for the relationships count of every doc on the page
    doc_ids = [str(r[0].id) for r in rows]
    rel_counts: dict[str, int] = {}
    if doc_ids:
        count_rows = await db.execute(
            select(EntityRelationship.document_id, func.count())
            .where(EntityRelationship.document_id.in_(doc_ids))
            .group_by(EntityRelationship.document_id)
        )
        rel_counts = {str(did): cnt for did, cnt in count_rows.fetchall()}

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
                "source_type": s_type,
                "source_title": s_title,
                "relationships_count": rel_counts.get(str(d.id), 0),
            }
            for (d, s_type, s_title) in rows
        ],
        "meta": {
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit if total else 0,
        },
    }


@router.get("/documents/{doc_id}")
async def get_document(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Document, Source.source_type, Source.title.label("source_title"))
        .join(Source, Document.source_id == Source.id, isouter=True)
        .where(Document.id == doc_id)
    )
    row = result.first()
    if not row:
        raise HTTPException(404, "Document not found")
    doc, s_type, s_title = row
    rel_count = (await db.execute(
        select(func.count())
        .select_from(EntityRelationship)
        .where(EntityRelationship.document_id == doc.id)
    )).scalar() or 0
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
            "source_type": s_type,
            "source_title": s_title,
            "relationships_count": rel_count,
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
                "origin_kind": r.origin_kind,
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
    hidden_keys: set[tuple[str, str]] = set()
    for etype, ids in by_type.items():
        model = _NAME_MODELS.get(etype)
        if not model:
            continue
        if etype == "person":
            rows = await db.execute(
                select(model.id, model.name_hebrew, model.title, model.position, model.ministry, model.hidden)
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
                if row[5]:
                    hidden_keys.add((etype, eid))
        else:
            rows = await db.execute(
                select(model.id, model.name_hebrew, model.hidden).where(model.id.in_(ids))
            )
            for row in rows.fetchall():
                eid = str(row[0])
                names[(etype, eid)] = row[1] or ""
                if row[2]:
                    hidden_keys.add((etype, eid))

    # Drop hidden entities and any edge that touches them, so the public
    # graph for this document never uses a generic placeholder as a node.
    nodes = [
        {
            "id": eid,
            "entity_type": etype,
            "name": names.get((etype, eid), ""),
            "extra": extras.get((etype, eid)),
        }
        for (etype, eid) in entity_keys
        if (etype, eid) not in hidden_keys
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
            "origin_kind": r.origin_kind,
            "document_id": str(r.document_id),
            "document_title": None,
            "document_url": None,
        }
        for r in rels
        if (r.source_entity_type, str(r.source_entity_id)) not in hidden_keys
        and (r.target_entity_type, str(r.target_entity_id)) not in hidden_keys
    ]

    return {"status": "ok", "data": {"nodes": nodes, "edges": edges}}


def _inline_disposition(filename: str) -> str:
    """Build a Content-Disposition value that survives non-ASCII filenames.

    HTTP headers must be Latin-1, so a Hebrew title in the `filename=`
    parameter raises a UnicodeEncodeError when Starlette serialises the
    response. RFC 5987's `filename*=UTF-8''...%XX...` form is supported
    by every modern browser and lets us keep the original Hebrew name.
    """
    import urllib.parse
    safe_ascii = filename.encode("ascii", "ignore").decode("ascii") or "document.pdf"
    safe_ascii = safe_ascii.replace('"', "").replace("\n", " ").strip() or "document.pdf"
    encoded = urllib.parse.quote(filename, safe="")
    return f'inline; filename="{safe_ascii}"; filename*=UTF-8\'\'{encoded}'


_MEDIA_TYPES: dict[str, str] = {
    "pdf": "application/pdf",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "csv": "text/csv",
}


@router.get("/documents/{doc_id}/pdf")
async def serve_public_pdf(doc_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Stream the document's binary contents (PDF or Excel) for inline
    viewing / download from the public UI.

    The route is named `/pdf` for backwards compatibility — it actually
    serves whatever bytes live in `Document.pdf_content`, with the right
    Content-Type derived from `file_format`. Tries disk first, falls back
    to the DB column.
    """
    result = await db.execute(select(Document).where(Document.id == doc_id))
    doc = result.scalars().first()
    if not doc:
        raise HTTPException(404, "Document not found")

    fmt = (doc.file_format or "pdf").lower()
    media_type = _MEDIA_TYPES.get(fmt, "application/octet-stream")
    extension = f".{fmt}" if fmt else ""
    raw_title = (doc.title or doc.id).replace('"', "").replace("\n", " ")
    filename = raw_title if raw_title.lower().endswith(extension) else f"{raw_title}{extension}"
    disposition = _inline_disposition(filename)

    # Disk first (cheap)
    pdf_path: Path | None = None
    if doc.file_path and Path(doc.file_path).is_file():
        pdf_path = Path(doc.file_path)
    else:
        candidate = settings.pdf_dir / f"{doc.id}.pdf"
        if candidate.is_file():
            pdf_path = candidate
    if pdf_path:
        return FileResponse(
            pdf_path,
            media_type=media_type,
            headers={"Content-Disposition": disposition},
        )

    # Otherwise pull bytes from DB
    result2 = await db.execute(
        select(Document).options(undefer(Document.pdf_content)).where(Document.id == doc_id)
    )
    doc = result2.scalars().first()
    if doc and doc.pdf_content:
        return Response(
            content=doc.pdf_content,
            media_type=media_type,
            headers={"Content-Disposition": disposition},
        )

    raise HTTPException(404, "File not found")
