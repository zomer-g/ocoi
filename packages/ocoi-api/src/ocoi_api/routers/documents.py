"""Document access endpoints."""

import uuid
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.models import Document, EntityRelationship

router = APIRouter(tags=["documents"])


@router.get("/documents")
async def list_documents(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, description="Filter by conversion_status"),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * limit
    query = select(Document)
    count_query = select(func.count()).select_from(Document)

    if status:
        query = query.where(Document.conversion_status == status)
        count_query = count_query.where(Document.conversion_status == status)

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
    result = await db.execute(select(Document).where(Document.id == doc_id))
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
