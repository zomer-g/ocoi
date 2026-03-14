"""CRUD operations for all entity types."""

import json
import uuid
from typing import Sequence

from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_db.models import (
    Association,
    Company,
    Document,
    Domain,
    EntityRelationship,
    ExtractionRun,
    Person,
    Source,
)


def _get_aliases(entity) -> list[str]:
    """Parse aliases JSON string into a list."""
    if not entity.aliases:
        return []
    try:
        return json.loads(entity.aliases)
    except (json.JSONDecodeError, TypeError):
        return []


def _add_alias(entity, old_name: str) -> None:
    """Add old_name to the entity's aliases list (if not already present)."""
    aliases = _get_aliases(entity)
    if old_name not in aliases:
        aliases.append(old_name)
        entity.aliases = json.dumps(aliases, ensure_ascii=False)


# --- Sources ---

async def get_or_create_source(
    session: AsyncSession,
    source_type: str,
    source_id: str,
    title: str,
    url: str,
    metadata_json: dict | None = None,
) -> Source:
    stmt = select(Source).where(
        Source.source_type == source_type,
        Source.source_id == source_id,
    ).limit(1)
    result = await session.execute(stmt)
    source = result.scalars().first()
    if source:
        source.last_fetched_at = func.now()
        return source
    source = Source(
        source_type=source_type,
        source_id=source_id,
        title=title,
        url=url,
        metadata_json=metadata_json or {},
    )
    session.add(source)
    await session.flush()
    return source


# --- Documents ---

async def create_document(
    session: AsyncSession,
    source_id: uuid.UUID,
    title: str,
    file_url: str,
    file_format: str = "pdf",
    file_size: int | None = None,
) -> Document:
    existing = await session.execute(
        select(Document).where(Document.file_url == file_url).limit(1)
    )
    if doc := existing.scalars().first():
        return doc
    doc = Document(
        source_id=source_id,
        title=title,
        file_url=file_url,
        file_format=file_format,
        file_size=file_size,
    )
    session.add(doc)
    await session.flush()
    return doc


async def get_documents_by_status(
    session: AsyncSession,
    status_field: str,
    status_value: str,
    limit: int = 100,
) -> Sequence[Document]:
    field = getattr(Document, status_field)
    result = await session.execute(
        select(Document).where(field == status_value).limit(limit)
    )
    return result.scalars().all()


async def update_document_markdown(
    session: AsyncSession,
    document_id: uuid.UUID,
    markdown_content: str,
    file_path: str,
) -> None:
    result = await session.execute(select(Document).where(Document.id == document_id))
    doc = result.scalar_one()
    doc.markdown_content = markdown_content
    doc.file_path = file_path
    doc.conversion_status = "converted"


# --- Entities ---

async def upsert_person(session: AsyncSession, name_hebrew: str, **kwargs) -> Person:
    # 1. Try exact name match
    result = await session.execute(
        select(Person).where(Person.name_hebrew == name_hebrew).limit(1)
    )
    person = result.scalars().first()
    if person:
        for key, value in kwargs.items():
            if value is not None:
                setattr(person, key, value)
        return person
    # 2. Try alias match — find entity where this name is stored as an alias
    result = await session.execute(
        select(Person).where(Person.aliases.contains(name_hebrew))
    )
    for p in result.scalars().all():
        if name_hebrew in _get_aliases(p):
            # Found via alias — update fields but keep the corrected canonical name
            for key, value in kwargs.items():
                if value is not None:
                    setattr(p, key, value)
            return p
    # 3. Create new
    person = Person(name_hebrew=name_hebrew, **kwargs)
    session.add(person)
    await session.flush()
    return person


async def upsert_company(session: AsyncSession, name_hebrew: str, **kwargs) -> Company:
    # 1. Try exact name match
    result = await session.execute(
        select(Company).where(Company.name_hebrew == name_hebrew).limit(1)
    )
    company = result.scalars().first()
    if company:
        for key, value in kwargs.items():
            if value is not None:
                setattr(company, key, value)
        return company
    # 2. Try alias match
    result = await session.execute(
        select(Company).where(Company.aliases.contains(name_hebrew))
    )
    for c in result.scalars().all():
        if name_hebrew in _get_aliases(c):
            for key, value in kwargs.items():
                if value is not None:
                    setattr(c, key, value)
            return c
    # 3. Create new
    company = Company(name_hebrew=name_hebrew, **kwargs)
    session.add(company)
    await session.flush()
    return company


async def upsert_association(session: AsyncSession, name_hebrew: str, **kwargs) -> Association:
    # 1. Try exact name match
    result = await session.execute(
        select(Association).where(Association.name_hebrew == name_hebrew).limit(1)
    )
    assoc = result.scalars().first()
    if assoc:
        for key, value in kwargs.items():
            if value is not None:
                setattr(assoc, key, value)
        return assoc
    # 2. Try alias match
    result = await session.execute(
        select(Association).where(Association.aliases.contains(name_hebrew))
    )
    for a in result.scalars().all():
        if name_hebrew in _get_aliases(a):
            for key, value in kwargs.items():
                if value is not None:
                    setattr(a, key, value)
            return a
    # 3. Create new
    assoc = Association(name_hebrew=name_hebrew, **kwargs)
    session.add(assoc)
    await session.flush()
    return assoc


async def upsert_domain(session: AsyncSession, name_hebrew: str, **kwargs) -> Domain:
    # 1. Try exact name match
    result = await session.execute(
        select(Domain).where(Domain.name_hebrew == name_hebrew).limit(1)
    )
    domain = result.scalars().first()
    if domain:
        return domain
    # 2. Try alias match
    result = await session.execute(
        select(Domain).where(Domain.aliases.contains(name_hebrew))
    )
    for d in result.scalars().all():
        if name_hebrew in _get_aliases(d):
            return d
    # 3. Create new
    domain = Domain(name_hebrew=name_hebrew, **kwargs)
    session.add(domain)
    await session.flush()
    return domain


async def create_relationship(
    session: AsyncSession,
    source_entity_type: str,
    source_entity_id: uuid.UUID,
    target_entity_type: str,
    target_entity_id: uuid.UUID,
    relationship_type: str,
    document_id: uuid.UUID,
    details: str | None = None,
    restriction_type: str | None = None,
    restriction_end_date=None,
    confidence: float = 0.5,
) -> EntityRelationship:
    # Check for existing relationship with same compound key
    existing = await session.execute(
        select(EntityRelationship).where(
            EntityRelationship.source_entity_type == source_entity_type,
            EntityRelationship.source_entity_id == str(source_entity_id),
            EntityRelationship.target_entity_type == target_entity_type,
            EntityRelationship.target_entity_id == str(target_entity_id),
            EntityRelationship.relationship_type == relationship_type,
            EntityRelationship.document_id == str(document_id),
        ).limit(1)
    )
    if rel := existing.scalars().first():
        return rel
    rel = EntityRelationship(
        source_entity_type=source_entity_type,
        source_entity_id=source_entity_id,
        target_entity_type=target_entity_type,
        target_entity_id=target_entity_id,
        relationship_type=relationship_type,
        document_id=document_id,
        details=details,
        restriction_type=restriction_type,
        restriction_end_date=restriction_end_date,
        confidence=confidence,
    )
    session.add(rel)
    await session.flush()
    return rel


async def create_extraction_run(
    session: AsyncSession,
    document_id: uuid.UUID,
    extractor_type: str,
    model_version: str | None = None,
    entities_found: int = 0,
    relationships_found: int = 0,
    raw_output_json: dict | None = None,
) -> ExtractionRun:
    run = ExtractionRun(
        document_id=document_id,
        extractor_type=extractor_type,
        model_version=model_version,
        entities_found=entities_found,
        relationships_found=relationships_found,
        raw_output_json=raw_output_json or {},
    )
    session.add(run)
    await session.flush()
    return run


# --- Query helpers ---

async def count_entities(session: AsyncSession) -> dict:
    counts = {}
    for model, key in [
        (Source, "sources"),
        (Document, "documents"),
        (Person, "persons"),
        (Company, "companies"),
        (Association, "associations"),
        (Domain, "domains"),
        (EntityRelationship, "relationships"),
    ]:
        result = await session.execute(select(func.count()).select_from(model))
        counts[key] = result.scalar()
    return counts
