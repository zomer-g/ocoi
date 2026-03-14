"""SQLAlchemy ORM models — compatible with both SQLite (local) and PostgreSQL (production)."""

import uuid
from datetime import date, datetime

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    func,
)
from sqlalchemy.types import JSON, TypeDecorator, CHAR
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# --- Cross-DB UUID type ---

class DBUUID(TypeDecorator):
    """UUID type that works on both SQLite (stored as CHAR(36)) and PostgreSQL."""

    impl = CHAR(36)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return str(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return str(value)
        return value


def new_uuid():
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False)
    source_id: Mapped[str] = mapped_column(String(500), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, default=dict)
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    documents = relationship("Document", back_populates="source")

    __table_args__ = (
        Index("ix_sources_source_type_source_id", "source_type", "source_id", unique=True),
    )


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    source_id: Mapped[str] = mapped_column(DBUUID(), ForeignKey("sources.id"), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    file_format: Mapped[str] = mapped_column(String(20), default="pdf")
    file_path: Mapped[str | None] = mapped_column(Text)
    file_url: Mapped[str] = mapped_column(Text, nullable=False)
    file_size: Mapped[int | None] = mapped_column(Integer)
    pdf_content: Mapped[bytes | None] = mapped_column(LargeBinary)
    markdown_content: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    conversion_status: Mapped[str] = mapped_column(String(20), default="pending")
    extraction_status: Mapped[str] = mapped_column(String(20), default="pending")
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())
    converted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    extracted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    source = relationship("Source", back_populates="documents")
    extraction_runs = relationship("ExtractionRun", back_populates="document")

    __table_args__ = (
        Index("ix_documents_conversion_status", "conversion_status"),
        Index("ix_documents_extraction_status", "extraction_status"),
    )


class Person(Base):
    __tablename__ = "persons"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    name_hebrew: Mapped[str] = mapped_column(Text, nullable=False)
    name_english: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(String(100))
    position: Mapped[str | None] = mapped_column(Text)
    ministry: Mapped[str | None] = mapped_column(Text)
    aliases: Mapped[str | None] = mapped_column(Text)  # JSON string for SQLite compat
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_persons_name_hebrew", "name_hebrew"),
    )


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    name_hebrew: Mapped[str] = mapped_column(Text, nullable=False)
    name_english: Mapped[str | None] = mapped_column(Text)
    registration_number: Mapped[str | None] = mapped_column(String(50))
    company_type: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str | None] = mapped_column(String(50))
    match_confidence: Mapped[float | None] = mapped_column(Float)
    registry_record_id: Mapped[str | None] = mapped_column(DBUUID(), ForeignKey("registry_records.id"), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_companies_name_hebrew", "name_hebrew"),
        Index("ix_companies_registration_number", "registration_number"),
    )


class Association(Base):
    __tablename__ = "associations"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    name_hebrew: Mapped[str] = mapped_column(Text, nullable=False)
    name_english: Mapped[str | None] = mapped_column(Text)
    registration_number: Mapped[str | None] = mapped_column(String(50))
    status: Mapped[str | None] = mapped_column(String(50))
    match_confidence: Mapped[float | None] = mapped_column(Float)
    registry_record_id: Mapped[str | None] = mapped_column(DBUUID(), ForeignKey("registry_records.id"), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_associations_name_hebrew", "name_hebrew"),
    )


class Domain(Base):
    __tablename__ = "domains"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    name_hebrew: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name_english: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())


class RegistryRecord(Base):
    """External registry records from Israeli government CKAN DATAGOV."""
    __tablename__ = "registry_records"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    source_type: Mapped[str] = mapped_column(String(30), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    name_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    registration_number: Mapped[str | None] = mapped_column(String(50))
    status: Mapped[str | None] = mapped_column(String(100))
    raw_data: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_registry_source_type", "source_type"),
        Index("ix_registry_reg_number", "registration_number"),
        Index("ix_registry_name_normalized", "name_normalized"),
    )


class RegistrySyncStatus(Base):
    """Tracks per-source sync state for external registries."""
    __tablename__ = "registry_sync_status"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    source_type: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime)
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    sync_status: Mapped[str] = mapped_column(String(20), default="never")
    error_message: Mapped[str | None] = mapped_column(Text)


class IgnoredResource(Base):
    """URLs marked as 'ignore' by the admin — skipped during search and import."""
    __tablename__ = "ignored_resources"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    file_url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    title: Mapped[str | None] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(String(20), default="ckan")
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())


class EntityRelationship(Base):
    __tablename__ = "entity_relationships"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    source_entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    source_entity_id: Mapped[str] = mapped_column(DBUUID(), nullable=False)
    target_entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    target_entity_id: Mapped[str] = mapped_column(DBUUID(), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(50), nullable=False)
    details: Mapped[str | None] = mapped_column(Text)
    restriction_type: Mapped[str | None] = mapped_column(String(20))
    restriction_end_date: Mapped[date | None] = mapped_column()
    document_id: Mapped[str] = mapped_column(DBUUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_rel_source", "source_entity_type", "source_entity_id"),
        Index("ix_rel_target", "target_entity_type", "target_entity_id"),
        Index("ix_rel_type", "relationship_type"),
    )


class ExtractionRun(Base):
    __tablename__ = "extraction_runs"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    document_id: Mapped[str] = mapped_column(DBUUID(), ForeignKey("documents.id", ondelete="CASCADE"), nullable=False)
    extractor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    model_version: Mapped[str | None] = mapped_column(String(100))
    entities_found: Mapped[int] = mapped_column(Integer, default=0)
    relationships_found: Mapped[int] = mapped_column(Integer, default=0)
    raw_output_json: Mapped[dict | None] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    document = relationship("Document", back_populates="extraction_runs")
