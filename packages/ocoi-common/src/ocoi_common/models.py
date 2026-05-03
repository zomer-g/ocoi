"""Shared Pydantic schemas used across all packages."""

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel


class SourceType(str, Enum):
    CKAN = "ckan"
    ODATA = "odata"


class ConversionStatus(str, Enum):
    PENDING = "pending"
    CONVERTED = "converted"
    FAILED = "failed"


class ExtractionStatus(str, Enum):
    PENDING = "pending"
    EXTRACTED = "extracted"
    FAILED = "failed"


class EntityType(str, Enum):
    PERSON = "person"
    COMPANY = "company"
    ASSOCIATION = "association"
    DOMAIN = "domain"


class RelationshipType(str, Enum):
    RESTRICTED_FROM = "restricted_from"
    OWNS = "owns"
    MANAGES = "manages"
    EMPLOYED_BY = "employed_by"
    RELATED_TO = "related_to"
    BOARD_MEMBER = "board_member"
    OPERATES_IN = "operates_in"
    FAMILY_MEMBER = "family_member"


class RestrictionType(str, Enum):
    FULL = "full"
    PARTIAL = "partial"
    COOLING_OFF = "cooling_off"


# --- Import schemas ---


class ImportedDocument(BaseModel):
    source_type: SourceType
    source_id: str
    title: str
    file_url: str
    file_format: str = "pdf"
    file_size: int | None = None
    metadata: dict = {}


class CkanDataset(BaseModel):
    id: str
    title: str
    notes: str | None = None
    metadata_created: str | None = None
    metadata_modified: str | None = None
    resources: list[dict] = []
    tags: list[dict] = []


class OdataRecord(BaseModel):
    """One conflict-of-interest declaration extracted from an odata.org.il ZIP."""
    name: str                           # Hebrew person name (from PDF filename / CSV)
    position: str | None = None         # E.g. "שר", "סגן שר", "מנכ\"ל"
    ministry: str | None = None         # E.g. "משרד האוצר"
    date: str | None = None             # ISO date from CSV match (may be None)
    url_name: str | None = None         # Gov.il slug from CSV (may be None)
    pdf_filename: str = ""              # Original filename inside the ZIP
    pdf_bytes: bytes = b""              # Raw PDF content extracted from the ZIP
    source_id: str = ""                 # Stable id for the source row
    raw_data: dict = {}


# --- Extraction schemas ---


class ExtractedPerson(BaseModel):
    name_hebrew: str
    name_english: str | None = None
    title: str | None = None
    position: str | None = None
    ministry: str | None = None


class ExtractedCompany(BaseModel):
    name_hebrew: str
    name_english: str | None = None
    company_type: str | None = None


class ExtractedAssociation(BaseModel):
    name_hebrew: str
    registration_number: str | None = None


class ExtractedDomain(BaseModel):
    name_hebrew: str


class ExtractedRelationship(BaseModel):
    source_type: EntityType
    source_name: str
    target_type: EntityType
    target_name: str
    relationship_type: RelationshipType
    details: str | None = None
    restriction_type: RestrictionType | None = None
    restriction_end_date: date | None = None
    confidence: float = 0.5


class ExtractionResult(BaseModel):
    persons: list[ExtractedPerson] = []
    companies: list[ExtractedCompany] = []
    associations: list[ExtractedAssociation] = []
    domains: list[ExtractedDomain] = []
    relationships: list[ExtractedRelationship] = []


# --- API response schemas ---


class EntitySummary(BaseModel):
    id: str
    entity_type: EntityType
    name: str
    extra: dict = {}


class ConnectionEdge(BaseModel):
    source_id: str
    source_type: EntityType
    source_name: str
    target_id: str
    target_type: EntityType
    target_name: str
    relationship_type: RelationshipType
    details: str | None = None
    document_id: str | None = None
    document_title: str | None = None
    document_url: str | None = None


class SubGraph(BaseModel):
    nodes: list[EntitySummary] = []
    edges: list[ConnectionEdge] = []


class PaginatedResponse(BaseModel):
    data: list
    total: int
    page: int
    limit: int
    pages: int
