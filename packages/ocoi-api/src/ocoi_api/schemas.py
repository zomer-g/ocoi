"""Pydantic request/response schemas for admin CRUD."""

from pydantic import BaseModel


class PersonCreate(BaseModel):
    name_hebrew: str
    name_english: str | None = None
    title: str | None = None
    position: str | None = None
    ministry: str | None = None


class PersonUpdate(BaseModel):
    name_hebrew: str | None = None
    name_english: str | None = None
    title: str | None = None
    position: str | None = None
    ministry: str | None = None
    aliases: list[str] | None = None


class CompanyCreate(BaseModel):
    name_hebrew: str
    name_english: str | None = None
    registration_number: str | None = None
    company_type: str | None = None
    status: str | None = None


class CompanyUpdate(BaseModel):
    name_hebrew: str | None = None
    name_english: str | None = None
    registration_number: str | None = None
    company_type: str | None = None
    status: str | None = None
    aliases: list[str] | None = None


class AssociationCreate(BaseModel):
    name_hebrew: str
    name_english: str | None = None
    registration_number: str | None = None


class AssociationUpdate(BaseModel):
    name_hebrew: str | None = None
    name_english: str | None = None
    registration_number: str | None = None
    aliases: list[str] | None = None


class DomainCreate(BaseModel):
    name_hebrew: str
    name_english: str | None = None
    description: str | None = None


class DomainUpdate(BaseModel):
    name_hebrew: str | None = None
    name_english: str | None = None
    description: str | None = None
    aliases: list[str] | None = None


class RelationshipCreate(BaseModel):
    source_entity_type: str
    source_entity_id: str
    target_entity_type: str
    target_entity_id: str
    relationship_type: str
    details: str | None = None
    restriction_type: str | None = None
    document_id: str
    confidence: float = 0.5
