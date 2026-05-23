"""SQLAlchemy ORM models — compatible with both SQLite (local) and PostgreSQL (production)."""

import uuid
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
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
from sqlalchemy.orm import DeclarativeBase, Mapped, deferred, mapped_column, relationship


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
    pdf_content: Mapped[bytes | None] = deferred(mapped_column(LargeBinary))
    markdown_content: Mapped[str | None] = deferred(mapped_column(Text))
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    conversion_status: Mapped[str] = mapped_column(String(20), default="pending")
    extraction_status: Mapped[str] = mapped_column(String(20), default="pending")
    # Human verification — a content manager has reviewed the LLM-extracted
    # relationships for this document and confirmed they're correct. The
    # cascade to EntityRelationship.verified is done at the API layer so
    # both pieces stay consistent.
    verified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    verified_by: Mapped[str | None] = mapped_column(
        DBUUID(), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())
    converted_at: Mapped[datetime | None] = mapped_column(DateTime())
    extracted_at: Mapped[datetime | None] = mapped_column(DateTime())

    source = relationship("Source", back_populates="documents")
    extraction_runs = relationship("ExtractionRun", back_populates="document")

    __table_args__ = (
        Index("ix_documents_conversion_status", "conversion_status"),
        Index("ix_documents_extraction_status", "extraction_status"),
        Index("ix_documents_verified", "verified"),
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
    # Generic / placeholder names (e.g. "עניינים אישיים") flip this to True
    # so the public API hides them and any relationship that touches them.
    hidden: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
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
    aliases: Mapped[str | None] = mapped_column(Text)  # JSON string for SQLite compat
    hidden: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
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
    aliases: Mapped[str | None] = mapped_column(Text)  # JSON string for SQLite compat
    hidden: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
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
    aliases: Mapped[str | None] = mapped_column(Text)  # JSON string for SQLite compat
    hidden: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
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
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime())
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
    # Where the row originated: "coi_declaration" (default — current LLM
    # extractions from conflict-of-interest documents), "mk_expense"
    # (rows imported from Knesset constituent-outreach expense Excels), …
    # Kept as a free-form short string so adding new sources doesn't need
    # an enum migration.
    origin_kind: Mapped[str] = mapped_column(
        String(40), nullable=False, default="coi_declaration",
        server_default="coi_declaration",
    )
    # Mirrors Document.verified — a human content-manager has signed off
    # on this specific edge. Cascaded from Document.verified by the
    # admin verify endpoint so the two flags stay aligned.
    verified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_rel_source", "source_entity_type", "source_entity_id"),
        Index("ix_rel_target", "target_entity_type", "target_entity_id"),
        Index("ix_rel_type", "relationship_type"),
        Index("ix_rel_origin_kind", "origin_kind"),
        Index("ix_rel_verified", "verified"),
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


class SiteContent(Base):
    """Key-value store for editable site content (header, footer, about page)."""
    __tablename__ = "site_content"

    key: Mapped[str] = mapped_column(String(50), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class EntityMatchProposal(Base):
    """A candidate match between two records — either two entities of the
    same type that look like duplicates, or an entity ↔ external-registry
    pairing — produced by the background scan jobs and queued for human
    review in the admin panel.

    The same table backs both Phase-1 duplicate-detection and the future
    registry-match-as-suggestion flow; `proposal_kind` discriminates.
    """
    __tablename__ = "entity_match_proposals"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)

    # 'duplicate'        — same-type entity / entity pair to consider merging
    # 'registry_match'   — entity that may map to a RegistryRecord row
    proposal_kind: Mapped[str] = mapped_column(String(30), nullable=False)

    # The "left" side — always one of our own entities (person/company/…).
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    entity_id: Mapped[str] = mapped_column(DBUUID(), nullable=False)

    # The "right" side. For 'duplicate' proposals it's another entity of
    # the same type (target_kind='entity'); for 'registry_match' it's a
    # RegistryRecord id (target_kind='registry').
    target_kind: Mapped[str] = mapped_column(String(20), nullable=False, default="entity")
    target_type: Mapped[str] = mapped_column(String(40), nullable=False)
    target_id: Mapped[str] = mapped_column(DBUUID(), nullable=False)

    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    # JSON-encoded list of strings describing which signals fired, e.g.
    # ["token_sort=0.97", "first_name_match", "alias_overlap"].
    reasons: Mapped[str | None] = mapped_column(Text)

    # 'pending' | 'approved' | 'rejected' | 'dismissed'
    # rejected = "not the same" (won't be suggested again)
    # dismissed = "skip for now, maybe revisit"
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", server_default="pending",
    )
    reviewed_by: Mapped[str | None] = mapped_column(
        DBUUID(), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_emp_status_kind", "status", "proposal_kind"),
        Index("ix_emp_entity", "entity_type", "entity_id"),
        Index("ix_emp_target", "target_type", "target_id"),
        # An entity-pair can have at most one open proposal at any time.
        # Different proposal kinds can coexist on the same entity though.
        Index(
            "ix_emp_pair_unique",
            "proposal_kind", "entity_type", "entity_id", "target_type", "target_id",
            unique=True,
        ),
    )


class User(Base):
    """Admin-panel user. Roles:

    * ``admin``           — implicit access to everything, can manage other users.
    * ``content_manager`` — has the per-user permission set in ``permissions``.
    * ``mcp_user``        — signed in via Google for MCP only. Has no admin
                            panel access; the role exists so admin queries
                            can filter MCP-only rows out and so the same
                            ``User`` table powers both surfaces.

    Emails listed in the ``ADMIN_EMAILS`` env var are auto-bootstrapped to a
    ``role='admin'`` row on first login (see ``ocoi_api.auth``).
    """
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    email: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    name: Mapped[str | None] = mapped_column(String(200))
    role: Mapped[str] = mapped_column(
        String(40), nullable=False,
        default="content_manager", server_default="content_manager",
    )
    # JSON-encoded list of permission keys. Admins ignore this field
    # (they have all permissions implicitly).
    permissions: Mapped[str | None] = mapped_column(Text)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_users_email", "email"),
    )


class Suggestion(Base):
    """User-submitted correction or note for a specific field of a document
    or extracted entity / relationship. Reviewed by admins.
    """
    __tablename__ = "suggestions"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)

    # What is being commented on
    target_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    # one of: 'document', 'person', 'company', 'association', 'domain', 'relationship'
    target_id: Mapped[str] = mapped_column(DBUUID(), nullable=False)
    field_name: Mapped[str] = mapped_column(String(60), nullable=False)
    # e.g. 'title', 'name_hebrew', 'position', 'ministry', 'details', 'relationship_type'

    # Optional document context (where the user was when submitting)
    document_id: Mapped[str | None] = mapped_column(
        DBUUID(), ForeignKey("documents.id", ondelete="SET NULL"), nullable=True,
    )

    # The note itself
    current_value: Mapped[str | None] = mapped_column(Text)
    proposed_value: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str | None] = mapped_column(Text)
    submitter_email: Mapped[str | None] = mapped_column(String(200))

    # Workflow
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    # one of: 'pending', 'approved', 'rejected'
    admin_notes: Mapped[str | None] = mapped_column(Text)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_suggestions_status", "status"),
        Index("ix_suggestions_target", "target_kind", "target_id"),
        Index("ix_suggestions_document", "document_id"),
        Index("ix_suggestions_created_at", "created_at"),
    )


# ─────────────────────────────────────────────────────────────────────────
# MCP server tables — OAuth 2.1 dynamic registration + per-user metering.
# Picked up by ``create_all_tables()`` on startup (this project doesn't
# use Alembic for new tables — only for column-level adds).
# ─────────────────────────────────────────────────────────────────────────


class OAuthClient(Base):
    """OAuth client registered via RFC 7591 dynamic client registration.

    An MCP host (Claude Desktop, Cursor, …) hits POST /oauth/register on
    first launch and gets back ``client_id``/``client_secret``. We keep
    the secret hashed; only the registration response shows it cleartext.
    """
    __tablename__ = "oauth_clients"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    client_secret_hash: Mapped[str | None] = mapped_column(String(128))
    # Some MCP clients register without a secret (public clients with PKCE).
    is_public: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false",
    )
    redirect_uris: Mapped[str] = mapped_column(Text, nullable=False)  # JSON array
    client_name: Mapped[str | None] = mapped_column(String(200))
    grant_types: Mapped[str | None] = mapped_column(Text)  # JSON array
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_oauth_clients_client_id", "client_id"),
    )


class OAuthAuthorizationCode(Base):
    """Single-use authorization code minted by /oauth/authorize and
    redeemed at /oauth/token. We store the hash so a stolen DB row can't
    immediately be redeemed."""
    __tablename__ = "oauth_authorization_codes"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    code_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[str] = mapped_column(
        DBUUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    redirect_uri: Mapped[str] = mapped_column(Text, nullable=False)
    code_challenge: Mapped[str | None] = mapped_column(String(200))
    code_challenge_method: Mapped[str | None] = mapped_column(String(20))
    scope: Mapped[str | None] = mapped_column(String(200))
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_oauth_codes_client_id", "client_id"),
        Index("ix_oauth_codes_user_id", "user_id"),
    )


class OAuthRefreshToken(Base):
    """Long-lived refresh token. Access tokens are JWTs and not stored;
    refresh tokens are opaque hashes so revocation is just a DB write."""
    __tablename__ = "oauth_refresh_tokens"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    client_id: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[str] = mapped_column(
        DBUUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    scope: Mapped[str | None] = mapped_column(String(200))
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())

    __table_args__ = (
        Index("ix_oauth_refresh_user_id", "user_id"),
        Index("ix_oauth_refresh_client_id", "client_id"),
    )


class UsageEvent(Base):
    """One row per MCP tool invocation. Drives admin reporting + the
    Stripe metered-billing usage records."""
    __tablename__ = "usage_events"

    id: Mapped[str] = mapped_column(DBUUID(), primary_key=True, default=new_uuid)
    user_id: Mapped[str] = mapped_column(
        DBUUID(), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    client_id: Mapped[str | None] = mapped_column(String(64))
    tool: Mapped[str] = mapped_column(String(80), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=func.now())
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    bytes_in: Mapped[int | None] = mapped_column(Integer)
    bytes_out: Mapped[int | None] = mapped_column(Integer)
    # ok | error | quota_exceeded
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ok")
    error_message: Mapped[str | None] = mapped_column(Text)
    # NULL = not yet pushed to Stripe. Set to a timestamp once a usage
    # record has been created on the customer's subscription item.
    stripe_pushed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_usage_user_started", "user_id", "started_at"),
        Index("ix_usage_pending_stripe", "stripe_pushed_at"),
        Index("ix_usage_tool", "tool"),
    )


class BillingAccount(Base):
    """One row per MCP user. Plan + quota live here; Stripe IDs link the
    row to the customer's subscription so the metered-billing batcher
    knows where to push usage."""
    __tablename__ = "billing_accounts"

    user_id: Mapped[str] = mapped_column(
        DBUUID(), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True,
    )
    stripe_customer_id: Mapped[str | None] = mapped_column(String(80))
    stripe_subscription_item_id: Mapped[str | None] = mapped_column(String(80))
    # 'free'    — usage logged, never pushed to Stripe.
    # 'metered' — usage pushed; Stripe invoices at the end of the cycle.
    plan: Mapped[str] = mapped_column(
        String(20), nullable=False, default="free", server_default="free",
    )
    # NULL = unlimited. 0 = effectively blocked. Anything else is the
    # hard cap per calendar month; quota_exceeded events are still logged
    # so we can see who's hitting the wall.
    monthly_quota: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, default=func.now())
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, default=func.now(), onupdate=func.now(),
    )
