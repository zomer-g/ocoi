from sqlalchemy import create_engine, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from ocoi_common.config import settings


def _is_sqlite(url: str) -> bool:
    return "sqlite" in url


def _is_postgres(url: str) -> bool:
    return "postgresql" in url or "postgres" in url


# --- Async engine ---
connect_args = {}
if _is_sqlite(settings.database_url):
    connect_args = {"check_same_thread": False}
elif _is_postgres(settings.database_url):
    # Set session timezone to Israel so func.now() returns Israel time
    connect_args = {"server_settings": {"timezone": "Asia/Jerusalem"}}

async_engine = create_async_engine(
    settings.database_url,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True,  # detect and replace broken connections
    pool_recycle=1800,  # recycle every 30min to avoid stale
)
async_session_factory = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)
# Background task session factory — expires objects on commit to free memory
bg_session_factory = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=True
)

# --- Sync engine ---
sync_connect_args = {}
if _is_sqlite(settings.database_url_sync):
    sync_connect_args = {"check_same_thread": False}

sync_engine = create_engine(
    settings.database_url_sync,
    echo=False,
    connect_args=sync_connect_args,
)
sync_session_factory = sessionmaker(sync_engine, class_=Session, expire_on_commit=False)


# Enable WAL mode and foreign keys for SQLite
if _is_sqlite(settings.database_url_sync):
    @event.listens_for(sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


async def get_async_session() -> AsyncSession:
    async with async_session_factory() as session:
        yield session


def get_sync_session() -> Session:
    with sync_session_factory() as session:
        yield session


async def create_all_tables():
    """Create all tables (for local dev with SQLite). In production use Alembic."""
    from ocoi_db.models import Base
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def run_migrations():
    """Run lightweight migrations for new columns that create_all won't add."""
    import logging
    from sqlalchemy import text as sa_text
    _log = logging.getLogger("ocoi.db.migrations")

    # --- Column migrations ---
    column_migrations = [
        ("documents", "pdf_content", "ALTER TABLE documents ADD COLUMN pdf_content BYTEA"),
        ("documents", "content_hash", "ALTER TABLE documents ADD COLUMN content_hash VARCHAR(64)"),
        ("documents", "converted_at", "ALTER TABLE documents ADD COLUMN converted_at TIMESTAMP"),
        ("documents", "extracted_at", "ALTER TABLE documents ADD COLUMN extracted_at TIMESTAMP"),
        ("companies", "registry_record_id", "ALTER TABLE companies ADD COLUMN registry_record_id CHAR(36)"),
        ("associations", "status", "ALTER TABLE associations ADD COLUMN status VARCHAR(50)"),
        ("associations", "match_confidence", "ALTER TABLE associations ADD COLUMN match_confidence FLOAT"),
        ("associations", "registry_record_id", "ALTER TABLE associations ADD COLUMN registry_record_id CHAR(36)"),
        # Aliases columns — store old/wrong names so LLM re-extraction still maps to corrected entities
        ("companies", "aliases", "ALTER TABLE companies ADD COLUMN aliases TEXT"),
        ("associations", "aliases", "ALTER TABLE associations ADD COLUMN aliases TEXT"),
        ("domains", "aliases", "ALTER TABLE domains ADD COLUMN aliases TEXT"),
    ]

    for table, column, sql in column_migrations:
        try:
            async with async_engine.begin() as conn:
                exists = await conn.run_sync(
                    lambda sync_conn, t=table, c=column: c in [
                        col["name"] for col in sync_conn.dialect.get_columns(sync_conn, t)
                    ]
                )
                if not exists:
                    _log.info(f"Adding column {table}.{column}")
                    await conn.execute(sa_text(sql))
                    _log.info(f"Column {table}.{column} added successfully")
        except Exception as e:
            _log.warning(f"Migration for {table}.{column} failed: {e}")

    # --- Alter TIMESTAMP → TIMESTAMPTZ for columns that receive tz-aware datetimes ---
    tz_alterations = [
        "ALTER TABLE documents ALTER COLUMN converted_at TYPE TIMESTAMPTZ USING converted_at AT TIME ZONE 'UTC'",
        "ALTER TABLE documents ALTER COLUMN extracted_at TYPE TIMESTAMPTZ USING extracted_at AT TIME ZONE 'UTC'",
        "ALTER TABLE registry_sync_status ALTER COLUMN last_synced_at TYPE TIMESTAMPTZ USING last_synced_at AT TIME ZONE 'UTC'",
    ]
    for sql in tz_alterations:
        try:
            async with async_engine.begin() as conn:
                await conn.execute(sa_text(sql))
            _log.info(f"Ran: {sql[:60]}...")
        except Exception as e:
            # Already TIMESTAMPTZ, column doesn't exist, or SQLite — all fine
            _log.warning(f"TZ migration skipped ({sql[:50]}...): {e}")

    # --- Dedup + unique indexes (run once, idempotent) ---
    await _run_dedup_and_indexes(_log)


async def _run_dedup_and_indexes(logger):
    """Deduplicate existing data and create unique indexes."""
    from sqlalchemy import text as sa_text

    async with async_engine.begin() as conn:
        # Check if unique index already exists — if so, skip everything
        idx_exists = await conn.run_sync(
            lambda sync_conn: "ix_documents_file_url_uniq" in [
                idx["name"] for idx in sync_conn.dialect.get_indexes(sync_conn, "documents")
            ]
        )
        if idx_exists:
            return  # Already migrated

        logger.info("Running dedup migration...")

        # 1. Dedup documents by file_url — keep the one with most content
        dups = await conn.execute(sa_text(
            "SELECT file_url, COUNT(*) as cnt FROM documents "
            "GROUP BY file_url HAVING COUNT(*) > 1"
        ))
        for row in dups.fetchall():
            file_url = row[0]
            # Keep the doc with most content (prefer markdown > pdf > newest)
            docs = await conn.execute(sa_text(
                "SELECT id, "
                "CASE WHEN markdown_content IS NOT NULL AND LENGTH(markdown_content) > 0 THEN 2 "
                "     WHEN pdf_content IS NOT NULL THEN 1 ELSE 0 END as score "
                "FROM documents WHERE file_url = :url "
                "ORDER BY score DESC, created_at DESC"
            ), {"url": file_url})
            doc_rows = docs.fetchall()
            keep_id = doc_rows[0][0]
            delete_ids = [r[0] for r in doc_rows[1:]]
            for did in delete_ids:
                await conn.execute(sa_text("DELETE FROM extraction_runs WHERE document_id = :did"), {"did": did})
                await conn.execute(sa_text("DELETE FROM entity_relationships WHERE document_id = :did"), {"did": did})
                await conn.execute(sa_text("DELETE FROM documents WHERE id = :did"), {"did": did})
            logger.info(f"Deduped file_url: kept {keep_id}, removed {len(delete_ids)} duplicates")

        # 2. Dedup documents by content_hash (non-NULL only)
        hash_dups = await conn.execute(sa_text(
            "SELECT content_hash, COUNT(*) as cnt FROM documents "
            "WHERE content_hash IS NOT NULL "
            "GROUP BY content_hash HAVING COUNT(*) > 1"
        ))
        for row in hash_dups.fetchall():
            chash = row[0]
            docs = await conn.execute(sa_text(
                "SELECT id, "
                "CASE WHEN markdown_content IS NOT NULL AND LENGTH(markdown_content) > 0 THEN 2 "
                "     WHEN pdf_content IS NOT NULL THEN 1 ELSE 0 END as score "
                "FROM documents WHERE content_hash = :h "
                "ORDER BY score DESC, created_at DESC"
            ), {"h": chash})
            doc_rows = docs.fetchall()
            keep_id = doc_rows[0][0]
            delete_ids = [r[0] for r in doc_rows[1:]]
            for did in delete_ids:
                await conn.execute(sa_text("DELETE FROM extraction_runs WHERE document_id = :did"), {"did": did})
                await conn.execute(sa_text("DELETE FROM entity_relationships WHERE document_id = :did"), {"did": did})
                await conn.execute(sa_text("DELETE FROM documents WHERE id = :did"), {"did": did})
            if delete_ids:
                logger.info(f"Deduped content_hash: kept {keep_id}, removed {len(delete_ids)} duplicates")

        # 3. Dedup entity_relationships by compound key
        await conn.execute(sa_text(
            "DELETE FROM entity_relationships WHERE id NOT IN ("
            "  SELECT MIN(id) FROM entity_relationships "
            "  GROUP BY source_entity_type, source_entity_id, "
            "           target_entity_type, target_entity_id, "
            "           relationship_type, document_id"
            ")"
        ))

        # 4. Create unique indexes
        await conn.execute(sa_text(
            "CREATE UNIQUE INDEX ix_documents_file_url_uniq ON documents(file_url)"
        ))
        logger.info("Created unique index on documents.file_url")

        try:
            await conn.execute(sa_text(
                "CREATE UNIQUE INDEX ix_documents_content_hash_uniq "
                "ON documents(content_hash) WHERE content_hash IS NOT NULL"
            ))
            logger.info("Created unique partial index on documents.content_hash")
        except Exception:
            # SQLite doesn't support partial indexes well — skip
            logger.warning("Partial unique index on content_hash skipped (SQLite?)")

        try:
            await conn.execute(sa_text(
                "CREATE UNIQUE INDEX ix_rel_compound ON entity_relationships("
                "source_entity_type, source_entity_id, "
                "target_entity_type, target_entity_id, "
                "relationship_type, document_id)"
            ))
            logger.info("Created unique compound index on entity_relationships")
        except Exception:
            logger.warning("Compound unique index on entity_relationships skipped")

        logger.info("Dedup migration complete")
