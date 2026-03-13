from sqlalchemy import create_engine, event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from ocoi_common.config import settings


def _is_sqlite(url: str) -> bool:
    return "sqlite" in url


# --- Async engine ---
connect_args = {}
if _is_sqlite(settings.database_url):
    connect_args = {"check_same_thread": False}

async_engine = create_async_engine(
    settings.database_url,
    echo=False,
    connect_args=connect_args,
)
async_session_factory = async_sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
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
    _log = logging.getLogger("ocoi.db.migrations")

    migrations = [
        ("documents", "pdf_content", "ALTER TABLE documents ADD COLUMN pdf_content BYTEA"),
        ("documents", "content_hash", "ALTER TABLE documents ADD COLUMN content_hash VARCHAR(64)"),
    ]

    async with async_engine.begin() as conn:
        for table, column, sql in migrations:
            exists = await conn.run_sync(
                lambda sync_conn, t=table, c=column: c in [
                    col["name"] for col in sync_conn.dialect.get_columns(sync_conn, t)
                ]
            )
            if not exists:
                _log.info(f"Adding column {table}.{column}")
                await conn.execute(__import__("sqlalchemy").text(sql))
                _log.info(f"Column {table}.{column} added successfully")
