"""FastAPI dependency injection for database sessions."""

from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_db.engine import get_async_session

# Re-export for use in routers
get_db = get_async_session
