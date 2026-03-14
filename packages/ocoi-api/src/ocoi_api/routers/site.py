"""Public site content endpoints — no auth required."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.models import SiteContent

router = APIRouter(prefix="/site", tags=["site"])

ALLOWED_KEYS = {"header_links", "footer_text", "about_content"}


@router.get("/content/{key}")
async def get_public_content(key: str, db: AsyncSession = Depends(get_db)):
    if key not in ALLOWED_KEYS:
        return {"status": "ok", "data": {"key": key, "value": ""}}
    row = await db.get(SiteContent, key)
    return {"status": "ok", "data": {"key": key, "value": row.value if row else ""}}
