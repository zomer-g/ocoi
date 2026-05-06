"""Public endpoints for user-submitted corrections / notes.

Anonymous users can POST a Suggestion against any document or extracted
entity / relationship. Admins review them through `/api/v1/admin/suggestions`.
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from ocoi_api.dependencies import get_db
from ocoi_db.models import Suggestion

router = APIRouter(tags=["suggestions"])


_ALLOWED_KINDS = {
    "document", "person", "company", "association", "domain", "relationship",
}


class SuggestionCreate(BaseModel):
    target_kind: str = Field(..., min_length=1, max_length=20)
    target_id: str = Field(..., min_length=1, max_length=64)
    field_name: str = Field(..., min_length=1, max_length=60)
    current_value: Optional[str] = Field(None, max_length=4000)
    proposed_value: Optional[str] = Field(None, max_length=4000)
    comment: Optional[str] = Field(None, max_length=4000)
    submitter_email: Optional[str] = Field(None, max_length=200)
    document_id: Optional[str] = Field(None, max_length=64)

    @field_validator("target_kind")
    @classmethod
    def _check_kind(cls, v: str) -> str:
        if v not in _ALLOWED_KINDS:
            raise ValueError(f"target_kind must be one of {sorted(_ALLOWED_KINDS)}")
        return v


@router.post("/suggestions")
async def submit_suggestion(
    body: SuggestionCreate,
    db: AsyncSession = Depends(get_db),
):
    """Anyone can submit a correction note. The body should describe what
    the user is commenting on (`target_kind`, `target_id`, `field_name`),
    the current value as they see it, and the proposed change."""
    # At least one of the two free-text fields must be provided so the
    # admin has something actionable to review.
    if not (body.proposed_value or body.comment):
        raise HTTPException(
            status_code=400,
            detail="Either proposed_value or comment must be provided.",
        )

    row = Suggestion(
        target_kind=body.target_kind,
        target_id=body.target_id,
        field_name=body.field_name,
        current_value=body.current_value,
        proposed_value=body.proposed_value,
        comment=body.comment,
        submitter_email=(body.submitter_email or None),
        document_id=body.document_id or None,
        status="pending",
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"status": "ok", "data": {"id": row.id}}
