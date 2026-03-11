"""JWT creation/validation and admin dependency."""

from datetime import datetime, timedelta, timezone

import jwt
from fastapi import HTTPException, Request

from ocoi_common.config import settings


def create_access_token(email: str, name: str) -> str:
    payload = {
        "sub": email,
        "name": name,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_expire_minutes),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")


async def get_current_admin(request: Request) -> dict:
    """FastAPI dependency: extract JWT from httpOnly cookie, check admin whitelist."""
    token = request.cookies.get("ocoi_auth")
    if not token:
        raise HTTPException(401, "Not authenticated")
    payload = decode_token(token)
    email = payload.get("sub", "").lower()
    if email not in settings.admin_email_set:
        raise HTTPException(403, "Not authorized")
    return payload
