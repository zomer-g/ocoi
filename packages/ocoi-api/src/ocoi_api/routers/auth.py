"""Google OAuth login, callback, me, and logout endpoints."""

from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse

from ocoi_api.auth import create_access_token, get_current_admin
from ocoi_common.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


@router.get("/login")
async def login():
    """Redirect to Google OAuth consent screen."""
    params = urlencode({
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "online",
        "prompt": "select_account",
    })
    return RedirectResponse(f"{GOOGLE_AUTH_URL}?{params}")


@router.get("/callback")
async def callback(code: str = ""):
    """Exchange Google auth code for user info, set JWT cookie."""
    if not code:
        return RedirectResponse("/admin/login?error=no_code")

    # Exchange code for access token
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(GOOGLE_TOKEN_URL, data={
            "client_id": settings.google_client_id,
            "client_secret": settings.google_client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": settings.google_redirect_uri,
        })

        if token_resp.status_code != 200:
            return RedirectResponse("/admin/login?error=token_exchange_failed")

        access_token = token_resp.json().get("access_token")
        if not access_token:
            return RedirectResponse("/admin/login?error=no_access_token")

        # Get user info
        userinfo_resp = await client.get(
            GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
        )

        if userinfo_resp.status_code != 200:
            return RedirectResponse("/admin/login?error=userinfo_failed")

        userinfo = userinfo_resp.json()

    email = userinfo.get("email", "").lower()
    name = userinfo.get("name", email)

    # Check admin whitelist
    if email not in settings.admin_email_set:
        return RedirectResponse("/admin/login?error=unauthorized")

    # Create JWT and set cookie
    token = create_access_token(email, name)
    response = RedirectResponse("/admin")
    response.set_cookie(
        key="ocoi_auth",
        value=token,
        httponly=True,
        samesite="lax",
        secure=settings.is_production,
        max_age=settings.jwt_expire_minutes * 60,
        path="/",
    )
    return response


@router.get("/me")
async def me(admin: dict = Depends(get_current_admin)):
    """Return current admin user info."""
    return {"email": admin.get("sub"), "name": admin.get("name")}


@router.post("/logout")
async def logout():
    """Clear the auth cookie."""
    response = RedirectResponse("/admin/login", status_code=303)
    response.delete_cookie("ocoi_auth", path="/")
    return response
