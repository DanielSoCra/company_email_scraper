from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from ..auth import authenticate_user, create_access_token
from ..config import get_settings

router = APIRouter()


@router.post("/login")
async def login(request: Request, email: str = Form(...), password: str = Form(...)):
    settings = get_settings()
    user_email = authenticate_user(email, password)
    if not user_email:
        return RedirectResponse(url="/?error=invalid_credentials", status_code=303)

    expires_delta = timedelta(days=settings.auth.jwt_expiration_days)
    token = create_access_token(user_email, expires_delta)

    redirect_to = request.query_params.get("next") or "/submit"
    response = RedirectResponse(url=redirect_to, status_code=303)
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        secure=settings.app.cookie_secure,
        samesite=settings.app.cookie_samesite,
        max_age=int(expires_delta.total_seconds()),
    )
    return response


@router.get("/logout")
async def logout():
    settings = get_settings()
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie(
        key="access_token",
        secure=settings.app.cookie_secure,
        samesite=settings.app.cookie_samesite,
    )
    return response
