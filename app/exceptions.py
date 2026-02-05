from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from .config import get_settings


class AuthenticationError(Exception):
    """Base error for authentication failures."""


class InvalidTokenError(AuthenticationError):
    """Raised when a JWT token is invalid."""


class ExpiredTokenError(AuthenticationError):
    """Raised when a JWT token is expired."""


def authentication_error_handler(request: Request, exc: AuthenticationError):
    message = str(exc) or "Authentication failed"
    return JSONResponse(status_code=401, content={"error": message})


def invalid_token_error_handler(request: Request, exc: InvalidTokenError):
    settings = get_settings()
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie(
        key="access_token",
        secure=settings.app.cookie_secure,
        samesite=settings.app.cookie_samesite,
    )
    return response
