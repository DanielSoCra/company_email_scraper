from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, Request
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError

from .config import get_settings
from .exceptions import ExpiredTokenError, InvalidTokenError


def create_access_token(email: str, expires_delta: timedelta) -> str:
    settings = get_settings()
    now = datetime.now(timezone.utc)
    expire = now + expires_delta
    payload = {
        "sub": email,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }
    return jwt.encode(payload, settings.auth.jwt_secret_key, algorithm="HS256")


def verify_token(token: str) -> str:
    settings = get_settings()
    try:
        payload = jwt.decode(
            token, settings.auth.jwt_secret_key, algorithms=["HS256"]
        )
        email = payload.get("sub")
        if not email:
            raise InvalidTokenError("Invalid token")
        return email
    except ExpiredSignatureError as exc:
        raise ExpiredTokenError("Token expired") from exc
    except JWTError as exc:
        raise InvalidTokenError("Invalid token") from exc


def authenticate_user(email: str, password: str) -> str | None:
    settings = get_settings()
    expected_password = settings.auth.users.get(email)
    if expected_password is None:
        return None
    if expected_password != password:
        return None
    return email


def get_current_user(request: Request) -> str:
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={"Location": "/"},
        )
    return verify_token(token)


def get_optional_user(request: Request) -> str | None:
    token = request.cookies.get("access_token")
    if not token:
        return None
    try:
        return verify_token(token)
    except (InvalidTokenError, ExpiredTokenError):
        return None
