from __future__ import annotations

from datetime import timedelta

from jose import jwt

from .auth import create_access_token
from .config import get_settings


def create_test_token(email: str) -> str:
    settings = get_settings()
    expires_delta = timedelta(days=settings.auth.jwt_expiration_days)
    return create_access_token(email, expires_delta)


def decode_token_unsafe(token: str) -> dict:
    return jwt.get_unverified_claims(token)
