import logging
import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

AI_BATCH_SIZE_MIN = 10
AI_BATCH_SIZE_MAX = 40


def _parse_users(users_str: str) -> dict[str, str]:
    """Parse USERS env var format: email1:pass1,email2:pass2 into dict."""
    users = {}
    if not users_str:
        return users
    for pair in users_str.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        email, password = pair.split(":", 1)
        users[email.strip()] = password.strip()
    return users


def _parse_bool(value: str, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _clamp_int(value: int, minimum: int, maximum: int, setting_name: str) -> int:
    if value < minimum:
        logger.warning("%s below minimum (%d); clamping to %d", setting_name, minimum, minimum)
        return minimum
    if value > maximum:
        logger.warning("%s above maximum (%d); clamping to %d", setting_name, maximum, maximum)
        return maximum
    return value


@dataclass
class AuthConfig:
    jwt_secret_key: str
    jwt_expiration_days: int = 7
    users: dict[str, str] = field(default_factory=dict)


@dataclass
class DatabaseConfig:
    url: str

    @property
    def async_url(self) -> str:
        """Convert postgresql:// to postgresql+asyncpg:// for async driver."""
        if self.url.startswith("postgresql://"):
            return self.url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return self.url


@dataclass
class APIConfig:
    manus_api_key: str = ""
    manus_api_url: str = "https://api.manus.ai/v1"
    gemini_api_key: str = ""
    gemini_model: str = "gemini-3-flash-preview"
    ai_batch_size: int = 25
    ai_max_retries: int = 3


@dataclass
class EmailConfig:
    smtp_host: str = "smtp.sendgrid.net"
    smtp_port: int = 587
    smtp_user: str = "apikey"
    smtp_password: str = ""
    from_email: str = "noreply@yourapp.com"
    enabled: bool = True


@dataclass
class AppConfig:
    base_url: str = "http://localhost:8000"
    port: int = 8000
    dev_mode: bool = False
    manus_rate_limit: int = 5
    manus_webhook_url: str = "http://localhost:8000/webhooks/manus"
    manus_polling_interval: int = 30
    job_timeout_hours: int = 12
    stream_batch_size: int = 100
    cookie_secure: bool = True
    cookie_samesite: str = "lax"


@dataclass
class Settings:
    auth: AuthConfig
    database: DatabaseConfig
    api: APIConfig
    email: EmailConfig
    app: AppConfig

    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables."""
        database_url = os.getenv("DATABASE_URL", "")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        jwt_secret = os.getenv("JWT_SECRET_KEY", "")
        if not jwt_secret:
            raise ValueError("JWT_SECRET_KEY environment variable is required")

        gemini_api_key = os.getenv("GEMINI_API_KEY", "")
        if not gemini_api_key:
            logger.warning(
                "GEMINI_API_KEY is not set; AI filtering will use fallback only."
            )

        manus_api_key = os.getenv("MANUS_API_KEY", "")
        if not manus_api_key:
            raise ValueError("MANUS_API_KEY environment variable is required")

        # Email configuration validation
        email_enabled = _parse_bool(os.getenv("EMAIL_ENABLED", ""), default=True)
        smtp_password = os.getenv("SMTP_PASSWORD", "")
        if email_enabled and not smtp_password:
            logger.warning(
                "SMTP_PASSWORD is not set; email notifications will fail. "
                "Set EMAIL_ENABLED=false to disable emails in development."
            )

        return cls(
            auth=AuthConfig(
                jwt_secret_key=jwt_secret,
                jwt_expiration_days=int(os.getenv("JWT_EXPIRATION_DAYS", "7")),
                users=_parse_users(os.getenv("USERS", "")),
            ),
            database=DatabaseConfig(url=database_url),
            api=APIConfig(
                manus_api_key=manus_api_key,
                manus_api_url=os.getenv("MANUS_API_URL", "https://api.manus.ai/v1"),
                gemini_api_key=gemini_api_key,
                gemini_model=os.getenv("GEMINI_MODEL", "gemini-3-flash-preview"),
                ai_batch_size=_clamp_int(
                    int(os.getenv("AI_BATCH_SIZE", "75")),
                    AI_BATCH_SIZE_MIN,
                    AI_BATCH_SIZE_MAX,
                    "AI_BATCH_SIZE",
                ),
                ai_max_retries=int(os.getenv("AI_MAX_RETRIES", "3")),
            ),
            email=EmailConfig(
                smtp_host=os.getenv("SMTP_HOST", "smtp.sendgrid.net"),
                smtp_port=int(os.getenv("SMTP_PORT", "587")),
                smtp_user=os.getenv("SMTP_USER", "apikey"),
                smtp_password=smtp_password,
                from_email=os.getenv("FROM_EMAIL", "noreply@yourapp.com"),
                enabled=email_enabled,
            ),
            app=AppConfig(
                base_url=os.getenv("BASE_URL", "http://localhost:8000"),
                port=int(os.getenv("PORT", "8000")),
                dev_mode=_parse_bool(os.getenv("DEV_MODE", ""), default=False),
                manus_rate_limit=int(os.getenv("MANUS_RATE_LIMIT", "5")),
                manus_webhook_url=os.getenv(
                    "MANUS_WEBHOOK_URL", "http://localhost:8000/webhooks/manus"
                ),
                manus_polling_interval=int(os.getenv("MANUS_POLLING_INTERVAL", "30")),
                job_timeout_hours=int(os.getenv("JOB_TIMEOUT_HOURS", "12")),
                stream_batch_size=int(os.getenv("STREAM_BATCH_SIZE", "100")),
                cookie_secure=_parse_bool(
                    os.getenv("COOKIE_SECURE", ""), default=True
                ),
                cookie_samesite=os.getenv("COOKIE_SAMESITE", "lax"),
            ),
        )


# Global settings instance, initialized lazily
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings
