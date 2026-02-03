import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


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
    anthropic_api_key: str = ""


@dataclass
class EmailConfig:
    smtp_host: str = "smtp.sendgrid.net"
    smtp_port: int = 587
    smtp_user: str = "apikey"
    smtp_password: str = ""
    from_email: str = "noreply@yourapp.com"


@dataclass
class AppConfig:
    base_url: str = "http://localhost:8000"
    manus_rate_limit: int = 5
    manus_webhook_url: str = "http://localhost:8000/webhooks/manus"
    job_timeout_hours: int = 12
    stream_batch_size: int = 100


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

        return cls(
            auth=AuthConfig(
                jwt_secret_key=jwt_secret,
                jwt_expiration_days=int(os.getenv("JWT_EXPIRATION_DAYS", "7")),
                users=_parse_users(os.getenv("USERS", "")),
            ),
            database=DatabaseConfig(url=database_url),
            api=APIConfig(
                manus_api_key=os.getenv("MANUS_API_KEY", ""),
                anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            ),
            email=EmailConfig(
                smtp_host=os.getenv("SMTP_HOST", "smtp.sendgrid.net"),
                smtp_port=int(os.getenv("SMTP_PORT", "587")),
                smtp_user=os.getenv("SMTP_USER", "apikey"),
                smtp_password=os.getenv("SMTP_PASSWORD", ""),
                from_email=os.getenv("FROM_EMAIL", "noreply@yourapp.com"),
            ),
            app=AppConfig(
                base_url=os.getenv("BASE_URL", "http://localhost:8000"),
                manus_rate_limit=int(os.getenv("MANUS_RATE_LIMIT", "5")),
                manus_webhook_url=os.getenv(
                    "MANUS_WEBHOOK_URL", "http://localhost:8000/webhooks/manus"
                ),
                job_timeout_hours=int(os.getenv("JOB_TIMEOUT_HOURS", "12")),
                stream_batch_size=int(os.getenv("STREAM_BATCH_SIZE", "100")),
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
