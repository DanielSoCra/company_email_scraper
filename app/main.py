import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import get_settings
from .database import check_db_health

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    # Startup
    settings = get_settings()
    logger.info("Starting Company Email Scraper")
    logger.info("Base URL: %s", settings.app.base_url)
    logger.info("Users configured: %d", len(settings.auth.users))

    db_healthy = await check_db_health()
    if db_healthy:
        logger.info("Database connection verified")
    else:
        logger.warning("Database connection failed - some features may not work")

    yield

    # Shutdown
    logger.info("Shutting down Company Email Scraper")


app = FastAPI(
    title="Company Email Scraper",
    description="Automated company email discovery pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

# Templates
templates = Jinja2Templates(directory="app/templates")


@app.get("/health")
async def health_check():
    """Health check endpoint that verifies database connectivity."""
    db_healthy = await check_db_health()

    status_code = 200 if db_healthy else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if db_healthy else "unhealthy",
            "database": "connected" if db_healthy else "disconnected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    )
