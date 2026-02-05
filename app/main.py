import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .auth import get_current_user, get_optional_user
from .config import get_settings
from .database import check_db_health
from .exceptions import (
    AuthenticationError,
    ExpiredTokenError,
    InvalidTokenError,
    authentication_error_handler,
    invalid_token_error_handler,
)
from .routes.auth import router as auth_router
from .routes.web import router as web_router
from .services.claude_filter_service import close_claude_http_client
from .services.job_processor import monitor_job_timeouts, resume_incomplete_jobs
from .services.manus_service import close_manus_http_client
from .task_registry import wait_for_active_jobs

logger = logging.getLogger(__name__)

# Global reference to timeout monitor task for graceful shutdown
_timeout_monitor_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    global _timeout_monitor_task

    # Startup
    settings = get_settings()
    logger.info("Starting Company Email Scraper")
    logger.info("Base URL: %s", settings.app.base_url)
    logger.info("Users configured: %d", len(settings.auth.users))

    db_healthy = await check_db_health()
    if db_healthy:
        logger.info("Database connection verified")

        # Resume any incomplete jobs from previous run
        try:
            resumed_count = await resume_incomplete_jobs()
            if resumed_count > 0:
                logger.info("Resumed %d incomplete jobs", resumed_count)
        except Exception as exc:
            logger.error("Failed to resume incomplete jobs: %s", exc)

        # Start the job timeout monitor
        _timeout_monitor_task = asyncio.create_task(monitor_job_timeouts())
        logger.info("Job timeout monitor started")
    else:
        logger.warning("Database connection failed - some features may not work")

    yield

    # Shutdown
    logger.info("Shutting down Company Email Scraper")

    # Wait for active jobs to complete current phase (5 minute timeout)
    await wait_for_active_jobs(timeout=300)

    # Cancel timeout monitor task
    if _timeout_monitor_task is not None:
        _timeout_monitor_task.cancel()
        try:
            await _timeout_monitor_task
        except asyncio.CancelledError:
            pass
        logger.info("Job timeout monitor stopped")

    # Close HTTP clients
    await close_claude_http_client()
    await close_manus_http_client()
    logger.info("HTTP clients closed")


app = FastAPI(
    title="Company Email Scraper",
    description="Automated company email discovery pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

# Static assets
app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent / "static"),
    name="static",
)

# Exception handlers
app.add_exception_handler(AuthenticationError, authentication_error_handler)
app.add_exception_handler(InvalidTokenError, invalid_token_error_handler)
app.add_exception_handler(ExpiredTokenError, invalid_token_error_handler)

# Routers
app.include_router(auth_router, tags=["auth"])
app.include_router(web_router, tags=["web"])

# Templates
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


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


@app.get("/protected-example")
async def protected_example(current_user: str = Depends(get_current_user)):
    return {"user": current_user}
