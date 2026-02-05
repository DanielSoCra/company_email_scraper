from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlmodel import select

from ..auth import get_current_user, get_optional_user
from ..database import get_session
from ..task_registry import register_job_task
from ..models import Job
from ..services.csv_service import generate_csv
from ..services.job_processor import process_job
from ..services.manus_service import handle_task_completion
from ..services.scraper_service import StadtbranchenbuchScraper

templates = Jinja2Templates(directory=Path(__file__).parent.parent / "templates")
router = APIRouter()
logger = logging.getLogger(__name__)


class ManusWebhookPayload(BaseModel):
    task_id: str
    status: str | None = None


def _is_valid_task_id(task_id: str) -> bool:
    return bool(task_id) and task_id.isalnum() and len(task_id) <= 50


async def _validate_manus_signature(request: Request) -> bool:
    signature = request.headers.get("X-Manus-Signature")
    if not signature:
        return True

    secret = os.getenv("MANUS_WEBHOOK_SECRET", "")
    if not secret:
        logger.warning("Manus webhook signature provided but MANUS_WEBHOOK_SECRET is not set.")
        return True

    body = await request.body()
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, digest)


def _flash_from_query(request: Request) -> dict[str, str | None]:
    params = request.query_params
    flash_type = None
    flash_message = None
    flash_job_id = params.get("job_id")

    error_code = params.get("error")
    success_code = params.get("success")
    info_code = params.get("info")

    if error_code == "invalid_credentials":
        flash_type = "error"
        flash_message = "Invalid email or password. Please try again."
    elif error_code == "missing_fields":
        flash_type = "error"
        flash_message = "Please provide both city and ZIP code."
    elif error_code == "city_not_found":
        flash_type = "error"
        flash_message = "City not found in Stadtbranchenbuch. Please check the spelling."
    elif error_code == "duplicate":
        flash_type = "info"
        flash_message = "A job with the same location is already in progress."
    elif success_code == "job_created":
        flash_type = "success"
        flash_message = "Job submitted. We will start processing shortly."
    elif info_code == "logged_in":
        flash_type = "info"
        flash_message = "You are already signed in."

    return {
        "flash_type": flash_type,
        "flash_message": flash_message,
        "flash_job_id": flash_job_id,
    }


@router.get("/")
async def login_page(
    request: Request,
    current_user: str | None = Depends(get_optional_user),
):
    flash = _flash_from_query(request)
    if current_user and not flash["flash_message"]:
        flash = {
            "flash_type": "info",
            "flash_message": "You are already signed in.",
            "flash_job_id": None,
        }
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "title": "Login · Company Email Scraper",
            "current_user": current_user,
            "next_path": request.query_params.get("next") or "/submit",
            **flash,
        },
    )


@router.get("/submit")
async def submit_page(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    flash = _flash_from_query(request)
    return templates.TemplateResponse(
        "submit.html",
        {
            "request": request,
            "title": "Submit Job · Company Email Scraper",
            "current_user": current_user,
            **flash,
        },
    )


@router.post("/scrape")
async def submit_job(
    request: Request,
    city: str = Form(...),
    zip_code: str = Form(...),
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    normalized_city = city.strip()
    normalized_zip = zip_code.strip()

    if not normalized_city or not normalized_zip:
        return RedirectResponse(url="/submit?error=missing_fields", status_code=303)

    # Validate city exists in Stadtbranchenbuch
    scraper = StadtbranchenbuchScraper()
    if not scraper.validate_city(normalized_city, normalized_zip):
        logger.warning(
            "City validation failed for city=%s zip=%s",
            normalized_city,
            normalized_zip,
        )
        return RedirectResponse(url="/submit?error=city_not_found", status_code=303)

    duplicate_stmt = (
        select(Job)
        .where(
            Job.user_email == current_user,
            Job.city == normalized_city,
            Job.zip_code == normalized_zip,
            Job.completed_at.is_(None),
        )
        .order_by(Job.created_at.desc())
    )
    existing_job = (await session.execute(duplicate_stmt)).scalars().first()

    if existing_job:
        return RedirectResponse(
            url=f"/submit?error=duplicate&job_id={existing_job.id}",
            status_code=303,
        )

    job_id = uuid4().hex
    job = Job(
        id=job_id,
        user_email=current_user,
        city=normalized_city,
        zip_code=normalized_zip,
        status="pending",
    )
    session.add(job)
    await session.commit()

    # Launch background job processing (after commit so job is visible)
    logger.info("Launching background processing for job %s", job_id)
    task = asyncio.create_task(process_job(job_id))
    register_job_task(task)  # Track for graceful shutdown

    return RedirectResponse(
        url=f"/history?success=job_created&job_id={job_id}", status_code=303
    )


@router.get("/history")
async def history_page(
    request: Request,
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    jobs_stmt = (
        select(Job)
        .where(Job.user_email == current_user)
        .order_by(Job.created_at.desc())
    )
    jobs = (await session.execute(jobs_stmt)).scalars().all()

    flash = _flash_from_query(request)
    return templates.TemplateResponse(
        "history.html",
        {
            "request": request,
            "title": "Job History · Company Email Scraper",
            "current_user": current_user,
            "jobs": jobs,
            **flash,
        },
    )


@router.get("/results/{job_id}")
async def download_results(
    job_id: str,
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    # Verify job exists and user owns it
    job = await session.get(Job, job_id)
    if not job or job.user_email != current_user:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Job is not completed (current status: {job.status})",
        )

    # Generate safe filename
    safe_city = re.sub(r"[^a-zA-Z0-9]", "_", job.city)
    safe_zip = re.sub(r"[^a-zA-Z0-9]", "_", job.zip_code)
    filename = f"companies_{safe_city}_{safe_zip}.csv"

    # Stream CSV response
    return StreamingResponse(
        generate_csv(job_id, session),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/webhooks/manus")
async def manus_webhook(
    payload: ManusWebhookPayload,
    request: Request,
    session=Depends(get_session),
):
    task_id = (payload.task_id or "").strip()
    logger.info("Received Manus webhook for task %s", task_id)

    if not await _validate_manus_signature(request):
        logger.warning("Invalid Manus webhook signature for task %s", task_id)
        return {"status": "processed"}

    if not _is_valid_task_id(task_id):
        logger.warning("Invalid Manus task id in webhook: %s", task_id)
        return {"status": "processed"}

    try:
        await handle_task_completion(task_id, session)
    except Exception as exc:
        logger.error("Error handling Manus webhook for task %s: %s", task_id, exc)

    return {"status": "processed"}
