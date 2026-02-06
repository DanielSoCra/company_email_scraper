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
from sqlmodel import func, select

from ..auth import get_current_user, get_optional_user
from ..config import get_settings
from ..database import get_session
from ..task_registry import register_job_task
from ..models import Company, Job
from ..services.csv_service import generate_csv
from ..services.job_processor import process_job
from ..services.manus_service import handle_task_completion, retry_company_visible
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
    elif error_code == "failed_exists":
        flash_type = "warning"
        flash_message = "A previous job for this location failed. Submit again to retry."
    elif success_code == "job_created":
        flash_type = "success"
        flash_message = "Job submitted. We will start processing shortly."
    elif success_code == "job_deleted":
        flash_type = "success"
        flash_message = "Job deleted successfully."
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
    params = request.query_params
    # Pass city/zip for retry confirmation
    prefill_city = params.get("city", "")
    prefill_zip = params.get("zip_code", "")
    show_retry_confirm = params.get("error") == "failed_exists"
    return templates.TemplateResponse(
        "submit.html",
        {
            "request": request,
            "title": "Submit Job · Company Email Scraper",
            "current_user": current_user,
            "prefill_city": prefill_city,
            "prefill_zip": prefill_zip,
            "show_retry_confirm": show_retry_confirm,
            **flash,
        },
    )


@router.post("/scrape")
async def submit_job(
    request: Request,
    city: str = Form(...),
    zip_code: str = Form(...),
    force: str = Form(""),
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    normalized_city = city.strip()
    normalized_zip = zip_code.strip()
    force_retry = force.strip().lower() == "true"

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

    # Check for existing incomplete jobs
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
        # Check if it's actively running or failed/timeout
        if existing_job.status in ("pending", "running"):
            # Truly in progress - block submission
            return RedirectResponse(
                url=f"/submit?error=duplicate&job_id={existing_job.id}",
                status_code=303,
            )
        elif existing_job.status in ("failed", "timeout"):
            # Failed job - allow retry with confirmation
            if not force_retry:
                from urllib.parse import quote
                return RedirectResponse(
                    url=f"/submit?error=failed_exists&job_id={existing_job.id}&city={quote(normalized_city)}&zip_code={quote(normalized_zip)}",
                    status_code=303,
                )
            # User confirmed retry - continue to create new job

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


def _escape_like(value: str) -> str:
    """Escape special characters for SQL LIKE patterns."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


PAGE_SIZE = 50


@router.get("/jobs/{job_id}")
async def job_detail_page(
    request: Request,
    job_id: str,
    page: int = 1,
    classification: str = "",
    enrichment: str = "",
    has_email: str = "",
    has_error: str = "",
    search: str = "",
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    job = await session.get(Job, job_id)
    if not job or job.user_email != current_user:
        raise HTTPException(status_code=404, detail="Job not found")

    # Build filtered company query
    query = select(Company).where(Company.job_id == job_id)
    count_query = select(func.count(Company.id)).where(Company.job_id == job_id)

    if classification:
        query = query.where(Company.ai_classification == classification)
        count_query = count_query.where(Company.ai_classification == classification)

    if enrichment:
        query = query.where(Company.enrichment_status == enrichment)
        count_query = count_query.where(Company.enrichment_status == enrichment)

    if has_email == "yes":
        query = query.where(Company.email.isnot(None))
        count_query = count_query.where(Company.email.isnot(None))
    elif has_email == "no":
        query = query.where(Company.email.is_(None))
        count_query = count_query.where(Company.email.is_(None))

    if has_error == "yes":
        query = query.where(Company.last_error.isnot(None))
        count_query = count_query.where(Company.last_error.isnot(None))

    if search.strip():
        pattern = f"%{_escape_like(search.strip())}%"
        query = query.where(Company.name.ilike(pattern))
        count_query = count_query.where(Company.name.ilike(pattern))

    total = (await session.execute(count_query)).scalar() or 0
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(1, min(page, total_pages))
    offset = (page - 1) * PAGE_SIZE

    companies = (
        (await session.execute(query.order_by(Company.name).offset(offset).limit(PAGE_SIZE)))
        .scalars()
        .all()
    )

    # Build phase timeline
    phases = []
    for phase_name in ("scraping", "filtering", "enriching", "exporting"):
        phases.append({
            "name": phase_name.capitalize(),
            "status": getattr(job, f"{phase_name}_status", None),
            "started_at": getattr(job, f"{phase_name}_started_at", None),
            "completed_at": getattr(job, f"{phase_name}_completed_at", None),
            "duration_seconds": getattr(job, f"{phase_name}_duration_seconds", None),
        })

    # Build filter state for template (preserves current filters on pagination)
    filters = {
        "classification": classification,
        "enrichment": enrichment,
        "has_email": has_email,
        "has_error": has_error,
        "search": search,
    }

    settings = get_settings()
    return templates.TemplateResponse(
        "job_detail.html",
        {
            "request": request,
            "title": f"Job Detail · {job.city} {job.zip_code}",
            "current_user": current_user,
            "job": job,
            "phases": phases,
            "companies": companies,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "filters": filters,
            "dev_mode": settings.app.dev_mode,
        },
    )


@router.post("/jobs/{job_id}/retry/{company_id}")
async def retry_company(
    job_id: str,
    company_id: str,
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """Retry Manus enrichment for a single company with visible task (dev only)."""
    settings = get_settings()
    if not settings.app.dev_mode:
        raise HTTPException(status_code=403, detail="Dev mode is not enabled")

    job = await session.get(Job, job_id)
    if not job or job.user_email != current_user:
        raise HTTPException(status_code=404, detail="Job not found")

    company = await session.get(Company, company_id)
    if not company or company.job_id != job_id:
        raise HTTPException(status_code=404, detail="Company not found")

    try:
        task_id = await retry_company_visible(company_id, session)
        logger.info("Dev retry: created visible Manus task %s for company %s", task_id, company_id)
    except Exception as exc:
        logger.error("Dev retry failed for company %s: %s", company_id, exc)
        raise HTTPException(status_code=500, detail=str(exc))

    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


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


@router.post("/jobs/{job_id}/delete")
async def delete_job(
    job_id: str,
    current_user: str = Depends(get_current_user),
    session=Depends(get_session),
):
    """Delete a job and all its associated data."""
    from ..models import Company, ManusTask

    job = await session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.user_email != current_user:
        raise HTTPException(status_code=403, detail="Not authorized to delete this job")

    # Delete associated companies
    companies_stmt = select(Company).where(Company.job_id == job_id)
    companies = (await session.execute(companies_stmt)).scalars().all()
    for company in companies:
        await session.delete(company)

    # Delete associated Manus tasks
    tasks_stmt = select(ManusTask).where(ManusTask.job_id == job_id)
    tasks = (await session.execute(tasks_stmt)).scalars().all()
    for task in tasks:
        await session.delete(task)

    # Delete the job
    await session.delete(job)
    await session.commit()

    logger.info("Deleted job %s and associated data", job_id)
    return RedirectResponse(url="/history?success=job_deleted", status_code=303)


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
