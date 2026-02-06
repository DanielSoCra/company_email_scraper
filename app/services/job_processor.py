"""Job processor service for orchestrating the scraping pipeline.

This module provides the main orchestration engine that processes jobs through
all phases: scraping, filtering, enriching, and exporting. It handles job
lifecycle management, error recovery, and timeout monitoring.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Callable, Awaitable
from uuid import uuid4

from sqlalchemy import func
from sqlmodel import select

from ..database import get_session_factory
from ..models import Company, Job, ManusTask
from .gemini_filter_service import create_ai_filter
from .email_service import send_completion_email, send_failure_email, send_timeout_email
from .manus_service import (
    create_tasks_streaming,
    resume_enrichment,
    update_job_enrichment_stats,
)
from .scraper_service import StadtbranchenbuchScraper
from ..task_registry import register_job_task

logger = logging.getLogger(__name__)

# Job timeout in hours
JOB_TIMEOUT_HOURS = 12

# Timeout monitor check interval in seconds
TIMEOUT_CHECK_INTERVAL = 300  # 5 minutes

# Enrichment completion poll interval in seconds
ENRICHMENT_POLL_INTERVAL = 30

# Phase retry configuration
PHASE_MAX_RETRIES = 3
PHASE_BACKOFF_DELAYS = [5, 15, 45]  # seconds


async def _run_phase_with_retry(
    phase_name: str,
    phase_func: Callable[[str, Any], Awaitable[bool]],
    job_id: str,
    session,
) -> bool:
    """Run a phase function with retry logic and backoff.

    Args:
        phase_name: Name of the phase for logging and error recording.
        phase_func: Async function that takes (job_id, session) and returns bool.
        job_id: The job ID to process.
        session: Database session.

    Returns:
        True if the phase succeeded, False if all retries exhausted.

    Raises:
        Exception: Re-raises the last exception after all retries are exhausted.
    """
    last_exception: Exception | None = None

    for attempt in range(PHASE_MAX_RETRIES):
        try:
            result = await phase_func(job_id, session)
            return result
        except Exception as exc:
            last_exception = exc
            logger.warning(
                "Phase %s failed for job %s (attempt %d/%d): %s",
                phase_name,
                job_id,
                attempt + 1,
                PHASE_MAX_RETRIES,
                exc,
            )

            if attempt < PHASE_MAX_RETRIES - 1:
                delay = PHASE_BACKOFF_DELAYS[min(attempt, len(PHASE_BACKOFF_DELAYS) - 1)]
                logger.info(
                    "Retrying phase %s for job %s in %d seconds",
                    phase_name,
                    job_id,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "Phase %s failed for job %s after %d retries",
                    phase_name,
                    job_id,
                    PHASE_MAX_RETRIES,
                )
                job = await session.get(Job, job_id)
                if job:
                    job.status = "failed"
                    job.updated_at = datetime.utcnow()
                    _append_job_error(
                        job,
                        f"{phase_name}_error",
                        f"Phase failed after {PHASE_MAX_RETRIES} retries: {exc}",
                        phase_name,
                    )
                    await session.commit()

                    # Send failure notification email
                    try:
                        await send_failure_email(job_id, session)
                        logger.info("Sent failure email for job %s", job_id)
                    except Exception as email_exc:
                        logger.error(
                            "Failed to send failure email for job %s: %s",
                            job_id,
                            email_exc,
                        )
                raise

    if last_exception:
        raise last_exception
    return False


async def update_job_phase(
    job_id: str,
    phase: str,
    status: str,
    session,
    start: bool = False,
    complete: bool = False,
) -> Job | None:
    """Update job phase and status fields.

    Args:
        job_id: The job ID to update.
        phase: The phase name (scraping, filtering, enriching, exporting).
        status: The status to set for the phase.
        session: Database session.
        start: Whether this is the start of the phase.
        complete: Whether this is the completion of the phase.

    Returns:
        Updated Job instance or None if not found.
    """
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when updating phase", job_id)
        return None

    job.current_phase = phase
    job.updated_at = datetime.utcnow()

    # Update phase-specific status field
    phase_status_field = f"{phase}_status"
    if hasattr(job, phase_status_field):
        setattr(job, phase_status_field, status)

    # Update phase timestamps
    if start:
        phase_started_field = f"{phase}_started_at"
        if hasattr(job, phase_started_field):
            setattr(job, phase_started_field, datetime.utcnow())

    if complete:
        phase_completed_field = f"{phase}_completed_at"
        phase_started_field = f"{phase}_started_at"
        phase_duration_field = f"{phase}_duration_seconds"

        if hasattr(job, phase_completed_field):
            completed_at = datetime.utcnow()
            setattr(job, phase_completed_field, completed_at)

            # Calculate duration
            if hasattr(job, phase_started_field) and hasattr(job, phase_duration_field):
                started_at = getattr(job, phase_started_field)
                if started_at:
                    duration = int((completed_at - started_at).total_seconds())
                    setattr(job, phase_duration_field, duration)

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("Failed to update job phase for job %s", job_id)
        raise

    return job


async def stream_companies_for_filtering(
    job_id: str,
    session,
    batch_size: int = 100,
) -> AsyncGenerator[list[Company], None]:
    """Yield batches of companies that need AI classification.

    Uses cursor-based pagination to avoid skipping companies when the result
    set changes (companies get classified and no longer match the filter).

    Args:
        job_id: The job ID to filter companies for.
        session: Database session.
        batch_size: Number of companies per batch.

    Yields:
        Batches of Company instances needing classification.
    """
    last_id: str | None = None
    while True:
        stmt = select(Company).where(
            Company.job_id == job_id,
            Company.filtered_out.is_(False),
            Company.ai_classification.is_(None),
        )
        if last_id is not None:
            stmt = stmt.where(Company.id > last_id)
        stmt = stmt.order_by(Company.id).limit(batch_size)

        result = await session.execute(stmt)
        companies = result.scalars().all()
        if not companies:
            break
        yield companies
        last_id = companies[-1].id


async def calculate_job_stats(job_id: str, session) -> Job | None:
    """Calculate and update job statistics.

    Args:
        job_id: The job ID to calculate stats for.
        session: Database session.

    Returns:
        Updated Job instance or None if not found.
    """
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when calculating stats", job_id)
        return None

    # Total scraped
    total_stmt = (
        select(func.count())
        .select_from(Company)
        .where(Company.job_id == job_id)
    )
    total_scraped = (await session.execute(total_stmt)).scalar_one()

    # Filtered valid (valid_b2b and not filtered_out)
    valid_stmt = (
        select(func.count())
        .select_from(Company)
        .where(
            Company.job_id == job_id,
            Company.ai_classification == "valid_b2b",
            Company.filtered_out.is_(False),
        )
    )
    filtered_valid = (await session.execute(valid_stmt)).scalar_one()

    # Filtered excluded
    excluded_stmt = (
        select(func.count())
        .select_from(Company)
        .where(
            Company.job_id == job_id,
            Company.filtered_out.is_(True),
        )
    )
    filtered_excluded = (await session.execute(excluded_stmt)).scalar_one()

    # Enrichment stats
    pending_stmt = (
        select(func.count())
        .select_from(Company)
        .where(Company.job_id == job_id, Company.enrichment_status == "pending")
    )
    completed_stmt = (
        select(func.count())
        .select_from(Company)
        .where(Company.job_id == job_id, Company.enrichment_status == "completed")
    )
    failed_stmt = (
        select(func.count())
        .select_from(Company)
        .where(Company.job_id == job_id, Company.enrichment_status == "failed")
    )
    emails_stmt = (
        select(func.count())
        .select_from(Company)
        .where(Company.job_id == job_id, Company.email.is_not(None))
    )

    enrichment_pending = (await session.execute(pending_stmt)).scalar_one()
    enrichment_completed = (await session.execute(completed_stmt)).scalar_one()
    enrichment_failed = (await session.execute(failed_stmt)).scalar_one()
    emails_found = (await session.execute(emails_stmt)).scalar_one()

    # Update job
    job.stats_scraped = int(total_scraped)
    job.stats_filtered_valid = int(filtered_valid)
    job.stats_filtered_excluded = int(filtered_excluded)
    job.stats_enrichment_pending = int(enrichment_pending)
    job.stats_enrichment_completed = int(enrichment_completed)
    job.stats_enrichment_failed = int(enrichment_failed)
    job.stats_emails_found = int(emails_found)

    # Calculate email coverage
    if filtered_valid > 0:
        job.stats_email_coverage = float(emails_found / filtered_valid * 100)
    else:
        job.stats_email_coverage = 0.0

    job.updated_at = datetime.utcnow()

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("Failed to update job stats for job %s", job_id)
        raise

    return job


def _append_job_error(job: Job, error_type: str, message: str, phase: str | None = None) -> None:
    """Append an error to the job's errors JSONB array."""
    if job.errors is None:
        job.errors = []

    error_entry: dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
        "type": error_type,
        "message": message,
    }
    if phase:
        error_entry["phase"] = phase

    job.errors.append(error_entry)


async def _run_scraping_phase(job_id: str, session) -> bool:
    """Execute the scraping phase.

    Args:
        job_id: The job ID to process.
        session: Database session.

    Returns:
        True if successful, False otherwise.
    """
    job = await session.get(Job, job_id)
    if not job:
        return False

    logger.info("Starting scraping phase for job %s", job_id)
    await update_job_phase(job_id, "scraping", "running", session, start=True)

    # Check if companies already exist (idempotency)
    existing_stmt = (
        select(func.count())
        .select_from(Company)
        .where(Company.job_id == job_id)
    )
    existing_count = (await session.execute(existing_stmt)).scalar_one()

    if existing_count > 0:
        logger.info(
            "Job %s already has %d companies, skipping scraping",
            job_id,
            existing_count,
        )
        await update_job_phase(job_id, "scraping", "completed", session, complete=True)
        return True

    scraper = StadtbranchenbuchScraper(min_delay=1.0, max_delay=2.0)
    batch: list[Company] = []
    batch_size = 100
    total_scraped = 0

    try:
        for company_data in scraper.scrape_city(job.city, job.zip_code):
            company = Company(
                id=uuid4().hex,
                job_id=job_id,
                name=company_data.get("name", ""),
                address=company_data.get("address"),
                street=company_data.get("street"),
                postal_code=company_data.get("postal_code"),
                city=company_data.get("city"),
                phone=company_data.get("phone"),
                category=company_data.get("category"),
                website=company_data.get("website"),
                detail_url=company_data.get("detail_url"),
            )
            batch.append(company)

            if len(batch) >= batch_size:
                for c in batch:
                    session.add(c)
                await session.commit()
                total_scraped += len(batch)
                logger.info("Inserted batch of %d companies (total: %d)", len(batch), total_scraped)
                batch = []

        # Insert remaining companies
        if batch:
            for c in batch:
                session.add(c)
            await session.commit()
            total_scraped += len(batch)
            logger.info("Inserted final batch of %d companies (total: %d)", len(batch), total_scraped)

    except Exception as exc:
        logger.exception("Scraping failed for job %s: %s", job_id, exc)
        job = await session.get(Job, job_id)
        if job:
            _append_job_error(job, "scraping_error", str(exc), "scraping")
            await session.commit()
        raise

    # Update stats and complete phase
    await calculate_job_stats(job_id, session)
    await update_job_phase(job_id, "scraping", "completed", session, complete=True)
    logger.info("Scraping phase completed for job %s: %d companies", job_id, total_scraped)

    return True


async def _run_filtering_phase(job_id: str, session) -> bool:
    """Execute the filtering phase.

    Args:
        job_id: The job ID to process.
        session: Database session.

    Returns:
        True if successful, False otherwise.
    """
    logger.info("Starting filtering phase for job %s", job_id)
    await update_job_phase(job_id, "filtering", "running", session, start=True)

    filter_service = create_ai_filter()
    total_processed = 0

    try:
        async for batch in stream_companies_for_filtering(job_id, session, batch_size=100):
            # Convert Company objects to dicts for the filter service
            company_dicts = [
                {
                    "id": c.id,
                    "name": c.name,
                    "category": c.category,
                    "website": c.website,
                    "address": c.address,
                }
                for c in batch
            ]

            # Classify batch
            classified = await filter_service.classify_batch(company_dicts)

            # Update companies with classification results
            classified_map = {c["id"]: c for c in classified if "id" in c}
            for company in batch:
                if company.id in classified_map:
                    result = classified_map[company.id]
                    company.ai_classification = result.get("ai_classification")
                    company.ai_classification_reason = result.get("ai_classification_reason")
                    company.filtered_out = result.get("filtered_out", False)
                    company.updated_at = datetime.utcnow()

            await session.commit()
            total_processed += len(batch)
            logger.info("Filtered batch of %d companies (total: %d)", len(batch), total_processed)

    except Exception as exc:
        logger.exception("Filtering failed for job %s: %s", job_id, exc)
        job = await session.get(Job, job_id)
        if job:
            _append_job_error(job, "filtering_error", str(exc), "filtering")
            await session.commit()
        raise

    # Update stats and complete phase
    await calculate_job_stats(job_id, session)
    await update_job_phase(job_id, "filtering", "completed", session, complete=True)
    logger.info("Filtering phase completed for job %s: %d companies processed", job_id, total_processed)

    return True


async def _run_enriching_phase(job_id: str, session) -> bool:
    """Execute the enrichment phase.

    Args:
        job_id: The job ID to process.
        session: Database session.

    Returns:
        True if successful, False otherwise.
    """
    logger.info("Starting enrichment phase for job %s", job_id)
    await update_job_phase(job_id, "enriching", "running", session, start=True)

    try:
        # Create Manus tasks for all eligible companies
        tasks_created = await create_tasks_streaming(job_id, session)
        logger.info("Created %d Manus tasks for job %s", tasks_created, job_id)

        # Wait for enrichment completion
        await _wait_for_enrichment_completion(job_id, session)

    except Exception as exc:
        logger.exception("Enrichment failed for job %s: %s", job_id, exc)
        job = await session.get(Job, job_id)
        if job:
            _append_job_error(job, "enrichment_error", str(exc), "enriching")
            await session.commit()
        raise

    # Update stats and complete phase
    await update_job_enrichment_stats(job_id, session)
    await update_job_phase(job_id, "enriching", "completed", session, complete=True)
    logger.info("Enrichment phase completed for job %s", job_id)

    return True


async def _wait_for_enrichment_completion(job_id: str, session) -> None:
    """Poll for enrichment completion.

    Args:
        job_id: The job ID to monitor.
        session: Database session.
    """
    while True:
        # Count pending enrichment tasks
        pending_stmt = (
            select(func.count())
            .select_from(ManusTask)
            .where(ManusTask.job_id == job_id, ManusTask.status == "pending")
        )
        pending_count = (await session.execute(pending_stmt)).scalar_one()

        if pending_count == 0:
            logger.info("All enrichment tasks completed for job %s", job_id)
            return

        logger.info(
            "Waiting for enrichment completion: %d tasks pending for job %s",
            pending_count,
            job_id,
        )
        await asyncio.sleep(ENRICHMENT_POLL_INTERVAL)


async def _run_exporting_phase(job_id: str, session) -> bool:
    """Execute the exporting phase (marks job as completed).

    Args:
        job_id: The job ID to process.
        session: Database session.

    Returns:
        True if successful, False otherwise.
    """
    logger.info("Starting exporting phase for job %s", job_id)
    await update_job_phase(job_id, "exporting", "running", session, start=True)

    # Calculate final stats
    await calculate_job_stats(job_id, session)

    # Mark job as completed
    job = await session.get(Job, job_id)
    if job:
        job.status = "completed"
        job.completed_at = datetime.utcnow()
        job.updated_at = datetime.utcnow()
        await session.commit()

        # Send completion notification email
        try:
            await send_completion_email(job_id, session)
            logger.info("Sent completion email for job %s", job_id)
        except Exception as email_exc:
            logger.error(
                "Failed to send completion email for job %s: %s",
                job_id,
                email_exc,
            )

    await update_job_phase(job_id, "exporting", "completed", session, complete=True)
    logger.info("Exporting phase completed for job %s", job_id)

    return True


async def process_job(job_id: str) -> None:
    """Main job processor that orchestrates all phases.

    Args:
        job_id: The job ID to process.
    """
    logger.info("Starting job processing for %s", job_id)

    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            # Update job to running status
            job = await session.get(Job, job_id)
            if not job:
                logger.error("Job %s not found", job_id)
                return

            job.status = "running"
            job.started_at = datetime.utcnow()
            job.updated_at = datetime.utcnow()
            await session.commit()

            # Phase 1: Scraping (with retry)
            await _run_phase_with_retry("scraping", _run_scraping_phase, job_id, session)

            # Phase 2: Filtering (with retry)
            await _run_phase_with_retry("filtering", _run_filtering_phase, job_id, session)

            # Phase 3: Enriching (with retry)
            await _run_phase_with_retry("enriching", _run_enriching_phase, job_id, session)

            # Phase 4: Exporting (with retry)
            await _run_phase_with_retry("exporting", _run_exporting_phase, job_id, session)

            logger.info("Job %s completed successfully", job_id)

        except Exception as exc:
            # Phase retry already marks job as failed, but catch unexpected errors
            logger.exception("Job %s failed: %s", job_id, exc)
            job = await session.get(Job, job_id)
            if job and job.status != "failed":
                job.status = "failed"
                job.updated_at = datetime.utcnow()
                _append_job_error(job, "job_error", str(exc), job.current_phase)
                await session.commit()


async def resume_incomplete_jobs() -> int:
    """Resume processing for any incomplete jobs.

    Called on application startup to resume jobs that were interrupted.

    Returns:
        Number of jobs resumed.
    """
    logger.info("Checking for incomplete jobs to resume")

    session_factory = get_session_factory()
    async with session_factory() as session:
        # Find jobs that are running or pending
        stmt = select(Job).where(Job.status.in_(["running", "pending"]))
        result = await session.execute(stmt)
        incomplete_jobs = result.scalars().all()

        if not incomplete_jobs:
            logger.info("No incomplete jobs found")
            return 0

        resumed_count = 0
        for job in incomplete_jobs:
            logger.info(
                "Resuming job %s (status=%s, phase=%s)",
                job.id,
                job.status,
                job.current_phase,
            )

            # Determine resume point based on current phase
            if job.current_phase == "enriching":
                # Resume enrichment using the manus_service resume function
                task = asyncio.create_task(_resume_enrichment_job(job.id))
                register_job_task(task)  # Track for graceful shutdown
            else:
                # For other phases, restart the job processor
                task = asyncio.create_task(process_job(job.id))
                register_job_task(task)  # Track for graceful shutdown

            resumed_count += 1

        logger.info("Resumed %d incomplete jobs", resumed_count)
        return resumed_count


async def _resume_enrichment_job(job_id: str) -> None:
    """Resume a job that was in the enrichment phase.

    Args:
        job_id: The job ID to resume.
    """
    logger.info("Resuming enrichment for job %s", job_id)

    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            # Resume enrichment
            result = await resume_enrichment(job_id, session)
            logger.info(
                "Resumed enrichment for job %s: new_tasks=%d, resumed_tasks=%d",
                job_id,
                result["new_tasks"],
                result["resumed_tasks"],
            )

            # Wait for completion
            await _wait_for_enrichment_completion(job_id, session)

            # Update stats and complete phase
            await update_job_enrichment_stats(job_id, session)
            await update_job_phase(job_id, "enriching", "completed", session, complete=True)

            # Run exporting phase
            await _run_exporting_phase(job_id, session)

            logger.info("Job %s completed after enrichment resume", job_id)

        except Exception as exc:
            logger.exception("Failed to resume enrichment for job %s: %s", job_id, exc)
            job = await session.get(Job, job_id)
            if job:
                job.status = "failed"
                _append_job_error(job, "resume_error", str(exc), "enriching")
                await session.commit()


async def monitor_job_timeouts() -> None:
    """Background task that monitors for job timeouts.

    Runs indefinitely, checking for timed-out jobs every 5 minutes.
    """
    logger.info("Starting job timeout monitor")

    while True:
        try:
            session_factory = get_session_factory()
            async with session_factory() as session:
                # Find jobs that have exceeded the timeout
                timeout_threshold = datetime.utcnow() - timedelta(hours=JOB_TIMEOUT_HOURS)
                stmt = select(Job).where(
                    Job.status == "running",
                    Job.started_at.is_not(None),
                    Job.started_at < timeout_threshold,
                )
                result = await session.execute(stmt)
                timed_out_jobs = result.scalars().all()

                for job in timed_out_jobs:
                    logger.warning(
                        "Job %s timed out after %d hours (phase=%s)",
                        job.id,
                        JOB_TIMEOUT_HOURS,
                        job.current_phase,
                    )

                    job.status = "timeout"
                    job.updated_at = datetime.utcnow()
                    _append_job_error(
                        job,
                        "timeout",
                        f"Job exceeded {JOB_TIMEOUT_HOURS}-hour limit",
                        job.current_phase,
                    )

                if timed_out_jobs:
                    await session.commit()
                    logger.info("Marked %d jobs as timed out", len(timed_out_jobs))

                    # Send timeout notification emails
                    for job in timed_out_jobs:
                        try:
                            await send_timeout_email(job.id, session)
                            logger.info("Sent timeout email for job %s", job.id)
                        except Exception as email_exc:
                            logger.error(
                                "Failed to send timeout email for job %s: %s",
                                job.id,
                                email_exc,
                            )

        except Exception as exc:
            logger.error("Timeout monitor error: %s", exc)
            # Sleep for a shorter period before retrying on error
            await asyncio.sleep(60)
            continue

        await asyncio.sleep(TIMEOUT_CHECK_INTERVAL)
