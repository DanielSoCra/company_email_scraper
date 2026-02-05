"""Email notification service for job status updates.

This module provides async email sending functionality using aiosmtplib
to notify users when their jobs complete, fail, or timeout.
"""

from __future__ import annotations

import logging
from email.message import EmailMessage

import aiosmtplib

from ..config import get_settings
from ..models import Job

logger = logging.getLogger(__name__)


async def _send_email(to_email: str, subject: str, body: str) -> None:
    """Send an email via SMTP.

    Args:
        to_email: Recipient email address.
        subject: Email subject line.
        body: Plain text email body.

    Raises:
        Exception: If email sending fails.
    """
    settings = get_settings()
    email_config = settings.email

    if not email_config.enabled:
        logger.info("Email notifications disabled; skipping email send")
        return

    if not email_config.smtp_password:
        logger.warning("SMTP_PASSWORD not configured; skipping email send")
        return

    message = EmailMessage()
    message["From"] = email_config.from_email
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)

    try:
        await aiosmtplib.send(
            message,
            hostname=email_config.smtp_host,
            port=email_config.smtp_port,
            username=email_config.smtp_user,
            password=email_config.smtp_password,
            start_tls=True,
        )
        logger.info("Email sent successfully to %s: %s", to_email, subject)
    except aiosmtplib.SMTPException as exc:
        logger.error("SMTP error sending email to %s: %s", to_email, exc)
        raise
    except Exception as exc:
        logger.error("Failed to send email to %s: %s", to_email, exc)
        raise


def _format_number(value: int) -> str:
    """Format a number with thousand separators."""
    return f"{value:,}"


def _format_percentage(value: float) -> str:
    """Format a percentage with one decimal place."""
    return f"{value:.1f}%"


async def send_completion_email(job_id: str, session) -> None:
    """Send email notification when a job completes successfully.

    Args:
        job_id: The completed job ID.
        session: Database session.
    """
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when sending completion email", job_id)
        return

    settings = get_settings()
    download_url = f"{settings.app.base_url}/results/{job_id}"

    subject = f"Scraping Complete - {job.city}"
    body = f"""Scraping Complete - {job.city}

Your scraping job has finished successfully.

Location: {job.city}, {job.zip_code}

Results:
- Total companies found: {_format_number(job.stats_scraped)}
- Valid B2B prospects: {_format_number(job.stats_filtered_valid)}
- Companies with emails: {_format_number(job.stats_emails_found)}
- Email coverage: {_format_percentage(job.stats_email_coverage)}
- Companies failed: {_format_number(job.stats_enrichment_failed)}

Download your results:
{download_url}

Note: You must be logged in to download results.
"""

    await _send_email(job.user_email, subject, body)


async def send_failure_email(job_id: str, session) -> None:
    """Send email notification when a job fails.

    Args:
        job_id: The failed job ID.
        session: Database session.
    """
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when sending failure email", job_id)
        return

    # Extract latest error from errors JSONB array
    error_message = "Unknown error"
    if job.errors and len(job.errors) > 0:
        latest_error = job.errors[-1]
        error_message = latest_error.get("message", "Unknown error")

    timestamp = job.updated_at.strftime("%Y-%m-%d %H:%M:%S UTC") if job.updated_at else "Unknown"

    subject = f"Scraping Failed - {job.city}"
    body = f"""Scraping Failed - {job.city}

Your scraping job encountered an error and could not complete.

Location: {job.city}, {job.zip_code}

Error: {error_message}

Timestamp: {timestamp}

Please try submitting a new job or contact support if the issue persists.
"""

    await _send_email(job.user_email, subject, body)


async def send_timeout_email(job_id: str, session) -> None:
    """Send email notification when a job times out.

    Args:
        job_id: The timed-out job ID.
        session: Database session.
    """
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when sending timeout email", job_id)
        return

    current_phase = job.current_phase or "unknown"

    subject = f"Job Timeout - {job.city}"
    body = f"""Job Timeout - {job.city}

Your scraping job exceeded the 12-hour time limit and was terminated.

Location: {job.city}, {job.zip_code}

Current phase when timeout occurred: {current_phase}

Partial results are not available. Please submit a new job if you need the data.
"""

    await _send_email(job.user_email, subject, body)
