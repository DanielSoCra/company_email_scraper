"""Manus API integration for company enrichment tasks."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import func
from sqlmodel import select

from ..config import get_settings
from ..models import Company, Job, ManusTask

logger = logging.getLogger(__name__)

_shared_manus_client: httpx.AsyncClient | None = None


def _get_manus_http_client() -> httpx.AsyncClient:
    """Return a shared AsyncClient for Manus API calls."""
    global _shared_manus_client
    if _shared_manus_client is None:
        timeout = httpx.Timeout(60.0)
        _shared_manus_client = httpx.AsyncClient(timeout=timeout)
    return _shared_manus_client


async def close_manus_http_client() -> None:
    """Close the shared AsyncClient used for Manus API calls."""
    global _shared_manus_client
    if _shared_manus_client is not None:
        await _shared_manus_client.aclose()
        _shared_manus_client = None


class RateLimiter:
    """Token bucket rate limiter for Manus task creation."""

    def __init__(self, rate_per_second: float = 5.0) -> None:
        self.rate_per_second = max(rate_per_second, 0.1)
        self.capacity = self.rate_per_second
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = max(now - self.last_refill, 0.0)
        self.last_refill = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait_seconds = (1 - self.tokens) / self.rate_per_second

            logger.debug("Rate limiter delaying for %.2f seconds", wait_seconds)
            await asyncio.sleep(wait_seconds)


class ManusClient:
    """Client for interacting with the Manus API."""

    RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
    BACKOFF_DELAYS = [5, 15, 45]

    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        api_url: str,
        rate_limiter: RateLimiter,
    ) -> None:
        self.client = client
        self.api_key = api_key
        self.api_url = api_url.rstrip("/")
        self.rate_limiter = rate_limiter

    async def create_task(
        self,
        company_data: dict[str, Any],
        metadata: dict[str, Any],
        project_id: str | None = None,
        hide_in_task_list: bool = True,
    ) -> dict[str, Any]:
        """Create a new Manus task for a company."""
        await self.rate_limiter.acquire()

        # Build prompt for email discovery
        company_name = company_data.get("name", "Unknown Company")
        website = company_data.get("website", "")
        address = company_data.get("address", "")
        city = company_data.get("city", "")
        category = company_data.get("category", "")
        phone = company_data.get("phone", "")
        detail_url = company_data.get("detail_url", "")

        prompt = _build_email_search_prompt(
            company_name, website, address, city, category, phone, detail_url,
        )

        payload = {
            "prompt": prompt,
            "metadata": metadata,
            "attachment": company_data,
            "hideInTaskList": hide_in_task_list,
        }
        if project_id:
            payload["projectId"] = project_id
        return await self._request_with_retry("POST", "/tasks", json_payload=payload)

    async def create_project(
        self,
        name: str,
        webhook_url: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new Manus project."""
        await self.rate_limiter.acquire()
        payload = {
            "name": name,
            "webhookUrl": webhook_url,
        }
        if metadata:
            payload["metadata"] = metadata
        return await self._request_with_retry("POST", "/projects", json_payload=payload)

    async def get_task(self, task_id: str) -> dict[str, Any]:
        """Fetch task details from Manus."""
        return await self._request_with_retry("GET", f"/tasks/{task_id}")

    async def delete_task(self, task_id: str) -> dict[str, Any]:
        """Delete a Manus task."""
        return await self._request_with_retry("DELETE", f"/tasks/{task_id}")

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        json_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.api_url}{path}"
        headers = {
            "API_KEY": self.api_key,
            "accept": "application/json",
            "content-type": "application/json",
        }

        logger.info("Manus request %s %s payload=%s", method, url, json_payload)

        for attempt in range(len(self.BACKOFF_DELAYS) + 1):
            try:
                response = await self.client.request(
                    method,
                    url,
                    headers=headers,
                    json=json_payload,
                )
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                if attempt >= len(self.BACKOFF_DELAYS):
                    logger.exception("Manus request failed after %d attempts", attempt + 1)
                    raise

                delay = self.BACKOFF_DELAYS[min(attempt, len(self.BACKOFF_DELAYS) - 1)]
                logger.warning(
                    "Manus request error on attempt %d/%d: %s; retrying in %s seconds",
                    attempt + 1,
                    len(self.BACKOFF_DELAYS) + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            if response.status_code in self.RETRY_STATUS_CODES:
                if attempt >= len(self.BACKOFF_DELAYS):
                    logger.error(
                        "Retryable status %s after %d attempts",
                        response.status_code,
                        attempt + 1,
                    )
                    response.raise_for_status()

                delay = self.BACKOFF_DELAYS[min(attempt, len(self.BACKOFF_DELAYS) - 1)]
                logger.warning(
                    "Retryable status %s on attempt %d/%d; retrying in %s seconds",
                    response.status_code,
                    attempt + 1,
                    len(self.BACKOFF_DELAYS) + 1,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError:
                # Log the response body to understand what the API is complaining about
                response_text = response.text if response.content else "(no body)"
                logger.error(
                    "Non-retryable response status %s for %s %s - Response: %s",
                    response.status_code,
                    method,
                    url,
                    response_text[:1000],  # Limit to 1000 chars
                )
                raise

            logger.debug("Manus response status %s", response.status_code)
            if response.content:
                try:
                    response_data = response.json()
                    logger.debug("Manus response payload: %s", response_data)
                    return response_data
                except ValueError:
                    return {"raw": response.text}
            return {}

        raise httpx.RequestError("Manus request failed after retries", request=None)


def create_manus_client() -> ManusClient:
    """Factory for creating a ManusClient instance."""
    settings = get_settings()
    if not settings.api.manus_api_key:
        raise ValueError("MANUS_API_KEY environment variable is required")

    rate_limiter = RateLimiter(settings.app.manus_rate_limit)
    return ManusClient(
        client=_get_manus_http_client(),
        api_key=settings.api.manus_api_key,
        api_url=settings.api.manus_api_url,
        rate_limiter=rate_limiter,
    )


def _build_email_search_prompt(
    company_name: str,
    website: str,
    address: str,
    city: str,
    category: str,
    phone: str,
    detail_url: str,
) -> str:
    """Build a structured German-language prompt for Manus email discovery.

    Applies prompt engineering best practices:
    - Clear role and intent context
    - Structured company data section
    - Prioritized step-by-step search strategy
    - Explicit quality criteria (positive framing)
    - Strict output format specification
    """
    # Company data block
    data_lines = [f"- Firmenname: {company_name}"]
    if website:
        data_lines.append(f"- Website: {website}")
    if address:
        data_lines.append(f"- Adresse: {address}")
    if city:
        data_lines.append(f"- Stadt: {city}")
    if category:
        data_lines.append(f"- Branche: {category}")
    if phone:
        data_lines.append(f"- Telefon: {phone}")
    if detail_url:
        data_lines.append(f"- Branchenbuch-Eintrag: {detail_url}")
    company_block = "\n".join(data_lines)

    return f"""Du bist ein Recherche-Assistent, der geschäftliche E-Mail-Adressen für deutsche Unternehmen findet. Die E-Mail-Adresse wird für seriöse B2B-Kontaktaufnahme benötigt.

<unternehmensdaten>
{company_block}
</unternehmensdaten>

<auftrag>
Finde eine aktive, allgemeine Geschäfts-E-Mail-Adresse für dieses Unternehmen. Gehe dabei systematisch in der folgenden Reihenfolge vor:

Schritt 1 – Unternehmenswebsite prüfen:
Öffne die Website des Unternehmens. Suche gezielt nach dem Impressum (in Deutschland gesetzlich vorgeschrieben und enthält fast immer eine E-Mail-Adresse). Prüfe zusätzlich die Kontaktseite und den Footer der Startseite.

Schritt 2 – Google-Suche:
Falls Schritt 1 keine E-Mail liefert, suche auf Google nach "{company_name} {city} E-Mail Kontakt" oder "{company_name} Impressum".

Schritt 3 – Branchenverzeichnisse:
Falls weiterhin keine E-Mail gefunden wurde, prüfe Einträge auf gelbeseiten.de, dasoertliche.de und anderen deutschen Branchenverzeichnissen.
</auftrag>

<qualitaetskriterien>
Bevorzuge allgemeine Geschäftsadressen in dieser Reihenfolge: info@, kontakt@, office@, mail@, post@.
Persönliche Adressen (z.B. vorname.nachname@) sind akzeptabel, wenn keine allgemeine Adresse verfügbar ist.
Die Domain der E-Mail-Adresse sollte zur Unternehmenswebsite passen.
</qualitaetskriterien>

<ausgabeformat>
Antworte ausschließlich mit der gefundenen E-Mail-Adresse, ohne zusätzlichen Text.

Beispiel bei Erfolg:
info@beispiel-firma.de

Falls keine E-Mail gefunden werden konnte, antworte genau mit:
KEINE_EMAIL_GEFUNDEN
</ausgabeformat>"""


def _build_company_payload(company: Company) -> dict[str, Any]:
    return {
        "id": company.id,
        "name": company.name,
        "address": company.address,
        "street": company.street,
        "postal_code": company.postal_code,
        "city": company.city,
        "phone": company.phone,
        "category": company.category,
        "website": company.website,
        "detail_url": company.detail_url,
    }


def _extract_task_id(task_response: dict[str, Any]) -> str | None:
    if not task_response:
        return None
    for key in ("id", "task_id"):
        value = task_response.get(key)
        if isinstance(value, str) and value:
            return value
    data = task_response.get("data")
    if isinstance(data, dict):
        value = data.get("id") or data.get("task_id")
        if isinstance(value, str) and value:
            return value
    return None


def _extract_project_id(project_response: dict[str, Any]) -> str | None:
    if not project_response:
        return None
    for key in ("project_id", "projectId", "id"):
        value = project_response.get(key)
        if isinstance(value, str) and value:
            return value
    data = project_response.get("data")
    if isinstance(data, dict):
        for key in ("project_id", "projectId", "id"):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
        project = data.get("project")
        if isinstance(project, dict):
            for key in ("project_id", "projectId", "id"):
                value = project.get(key)
                if isinstance(value, str) and value:
                    return value
    project = project_response.get("project")
    if isinstance(project, dict):
        for key in ("project_id", "projectId", "id"):
            value = project.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _extract_webhook_id(project_response: dict[str, Any]) -> str | None:
    if not project_response:
        return None
    for key in ("webhook_id", "webhookId", "webhookID"):
        value = project_response.get(key)
        if isinstance(value, str) and value:
            return value
    data = project_response.get("data")
    if isinstance(data, dict):
        for key in ("webhook_id", "webhookId", "webhookID"):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
        webhook = data.get("webhook")
        if isinstance(webhook, dict):
            for key in ("id", "webhook_id", "webhookId"):
                value = webhook.get(key)
                if isinstance(value, str) and value:
                    return value
    webhook = project_response.get("webhook")
    if isinstance(webhook, dict):
        for key in ("id", "webhook_id", "webhookId"):
            value = webhook.get(key)
            if isinstance(value, str) and value:
                return value
    if isinstance(webhook, str) and webhook:
        return webhook
    return None


def _get_task_status(task_response: dict[str, Any]) -> str | None:
    if not task_response:
        return None
    status = task_response.get("status")
    if isinstance(status, str):
        return status.lower()
    data = task_response.get("data")
    if isinstance(data, dict):
        status = data.get("status")
        if isinstance(status, str):
            return status.lower()
    return None


def _get_task_error(task_response: dict[str, Any]) -> str | None:
    if not task_response:
        return None
    for key in ("error", "message", "reason"):
        value = task_response.get(key)
        if isinstance(value, str) and value:
            return value
    data = task_response.get("data")
    if isinstance(data, dict):
        for key in ("error", "message", "reason"):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value
    return None


_EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _extract_email_from_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                return _extract_email_from_value(parsed)
        match = _EMAIL_REGEX.search(value)
        if match:
            return match.group(0)
        return None
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() == "email" and isinstance(item, str):
                if _EMAIL_REGEX.fullmatch(item.strip()):
                    return item.strip()
        for key in ("output", "result", "data", "files", "content", "text"):
            if key in value:
                found = _extract_email_from_value(value[key])
                if found:
                    return found
        for item in value.values():
            found = _extract_email_from_value(item)
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = _extract_email_from_value(item)
            if found:
                return found
    return None


def _extract_email(task_response: dict[str, Any]) -> str | None:
    return _extract_email_from_value(task_response)


async def get_companies_for_enrichment(
    job_id: str,
    session,
    batch_size: int = 100,
) -> AsyncGenerator[list[Company], None]:
    """Yield company batches that need enrichment.

    Uses cursor-based pagination to avoid skipping companies when the result
    set changes (companies get tasks created and status changes).
    """
    last_id: str | None = None
    while True:
        stmt = select(Company).where(
            Company.job_id == job_id,
            Company.filtered_out.is_(False),
            Company.ai_classification == "valid_b2b",
            Company.enrichment_status == "pending",
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


async def get_pending_tasks(job_id: str, session) -> list[ManusTask]:
    stmt = (
        select(ManusTask)
        .where(ManusTask.job_id == job_id, ManusTask.status == "pending")
        .order_by(ManusTask.created_at)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def update_job_enrichment_stats(job_id: str, session) -> Job | None:
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when updating enrichment stats", job_id)
        return None

    total_stmt = select(func.count()).select_from(Company).where(Company.job_id == job_id)
    pending_stmt = select(func.count()).select_from(Company).where(
        Company.job_id == job_id, Company.enrichment_status == "pending"
    )
    completed_stmt = select(func.count()).select_from(Company).where(
        Company.job_id == job_id, Company.enrichment_status == "completed"
    )
    failed_stmt = select(func.count()).select_from(Company).where(
        Company.job_id == job_id, Company.enrichment_status == "failed"
    )
    emails_stmt = select(func.count()).select_from(Company).where(
        Company.job_id == job_id, Company.email.is_not(None)
    )

    total = (await session.execute(total_stmt)).scalar_one()
    pending = (await session.execute(pending_stmt)).scalar_one()
    completed = (await session.execute(completed_stmt)).scalar_one()
    failed = (await session.execute(failed_stmt)).scalar_one()
    emails_found = (await session.execute(emails_stmt)).scalar_one()

    job.stats_enrichment_pending = int(pending)
    job.stats_enrichment_completed = int(completed)
    job.stats_enrichment_failed = int(failed)
    job.stats_emails_found = int(emails_found)
    job.stats_email_coverage = float(emails_found / total * 100) if total else 0.0

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("Failed to update enrichment stats for job %s", job_id)
        raise

    return job


async def create_project(job_id: str, session) -> Job | None:
    """Create a Manus project for the job and persist project/webhook ids."""
    settings = get_settings()
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found when creating Manus project", job_id)
        return None

    if job.manus_project_id:
        return job

    manus_client = create_manus_client()
    project_name = f"job-{job_id}"

    try:
        project_response = await manus_client.create_project(
            name=project_name,
            webhook_url=settings.app.manus_webhook_url,
            metadata={"job_id": job_id},
        )
    except Exception:
        logger.exception("Failed to create Manus project for job %s", job_id)
        raise

    project_id = _extract_project_id(project_response)
    if not project_id:
        raise ValueError(f"Manus project creation missing project id for job {job_id}")

    webhook_id = _extract_webhook_id(project_response)
    job.manus_project_id = project_id
    job.manus_webhook_id = webhook_id

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("Failed to persist Manus project ids for job %s", job_id)
        raise

    return job


async def create_tasks_streaming(job_id: str, session) -> int:
    """Create Manus tasks for eligible companies in streaming batches."""
    settings = get_settings()
    manus_client = create_manus_client()
    job = await create_project(job_id, session)
    if not job or not job.manus_project_id:
        raise ValueError(f"Missing Manus project id for job {job_id}")
    project_id = job.manus_project_id
    total_created = 0

    async for batch in get_companies_for_enrichment(
        job_id, session, batch_size=settings.app.stream_batch_size
    ):
        for company in batch:
            metadata = {"job_id": job_id, "company_id": company.id}
            try:
                task_response = await manus_client.create_task(
                    _build_company_payload(company),
                    metadata,
                    project_id=project_id,
                )
            except Exception as exc:
                logger.error(
                    "Failed to create Manus task for company %s: %s",
                    company.id,
                    exc,
                )
                company.retry_count += 1
                company.last_error = f"Manus task creation failed: {exc}"
                company.error_timestamp = datetime.utcnow()
                continue

            task_id = _extract_task_id(task_response)
            if not task_id:
                logger.error(
                    "Manus task creation returned no task id for company %s",
                    company.id,
                )
                company.retry_count += 1
                company.last_error = "Manus task creation missing task id"
                company.error_timestamp = datetime.utcnow()
                continue

            manus_task = ManusTask(
                id=task_id,
                job_id=job_id,
                company_id=company.id,
                status="pending",
            )
            session.add(manus_task)
            total_created += 1

        try:
            await session.commit()
        except Exception:
            await session.rollback()
            logger.exception("Database error while creating Manus tasks for job %s", job_id)
            raise

        logger.info("Created %d Manus tasks for job %s", total_created, job_id)

    pending_tasks = await get_pending_tasks(job_id, session)
    if pending_tasks:
        logger.info("Scheduling Manus polling for job %s", job_id)
        asyncio.create_task(poll_pending_tasks(job_id))

    return total_created


async def handle_task_completion(task_id: str, session) -> bool:
    """Handle completion webhook for a Manus task."""
    manus_client = create_manus_client()
    stmt = select(ManusTask).where(ManusTask.id == task_id)
    manus_task = (await session.execute(stmt)).scalars().first()

    if not manus_task:
        logger.warning("Received Manus webhook for unknown task %s", task_id)
        return False

    company = await session.get(Company, manus_task.company_id)
    if not company:
        logger.warning(
            "Manus task %s references missing company %s",
            task_id,
            manus_task.company_id,
        )

    task_response = await manus_client.get_task(task_id)
    task_status = _get_task_status(task_response) or "completed"
    now = datetime.utcnow()

    manus_task.status = task_status
    manus_task.updated_at = now
    manus_task.result = task_response

    if task_status in {"completed", "failed"}:
        manus_task.completed_at = now

    if task_status == "failed":
        error_message = _get_task_error(task_response) or "Manus task failed"
        manus_task.error = error_message
        if company:
            company.enrichment_status = "failed"
            company.last_error = error_message
            company.error_timestamp = now
    else:
        email = _extract_email(task_response)
        if company:
            if company.email:
                logger.info(
                    "Company %s already has email; skipping email update",
                    company.id,
                )
            else:
                if email:
                    company.email = email
                    company.email_source = "manus"
                elif not company.email:
                    company.last_error = "Manus task completed without email"
                    company.error_timestamp = now
            company.enrichment_status = "completed"

    try:
        await session.commit()
    except Exception:
        await session.rollback()
        logger.exception("Failed to persist Manus task completion for %s", task_id)
        raise

    return True


async def poll_pending_tasks(job_id: str) -> None:
    """Polling fallback when webhooks do not arrive.

    Creates its own session per iteration to avoid sharing sessions across
    concurrent tasks.
    """
    from ..database import get_session_factory

    settings = get_settings()
    manus_client = create_manus_client()
    attempts: dict[str, int] = {}
    session_factory = get_session_factory()

    while True:
        async with session_factory() as session:
            pending_tasks = await get_pending_tasks(job_id, session)
            if not pending_tasks:
                return

            logger.info("Polling %d pending tasks for job %s", len(pending_tasks), job_id)
            for manus_task in pending_tasks:
                try:
                    task_response = await manus_client.get_task(manus_task.id)
                except Exception as exc:
                    attempts[manus_task.id] = attempts.get(manus_task.id, 0) + 1
                    logger.warning(
                        "Polling failed for Manus task %s (attempt %d): %s",
                        manus_task.id,
                        attempts[manus_task.id],
                        exc,
                    )
                    if attempts[manus_task.id] >= 3:
                        manus_task.status = "failed"
                        manus_task.error = f"Polling failed: {exc}"
                        manus_task.completed_at = datetime.utcnow()
                        try:
                            await session.commit()
                        except Exception:
                            await session.rollback()
                            logger.exception(
                                "Failed to mark Manus task %s as failed after polling",
                                manus_task.id,
                            )
                            raise
                    continue

                status = _get_task_status(task_response)
                if status in {"completed", "failed"}:
                    await handle_task_completion(manus_task.id, session)

        await asyncio.sleep(settings.app.manus_polling_interval)


async def resume_enrichment(job_id: str, session) -> dict[str, int]:
    """Resume Manus enrichment for a job after restart."""
    settings = get_settings()
    manus_client = create_manus_client()
    job = await create_project(job_id, session)
    if not job or not job.manus_project_id:
        raise ValueError(f"Missing Manus project id for job {job_id}")
    project_id = job.manus_project_id

    pending_stmt = select(Company).where(
        Company.job_id == job_id,
        Company.filtered_out.is_(False),
        Company.ai_classification == "valid_b2b",
        Company.enrichment_status == "pending",
    )
    pending_companies = (await session.execute(pending_stmt)).scalars().all()

    task_stmt = select(ManusTask.company_id, ManusTask.status).where(
        ManusTask.job_id == job_id
    )
    task_rows = (await session.execute(task_stmt)).all()
    existing_task_company_ids = {row[0] for row in task_rows}

    companies_to_create = [
        company for company in pending_companies if company.id not in existing_task_company_ids
    ]

    new_tasks = 0
    batch_size = settings.app.stream_batch_size
    for idx in range(0, len(companies_to_create), batch_size):
        batch = companies_to_create[idx : idx + batch_size]
        for company in batch:
            metadata = {"job_id": job_id, "company_id": company.id}
            try:
                task_response = await manus_client.create_task(
                    _build_company_payload(company),
                    metadata,
                    project_id=project_id,
                )
            except Exception as exc:
                logger.error(
                    "Failed to create Manus task during resume for company %s: %s",
                    company.id,
                    exc,
                )
                company.retry_count += 1
                company.last_error = f"Manus task creation failed: {exc}"
                company.error_timestamp = datetime.utcnow()
                continue

            task_id = _extract_task_id(task_response)
            if not task_id:
                logger.error(
                    "Manus task creation returned no task id for company %s",
                    company.id,
                )
                company.retry_count += 1
                company.last_error = "Manus task creation missing task id"
                company.error_timestamp = datetime.utcnow()
                continue

            session.add(
                ManusTask(
                    id=task_id,
                    job_id=job_id,
                    company_id=company.id,
                    status="pending",
                )
            )
            new_tasks += 1

        try:
            await session.commit()
        except Exception:
            await session.rollback()
            logger.exception("Database error while resuming Manus tasks for job %s", job_id)
            raise

    resumed_tasks = sum(1 for _, status in task_rows if status == "pending")
    if resumed_tasks:
        await poll_pending_tasks(job_id)

    return {"new_tasks": new_tasks, "resumed_tasks": resumed_tasks}


async def retry_company_visible(company_id: str, session) -> str:
    """Re-run Manus enrichment for a single company with hideInTaskList=False.

    Resets the company enrichment state, creates a new visible Manus task,
    and kicks off polling. Returns the new Manus task ID.
    """
    company = await session.get(Company, company_id)
    if not company:
        raise ValueError(f"Company {company_id} not found")

    job = await session.get(Job, company.job_id)
    if not job:
        raise ValueError(f"Job {company.job_id} not found")

    # Reset company enrichment state
    company.enrichment_status = "pending"
    company.email = None
    company.email_source = None
    company.last_error = None
    company.error_timestamp = None

    # Ensure project exists
    if not job.manus_project_id:
        await create_project(job.id, session)
        await session.refresh(job)

    manus_client = create_manus_client()
    metadata = {"job_id": job.id, "company_id": company.id}

    task_response = await manus_client.create_task(
        _build_company_payload(company),
        metadata,
        project_id=job.manus_project_id,
        hide_in_task_list=False,
    )

    task_id = _extract_task_id(task_response)
    if not task_id:
        raise ValueError("Manus task creation returned no task id")

    manus_task = ManusTask(
        id=task_id,
        job_id=job.id,
        company_id=company.id,
        status="pending",
    )
    session.add(manus_task)
    await session.commit()

    # Start polling for this job so the result gets picked up
    asyncio.create_task(poll_pending_tasks(job.id))

    logger.info(
        "Created visible Manus task %s for company %s (debug retry)",
        task_id,
        company_id,
    )
    return task_id


async def cleanup_tasks(job_id: str, session) -> int:
    """Delete Manus tasks after job completion to save API quota."""
    job = await session.get(Job, job_id)
    if not job:
        logger.warning("Job %s not found for Manus cleanup", job_id)
        return 0
    if job.status not in {"completed", "failed"}:
        logger.info("Skipping Manus cleanup for job %s with status %s", job_id, job.status)
        return 0

    manus_client = create_manus_client()
    stmt = select(ManusTask).where(ManusTask.job_id == job_id)
    tasks = (await session.execute(stmt)).scalars().all()

    deleted = 0
    for task in tasks:
        try:
            await manus_client.delete_task(task.id)
        except Exception as exc:
            logger.error("Failed to delete Manus task %s: %s", task.id, exc)
            continue
        deleted += 1

    logger.info("Deleted %d Manus tasks for job %s", deleted, job_id)
    return deleted
