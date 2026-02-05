"""CSV generation service for exporting company data.

This module provides functionality to generate CSV files from company data
in the database, using streaming to handle large datasets efficiently.
"""

from __future__ import annotations

import csv
import logging
from io import StringIO
from typing import AsyncGenerator

from sqlmodel import select

from ..models import Company

logger = logging.getLogger(__name__)

# CSV header columns
CSV_HEADER = ["Name", "Address", "City", "ZIP", "Phone", "Category", "Website", "Email", "Status", "Error"]

# Batch size for streaming companies
STREAM_BATCH_SIZE = 100


def _format_status(enrichment_status: str | None) -> str:
    """Convert enrichment_status to user-friendly status string."""
    if enrichment_status == "completed":
        return "success"
    return "failed"


def _format_value(value: str | None) -> str:
    """Convert None/null values to empty strings."""
    return value if value is not None else ""


def _company_to_row(company: Company) -> list[str]:
    """Convert a Company object to a CSV row."""
    return [
        _format_value(company.name),
        _format_value(company.address),
        _format_value(company.city),
        _format_value(company.postal_code),
        _format_value(company.phone),
        _format_value(company.category),
        _format_value(company.website),
        _format_value(company.email),
        _format_status(company.enrichment_status),
        _format_value(company.last_error) if company.enrichment_status == "failed" else "",
    ]


def _rows_to_csv(rows: list[list[str]]) -> str:
    """Convert rows to CSV string using proper escaping."""
    output = StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows)
    return output.getvalue()


async def generate_csv(job_id: str, session) -> AsyncGenerator[str, None]:
    """Generate CSV data for a job, streaming companies in batches.

    Args:
        job_id: The job ID to export companies for.
        session: Database session.

    Yields:
        CSV string chunks (header first, then batches of company rows).
    """
    logger.info("Starting CSV generation for job %s", job_id)

    # Yield header row
    yield _rows_to_csv([CSV_HEADER])

    # Stream companies in batches using cursor-based pagination
    last_id: str | None = None
    total_rows = 0

    while True:
        stmt = select(Company).where(
            Company.job_id == job_id,
            Company.filtered_out.is_(False),
        )
        if last_id is not None:
            stmt = stmt.where(Company.id > last_id)
        stmt = stmt.order_by(Company.id).limit(STREAM_BATCH_SIZE)

        result = await session.execute(stmt)
        companies = result.scalars().all()

        if not companies:
            break

        # Convert companies to CSV rows
        rows = [_company_to_row(company) for company in companies]
        yield _rows_to_csv(rows)

        total_rows += len(companies)
        last_id = companies[-1].id

        logger.debug(
            "CSV generation progress for job %s: %d rows",
            job_id,
            total_rows,
        )

    logger.info(
        "CSV generation completed for job %s: %d total rows",
        job_id,
        total_rows,
    )
