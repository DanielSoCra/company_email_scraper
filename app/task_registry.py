"""Task registry for tracking active job tasks for graceful shutdown."""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

# Track active job tasks for graceful shutdown
_active_job_tasks: set[asyncio.Task] = set()


def register_job_task(task: asyncio.Task) -> None:
    """Register an active job processing task."""
    _active_job_tasks.add(task)
    task.add_done_callback(_active_job_tasks.discard)


async def wait_for_active_jobs(timeout: int = 300) -> None:
    """Wait for active job tasks to complete with timeout."""
    if not _active_job_tasks:
        return

    logger.info("Waiting for %d active job tasks to complete...", len(_active_job_tasks))
    try:
        await asyncio.wait_for(
            asyncio.gather(*_active_job_tasks, return_exceptions=True),
            timeout=timeout
        )
        logger.info("All job tasks completed")
    except asyncio.TimeoutError:
        logger.warning("Job tasks did not complete within %d seconds, forcing shutdown", timeout)
