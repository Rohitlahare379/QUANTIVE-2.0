"""
Dramatiq Worker Actors for Background Gap Repair & Proactive Reconciliation.
"""

import asyncio
import logging
from datetime import timedelta
import dramatiq
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.services.gap_repair import GapRepairService

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
AsyncSessionMaker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


async def _run_gap_repair_worker(lease_minutes: int = 5):
    service = GapRepairService(session_factory=AsyncSessionMaker)
    claimed = await service.process_next_job(lease_duration=timedelta(minutes=lease_minutes))
    if claimed:
        logger.info("Processed a pending gap repair job successfully.")
    else:
        logger.debug("No pending gap repair jobs to process.")


async def _run_gap_scan_and_schedule(lookback_hours: int = 24):
    service = GapRepairService(session_factory=AsyncSessionMaker)
    jobs = await service.scan_and_schedule_active_assets(lookback_window=timedelta(hours=lookback_hours))
    logger.info(f"Scan scheduled {len(jobs)} gap repair jobs.")


@dramatiq.actor(queue_name="gap_repair", max_retries=3)
def process_gap_repair_job():
    """Worker actor to process the next pending gap repair job."""
    logger.info("Executing gap repair worker task")
    asyncio.run(_run_gap_repair_worker())


@dramatiq.actor(queue_name="gap_repair", max_retries=3)
def scan_gaps_and_schedule():
    """Worker actor to scan active assets and schedule missing gap repair jobs."""
    logger.info("Executing periodic gap scanner task")
    asyncio.run(_run_gap_scan_and_schedule())
