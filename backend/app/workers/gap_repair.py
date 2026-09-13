"""Bounded Dramatiq entry points for durable gap repair."""

import asyncio
import logging
from datetime import timedelta
from typing import Optional

import dramatiq
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.services.gap_repair import GapRepairService
from app.workers.config import broker, close_async_redis_for_current_loop  # configure broker before actor registration

logger = logging.getLogger(__name__)


def _session_factory_for_actor() -> tuple[async_sessionmaker[AsyncSession], object]:
    """Create an engine inside the actor's event loop, never reuse it cross-loop."""
    engine = create_async_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False), engine


async def _run_gap_repair_worker(
    lease_minutes: int = 5,
    *,
    max_jobs: Optional[int] = None,
    session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
) -> int:
    max_jobs = settings.GAP_REPAIR_MAX_JOBS_PER_RUN if max_jobs is None else max_jobs
    if max_jobs <= 0:
        raise ValueError("max_jobs must be positive")
    owns_engine = session_factory is None
    engine = None
    if session_factory is None:
        session_factory, engine = _session_factory_for_actor()
    try:
        service = GapRepairService(session_factory=session_factory)
        processed = 0
        while processed < max_jobs:
            claimed = await service.process_next_job(
                lease_duration=timedelta(minutes=lease_minutes),
                respect_retry_schedule=True,
                # Durable retry state owns expected transient failures.  A broker
                # retry here would immediately wake a still-not-ready row.
                raise_on_attempt_error=False,
            )
            if not claimed:
                break
            processed += 1
        return processed
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()
        if owns_engine:
            await close_async_redis_for_current_loop()


async def _run_gap_scan_and_schedule(
    lookback_hours: int = 24,
    *,
    session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
) -> int:
    owns_engine = session_factory is None
    engine = None
    if session_factory is None:
        session_factory, engine = _session_factory_for_actor()
    try:
        service = GapRepairService(session_factory=session_factory)
        jobs = await service.scan_and_schedule_active_assets(
            lookback_window=timedelta(hours=lookback_hours),
            max_assets=settings.GAP_REPAIR_MAX_ASSETS_PER_SCAN,
            max_jobs=settings.GAP_REPAIR_MAX_JOBS_PER_SCAN,
        )
        logger.info("Gap scan scheduled/coalesced %s job(s)", len(jobs))
        return len(jobs)
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()
        if owns_engine:
            await close_async_redis_for_current_loop()


@dramatiq.actor(queue_name="gap_repair", max_retries=3)
def process_gap_repair_job() -> None:
    """Process only a bounded durable-job slice and then wake downstream work."""
    processed = asyncio.run(_run_gap_repair_worker())
    logger.info("Gap repair worker processed %s job(s)", processed)
    if processed:
        # A historical repair parks itself in AWAITING_MERGE instead of retrying
        # REST.  Wake promotion and CAGG workers; both have their own leases.
        from app.workers.cagg_refresh import process_cagg_refresh
        from app.workers.historical_merge import process_historical_merge

        process_historical_merge.send()
        process_cagg_refresh.send()


@dramatiq.actor(queue_name="gap_repair", max_retries=3)
def scan_gaps_and_schedule() -> None:
    """Page active Binance assets and wake one bounded repair run."""
    asyncio.run(_run_gap_scan_and_schedule())
    process_gap_repair_job.send()
