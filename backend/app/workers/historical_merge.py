"""Bounded Dramatiq entry point for durable historical promotion."""

import asyncio
import logging
from typing import Optional

import dramatiq
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.services.historical_merge import HistoricalMergeService
from app.workers.config import broker  # configure broker before actor registration

logger = logging.getLogger(__name__)


def _session_factory_for_actor() -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False), engine


async def _run_historical_merge(
    *,
    max_jobs: Optional[int] = None,
    max_schedule_days: Optional[int] = None,
    page_size: Optional[int] = None,
    session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
) -> int:
    owns_engine = session_factory is None
    engine = None
    if session_factory is None:
        session_factory, engine = _session_factory_for_actor()
    try:
        service = HistoricalMergeService(session_factory)
        return await service.process_available_jobs(
            max_jobs=settings.HISTORICAL_MERGE_MAX_JOBS_PER_RUN if max_jobs is None else max_jobs,
            max_schedule_days=(
                settings.HISTORICAL_MERGE_MAX_SCHEDULE_DAYS
                if max_schedule_days is None
                else max_schedule_days
            ),
            page_size=settings.HISTORICAL_MERGE_PAGE_SIZE if page_size is None else page_size,
            respect_retry_schedule=True,
            raise_on_attempt_error=False,
        )
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()


@dramatiq.actor(queue_name="historical_merge", max_retries=3)
def process_historical_merge() -> None:
    """Promote a bounded day slice, then wake dependent repair/aggregation work."""
    merged_days = asyncio.run(_run_historical_merge())
    logger.info("Historical merge worker processed %s day job(s)", merged_days)
    if merged_days:
        from app.workers.cagg_refresh import process_cagg_refresh
        from app.workers.gap_repair import process_gap_repair_job

        # Newly canonical historical windows requeue their AWAITING_MERGE gap job
        # without another REST download; CAGG jobs are coalesced by their table.
        process_gap_repair_job.send()
        process_cagg_refresh.send()
