"""Bounded Dramatiq entry point for fenced CAGG refresh jobs."""

import asyncio
import logging
from typing import Optional

import dramatiq
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.services.cagg_refresh import CaggRefreshService
from app.workers.config import broker  # configure broker before actor registration

logger = logging.getLogger(__name__)


def _session_factory_for_actor() -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False), engine


async def _run_cagg_refresh(
    *,
    max_jobs: Optional[int] = None,
    session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
) -> int:
    max_jobs = settings.CAGG_REFRESH_MAX_JOBS_PER_RUN if max_jobs is None else max_jobs
    if max_jobs <= 0:
        raise ValueError("max_jobs must be positive")
    owns_engine = session_factory is None
    engine = None
    if session_factory is None:
        session_factory, engine = _session_factory_for_actor()
    try:
        service = CaggRefreshService(session_factory=session_factory)
        processed = 0
        while processed < max_jobs:
            claimed = await service.process_pending_jobs(
                respect_retry_schedule=True,
                raise_on_attempt_error=False,
            )
            if not claimed:
                break
            processed += 1
        return processed
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()


@dramatiq.actor(queue_name="cagg_refresh", max_retries=3)
def process_cagg_refresh() -> None:
    processed = asyncio.run(_run_cagg_refresh())
    logger.info("CAGG refresh worker processed %s job(s)", processed)
