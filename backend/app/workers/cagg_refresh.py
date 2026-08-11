import asyncio
import logging
import dramatiq
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.services.cagg_refresh import CaggRefreshService

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.sqlalchemy_database_uri)
AsyncSessionMaker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

async def _run_cagg_refresh():
    service = CaggRefreshService(session_factory=AsyncSessionMaker)
    await service.process_pending_jobs()

@dramatiq.actor(queue_name="cagg_refresh", max_retries=3)
def process_cagg_refresh():
    """Worker actor to process pending CAGG refresh jobs."""
    logger.info("Executing CAGG refresh task")
    asyncio.run(_run_cagg_refresh())
