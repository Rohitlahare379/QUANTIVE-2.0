import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import dramatiq
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.workers.config import get_async_redis
from app.core.config import settings
from app.connectors.binance import BinanceClient
from app.services.ingestion import IngestionService
from app.connectors.exceptions import APIError, NetworkError, RateLimitError, TemporaryBanError

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.sqlalchemy_database_uri)
AsyncSessionMaker = async_sessionmaker(engine, expire_on_commit=False)

class RetryableError(Exception):
    """Exception mapped to Dramatiq's retry policy."""
    pass

class PermanentError(Exception):
    """Exception that causes the job to fail immediately without retries."""
    pass

def _parse_time(time_str: str) -> datetime:
    return datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)

async def _run_sync(asset_id: int, symbol: str, start_time: datetime, end_time: datetime):
    redis_client = get_async_redis()
    lock_key = f"lock:sync:asset_{asset_id}"
    
    lock = redis_client.lock(lock_key, timeout=settings.WORKER_LOCK_TIMEOUT_SECONDS)
    
    acquired = await lock.acquire(blocking=False)
    if not acquired:
        logger.warning(f"Failed to acquire lock for asset {asset_id} ({symbol}). Skipping sync.")
        return

    try:
        async with AsyncSessionMaker() as db:
            async with BinanceClient() as client:
                service = IngestionService(db_session=db, binance_client=client)
                try:
                    await service.sync_asset(asset_id, symbol, start_time, end_time)
                except (NetworkError, RateLimitError, TemporaryBanError) as e:
                    logger.warning(f"Transient error during sync for {symbol}: {e}")
                    raise RetryableError(str(e)) from e
                except APIError as e:
                    logger.error(f"Permanent API error for {symbol}: {e}. Discarding job.")
                    raise PermanentError(str(e)) from e
    finally:
        try:
            await lock.release()
        except Exception:
            pass


@dramatiq.actor(queue_name="gap_repair", max_retries=5, throws=(RetryableError,))
def gap_repair_job(asset_id: int, symbol: str, gap_start_str: str, gap_end_str: str):
    """Highest priority. Syncs a specific gap."""
    start_time = _parse_time(gap_start_str)
    end_time = _parse_time(gap_end_str)
    logger.info(f"Starting gap_repair_job for {symbol}: {start_time} -> {end_time}")
    asyncio.run(_run_sync(asset_id, symbol, start_time, end_time))


@dramatiq.actor(queue_name="daily_update", max_retries=5, throws=(RetryableError,))
def daily_update_job(asset_id: int, symbol: str):
    """Medium priority. Syncs the last 24 hours."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - relativedelta(days=1)
    logger.info(f"Starting daily_update_job for {symbol}: {start_time} -> {end_time}")
    asyncio.run(_run_sync(asset_id, symbol, start_time, end_time))


@dramatiq.actor(queue_name="historical_backfill", max_retries=5)
def full_historical_sync_job(asset_id: int, symbol: str, start_year: int):
    """Low priority. Splits a massive multi-year backfill into 1-month chunks."""
    start_time = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    end_time = datetime.now(timezone.utc)
    
    logger.info(f"Orchestrating full_historical_sync_job for {symbol} from {start_year}")
    
    current = start_time
    while current < end_time:
        next_month = current + relativedelta(months=1)
        chunk_end = min(next_month, end_time)
        
        # Enqueue the chunk to the generic backfill worker
        sync_asset_job.send(asset_id, symbol, current.isoformat(), chunk_end.isoformat())
        
        current = next_month

@dramatiq.actor(queue_name="historical_backfill", max_retries=5, throws=(RetryableError,))
def sync_asset_job(asset_id: int, symbol: str, start_time_str: str, end_time_str: str):
    """Base worker for historical backfill chunks."""
    start_time = _parse_time(start_time_str)
    end_time = _parse_time(end_time_str)
    logger.info(f"Starting sync_asset_job for {symbol}: {start_time} -> {end_time}")
    asyncio.run(_run_sync(asset_id, symbol, start_time, end_time))
