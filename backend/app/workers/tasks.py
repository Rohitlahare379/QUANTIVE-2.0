"""Compatibility actors that now enqueue durable reconciliation work.

No production actor in this module opens a Binance stream directly.  The old
direct-sync helper remains for explicit compatibility/testing only; all actor
paths hand ranges to the fenced GapRepairJob state machine.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import dramatiq
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.connectors.binance import BinanceClient
from app.connectors.exceptions import APIError, NetworkError, RateLimitError, TemporaryBanError
from app.core.config import settings
from app.services.gap_repair import GapRepairService
from app.services.ingestion import IngestionService
from app.workers.config import close_async_redis_for_current_loop, get_async_redis

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """A transient direct-compatibility failure, eligible for Dramatiq retry."""


class PermanentError(Exception):
    """A direct-compatibility failure that must not be retried."""


def _session_factory_for_run() -> tuple[async_sessionmaker[AsyncSession], object]:
    engine = create_async_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
    return async_sessionmaker(engine, expire_on_commit=False), engine


def _parse_time(time_str: str) -> datetime:
    value = datetime.fromisoformat(time_str)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _run_sync(asset_id: int, symbol: str, start_time: datetime, end_time: datetime) -> None:
    """Deprecated direct helper kept outside normal actor routing.

    It creates/disposes its pool inside this event loop and retains no database
    connection across Binance network work (``sync_asset`` explicitly releases
    its coverage read before streaming).
    """
    redis_client = get_async_redis()
    lock = redis_client.lock(f"lock:sync:asset_{asset_id}", timeout=settings.WORKER_LOCK_TIMEOUT_SECONDS)
    acquired = await lock.acquire(blocking=False)
    if not acquired:
        logger.warning("Direct compatibility sync lock busy for asset %s", asset_id)
        await close_async_redis_for_current_loop()
        return
    session_factory, engine = _session_factory_for_run()
    try:
        async with session_factory() as db:
            async with BinanceClient() as client:
                service = IngestionService(db_session=db, binance_client=client)
                try:
                    await service.sync_asset(asset_id, symbol, start_time, end_time)
                except (NetworkError, RateLimitError, TemporaryBanError) as exc:
                    raise RetryableError(str(exc)) from exc
                except APIError as exc:
                    raise PermanentError(str(exc)) from exc
    finally:
        try:
            await lock.release()
        finally:
            await engine.dispose()
            await close_async_redis_for_current_loop()


async def _schedule_durable_repair(
    asset_id: int, symbol: str, start_time: datetime, end_time: datetime
) -> bool:
    """Persist a repair request only; the durable worker owns Binance I/O."""
    session_factory, engine = _session_factory_for_run()
    try:
        job = await GapRepairService(session_factory).schedule_repair_job(
            asset_id=asset_id,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
        )
        return job is not None
    finally:
        await engine.dispose()
        await close_async_redis_for_current_loop()


def _wake_gap_repair_worker() -> None:
    # Delayed import avoids circular actor registration at module import time.
    from app.workers.gap_repair import process_gap_repair_job

    process_gap_repair_job.send()


@dramatiq.actor(queue_name="gap_repair", max_retries=5, throws=(PermanentError,))
def gap_repair_job(asset_id: int, symbol: str, gap_start_str: str, gap_end_str: str) -> None:
    """Legacy API: enqueue a durable exact repair range, never direct REST work."""
    if asyncio.run(_schedule_durable_repair(asset_id, symbol, _parse_time(gap_start_str), _parse_time(gap_end_str))):
        _wake_gap_repair_worker()


@dramatiq.actor(queue_name="daily_update", max_retries=5, throws=(PermanentError,))
def daily_update_job(asset_id: int, symbol: str) -> None:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - relativedelta(days=1)
    if asyncio.run(_schedule_durable_repair(asset_id, symbol, start_time, end_time)):
        _wake_gap_repair_worker()


@dramatiq.actor(queue_name="historical_backfill", max_retries=5, throws=(PermanentError,))
def full_historical_sync_job(
    asset_id: int,
    symbol: str,
    start_year: int,
    cursor_time_str: Optional[str] = None,
) -> None:
    """Emit one month and one continuation, rather than materializing years of jobs."""
    current = _parse_time(cursor_time_str) if cursor_time_str else datetime(start_year, 1, 1, tzinfo=timezone.utc)
    end_time = datetime.now(timezone.utc)
    if current >= end_time:
        return
    chunk_end = min(current + relativedelta(months=1), end_time)
    # The continuation is emitted only after this one durable range has been
    # persisted by ``sync_asset_job``.  This keeps the orchestrator O(1) in
    # memory and avoids enqueuing a multi-year backlog in one invocation.
    sync_asset_job.send(
        asset_id,
        symbol,
        current.isoformat(),
        chunk_end.isoformat(),
        start_year,
    )


@dramatiq.actor(queue_name="historical_backfill", max_retries=5, throws=(PermanentError,))
def sync_asset_job(
    asset_id: int,
    symbol: str,
    start_time_str: str,
    end_time_str: str,
    continuation_start_year: Optional[int] = None,
) -> None:
    """Legacy backfill chunk entry point routed through the durable job table."""
    if asyncio.run(
        _schedule_durable_repair(asset_id, symbol, _parse_time(start_time_str), _parse_time(end_time_str))
    ):
        _wake_gap_repair_worker()
        if continuation_start_year is not None:
            full_historical_sync_job.send(
                asset_id,
                symbol,
                continuation_start_year,
                _parse_time(end_time_str).isoformat(),
            )
