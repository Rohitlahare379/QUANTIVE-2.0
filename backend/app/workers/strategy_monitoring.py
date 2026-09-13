"""Dramatiq entry point for bounded strategy-divergence monitoring.

The monitoring worker reads only durable comparison output and runtime state.
It does not take part in exchange transport or live candle ingestion.  Redis is
used solely to coalesce scheduler wake-ups; an ownership token prevents an
expired worker from releasing a newer worker's lease.
"""

import asyncio
import logging
import uuid

import dramatiq
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.services.strategy_monitoring import StrategyMonitoringService
from app.workers.config import broker, redis_client  # Configure Redis before actor registration.

logger = logging.getLogger(__name__)

# Dramatiq invokes this actor through ``asyncio.run``.  A pooled asyncpg
# connection belongs to the loop which created it, so use NullPool here rather
# than reusing a connection across closed actor event loops.
engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionMaker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

_DISPATCH_LEASE_KEY = "quantive:strategy-monitoring:dispatch-lease"
_RELEASE_DISPATCH_LEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


async def _run_strategy_monitoring() -> int:
    return await StrategyMonitoringService.process_active(AsyncSessionMaker)


def enqueue_strategy_monitoring() -> bool:
    """Send at most one outstanding bounded monitoring message.

    Redis failures fail closed: monitoring is not enqueued without a durable
    coalescing lease, avoiding an unbounded backlog if Redis is unavailable.
    """
    token = uuid.uuid4().hex
    try:
        acquired = redis_client.set(
            _DISPATCH_LEASE_KEY,
            token,
            nx=True,
            ex=settings.STRATEGY_MONITORING_DISPATCH_LEASE_SECONDS,
        )
    except Exception:
        logger.exception("Unable to acquire strategy monitoring dispatch lease")
        return False
    if not acquired:
        return False

    try:
        process_strategy_monitoring.send(token)
    except Exception:
        _release_dispatch_lease(token)
        raise
    return True


def _release_dispatch_lease(token: str) -> None:
    """Release only a lease still owned by ``token``."""
    try:
        redis_client.eval(_RELEASE_DISPATCH_LEASE, 1, _DISPATCH_LEASE_KEY, token)
    except Exception:
        # Lease expiry is crash recovery.  An unconditional delete would be
        # unsafe because another scheduler may already own a renewed lease.
        logger.exception("Unable to release strategy monitoring dispatch lease")


@dramatiq.actor(queue_name="strategy_monitoring", max_retries=3)
def process_strategy_monitoring(dispatch_token: str | None = None) -> None:
    """Process a bounded set of active monitoring bindings."""
    try:
        processed = asyncio.run(_run_strategy_monitoring())
        logger.info("Strategy monitoring worker processed %s comparison run(s)", processed)
    finally:
        if dispatch_token is not None:
            _release_dispatch_lease(dispatch_token)
