"""Dramatiq entry point for bounded live-strategy observation.

The actor deliberately receives no exchange messages. It polls canonical
database tables only after the normal ingestion path has validated and
persisted candles.
"""

import asyncio
import logging
import uuid

import dramatiq
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.services.live_strategy import LiveStrategyEvaluatorService
from app.workers.config import broker, redis_client  # Configure Redis before actor registration.

logger = logging.getLogger(__name__)

# Dramatiq executes this actor with asyncio.run; asyncpg pooled connections are
# loop-affine and must not outlive the loop that created them.
engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionMaker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

_DISPATCH_LEASE_KEY = "quantive:live-strategy:dispatch-lease"
_RELEASE_DISPATCH_LEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


async def _run_live_strategy_evaluation() -> int:
    return await LiveStrategyEvaluatorService.process_active(AsyncSessionMaker)


def enqueue_live_strategy_evaluation() -> bool:
    """Send at most one outstanding bounded evaluation message.

    The Redis value is an ownership token, not a boolean lock. A worker only
    releases the lease it acquired, so an expired lease cannot delete a newer
    owner's scheduling state. Redis failure schedules no work, which is safer
    than creating an unbounded message backlog.
    """
    token = uuid.uuid4().hex
    try:
        acquired = redis_client.set(
            _DISPATCH_LEASE_KEY,
            token,
            nx=True,
            ex=settings.LIVE_STRATEGY_DISPATCH_LEASE_SECONDS,
        )
    except Exception:
        logger.exception("Unable to acquire live strategy dispatch lease")
        return False
    if not acquired:
        return False

    try:
        process_live_strategies.send(token)
    except Exception:
        _release_dispatch_lease(token)
        raise
    return True


def _release_dispatch_lease(token: str) -> None:
    try:
        redis_client.eval(_RELEASE_DISPATCH_LEASE, 1, _DISPATCH_LEASE_KEY, token)
    except Exception:
        # Expiry provides crash recovery; a release error must not hide the
        # evaluation outcome or allow an unsafe unconditional delete.
        logger.exception("Unable to release live strategy dispatch lease")


@dramatiq.actor(queue_name="live_strategy", max_retries=3)
def process_live_strategies(dispatch_token: str | None = None) -> None:
    """Evaluate a bounded batch of every active strategy runtime."""
    try:
        processed = asyncio.run(_run_live_strategy_evaluation())
        logger.info("Live strategy worker evaluated %s canonical candle(s)", processed)
    finally:
        if dispatch_token is not None:
            _release_dispatch_lease(dispatch_token)
