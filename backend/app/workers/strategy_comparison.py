"""Coalesced worker for bounded canonical strategy-comparison production."""

from __future__ import annotations

import asyncio
import logging
import uuid

import dramatiq
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.services.strategy_monitoring import StrategyComparisonProducerService
from app.workers.config import broker, redis_client  # configure broker before actor registration


logger = logging.getLogger(__name__)

# This actor is invoked through asyncio.run, so a connection cannot outlive its
# event loop.  A new connection per operation is safer than cross-loop pooling.
engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionMaker = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

_DISPATCH_LEASE_KEY = "quantive:strategy-comparison:dispatch-lease"
_RELEASE_DISPATCH_LEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


async def _run_strategy_comparison_production() -> int:
    return await StrategyComparisonProducerService.process_active(AsyncSessionMaker)


def enqueue_strategy_comparison_production() -> bool:
    """Schedule at most one bounded producer message; Redis failure is closed."""
    token = uuid.uuid4().hex
    try:
        acquired = redis_client.set(
            _DISPATCH_LEASE_KEY,
            token,
            nx=True,
            ex=settings.STRATEGY_COMPARISON_DISPATCH_LEASE_SECONDS,
        )
    except Exception:
        logger.exception("Unable to acquire strategy comparison dispatch lease")
        return False
    if not acquired:
        return False
    try:
        process_strategy_comparisons.send(token)
    except Exception:
        _release_dispatch_lease(token)
        raise
    return True


def _release_dispatch_lease(token: str) -> None:
    try:
        redis_client.eval(_RELEASE_DISPATCH_LEASE, 1, _DISPATCH_LEASE_KEY, token)
    except Exception:
        # Token expiry is restart recovery; never delete a successor's lease.
        logger.exception("Unable to release strategy comparison dispatch lease")


@dramatiq.actor(queue_name="strategy_comparison", max_retries=3)
def process_strategy_comparisons(dispatch_token: str | None = None) -> None:
    try:
        produced = asyncio.run(_run_strategy_comparison_production())
        logger.info("Strategy comparison producer persisted %s comparison run(s)", produced)
    finally:
        if dispatch_token is not None:
            _release_dispatch_lease(dispatch_token)
