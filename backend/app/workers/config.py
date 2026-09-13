import dramatiq
import asyncio
import weakref
from contextlib import asynccontextmanager
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import Retries, TimeLimit, AgeLimit, Callbacks
import redis.asyncio as redis_async
import redis as redis_sync
from app.core.config import settings

# Setup Sync Redis for Broker
_REDIS_CONNECTION_OPTIONS = {
    "socket_connect_timeout": settings.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
    "socket_timeout": settings.REDIS_SOCKET_TIMEOUT_SECONDS,
    "max_connections": settings.REDIS_MAX_CONNECTIONS,
}

redis_client = redis_sync.Redis.from_url(settings.REDIS_URL, **_REDIS_CONNECTION_OPTIONS)
# ``RedisBroker(url=...)`` constructs its own ConnectionPool and discards the
# passed timeout/pool options.  Supply an explicit bounded client instead so
# Dramatiq cannot retain an unbounded socket pool during a Redis outage.
broker_redis_pool = redis_sync.ConnectionPool.from_url(
    settings.REDIS_URL, **_REDIS_CONNECTION_OPTIONS
)
broker_redis_client = redis_sync.Redis(connection_pool=broker_redis_pool)
broker = RedisBroker(
    client=broker_redis_client,
    middleware=[
        Retries(max_retries=5),
        TimeLimit(time_limit=3600000), # 1 hour max
        AgeLimit(max_age=86400000),    # 1 day max in queue
        Callbacks(),
    ]
)

dramatiq.set_broker(broker)

# Async Redis connections are event-loop bound.  Dramatiq's synchronous actors
# intentionally use a fresh ``asyncio.run`` loop per invocation, so a single
# module-level async pool can otherwise hand an asyncpg-style closed-loop
# connection to a later actor.  Cache one client per live loop and expose an
# explicit cleanup hook for bounded actor lifecycles.
_async_redis_clients: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
_async_redis_probe_semaphores: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


class RedisProbeCapacityError(RuntimeError):
    """A public readiness/metrics request exceeded its bounded Redis probe budget."""


def get_async_redis():
    loop = asyncio.get_running_loop()
    client = _async_redis_clients.get(loop)
    if client is None:
        client = redis_async.Redis.from_url(settings.REDIS_URL, **_REDIS_CONNECTION_OPTIONS)
        _async_redis_clients[loop] = client
    return client


async def close_async_redis_for_current_loop() -> None:
    loop = asyncio.get_running_loop()
    client = _async_redis_clients.pop(loop, None)
    _async_redis_probe_semaphores.pop(loop, None)
    if client is not None:
        await client.aclose()


@asynccontextmanager
async def borrow_async_redis_for_probe():
    """Bound concurrent public dependency probes per event loop.

    The API's normal Redis client is shared with rate limiting and lease work.
    Readiness and Prometheus endpoints are intentionally public, so a scrape
    storm must not occupy every connection in that finite pool.  A saturated
    probe budget fails promptly and is surfaced as dependency-unavailable by
    the caller rather than creating unbounded queued tasks/connections.
    """
    loop = asyncio.get_running_loop()
    semaphore = _async_redis_probe_semaphores.get(loop)
    if semaphore is None:
        semaphore = asyncio.BoundedSemaphore(settings.REDIS_PROBE_MAX_CONCURRENCY)
        _async_redis_probe_semaphores[loop] = semaphore

    acquired = False
    try:
        try:
            await asyncio.wait_for(
                semaphore.acquire(), timeout=settings.REDIS_PROBE_ACQUIRE_TIMEOUT_SECONDS
            )
            acquired = True
        except asyncio.TimeoutError as exc:
            raise RedisProbeCapacityError("Redis dependency probe capacity is saturated") from exc
        yield get_async_redis()
    finally:
        if acquired:
            semaphore.release()
