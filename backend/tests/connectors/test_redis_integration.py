"""Real Redis acceptance tests for global limits and ownership leases.

These tests intentionally require a reachable Redis instance; they are not
fakeredis substitutes and must run in the isolated Compose verification stack.
"""

import asyncio
import uuid

import pytest
import pytest_asyncio
import redis.asyncio as redis_async

from app.connectors import rate_limiter as rate_limiter_module
from app.connectors.rate_limiter import GlobalRateLimiter
from app.core.config import settings
from app.services.ws_sharding.lease import ShardLeaseManager


pytestmark = pytest.mark.redis


@pytest_asyncio.fixture
async def real_redis():
    client = redis_async.Redis.from_url(
        settings.REDIS_URL,
        socket_connect_timeout=settings.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=settings.REDIS_SOCKET_TIMEOUT_SECONDS,
    )
    await client.ping()
    try:
        yield client
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_global_rate_limiter_is_atomic_across_concurrent_instances(real_redis, monkeypatch):
    key = f"quantive:test:rate-limit:{uuid.uuid4().hex}"
    monkeypatch.setattr(rate_limiter_module, "get_async_redis", lambda: real_redis)
    limiters = [GlobalRateLimiter(key=key) for _ in range(20)]
    for limiter in limiters:
        limiter.capacity = 7
        limiter.rate = 0.001

    admitted = await asyncio.gather(*(limiter.acquire() for limiter in limiters))

    assert sum(admitted) == 7
    await real_redis.delete(key)


@pytest.mark.asyncio
async def test_rate_limiter_fails_closed_for_a_real_unreachable_redis_endpoint(monkeypatch):
    unavailable = redis_async.Redis.from_url(
        "redis://127.0.0.1:1/0",
        socket_connect_timeout=0.01,
        socket_timeout=0.01,
    )
    monkeypatch.setattr(rate_limiter_module, "get_async_redis", lambda: unavailable)
    limiter = GlobalRateLimiter(key=f"quantive:test:unavailable:{uuid.uuid4().hex}")
    try:
        assert await limiter.acquire() is False
    finally:
        await unavailable.aclose()


@pytest.mark.asyncio
async def test_redis_lease_owner_cannot_be_stolen_or_mutate_after_expiry(real_redis):
    prefix = f"quantive:test:lease:{uuid.uuid4().hex}"
    worker_a = ShardLeaseManager(redis_client=real_redis, key_prefix=prefix, lease_ttl_seconds=0.05)
    worker_b = ShardLeaseManager(redis_client=real_redis, key_prefix=prefix, lease_ttl_seconds=0.05)

    claim_a = await worker_a.acquire_shard_lease(0, "worker-a")
    assert claim_a is not None
    assert await worker_b.acquire_shard_lease(0, "worker-b") is None
    assert await worker_a.renew_shard_lease(0, claim_a, ttl_seconds=0.05) is True

    await asyncio.sleep(0.08)
    claim_b = await worker_b.acquire_shard_lease(0, "worker-b")
    assert claim_b is not None
    assert claim_b.fencing_token == claim_a.fencing_token + 1
    assert await worker_a.renew_shard_lease(0, claim_a, ttl_seconds=0.05) is False
    assert await worker_a.release_shard_lease(0, claim_a) is False
    assert await worker_b.release_shard_lease(0, claim_b) is True
    await real_redis.delete(worker_a._get_fencing_key(0))
