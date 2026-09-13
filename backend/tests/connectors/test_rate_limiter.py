import asyncio

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from app.connectors.rate_limiter import GlobalRateLimiter, TOKEN_BUCKET_LUA
from app.connectors.binance import BinanceClient
from app.core.config import settings


def test_binance_client_construction_does_not_require_a_running_event_loop():
    """Synchronous setup must not create a loop-bound Redis client eagerly."""
    client = BinanceClient()

    assert client.rate_limiter.redis is None
    assert client.rate_limiter._script is None


def test_redis_clients_have_finite_connect_and_command_timeouts():
    """A network partition must fence leases/rate limiting instead of hanging forever."""
    from app.workers.config import broker, redis_client
    from app.api.dependencies import limiter

    for client in (redis_client, broker.client, limiter._storage.storage):
        kwargs = client.connection_pool.connection_kwargs
        assert kwargs["socket_connect_timeout"] == settings.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS
        assert kwargs["socket_timeout"] == settings.REDIS_SOCKET_TIMEOUT_SECONDS
        assert client.connection_pool.max_connections == settings.REDIS_MAX_CONNECTIONS


@pytest.mark.asyncio
async def test_public_redis_probe_gate_is_bounded_under_concurrent_requests(monkeypatch):
    """Readiness/metrics bursts are rejected before they can consume an unbounded pool."""
    from app.workers import config as worker_config

    worker_config._async_redis_probe_semaphores.clear()
    monkeypatch.setattr(settings, "REDIS_PROBE_MAX_CONCURRENCY", 1)
    monkeypatch.setattr(settings, "REDIS_PROBE_ACQUIRE_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(worker_config, "get_async_redis", lambda: MagicMock())

    entered = asyncio.Event()
    release = asyncio.Event()

    async def hold_one_probe():
        async with worker_config.borrow_async_redis_for_probe():
            entered.set()
            await release.wait()

    holder = asyncio.create_task(hold_one_probe())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        with pytest.raises(worker_config.RedisProbeCapacityError, match="capacity is saturated"):
            async with worker_config.borrow_async_redis_for_probe():
                pytest.fail("second concurrent public probe must not be admitted")
    finally:
        release.set()
        await holder
        worker_config._async_redis_probe_semaphores.clear()

@pytest.mark.asyncio
async def test_rate_limiter_success():
    with patch("app.connectors.rate_limiter.get_async_redis") as mock_get_redis:
        mock_redis = AsyncMock()
        mock_script = AsyncMock(return_value=1)
        mock_redis.register_script = MagicMock(return_value=mock_script)
        mock_get_redis.return_value = mock_redis
        
        limiter = GlobalRateLimiter()
        has_tokens = await limiter.acquire(weight=2)
        
        assert has_tokens is True
        mock_script.assert_awaited_once()
        args_passed = mock_script.call_args[1]["args"]
        assert args_passed[0] == settings.BINANCE_GLOBAL_WEIGHT_CAPACITY
        assert args_passed[1] == settings.BINANCE_GLOBAL_WEIGHT_REFILL_RATE
        assert args_passed[2] == 2 # weight
        assert args_passed == [
            settings.BINANCE_GLOBAL_WEIGHT_CAPACITY,
            settings.BINANCE_GLOBAL_WEIGHT_REFILL_RATE,
            2,
        ]
        # The shared Redis clock, rather than local worker time, is part of the
        # atomic script.  This guards against distributed clock-skew minting.
        assert 'redis.call("TIME")' in TOKEN_BUCKET_LUA

@pytest.mark.asyncio
async def test_rate_limiter_exhausted():
    with patch("app.connectors.rate_limiter.get_async_redis") as mock_get_redis:
        mock_redis = AsyncMock()
        mock_script = AsyncMock(return_value=0)
        mock_redis.register_script = MagicMock(return_value=mock_script)
        mock_get_redis.return_value = mock_redis
        
        limiter = GlobalRateLimiter()
        has_tokens = await limiter.acquire(weight=2)
        
        assert has_tokens is False

@pytest.mark.asyncio
async def test_rate_limiter_fail_closed_on_redis_error():
    with patch("app.connectors.rate_limiter.get_async_redis") as mock_get_redis:
        mock_redis = AsyncMock()
        mock_script = AsyncMock(side_effect=Exception("Redis connection lost"))
        mock_redis.register_script = MagicMock(return_value=mock_script)
        mock_get_redis.return_value = mock_redis
        
        limiter = GlobalRateLimiter()
        has_tokens = await limiter.acquire(weight=2)
        
        # We explicitly fail-closed to protect the IP ban margin
        assert has_tokens is False


@pytest.mark.asyncio
@pytest.mark.parametrize("weight", [0, -1, True, 1.5, settings.BINANCE_GLOBAL_WEIGHT_CAPACITY + 1])
async def test_rate_limiter_rejects_invalid_or_unfulfillable_weight_without_calling_redis(weight):
    """Malformed callers cannot mint tokens or generate a retry storm."""
    with patch("app.connectors.rate_limiter.get_async_redis") as mock_get_redis:
        mock_redis = AsyncMock()
        mock_script = AsyncMock(return_value=1)
        mock_redis.register_script = MagicMock(return_value=mock_script)
        mock_get_redis.return_value = mock_redis

        limiter = GlobalRateLimiter()
        assert await limiter.acquire(weight=weight) is False
        mock_script.assert_not_awaited()
