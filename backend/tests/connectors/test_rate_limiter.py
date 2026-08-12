import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch
from app.connectors.rate_limiter import GlobalRateLimiter
from app.core.config import settings

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
        assert isinstance(args_passed[3], float) # timestamp

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
