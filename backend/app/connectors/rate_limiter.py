import time
import logging
from app.workers.config import get_async_redis
from app.core.config import settings

logger = logging.getLogger(__name__)

# Redis Lua Script for a Token Bucket Rate Limiter
# Guarantees atomic evaluation across 5,000+ distributed workers
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

local bucket = redis.call("HMGET", key, "tokens", "last_update")
local tokens = tonumber(bucket[1])
local last_update = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    last_update = now
end

local elapsed = math.max(0, now - last_update)
tokens = math.min(capacity, tokens + elapsed * rate)

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_update", now)
    -- Expire key cleanly if no activity happens for the full recharge cycle
    local expire_time = math.ceil(capacity / rate) + 1
    redis.call("EXPIRE", key, expire_time)
    return 1
else
    return 0
end
"""

class GlobalRateLimiter:
    def __init__(self, key: str = "quantive:binance_rate_limit"):
        self.key = key
        self.capacity = settings.BINANCE_GLOBAL_WEIGHT_CAPACITY
        self.rate = settings.BINANCE_GLOBAL_WEIGHT_REFILL_RATE
        self.redis = get_async_redis()
        # The script is parsed by Redis once
        self._script = self.redis.register_script(TOKEN_BUCKET_LUA)

    async def acquire(self, weight: int = 1) -> bool:
        """
        Attempts to deduct `weight` tokens from the global bucket.
        Returns True if successful, False if the bucket is exhausted.
        """
        now = time.time()
        try:
            # result is 1 (success) or 0 (failure)
            result = await self._script(
                keys=[self.key],
                args=[self.capacity, self.rate, weight, now]
            )
            return bool(result)
        except Exception as e:
            # If Redis crashes, we fallback to False to fail-closed and protect Binance IP ban.
            # Alternatively, if we want to fail-open, we return True, but IP bans are catastrophic.
            logger.error(f"Redis rate limiting script failed: {e}")
            return False
