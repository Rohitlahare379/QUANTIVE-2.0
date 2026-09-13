import logging
from typing import Any

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
-- Use Redis' clock, not the clock of whichever worker happens to make this
-- request.  Workers can run on hosts with skewed clocks; letting their local
-- time drive refill would allow a fast clock to mint tokens globally.
local redis_time = redis.call("TIME")
local now = tonumber(redis_time[1]) + (tonumber(redis_time[2]) / 1000000)

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
        # A BinanceClient is often constructed by synchronous application
        # setup code and entered later inside an async worker.  Async Redis
        # clients are loop-bound, so acquiring one here would make mere client
        # construction fail outside a running loop (or retain a client from a
        # different loop).  Resolve and cache the script only at the async
        # acquire boundary instead.
        self.redis: Any | None = None
        self._script: Any | None = None

    def _script_for_current_loop(self):
        """Return a script bound to the current loop's Redis client."""
        redis = get_async_redis()
        if redis is not self.redis:
            self.redis = redis
            # register_script is local client setup; script execution remains
            # atomic in Redis and any connection failure is handled by
            # ``acquire``'s fail-closed boundary.
            self._script = redis.register_script(TOKEN_BUCKET_LUA)
        assert self._script is not None
        return self._script

    async def acquire(self, weight: int = 1) -> bool:
        """
        Attempts to deduct `weight` tokens from the global bucket.
        Returns True if successful, False if the bucket is exhausted.
        """
        # A caller must never be able to add tokens by supplying a negative
        # weight, nor ask for a request that can never fit the bucket.  Treat
        # malformed requests exactly like an unavailable limiter: fail closed.
        if isinstance(weight, bool) or not isinstance(weight, int) or weight <= 0 or weight > self.capacity:
            logger.warning("Rejected invalid Binance rate-limit weight: %r", weight)
            return False
        try:
            script = self._script_for_current_loop()
            # result is 1 (success) or 0 (failure).  The Lua script obtains its
            # timestamp atomically from Redis, so all distributed workers share
            # the same refill clock.
            result = await script(
                keys=[self.key],
                args=[self.capacity, self.rate, weight]
            )
            return bool(result)
        except Exception as e:
            # If Redis crashes, we fallback to False to fail-closed and protect Binance IP ban.
            # Alternatively, if we want to fail-open, we return True, but IP bans are catastrophic.
            logger.error(f"Redis rate limiting script failed: {e}")
            return False
