import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import Retries, TimeLimit, AgeLimit, Callbacks
import redis.asyncio as redis_async
import redis as redis_sync
from app.core.config import settings

# Setup Sync Redis for Broker
redis_client = redis_sync.Redis.from_url(settings.REDIS_URL)
broker = RedisBroker(
    url=settings.REDIS_URL,
    middleware=[
        Retries(max_retries=5),
        TimeLimit(time_limit=3600000), # 1 hour max
        AgeLimit(max_age=86400000),    # 1 day max in queue
        Callbacks(),
    ]
)

dramatiq.set_broker(broker)

# Provide Async Redis for locking
redis_pool = redis_async.ConnectionPool.from_url(settings.REDIS_URL)

def get_async_redis():
    return redis_async.Redis(connection_pool=redis_pool)
