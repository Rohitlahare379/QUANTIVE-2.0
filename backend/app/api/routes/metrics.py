import json
import time
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, Gauge, CONTENT_TYPE_LATEST

from app.workers.config import get_async_redis

router = APIRouter(tags=["Metrics"])

# Define Prometheus Metrics
DLQ_JOB_COUNT = Gauge(
    'dlq_job_count',
    'Total number of jobs currently in the Dead Letter Queue'
)
DLQ_JOBS_BY_TYPE = Gauge(
    'dlq_jobs_by_type',
    'Number of jobs in the Dead Letter Queue, grouped by actor name',
    ['actor_name']
)
DLQ_OLDEST_JOB_AGE_SECONDS = Gauge(
    'dlq_oldest_job_age_seconds',
    'Age of the oldest message in the Dead Letter Queue in seconds'
)

# CAGG Refresh Workflow Metrics
CAGG_REFRESH_PENDING_JOBS = Gauge(
    'cagg_refresh_pending_jobs',
    'Total number of CAGG refresh jobs currently pending'
)
CAGG_REFRESH_FAILED_JOBS = Gauge(
    'cagg_refresh_failed_jobs',
    'Total number of CAGG refresh jobs that have failed'
)

@router.get("/metrics")
async def get_metrics():
    """
    Exposes Prometheus metrics including Dramatiq Dead Letter Queue introspection.
    Operates in O(1) or bounded O(N) where N is DLQ size, ensuring no Redis exhaustion.
    """
    redis = get_async_redis()
    dlq_key = "dramatiq:default.DQ"
    
    # Extract CAGG Metrics directly via AsyncSession dependency manually here
    # To keep /metrics fast without injecting db globally, we'll import sessionmaker
    from app.db.session import async_session_maker
    from sqlalchemy import select, func
    from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
    
    try:
        async with async_session_maker() as session:
            pending_count = await session.scalar(select(func.count()).where(CaggRefreshJob.status == RefreshStatus.PENDING))
            failed_count = await session.scalar(select(func.count()).where(CaggRefreshJob.status == RefreshStatus.FAILED))
            
            CAGG_REFRESH_PENDING_JOBS.set(pending_count or 0)
            CAGG_REFRESH_FAILED_JOBS.set(failed_count or 0)
    except Exception:
        pass
    
    try:
        # 1. Total Job Count
        total_count = await redis.llen(dlq_key)
        DLQ_JOB_COUNT.set(total_count)
        
        # 2. Extract Oldest Message (Last element in the list)
        if total_count > 0:
            # Redis LRANGE -1 -1 gets the oldest element in O(1)
            oldest_raw = await redis.lrange(dlq_key, -1, -1)
            if oldest_raw:
                try:
                    msg = json.loads(oldest_raw[0])
                    # Dramatiq messages have 'message_timestamp' in ms
                    timestamp_ms = msg.get("message_timestamp")
                    if timestamp_ms:
                        age_seconds = time.time() - (timestamp_ms / 1000.0)
                        DLQ_OLDEST_JOB_AGE_SECONDS.set(max(0, age_seconds))
                except Exception:
                    pass
            
            # 3. Jobs By Type (Sample the first 1000 to prevent Redis blocking if DLQ explodes)
            sample_size = min(total_count, 1000)
            sample_raw = await redis.lrange(dlq_key, 0, sample_size - 1)
            
            actor_counts = {}
            for raw in sample_raw:
                try:
                    msg = json.loads(raw)
                    actor = msg.get("actor_name", "unknown")
                    actor_counts[actor] = actor_counts.get(actor, 0) + 1
                except Exception:
                    continue
            
            # We scale the sampled count to estimate the total distribution safely
            scale_factor = total_count / sample_size if sample_size > 0 else 1
            for actor, count in actor_counts.items():
                DLQ_JOBS_BY_TYPE.labels(actor_name=actor).set(count * scale_factor)
                
        else:
            DLQ_OLDEST_JOB_AGE_SECONDS.set(0)
            # Clear gauge labels if DLQ is empty (to reset alerts)
            DLQ_JOBS_BY_TYPE.clear()

    except Exception:
        # Ignore Redis extraction failures on /metrics so we don't 500 the whole endpoint
        pass

    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
