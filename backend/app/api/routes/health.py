"""Liveness and dependency-readiness endpoints.

``/health`` deliberately remains a process liveness probe.  Deployments that
need to decide whether traffic is safe must use ``/ready``: reporting a process
as healthy while PostgreSQL or Redis is unavailable masks data-loss and worker
ownership failures.
"""

import asyncio
import logging

from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.api.dependencies import get_db
from app.db.session import async_session_maker
from app.workers.config import borrow_async_redis_for_probe

router = APIRouter(tags=["Health"])
logger = logging.getLogger(__name__)

@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    # A liveness endpoint must not depend on a potentially unavailable external
    # dependency; callers can distinguish this from readiness below.
    return {"status": "healthy", "kind": "liveness"}


async def _database_ready() -> bool:
    try:
        async with async_session_maker() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        logger.exception("Database readiness check failed")
        return False


async def _redis_ready() -> bool:
    try:
        # Do not close the shared Redis connection pool from a request.  The
        # process-wide client is owned by workers.config, and a finite probe
        # gate prevents public readiness traffic from exhausting its pool.
        async with borrow_async_redis_for_probe() as redis:
            await redis.ping()
        return True
    except Exception:
        logger.exception("Redis readiness check failed")
        return False


@router.get("/ready")
async def readiness_check():
    """Return 503 whenever a required persistence/lease dependency is down."""
    database, redis = await asyncio.gather(_database_ready(), _redis_ready())
    checks = {
        "database": "reachable" if database else "unavailable",
        "redis": "reachable" if redis else "unavailable",
    }
    if not (database and redis):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "unready", "checks": checks},
        )
    return {"status": "ready", "checks": checks}

@router.get("/health/db", status_code=status.HTTP_200_OK)
async def db_health_check(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "reachable"}
    except Exception:
        logger.exception("Database health check failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed"
        )
