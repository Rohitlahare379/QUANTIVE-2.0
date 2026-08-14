"""
WebSocket Shard Supervisor Worker Runtime.

Dedicated worker process entrypoint for supervising WebSocket shard ownership.
Runs independently from FastAPI API processes to prevent duplicate supervisors across API replicas.
"""

import asyncio
import logging
import signal
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.services.ws_sharding.registry import AssetRegistryResolver
from app.services.ws_sharding.supervisor import ShardSupervisor

logger = logging.getLogger(__name__)


async def fetch_active_symbols_from_db() -> List[str]:
    """
    Fetches the list of active symbols from the AssetRegistry table in PostgreSQL.
    Returns an empty list if database is unavailable.
    """
    try:
        engine = create_async_engine(settings.sqlalchemy_database_uri)
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            stmt = select(AssetRegistry.symbol).where(AssetRegistry.is_active.is_(True))
            result = await session.execute(stmt)
            symbols = [r[0] for r in result.fetchall()]
        await engine.dispose()
        return symbols
    except Exception as e:
        logger.warning(f"Could not load active symbols from database: {e}. Running with empty symbol registry.")
        return []


async def run_ws_shard_supervisor(
    candidate_shards: Optional[List[int]] = None,
    symbols: Optional[List[str]] = None,
    check_interval_seconds: float = 2.0
) -> None:
    """
    Main execution loop for a WebSocket Shard Supervisor worker process.
    Handles SIGTERM and SIGINT for graceful shutdown and lease release.
    """
    engine = create_async_engine(
        settings.sqlalchemy_database_uri,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    asset_resolver = AssetRegistryResolver(session_factory=session_factory)
    await asset_resolver.load_cache()

    if symbols is None:
        symbols = await fetch_active_symbols_from_db()

    supervisor = ShardSupervisor(
        candidate_shards=candidate_shards,
        symbols=symbols,
        session_factory=session_factory,
        asset_resolver=asset_resolver,
    )

    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Received termination signal. Initiating graceful shard shutdown...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, RuntimeError):
            # Windows or non-main thread environments
            pass

    logger.info(
        f"Starting WebSocket Shard Supervisor for worker {supervisor.worker_id} (Candidates: {supervisor.candidate_shards})"
    )
    await supervisor.start()

    try:
        while not stop_event.is_set():
            # Periodically re-evaluate unowned candidate shards (e.g. recovering from crashed workers)
            await supervisor.attempt_acquire_all_candidates()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=check_interval_seconds)
            except asyncio.TimeoutError:
                pass
    finally:
        logger.info("Stopping WebSocket Shard Supervisor and releasing leases...")
        await supervisor.shutdown()
        logger.info("WebSocket Shard Supervisor stopped cleanly.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(run_ws_shard_supervisor())
