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
from app.connectors.binance_ws import BinanceWebSocketClient
from app.models.asset_registry import AssetRegistry
from app.services.ws_sharding.registry import AssetRegistryResolver, BINANCE_EXCHANGE
from app.services.ws_sharding.supervisor import ShardSupervisor

logger = logging.getLogger(__name__)


async def fetch_active_symbols_from_db() -> List[str]:
    """
    Fetches the list of active symbols from the AssetRegistry table in PostgreSQL.
    Raises when the authoritative registry is unavailable rather than allowing
    an empty worker to acquire and black-hole live-data leases.
    """
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    try:
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            # This worker owns Binance transport only.  A same-symbol asset on
            # another exchange must not create a Binance subscription or receive
            # Binance candles.
            stmt = select(AssetRegistry.symbol).where(
                AssetRegistry.is_active.is_(True),
                AssetRegistry.exchange == BINANCE_EXCHANGE,
            )
            result = await session.execute(stmt)
            symbols = [r[0] for r in result.fetchall()]
        return symbols
    except Exception:
        # Starting a zero-symbol supervisor still acquires distributed leases,
        # which prevents a healthy worker from ingesting.  Let the process
        # fail/restart instead of silently black-holing all live data.
        logger.exception("Could not load active symbols; refusing to start an empty shard supervisor")
        raise
    finally:
        await engine.dispose()


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
    try:
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
            websocket_client_factory=BinanceWebSocketClient,
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
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    asyncio.run(run_ws_shard_supervisor())
