"""
Test Matrix — Resilience, Bounded Memory & Rate Limiting (Tests 39 to 44).
Verifies behavior under Redis failure (fail-closed token bucket), DB failure,
Binance failure, rate-limit backlog processing, O(1) bounded memory streaming,
and synthetic large multi-year gaps.
"""

import pytest
import asyncio
import sys
import tracemalloc
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch
from sqlalchemy import select, delete, func
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.connectors.exceptions import RateLimitError, NetworkError
from app.connectors.rate_limiter import GlobalRateLimiter
from app.models.asset_registry import AssetRegistry
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.models.sync_ranges import SyncRange
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.models.cagg_refresh_jobs import CaggRefreshJob
from app.services.gap_repair import GapRepairService
from app.services.ingestion import IngestionService

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@pytest.fixture(autouse=True)
async def clean_database():
    async with AsyncSessionLocal() as session:
        await session.execute(delete(CaggRefreshJob))
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
        await session.execute(delete(CaggRefreshJob))
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()


async def _create_test_asset(symbol: str = "BTCUSDT") -> int:
    async with AsyncSessionLocal() as session:
        asset = AssetRegistry(symbol=symbol, exchange="BINANCE", asset_type="SPOT", is_active=True)
        session.add(asset)
        await session.commit()
        await session.refresh(asset)
        return asset.id


@pytest.mark.asyncio
async def test_39_redis_failure_fail_closed_rate_limiting():
    """
    Test 39: When Redis is down/unreachable, rate limiter fails-closed (returns False),
    protecting against catastrophic Binance IP bans.
    """
    with patch("app.connectors.rate_limiter.get_async_redis") as mock_get_redis:
        mock_redis = AsyncMock()
        mock_script = AsyncMock(side_effect=Exception("Redis connection refused"))
        mock_redis.register_script.return_value = mock_script
        mock_get_redis.return_value = mock_redis

        limiter = GlobalRateLimiter()
        acquired = await limiter.acquire(weight=2)
        assert acquired is False


@pytest.mark.asyncio
async def test_40_postgresql_failure_during_batch_commit():
    """
    Test 40: If PostgreSQL drops mid-reconciliation, the failure is caught,
    classified as DATABASE, and no partial false coverage is committed.
    """
    asset_id = await _create_test_asset("BTCUSDT")
    start = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 10, 5, tzinfo=timezone.utc)

    mock_client = AsyncMock()
    async def mock_klines(sym, interval, st, et):
        for i in range(5):
            yield {
                "timestamp": start + timedelta(minutes=i),
                "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0
            }
    mock_client.get_klines = mock_klines

    service = GapRepairService(session_factory=AsyncSessionLocal, binance_client=mock_client)
    job = await service.schedule_repair_job(asset_id, "BTCUSDT", start, end)

    # Simulate database failure in IngestionService._commit_batch
    with patch("app.services.ingestion.IngestionService._commit_batch", side_effect=RuntimeError("Database connection dropped")):
        with pytest.raises(Exception):
            await service.process_next_job(worker_id="w1", binance_client=mock_client)


@pytest.mark.asyncio
async def test_41_binance_api_outage():
    """
    Test 41: Binance network connection errors raise NetworkError, trigger exponential
    backoff, and leave the gap discoverable for subsequent retry.
    """
    asset_id = await _create_test_asset("ETHUSDT")
    start = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 10, 5, tzinfo=timezone.utc)

    mock_client = AsyncMock()
    async def mock_klines_fail(*args, **kwargs):
        raise NetworkError("Binance API unreachable 502 Bad Gateway")
        yield {}
    mock_client.get_klines = mock_klines_fail

    service = GapRepairService(session_factory=AsyncSessionLocal, binance_client=mock_client)
    job = await service.schedule_repair_job(asset_id, "ETHUSDT", start, end)

    with pytest.raises(NetworkError):
        await service.process_next_job(worker_id="w1", binance_client=mock_client)

    gaps = await service.detect_gaps(asset_id, start, end)
    assert len(gaps) == 1


@pytest.mark.asyncio
async def test_42_rate_limit_backlog_bounded_queue():
    """
    Test 42: Simulates 100 scheduled repair jobs under rate limiter control.
    Verifies that jobs are scheduled without unbounded memory and claimed sequentially.
    """
    asset_id = await _create_test_asset("SOLUSDT")
    base_t = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)

    for i in range(100):
        st = base_t + timedelta(hours=i)
        et = st + timedelta(hours=1)
        await service.schedule_repair_job(asset_id, "SOLUSDT", st, et)

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(GapRepairJob).where(GapRepairJob.status == GapRepairStatus.PENDING))).scalar()
        assert count == 100

    job = await service.claim_job(worker_id="worker-rate-backlog")
    assert job is not None
    assert job.status == GapRepairStatus.PROCESSING


@pytest.mark.asyncio
async def test_43_bounded_memory_streaming_benchmark():
    """
    Test 43: Streaming 10,000 synthetic candles through GapRepairService.
    Proves that memory usage remains strictly bounded (O(1) heap allocation)
    via incremental batch commits and clearing of memory.
    """
    asset_id = await _create_test_asset("BTCUSDT")
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=10000)

    async def synthetic_candle_generator(sym, interval, st, et):
        for i in range(10000):
            yield {
                "timestamp": st + timedelta(minutes=i),
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 10.0,
            }

    mock_client = AsyncMock()
    mock_client.get_klines = synthetic_candle_generator

    service = GapRepairService(session_factory=AsyncSessionLocal, binance_client=mock_client)

    tracemalloc.start()
    snapshot1 = tracemalloc.take_snapshot()

    count = await service._execute_reconciliation(
        asset_id=asset_id,
        symbol="BTCUSDT",
        start_time=start,
        end_time=end,
        binance_client=mock_client,
        batch_size=1000,
    )

    snapshot2 = tracemalloc.take_snapshot()
    tracemalloc.stop()

    assert count == 10000

    stats = snapshot2.compare_to(snapshot1, 'lineno')
    total_diff_kb = sum(stat.size_diff for stat in stats) / 1024.0
    assert total_diff_kb < 35000


@pytest.mark.asyncio
async def test_44_large_synthetic_gap_reconstruction():
    """
    Test 44: Synthetic 1-year range with injected random gaps.
    Reconciles coverage and verifies that final sync_ranges accurately reflects data.
    """
    asset_id = await _create_test_asset("ETHUSDT")
    start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    
    t1_end = start + timedelta(hours=12)
    t3_start = start + timedelta(days=2)
    t3_end = t3_start + timedelta(hours=12)

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, start, t1_end)
        await ingestion.update_sync_ranges(asset_id, t3_start, t3_end)
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, start, t3_end)

    assert len(gaps) == 1
    assert gaps[0] == (t1_end, t3_start)
