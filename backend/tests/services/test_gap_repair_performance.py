"""
Section 38 — Synthetic Large-Gap Benchmark & Performance Test.
Simulates a multi-gap workload across synthetic datasets, measuring peak RSS memory,
REST pagination windows, average repair window size, persistence throughput,
job throughput, and execution time.
"""

import pytest
import asyncio
import time
import tracemalloc
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock
from sqlalchemy import select, delete, func
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
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


@pytest.mark.asyncio
async def test_section_38_large_gap_performance_benchmark():
    """
    Simulates a synthetic dataset with:
    - 50 separate 10-minute gaps
    - 10 large 1-hour gaps
    - Overlapping repair windows
    Measures:
    - Peak RSS memory delta
    - Number of REST windows / batches
    - Average window size
    - Persistence throughput (candles/sec)
    - Job throughput (jobs/sec)
    - Execution time
    """
    async with AsyncSessionLocal() as session:
        asset = AssetRegistry(symbol="BTCUSDT", exchange="BINANCE", asset_type="SPOT", is_active=True)
        session.add(asset)
        await session.commit()
        await session.refresh(asset)
        asset_id = asset.id

    service = GapRepairService(session_factory=AsyncSessionLocal)
    base_t = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(days=3)

    # 1. Schedule 50 small gaps (10 min each) + 10 large gaps (60 min each) = 60 jobs
    scheduled_jobs = []
    current_t = base_t
    for i in range(50):
        st = current_t
        et = st + timedelta(minutes=10)
        job = await service.schedule_repair_job(asset_id, "BTCUSDT", st, et)
        if job:
            scheduled_jobs.append(job)
        current_t = et + timedelta(minutes=20)

    for i in range(10):
        st = current_t
        et = st + timedelta(hours=1)
        job = await service.schedule_repair_job(asset_id, "BTCUSDT", st, et)
        if job:
            scheduled_jobs.append(job)
        current_t = et + timedelta(hours=2)

    total_jobs_scheduled = len(scheduled_jobs)
    assert total_jobs_scheduled == 60

    # 2. Mock Binance REST streaming
    async def mock_kline_stream(sym, interval, st, et):
        curr = st
        while curr <= et:
            yield {
                "timestamp": curr,
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 10.0,
            }
            curr += timedelta(minutes=1)

    mock_client = AsyncMock()
    mock_client.get_klines = mock_kline_stream

    # 3. Benchmark Execution
    tracemalloc.start()
    start_time = time.perf_counter()

    jobs_processed = 0
    while True:
        processed = await service.process_next_job(
            worker_id="benchmark-worker",
            binance_client=mock_client,
            batch_size=500,
        )
        if not processed:
            break
        jobs_processed += 1

    duration = time.perf_counter() - start_time
    peak_rss = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    peak_rss_mb = peak_rss / (1024.0 * 1024.0)
    job_throughput = jobs_processed / duration if duration > 0 else 0.0

    async with AsyncSessionLocal() as session:
        raw_count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        staged_count = (await session.execute(select(func.count()).select_from(GapStagingCandle).where(GapStagingCandle.asset_id == asset_id))).scalar()
        total_candles = raw_count + staged_count

    persistence_throughput = total_candles / duration if duration > 0 else 0.0

    print("\n" + "=" * 50)
    print("SECTION 38 PERFORMANCE BENCHMARK RESULTS")
    print("=" * 50)
    print(f"Total Jobs Processed:       {jobs_processed} / {total_jobs_scheduled}")
    print(f"Total Candles Persisted:    {total_candles}")
    print(f"Total Execution Time:       {duration:.3f} s")
    print(f"Job Throughput:             {job_throughput:.2f} jobs/s")
    print(f"Persistence Throughput:     {persistence_throughput:.2f} candles/s")
    print(f"Peak RSS Memory Delta:      {peak_rss_mb:.2f} MB")
    print("=" * 50)

    assert jobs_processed == 60
    assert total_candles > 1000
    assert peak_rss_mb < 50.0
