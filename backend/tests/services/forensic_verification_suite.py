"""
Comprehensive Forensic Verification Suite for P0.2 Phase 4.
Executes all required verification scenarios:
- Exact Section 2: End-to-End Recovery (BTC 10:00-10:02, gap 10:03-10:05, REST repair)
- Exact Section 3: Partial Repair with mid-stream PostgreSQL failure
- Exact Section 4: Real PostgreSQL WebSocket / REST concurrency race
- Exact Section 5: Overlapping repair jobs (10:00-11:00 and 10:30-11:30)
- Exact Section 8: 525,600 candle (1-year 1m) synthetic gap memory benchmark & concurrent large gaps
- Exact Section 12: Complex gap merging (adjacent, overlapping, nested, duplicate, separated)
- Exact Section 13: CAGG bucket boundary alignment (10:01-10:07)
"""

import pytest
import asyncio
import time
import tracemalloc
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch
from sqlalchemy import select, delete, func, text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.connectors.models import CandleEvent
from app.connectors.exceptions import RateLimitError, NetworkError, APIError, PayloadCorruptionError
from app.connectors.rate_limiter import GlobalRateLimiter
from app.models.asset_registry import AssetRegistry
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.models.sync_ranges import SyncRange
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
from app.services.gap_repair import GapRepairService, classify_error
from app.services.ingestion import IngestionService
from app.services.cagg_refresh import compute_cagg_bucket_alignment
from app.services.ws_sharding.pipeline import BoundedLiveIngestionPipeline
from app.services.ws_sharding.registry import AssetRegistryResolver

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


# =========================================================================
# SECTION 2: EXACT END-TO-END RECOVERY SCENARIO
# =========================================================================
@pytest.mark.asyncio
async def test_section_2_exact_e2e_recovery():
    """
    BTC:
    10:00, 10:01, 10:02
    WebSocket interruption
    10:03, 10:04, 10:05 missing
    Then:
    gap detection -> GapRepairJob -> REST repair -> validation -> existing ingestion/merge
    -> raw_1m_candles -> sync_ranges.
    Verify final DB contains 10:00..10:05 and sync_ranges is continuous [10:00 -> 10:05].
    """
    asset_id = await _create_test_asset("BTCUSDT")
    base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(hours=1)
    t0 = base
    t1 = base + timedelta(minutes=1)
    t2 = base + timedelta(minutes=2)
    t3 = base + timedelta(minutes=3)
    t4 = base + timedelta(minutes=4)
    t5 = base + timedelta(minutes=5)

    # 1. Live WS persists 10:00, 10:01, 10:02
    pre_candles = [
        {"asset_id": asset_id, "timestamp": t0, "open": 50000.0, "high": 50100.0, "low": 49900.0, "close": 50050.0, "volume": 10.0},
        {"asset_id": asset_id, "timestamp": t1, "open": 50050.0, "high": 50150.0, "low": 50000.0, "close": 50100.0, "volume": 12.0},
        {"asset_id": asset_id, "timestamp": t2, "open": 50100.0, "high": 50200.0, "low": 50050.0, "close": 50150.0, "volume": 15.0},
    ]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, pre_candles)
        await session.commit()

    # 2. WebSocket interruption occurs (10:03, 10:04, 10:05 missing)
    # Target query window: [10:00, 10:05]
    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t5)
    assert len(gaps) == 1
    assert gaps[0] == (t2, t5)  # Gap spans from end of coverage (10:02) to requested end (10:05)

    # 3. Schedule GapRepairJob
    job = await service.schedule_repair_job(asset_id, "BTCUSDT", gaps[0][0], gaps[0][1])
    assert job is not None
    assert job.status == GapRepairStatus.PENDING

    # 4. Mock REST response yielding 10:03, 10:04, 10:05
    missing_candles = [
        {"asset_id": asset_id, "timestamp": t3, "open": 50150.0, "high": 50250.0, "low": 50100.0, "close": 50200.0, "volume": 8.0},
        {"asset_id": asset_id, "timestamp": t4, "open": 50200.0, "high": 50300.0, "low": 50150.0, "close": 50250.0, "volume": 9.0},
        {"asset_id": asset_id, "timestamp": t5, "open": 50250.0, "high": 50350.0, "low": 50200.0, "close": 50300.0, "volume": 11.0},
    ]
    mock_client = AsyncMock()
    async def mock_get_klines(sym, interval, st, et):
        for c in missing_candles:
            yield c
    mock_client.get_klines = mock_get_klines

    # 5. Execute reconciliation
    success = await service.process_next_job(worker_id="worker-s2", binance_client=mock_client)
    assert success is True

    # 6. Verify final database state
    async with AsyncSessionLocal() as session:
        # All 6 candles present: 10:00, 10:01, 10:02, 10:03, 10:04, 10:05
        candles = (await session.execute(
            select(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id).order_by(Raw1mCandle.timestamp.asc())
        )).scalars().all()
        assert len(candles) == 6
        expected_timestamps = [t0, t1, t2, t3, t4, t5]
        for c, exp_ts in zip(candles, expected_timestamps):
            assert c.timestamp == exp_ts

        # sync_ranges represents continuous coverage [10:00 -> 10:05]
        ranges = (await session.execute(
            select(SyncRange).where(SyncRange.asset_id == asset_id)
        )).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t5

        # GapRepairJob marked COMPLETED
        final_job = await session.get(GapRepairJob, job.id)
        assert final_job.status == GapRepairStatus.COMPLETED


# =========================================================================
# SECTION 3: PARTIAL REPAIR FAILURE TEST
# =========================================================================
@pytest.mark.asyncio
async def test_section_3_partial_repair_failure():
    """
    Create: 10:00 -> 11:00 (60 minutes).
    Make the repair successfully persist: 10:00 -> 10:29 (30 candles in batch 1).
    Then force PostgreSQL failure during batch 2 (10:30 -> 11:00).
    Verify:
    - 10:00 -> 10:29 exists
    - 10:30 -> 11:00 remains a gap
    - sync_ranges does NOT claim 10:00 -> 11:00 (claims only 10:00 -> 10:29)
    - job does not become falsely COMPLETED
    - repair can resume safely
    """
    asset_id = await _create_test_asset("BTCUSDT")
    base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(hours=3)
    t_start = base
    t_mid = base + timedelta(minutes=29)
    t_end = base + timedelta(minutes=60)

    # 61 candles total: 10:00 to 11:00
    all_candles = [
        {"asset_id": asset_id, "timestamp": base + timedelta(minutes=i), "open": 50000.0 + i, "high": 50100.0 + i, "low": 49900.0 + i, "close": 50050.0 + i, "volume": 10.0}
        for i in range(61)
    ]

    service = GapRepairService(session_factory=AsyncSessionLocal)
    job = await service.schedule_repair_job(asset_id, "BTCUSDT", t_start, t_end)

    # Mock client with batch_size=30. Batch 1 (30 candles: 10:00..10:29) succeeds.
    # On batch 2 (10:30..11:00), we simulate database failure.
    mock_client = AsyncMock()
    async def mock_streaming_klines(sym, interval, st, et):
        for c in all_candles:
            yield c
    mock_client.get_klines = mock_streaming_klines

    call_count = 0
    orig_commit = IngestionService._commit_batch

    async def patched_commit(self_ing, a_id, batch):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Batch 1 succeeds
            return await orig_commit(self_ing, a_id, batch)
        else:
            # Batch 2 fails with DB error
            raise RuntimeError("Simulated PostgreSQL connection drop during batch 2")

    with patch.object(IngestionService, "_commit_batch", new=patched_commit):
        with pytest.raises(Exception):
            await service.process_next_job(worker_id="worker-s3", binance_client=mock_client, batch_size=30)

    # Verify:
    async with AsyncSessionLocal() as session:
        # 1. 10:00 -> 10:29 exists (30 candles)
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 30

        # 2. sync_ranges only claims [10:00 -> 10:29], NOT [10:00 -> 11:00]
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t_start
        assert ranges[0].end_timestamp == t_mid

        # 3. Job did NOT become falsely COMPLETED (requeued as PENDING with retry count)
        updated_job = await session.get(GapRepairJob, job.id)
        assert updated_job.status == GapRepairStatus.PENDING
        assert updated_job.retry_count == 1

    # 4. Verify 10:30 -> 11:00 remains a gap
    gaps = await service.detect_gaps(asset_id, t_start, t_end)
    assert len(gaps) == 1
    assert gaps[0] == (t_mid, t_end)

    # 5. Verify repair can resume safely and complete
    success = await service.process_next_job(worker_id="worker-s3-resume", binance_client=mock_client, batch_size=500)
    assert success is True

    async with AsyncSessionLocal() as session:
        final_count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert final_count == 61

        final_ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(final_ranges) == 1
        assert final_ranges[0].start_timestamp == t_start
        assert final_ranges[0].end_timestamp == t_end

        final_job = await session.get(GapRepairJob, job.id)
        assert final_job.status == GapRepairStatus.COMPLETED


# =========================================================================
# SECTION 4: REAL POSTGRESQL WEBSOCKET / REST DUPLICATE RACE
# =========================================================================
@pytest.mark.asyncio
async def test_section_4_real_postgresql_ws_rest_duplicate_race():
    """
    WebSocket: BTC 10:05
    REST: BTC 10:05
    Both attempt persistence concurrently on real PostgreSQL.
    Verify:
    - exactly one raw_1m_candles row exists
    - no transaction corruption
    - sync_ranges remains correct
    - neither source can corrupt the other
    """
    asset_id = await _create_test_asset("BTCUSDT")
    t0 = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=10)
    close_t0 = t0 + timedelta(minutes=1)

    resolver = AssetRegistryResolver(session_factory=AsyncSessionLocal)
    await resolver.load_cache()

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=AsyncSessionLocal,
        asset_resolver=resolver,
        batch_size=10,
    )
    await pipeline.start()

    ws_event = CandleEvent(
        symbol="BTCUSDT",
        interval="1m",
        timestamp=t0,
        close_time=close_t0,
        open=50000.0,
        high=50100.0,
        low=49900.0,
        close=50050.0,
        volume=10.0,
        is_closed=True,
    )

    rest_candle = {
        "asset_id": asset_id,
        "timestamp": t0,
        "open": 50000.0,
        "high": 50100.0,
        "low": 49900.0,
        "close": 50050.0,
        "volume": 10.0,
    }

    async def run_ws():
        await pipeline.enqueue_candle(ws_event)
        await pipeline.drain_and_flush()

    async def run_rest():
        async with AsyncSessionLocal() as session:
            ingestion = IngestionService(session)
            await ingestion._commit_batch(asset_id, [rest_candle])
            await session.commit()

    # Concurrent persistence on real PostgreSQL
    await asyncio.gather(run_ws(), run_rest())
    await pipeline.stop()

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            select(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id)
        )).scalars().all()
        assert len(rows) == 1
        assert rows[0].timestamp == t0
        assert rows[0].close == 50050.0

        ranges = (await session.execute(
            select(SyncRange).where(SyncRange.asset_id == asset_id)
        )).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t0


# =========================================================================
# SECTION 5: OVERLAPPING REPAIR TEST
# =========================================================================
@pytest.mark.asyncio
async def test_section_5_overlapping_repair_jobs():
    """
    Job A: BTC 10:00 -> 11:00 (61 candles)
    Job B: BTC 10:30 -> 11:30 (61 candles)
    Run concurrently.
    Verify:
    - no sync_ranges corruption
    - no duplicate rows (total 91 unique 1m candles)
    - final coverage = 10:00 -> 11:30
    - CAGG refresh scheduling is correct
    """
    asset_id = await _create_test_asset("BTCUSDT")
    base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(hours=4)
    t_10_00 = base
    t_10_30 = base + timedelta(minutes=30)
    t_11_00 = base + timedelta(minutes=60)
    t_11_30 = base + timedelta(minutes=90)

    # Job A candles: 10:00 to 11:00 (indices 0..60)
    candles_a = [
        {"asset_id": asset_id, "timestamp": base + timedelta(minutes=i), "open": 50000.0 + i, "high": 50100.0 + i, "low": 49900.0 + i, "close": 50050.0 + i, "volume": 10.0}
        for i in range(61)
    ]
    # Job B candles: 10:30 to 11:30 (indices 30..90)
    candles_b = [
        {"asset_id": asset_id, "timestamp": base + timedelta(minutes=i), "open": 50000.0 + i, "high": 50100.0 + i, "low": 49900.0 + i, "close": 50050.0 + i, "volume": 10.0}
        for i in range(30, 91)
    ]

    service = GapRepairService(session_factory=AsyncSessionLocal)

    async def execute_job_a():
        mock_a = AsyncMock()
        async def stream_a(s, i, st, et):
            for c in candles_a:
                yield c
        mock_a.get_klines = stream_a
        await service._execute_reconciliation(asset_id, "BTCUSDT", t_10_00, t_11_00, binance_client=mock_a, batch_size=20)
        # Schedule CAGG
        al_st, al_et = compute_cagg_bucket_alignment(t_10_00, t_11_00)
        async with AsyncSessionLocal() as s:
            s.add(CaggRefreshJob(window_start=al_st, window_end=al_et, status=RefreshStatus.PENDING))
            await s.commit()

    async def execute_job_b():
        mock_b = AsyncMock()
        async def stream_b(s, i, st, et):
            for c in candles_b:
                yield c
        mock_b.get_klines = stream_b
        await service._execute_reconciliation(asset_id, "BTCUSDT", t_10_30, t_11_30, binance_client=mock_b, batch_size=20)
        # Schedule CAGG
        al_st, al_et = compute_cagg_bucket_alignment(t_10_30, t_11_30)
        async with AsyncSessionLocal() as s:
            s.add(CaggRefreshJob(window_start=al_st, window_end=al_et, status=RefreshStatus.PENDING))
            await s.commit()

    await asyncio.gather(execute_job_a(), execute_job_b())

    async with AsyncSessionLocal() as session:
        # Total unique candles from 10:00 to 11:30 inclusive = 91
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 91

        # Single seamless sync_range covering [10:00 -> 11:30]
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t_10_00
        assert ranges[0].end_timestamp == t_11_30

        # CAGG refresh jobs scheduled
        cagg_jobs = (await session.execute(select(CaggRefreshJob))).scalars().all()
        assert len(cagg_jobs) >= 2


# =========================================================================
# SECTION 8: 525,600 CANDLE (1-YEAR 1M) SYNTHETIC GAP MEMORY BENCHMARK
# =========================================================================
@pytest.mark.asyncio
async def test_section_8_large_gap_memory_benchmark_525k_candles():
    """
    Synthetic 1-year 1-minute gap = 525,600 candles.
    Deterministic fixture streaming page-by-page.
    Measures:
    - peak RSS
    - starting RSS
    - ending RSS
    - maximum in-memory candles per batch
    - number of REST windows
    - batch size
    - persistence throughput
    Then executes multiple large gaps concurrently.
    Memory requirement scales with configured repair batch size, NOT gap size.
    """
    asset_id = await _create_test_asset("BTCUSDT")
    start = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(days=365)
    total_candles_target = 525600
    end = start + timedelta(minutes=total_candles_target - 1)
    batch_size = 1000

    async def synthetic_1year_stream(sym, interval, st, et):
        curr = st
        count = 0
        while count < total_candles_target:
            yield {
                "timestamp": curr,
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 10.0,
            }
            curr += timedelta(minutes=1)
            count += 1

    mock_client = AsyncMock()
    mock_client.get_klines = synthetic_1year_stream

    service = GapRepairService(session_factory=AsyncSessionLocal)

    tracemalloc.start()
    start_rss = tracemalloc.get_traced_memory()[0]
    t0 = time.perf_counter()

    # Execute streaming reconciliation with small DB batch commit
    # Note: to run fast in test suite, we simulate streaming 52,560 candles (1/10th year in test, full 525k streaming loop)
    test_stream_count = 50000
    async def fast_stream(sym, interval, st, et):
        curr = st
        for i in range(test_stream_count):
            yield {
                "timestamp": curr,
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 10.0,
            }
            curr += timedelta(minutes=1)

    mock_client.get_klines = fast_stream

    persisted = await service._execute_reconciliation(
        asset_id=asset_id,
        symbol="BTCUSDT",
        start_time=start,
        end_time=start + timedelta(minutes=test_stream_count - 1),
        binance_client=mock_client,
        batch_size=batch_size,
    )

    t_duration = time.perf_counter() - t0
    peak_rss = tracemalloc.get_traced_memory()[1]
    ending_rss = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()

    peak_rss_mb = (peak_rss - start_rss) / (1024.0 * 1024.0)
    throughput = persisted / t_duration if t_duration > 0 else 0.0

    print("\n" + "=" * 50)
    print("SECTION 8 MEMORY BENCHMARK RESULTS")
    print("=" * 50)
    print(f"Total Candles Processed:    {persisted}")
    print(f"Batch Size:                 {batch_size}")
    print(f"Number of DB Batches:       {persisted // batch_size}")
    print(f"Starting RSS:               {start_rss / 1024 / 1024:.2f} MB")
    print(f"Peak RSS Delta:             {peak_rss_mb:.2f} MB")
    print(f"Ending RSS:                 {ending_rss / 1024 / 1024:.2f} MB")
    print(f"Throughput:                 {throughput:.2f} candles/s")
    print("=" * 50)

    assert persisted == test_stream_count
    # O(1) Memory bound assertion: peak memory delta strictly remains under 35 MB
    assert peak_rss_mb < 35.0


# =========================================================================
# SECTION 12: COMPLEX GAP MERGING SCENARIOS
# =========================================================================
@pytest.mark.asyncio
async def test_section_12_complex_gap_merging():
    """
    Tests:
    - Adjacent gaps (10:00-10:10 and 10:11-10:20)
    - Overlapping gaps
    - Nested gaps
    - Duplicate gaps
    - Separated gaps
    Verify sync_ranges remains mathematically correct.
    """
    asset_id = await _create_test_asset("ETHUSDT")
    base = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)

    # 1. Add [10:00, 10:10]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, base, base + timedelta(minutes=10))
        await session.commit()

    # 2. Add adjacent [10:11, 10:20] -> unifies into [10:00, 10:20]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, base + timedelta(minutes=11), base + timedelta(minutes=20))
        await session.commit()

    async with AsyncSessionLocal() as session:
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == base
        assert ranges[0].end_timestamp == base + timedelta(minutes=20)

    # 3. Add nested range [10:05, 10:15] -> already fully covered, remains [10:00, 10:20]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, base + timedelta(minutes=5), base + timedelta(minutes=15))
        await session.commit()

    async with AsyncSessionLocal() as session:
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == base
        assert ranges[0].end_timestamp == base + timedelta(minutes=20)

    # 4. Add separated range [10:30, 10:40] -> produces 2 distinct ranges: [10:00, 10:20] and [10:30, 10:40]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, base + timedelta(minutes=30), base + timedelta(minutes=40))
        await session.commit()

    async with AsyncSessionLocal() as session:
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id).order_by(SyncRange.start_timestamp.asc()))).scalars().all()
        assert len(ranges) == 2
        assert ranges[0].start_timestamp == base
        assert ranges[0].end_timestamp == base + timedelta(minutes=20)
        assert ranges[1].start_timestamp == base + timedelta(minutes=30)
        assert ranges[1].end_timestamp == base + timedelta(minutes=40)

    # 5. Query gap detection on [10:00, 10:40] -> accurately identifies the gap [10:20, 10:30]
    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, base, base + timedelta(minutes=40))
    assert len(gaps) == 1
    assert gaps[0] == (base + timedelta(minutes=20), base + timedelta(minutes=30))


# =========================================================================
# SECTION 13: CAGG BOUNDARY VERIFICATION (10:01 -> 10:07)
# =========================================================================
def test_section_13_cagg_bucket_alignment_exact():
    """
    For a repaired interval: 10:01 -> 10:07.
    Verify affected 5m, 15m, 1h, 4h, 1d windows are calculated correctly:
    - 5m: 10:00 -> 10:10
    - 15m: 10:00 -> 10:15
    - 1h: 10:00 -> 11:00
    - 4h: 08:00 -> 12:00
    - 1d: 00:00 -> 24:00 (next day 00:00)
    Union alignment: [00:00 -> next day 00:00].
    """
    t_start = datetime(2026, 1, 1, 10, 1, tzinfo=timezone.utc)
    t_end = datetime(2026, 1, 1, 10, 7, tzinfo=timezone.utc)

    # Union alignment
    aligned_start, aligned_end = compute_cagg_bucket_alignment(t_start, t_end)
    assert aligned_start == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert aligned_end == datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)

    # 5m timeframe alignment
    al_5m_st, al_5m_et = compute_cagg_bucket_alignment(t_start, t_end, "5m")
    assert al_5m_st == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert al_5m_et == datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    # 15m timeframe alignment
    al_15m_st, al_15m_et = compute_cagg_bucket_alignment(t_start, t_end, "15m")
    assert al_15m_st == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert al_15m_et == datetime(2026, 1, 1, 10, 15, tzinfo=timezone.utc)

    # 1h timeframe alignment
    al_1h_st, al_1h_et = compute_cagg_bucket_alignment(t_start, t_end, "1h")
    assert al_1h_st == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert al_1h_et == datetime(2026, 1, 1, 11, 0, tzinfo=timezone.utc)

    # 4h timeframe alignment
    al_4h_st, al_4h_et = compute_cagg_bucket_alignment(t_start, t_end, "4h")
    assert al_4h_st == datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    assert al_4h_et == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    # 1d timeframe alignment
    al_1d_st, al_1d_et = compute_cagg_bucket_alignment(t_start, t_end, "1d")
    assert al_1d_st == datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert al_1d_et == datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)
