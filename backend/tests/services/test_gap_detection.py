"""
Test Matrix — Gap Detection (Tests 1 to 8).
Verifies exact gap discovery, inclusive/exclusive boundary semantics, multi-interval gaps,
adjacent gaps, overlapping ranges, and duplicate requests against real PostgreSQL / AsyncSession.
"""

import pytest
from datetime import datetime, timezone, timedelta
from sqlalchemy import delete
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.sync_ranges import SyncRange
from app.models.gap_repair_jobs import GapRepairJob
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.services.gap_repair import GapRepairService
from app.services.ingestion import IngestionService

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@pytest.fixture(autouse=True)
async def clean_database():
    async with AsyncSessionLocal() as session:
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
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
async def test_1_one_minute_gap():
    """
    Test 1: Detects a single 1-minute missing gap.
    Coverage exists: 10:00 -> 10:02 and 10:04 -> 10:10.
    Missing: 10:02 -> 10:04 (10:03 candle).
    """
    asset_id = await _create_test_asset("BTCUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 10, 2, tzinfo=timezone.utc)
    t4 = datetime(2026, 1, 1, 10, 4, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t0, end_timestamp=t2))
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t4, end_timestamp=t10))
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t10)

    assert len(gaps) == 1
    assert gaps[0] == (t2, t4)


@pytest.mark.asyncio
async def test_2_multi_minute_gap():
    """
    Test 2: Detects a multi-minute gap (e.g. 15 minutes missing).
    """
    asset_id = await _create_test_asset("ETHUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)
    t25 = datetime(2026, 1, 1, 10, 25, tzinfo=timezone.utc)
    t40 = datetime(2026, 1, 1, 10, 40, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t0, end_timestamp=t10))
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t25, end_timestamp=t40))
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t40)

    assert len(gaps) == 1
    assert gaps[0] == (t10, t25)


@pytest.mark.asyncio
async def test_3_multi_hour_gap():
    """
    Test 3: Detects a multi-hour gap (e.g. 6 hours missing between coverage ranges).
    """
    asset_id = await _create_test_asset("SOLUSDT")
    t_start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    t_h2 = datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)
    t_h8 = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
    t_h12 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t_start, end_timestamp=t_h2))
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t_h8, end_timestamp=t_h12))
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t_start, t_h12)

    assert len(gaps) == 1
    assert gaps[0] == (t_h2, t_h8)


@pytest.mark.asyncio
async def test_4_multi_day_gap():
    """
    Test 4: Detects a multi-day missing interval (e.g. 7 days missing).
    """
    asset_id = await _create_test_asset("ADAUSDT")
    t_jan1 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    t_jan3 = datetime(2026, 1, 3, 0, 0, tzinfo=timezone.utc)
    t_jan10 = datetime(2026, 1, 10, 0, 0, tzinfo=timezone.utc)
    t_jan15 = datetime(2026, 1, 15, 0, 0, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t_jan1, end_timestamp=t_jan3))
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t_jan10, end_timestamp=t_jan15))
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t_jan1, t_jan15)

    assert len(gaps) == 1
    assert gaps[0] == (t_jan3, t_jan10)


@pytest.mark.asyncio
async def test_5_multiple_separated_gaps():
    """
    Test 5: Detects multiple independent gaps within a single query window.
    """
    asset_id = await _create_test_asset("AVAXUSDT")
    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 1, 1, 3, 0, tzinfo=timezone.utc)
    t4 = datetime(2026, 1, 1, 4, 0, tzinfo=timezone.utc)
    t5 = datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc)

    # Existing: [t0, t1], [t2, t3], [t4, t5]
    async with AsyncSessionLocal() as session:
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t0, end_timestamp=t1))
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t2, end_timestamp=t3))
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t4, end_timestamp=t5))
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t5)

    assert len(gaps) == 2
    assert gaps[0] == (t1, t2)
    assert gaps[1] == (t3, t4)


@pytest.mark.asyncio
async def test_6_adjacent_gaps_merge_semantics():
    """
    Test 6: Verifies sync_ranges merger behavior on adjacent ranges.
    When two adjacent ranges are merged in DB, detect_gaps sees zero gaps.
    """
    asset_id = await _create_test_asset("DOGEUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)
    t11 = datetime(2026, 1, 1, 10, 11, tzinfo=timezone.utc)
    t20 = datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc)

    # First add [10:00, 10:10]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, t0, t10)
        await session.commit()

    # Now add adjacent [10:11, 10:20] -> update_sync_ranges merges them into [10:00, 10:20]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, t11, t20)
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t20)

    # Contiguous coverage: zero gaps
    assert len(gaps) == 0


@pytest.mark.asyncio
async def test_7_overlapping_gaps_query():
    """
    Test 7: Query window extending before existing start and after existing end.
    """
    asset_id = await _create_test_asset("DOTUSDT")
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)
    t20 = datetime(2026, 1, 1, 10, 20, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        session.add(SyncRange(asset_id=asset_id, start_timestamp=t10, end_timestamp=t20))
        await session.commit()

    # Query from 10:00 to 10:30 (wraps around existing [10:10, 10:20])
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t30 = datetime(2026, 1, 1, 10, 30, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t30)

    assert len(gaps) == 2
    assert gaps[0] == (t0, t10)
    assert gaps[1] == (t20, t30)


@pytest.mark.asyncio
async def test_8_duplicate_gap_request():
    """
    Test 8: Running gap detection repeatedly produces deterministic, idempotent results.
    """
    asset_id = await _create_test_asset("LINKUSDT")
    t0 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)

    gaps1 = await service.detect_gaps(asset_id, t0, t10)
    gaps2 = await service.detect_gaps(asset_id, t0, t10)

    assert gaps1 == gaps2
    assert len(gaps1) == 1
    assert gaps1[0] == (t0, t10)
