"""
Test Matrix — Merge Invariants & Persistence Safety (Tests 17 to 22).
Verifies successful merge into raw_1m_candles & sync_ranges, duplicate merge idempotency,
partial repair coverage correctness (no false coverage), transaction rollback on error,
overlapping repairs, and concurrent repair execution against real PostgreSQL.
"""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete, func
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.models.sync_ranges import SyncRange
from app.models.gap_repair_jobs import GapRepairJob
from app.services.ingestion import IngestionService
from app.connectors.exceptions import PayloadCorruptionError

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
async def test_17_successful_merge():
    """
    Test 17: Valid candle batch inserts into raw_1m_candles and updates sync_ranges correctly.
    """
    asset_id = await _create_test_asset("BTCUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=10)
    t1 = now - timedelta(minutes=9)
    t2 = now - timedelta(minutes=8)

    candles = [
        {"asset_id": asset_id, "timestamp": t0, "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0},
        {"asset_id": asset_id, "timestamp": t1, "open": 102.0, "high": 106.0, "low": 101.0, "close": 105.0, "volume": 15.0},
        {"asset_id": asset_id, "timestamp": t2, "open": 105.0, "high": 108.0, "low": 104.0, "close": 107.0, "volume": 20.0},
    ]

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, candles)
        await session.commit()

    async with AsyncSessionLocal() as session:
        stmt = select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id)
        count = (await session.execute(stmt)).scalar()
        assert count == 3

        range_stmt = select(SyncRange).where(SyncRange.asset_id == asset_id)
        ranges = (await session.execute(range_stmt)).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t2


@pytest.mark.asyncio
async def test_18_duplicate_merge_idempotency():
    """
    Test 18: Inserting the exact same batch twice is completely idempotent.
    No duplicates, no corrupted sync_ranges.
    """
    asset_id = await _create_test_asset("ETHUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=5)
    t1 = now - timedelta(minutes=4)

    candles = [
        {"asset_id": asset_id, "timestamp": t0, "open": 2000.0, "high": 2010.0, "low": 1990.0, "close": 2005.0, "volume": 50.0},
        {"asset_id": asset_id, "timestamp": t1, "open": 2005.0, "high": 2020.0, "low": 2000.0, "close": 2015.0, "volume": 60.0},
    ]

    # Commit first time
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, candles)
        await session.commit()

    # Commit second time (exact duplicate)
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, candles)
        await session.commit()

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 2

        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t1


@pytest.mark.asyncio
async def test_19_partial_repair_coverage_correctness():
    """
    Test 19: If a batch has internal gaps (e.g. 10:00, 10:01, gap at 10:02, 10:03, 10:04),
    sync_ranges records exactly the contiguous sub-ranges [10:00, 10:01] and [10:03, 10:04].
    Never claims false continuous coverage.
    """
    asset_id = await _create_test_asset("SOLUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=10)
    t1 = now - timedelta(minutes=9)
    t3 = now - timedelta(minutes=7)
    t4 = now - timedelta(minutes=6)

    candles = [
        {"asset_id": asset_id, "timestamp": t0, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 5.0},
        {"asset_id": asset_id, "timestamp": t1, "open": 100.5, "high": 102.0, "low": 100.0, "close": 101.0, "volume": 6.0},
        {"asset_id": asset_id, "timestamp": t3, "open": 101.0, "high": 103.0, "low": 100.5, "close": 102.0, "volume": 7.0},
        {"asset_id": asset_id, "timestamp": t4, "open": 102.0, "high": 104.0, "low": 101.5, "close": 103.0, "volume": 8.0},
    ]

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, candles)
        await session.commit()

    async with AsyncSessionLocal() as session:
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id).order_by(SyncRange.start_timestamp.asc()))).scalars().all()
        assert len(ranges) == 2
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t1
        assert ranges[1].start_timestamp == t3
        assert ranges[1].end_timestamp == t4


@pytest.mark.asyncio
async def test_20_transaction_rollback_on_error():
    """
    Test 20: Out-of-order or duplicate timestamp in batch raises PayloadCorruptionError
    and rolls back transaction completely. No partial dirty state.
    """
    asset_id = await _create_test_asset("BNBUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=5)
    t1 = now - timedelta(minutes=4)

    corrupted_candles = [
        {"asset_id": asset_id, "timestamp": t1, "open": 500.0, "high": 505.0, "low": 495.0, "close": 502.0, "volume": 10.0},
        {"asset_id": asset_id, "timestamp": t0, "open": 498.0, "high": 501.0, "low": 494.0, "close": 500.0, "volume": 12.0},
    ]

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        with pytest.raises(PayloadCorruptionError):
            await ingestion._commit_batch(asset_id, corrupted_candles)

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 0

        range_count = (await session.execute(select(func.count()).select_from(SyncRange).where(SyncRange.asset_id == asset_id))).scalar()
        assert range_count == 0


@pytest.mark.asyncio
async def test_21_overlapping_repairs_merge_cleanly():
    """
    Test 21: Overlapping repair ranges (e.g. 10:00 -> 10:30 and 10:20 -> 10:50)
    merge into a single continuous [10:00 -> 10:50] range without constraint violation.
    """
    asset_id = await _create_test_asset("AVAXUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=50)
    t30 = now - timedelta(minutes=20)
    t20 = now - timedelta(minutes=30)
    t50 = now

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, t0, t30)
        await session.commit()

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion.update_sync_ranges(asset_id, t20, t50)
        await session.commit()

    async with AsyncSessionLocal() as session:
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t50


@pytest.mark.asyncio
async def test_22_concurrent_same_gap_repair():
    """
    Test 22: Two concurrent workers inserting the same candle batch to PostgreSQL.
    ON CONFLICT DO NOTHING ensures both succeed without deadlock or constraint errors.
    """
    asset_id = await _create_test_asset("XRPUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=5)
    t1 = now - timedelta(minutes=4)

    candles = [
        {"asset_id": asset_id, "timestamp": t0, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 1000.0},
        {"asset_id": asset_id, "timestamp": t1, "open": 1.05, "high": 1.15, "low": 1.0, "close": 1.1, "volume": 1200.0},
    ]

    async def worker_commit():
        async with AsyncSessionLocal() as session:
            ingestion = IngestionService(session)
            await ingestion.insert_candle_batch(candles, target_model=Raw1mCandle)
            await session.commit()

    # Run both concurrently
    await asyncio.gather(worker_commit(), worker_commit())

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 2
