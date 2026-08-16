"""
Test Matrix — End-to-End Reconciliation (Tests 45 to 48).
Verifies complete E2E lifecycle from WebSocket outage to gap discovery, REST repair,
raw_1m_candles insertion, sync_ranges update, CAGG refresh scheduling, and worker crash recovery.
"""

import pytest
import asyncio
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
from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
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
async def test_45_and_46_and_47_e2e_ws_outage_to_rest_repair_and_cagg():
    """
    Tests 45, 46, 47: Full End-to-End flow:
    1. Pre-outage live WS data: 10:00 -> 10:10
    2. Connection outage: 10:11 -> 10:20 (10 missing candles)
    3. Reconnect live WS data: 10:21 -> 10:25
    4. Gap detected: [10:10, 10:21]
    5. REST repair job scheduled & executed
    6. Raw 1m candles completed (10:00 -> 10:25 all 26 candles present)
    7. sync_ranges updated into a single continuous range [10:00 -> 10:25]
    8. CAGG refresh job created for affected bucket
    """
    asset_id = await _create_test_asset("BTCUSDT")
    base_t = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=30)
    t0 = base_t
    t10 = base_t + timedelta(minutes=10)
    t21 = base_t + timedelta(minutes=21)
    t25 = base_t + timedelta(minutes=25)

    # 1. Ingest pre-outage candles (10:00 -> 10:10)
    pre_candles = [
        {"asset_id": asset_id, "timestamp": t0 + timedelta(minutes=i), "open": 50000.0 + i, "high": 50100.0 + i, "low": 49900.0 + i, "close": 50050.0 + i, "volume": 10.0}
        for i in range(11)
    ]

    # 2. Ingest post-reconnect candles (10:21 -> 10:25)
    post_candles = [
        {"asset_id": asset_id, "timestamp": t21 + timedelta(minutes=i), "open": 50200.0 + i, "high": 50300.0 + i, "low": 50100.0 + i, "close": 50250.0 + i, "volume": 12.0}
        for i in range(5)
    ]

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, pre_candles)
        await ingestion._commit_batch(asset_id, post_candles)
        await session.commit()

    # 3. Detect gap
    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t25)
    assert len(gaps) == 1
    gap_start, gap_end = gaps[0]
    assert gap_start == t10
    assert gap_end == t21

    # 4. Mock REST client yielding missing candles 10:11 -> 10:20 (10 candles)
    missing_candles = [
        {"asset_id": asset_id, "timestamp": t0 + timedelta(minutes=i), "open": 50100.0 + i, "high": 50150.0 + i, "low": 50050.0 + i, "close": 50120.0 + i, "volume": 8.0}
        for i in range(11, 21)
    ]

    mock_client = AsyncMock()
    async def mock_get_klines(sym, interval, st, et):
        for c in missing_candles:
            yield c
    mock_client.get_klines = mock_get_klines

    # 5. Schedule & Process Gap Repair Job
    job = await service.schedule_repair_job(asset_id, "BTCUSDT", gap_start, gap_end)
    assert job is not None
    assert job.status == GapRepairStatus.PENDING

    success = await service.process_next_job(worker_id="e2e-worker-1", binance_client=mock_client)
    assert success is True

    # 6. Verify Raw 1m candles are complete: 11 pre + 10 gap + 5 post = 26 candles
    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 26

        # 7. Verify sync_ranges is ONE continuous range [t0 -> t25]
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t25

        # 8. Verify CAGG refresh job was created
        cagg_jobs = (await session.execute(select(CaggRefreshJob))).scalars().all()
        assert len(cagg_jobs) >= 1
        assert cagg_jobs[0].status == RefreshStatus.PENDING

        final_job = await session.get(GapRepairJob, job.id)
        assert final_job.status == GapRepairStatus.COMPLETED


@pytest.mark.asyncio
async def test_48_worker_crash_reclaim_and_completion():
    """
    Test 48: Worker A claims job, downloads half the gap, crashes.
    Lease expires. Worker B reclaims the job, completes repair,
    yielding full continuous coverage.
    """
    asset_id = await _create_test_asset("SOLUSDT")
    base_t = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=20)
    start_t = base_t
    end_t = base_t + timedelta(minutes=10)

    service = GapRepairService(session_factory=AsyncSessionLocal)
    job = await service.schedule_repair_job(asset_id, "SOLUSDT", start_t, end_t)

    # Worker A claims job with short lease
    claimed = await service.claim_job(worker_id="crashed-worker-A", lease_duration=timedelta(milliseconds=50))
    assert claimed is not None

    # Worker A persists first 3 candles, then crashes
    partial_candles = [
        {"asset_id": asset_id, "timestamp": start_t + timedelta(minutes=i), "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0}
        for i in range(3)
    ]
    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, partial_candles)
        await session.commit()

    # Wait for Worker A's lease to expire
    await asyncio.sleep(0.1)

    # Worker B prepares full gap data
    full_candles = [
        {"asset_id": asset_id, "timestamp": start_t + timedelta(minutes=i), "open": 100.0, "high": 105.0, "low": 95.0, "close": 102.0, "volume": 10.0}
        for i in range(11)
    ]
    mock_client = AsyncMock()
    async def mock_get_klines(sym, interval, st, et):
        for c in full_candles:
            yield c
    mock_client.get_klines = mock_get_klines

    # Worker B reclaims and finishes
    success = await service.process_next_job(worker_id="hero-worker-B", binance_client=mock_client)
    assert success is True

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 11

        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == start_t
        assert ranges[0].end_timestamp == end_t

        final_job = await session.get(GapRepairJob, job.id)
        assert final_job.status == GapRepairStatus.COMPLETED
        assert final_job.worker_id == "hero-worker-B"
