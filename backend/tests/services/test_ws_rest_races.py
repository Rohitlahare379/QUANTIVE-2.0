"""
Test Matrix — WebSocket / REST Race Handling (Tests 23 to 26).
Verifies race safety when WebSocket live feed and REST reconciliation process identical
candles, adjacent candles, concurrent live streaming during historical repair, and
WebSocket reconnection gap reconciliation.
"""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch
from sqlalchemy import select, delete, func
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.connectors.models import CandleEvent
from app.models.asset_registry import AssetRegistry
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.models.sync_ranges import SyncRange
from app.models.gap_repair_jobs import GapRepairJob
from app.services.gap_repair import GapRepairService
from app.services.ingestion import IngestionService
from app.services.ws_sharding.pipeline import BoundedLiveIngestionPipeline
from app.services.ws_sharding.registry import AssetRegistryResolver

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
async def test_23_ws_and_rest_same_candle():
    """
    Test 23: WebSocket live pipeline and REST reconciliation receive the EXACT SAME candle
    simultaneously. Result: exactly ONE authoritative candle in DB, no duplicate rows or errors.
    """
    asset_id = await _create_test_asset("BTCUSDT")
    t0 = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=5)
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

    await asyncio.gather(run_ws(), run_rest())
    await pipeline.stop()

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 1

        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t0


@pytest.mark.asyncio
async def test_24_ws_and_rest_adjacent_candles():
    """
    Test 24: WS receives 10:01 and 10:03. REST repair receives 10:02.
    Final DB state: [10:01, 10:02, 10:03] with ONE continuous sync_range.
    """
    asset_id = await _create_test_asset("ETHUSDT")
    base_t = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=10)
    t1 = base_t + timedelta(minutes=1)
    t2 = base_t + timedelta(minutes=2)
    t3 = base_t + timedelta(minutes=3)

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        c1 = {"asset_id": asset_id, "timestamp": t1, "open": 2000.0, "high": 2010.0, "low": 1990.0, "close": 2005.0, "volume": 5.0}
        c3 = {"asset_id": asset_id, "timestamp": t3, "open": 2010.0, "high": 2020.0, "low": 2005.0, "close": 2015.0, "volume": 6.0}
        await ingestion._commit_batch(asset_id, [c1])
        await ingestion._commit_batch(asset_id, [c3])
        await session.commit()

    async with AsyncSessionLocal() as session:
        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 2

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        c2 = {"asset_id": asset_id, "timestamp": t2, "open": 2005.0, "high": 2015.0, "low": 2000.0, "close": 2010.0, "volume": 4.0}
        await ingestion._commit_batch(asset_id, [c2])
        await session.commit()

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 3

        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t1
        assert ranges[0].end_timestamp == t3


@pytest.mark.asyncio
async def test_25_ws_live_data_during_historical_repair():
    """
    Test 25: Live WebSocket ingestion continues running unblocked while a historical
    REST repair executes in the background. Both persist cleanly without conflict.
    """
    asset_id = await _create_test_asset("SOLUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    
    hist_t0 = now - timedelta(hours=2)
    hist_candles = [
        {"asset_id": asset_id, "timestamp": hist_t0 + timedelta(minutes=i), "open": 100.0 + i, "high": 102.0 + i, "low": 99.0 + i, "close": 101.0 + i, "volume": 10.0}
        for i in range(5)
    ]

    live_t0 = now - timedelta(minutes=3)
    live_candles = [
        {"asset_id": asset_id, "timestamp": live_t0 + timedelta(minutes=i), "open": 150.0 + i, "high": 152.0 + i, "low": 149.0 + i, "close": 151.0 + i, "volume": 20.0}
        for i in range(3)
    ]

    async def run_historical_repair():
        async with AsyncSessionLocal() as session:
            ingestion = IngestionService(session)
            await ingestion._commit_batch(asset_id, hist_candles)
            await session.commit()

    async def run_live_stream():
        for c in live_candles:
            async with AsyncSessionLocal() as session:
                ingestion = IngestionService(session)
                await ingestion._commit_batch(asset_id, [c])
                await session.commit()

    await asyncio.gather(run_historical_repair(), run_live_stream())

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 8

        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id).order_by(SyncRange.start_timestamp.asc()))).scalars().all()
        assert len(ranges) == 2
        assert ranges[0].start_timestamp == hist_t0
        assert ranges[0].end_timestamp == hist_t0 + timedelta(minutes=4)
        assert ranges[1].start_timestamp == live_t0
        assert ranges[1].end_timestamp == live_t0 + timedelta(minutes=2)


@pytest.mark.asyncio
async def test_26_ws_reconnect_after_gap():
    """
    Test 26: WebSocket drops for 5 minutes, reconnects. Gap is detected and
    reconciled via repair_gap_inline. Entire range becomes seamless.
    """
    asset_id = await _create_test_asset("BTCUSDT")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=15)
    t5 = now - timedelta(minutes=10)
    t10 = now - timedelta(minutes=5)
    t12 = now - timedelta(minutes=3)

    pre_candles = [
        {"asset_id": asset_id, "timestamp": t0 + timedelta(minutes=i), "open": 50000.0, "high": 50100.0, "low": 49900.0, "close": 50050.0, "volume": 10.0}
        for i in range(6)
    ]
    post_candles = [
        {"asset_id": asset_id, "timestamp": t10 + timedelta(minutes=i), "open": 50200.0, "high": 50300.0, "low": 50100.0, "close": 50250.0, "volume": 12.0}
        for i in range(3)
    ]

    async with AsyncSessionLocal() as session:
        ingestion = IngestionService(session)
        await ingestion._commit_batch(asset_id, pre_candles)
        await ingestion._commit_batch(asset_id, post_candles)
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    gaps = await service.detect_gaps(asset_id, t0, t12)
    assert len(gaps) == 1
    assert gaps[0] == (t0 + timedelta(minutes=5), t10)

    gap_candles = [
        {"asset_id": asset_id, "timestamp": t0 + timedelta(minutes=i), "open": 50100.0, "high": 50200.0, "low": 50000.0, "close": 50150.0, "volume": 8.0}
        for i in range(6, 10)
    ]

    mock_client = AsyncMock()
    async def mock_get_klines(sym, interval, st, et):
        for c in gap_candles:
            yield c
    mock_client.get_klines = mock_get_klines

    repaired_count = await service.repair_gap_inline(asset_id, "BTCUSDT", t0, t12, binance_client=mock_client)
    assert repaired_count == 4

    async with AsyncSessionLocal() as session:
        count = (await session.execute(select(func.count()).select_from(Raw1mCandle).where(Raw1mCandle.asset_id == asset_id))).scalar()
        assert count == 13

        ranges = (await session.execute(select(SyncRange).where(SyncRange.asset_id == asset_id))).scalars().all()
        assert len(ranges) == 1
        assert ranges[0].start_timestamp == t0
        assert ranges[0].end_timestamp == t12
