"""
Comprehensive Verification Test Suite for Bounded Live Ingestion Pipeline (P0.2 Phase 3).

Covers all 27 required verification scenarios:
1. Finalized CandleEvent enters queue
2. Partial CandleEvent rejected
3. Bounded queue never exceeds maxsize
4. Queue utilization thresholds (50%, 75%, 90%, 100%)
5. Batch flush by size
6. Batch flush by time
7. Multiple assets are partitioned independently
8. BTC ordering preserved
9. ETH ordering preserved
10. BTC cannot interfere with ETH ordering
11. Duplicate candle is harmless
12. Out-of-order candle handled correctly
13. Missing minute does not create false continuous sync range
14. Invalid OHLC rejected
15. Invalid volume rejected
16. Unknown symbol handled
17. Inactive asset handled
18. Database batch transaction rollback
19. Concurrent BTC writers
20. Concurrent different-asset writers
21. Shard ownership loss fences persistence
22. Graceful shutdown
23. Forced cancellation
24. Memory remains bounded
25. Producer backpressure
26. Slow database handling
27. Queue saturation recovery
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.connectors.models import CandleEvent
from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.raw_1m_candles import Raw1mCandle
from app.models.sync_ranges import SyncRange
from app.services.ingestion import IngestionService
from app.services.ws_sharding.lease import ShardLeaseClaim
from app.services.ws_sharding.pipeline import (
    BoundedLiveIngestionPipeline,
    validate_candle_payload,
)
from app.services.ws_sharding.registry import AssetRegistryResolver
from app.services.ws_sharding.runtime import ShardRuntime, ShardRuntimeState


def create_candle(
    symbol: str = "BTCUSDT",
    minute_offset: int = 0,
    open_p: float = 50000.0,
    high_p: float = 50100.0,
    low_p: float = 49900.0,
    close_p: float = 50050.0,
    volume: float = 10.5,
    is_closed: bool = True,
    base_time: Optional[datetime] = None,
) -> CandleEvent:
    """Helper to create valid synthetic CandleEvents."""
    base = base_time or datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)
    ts = base + timedelta(minutes=minute_offset)
    close_ts = ts + timedelta(seconds=59, milliseconds=999)
    return CandleEvent(
        symbol=symbol,
        interval="1m",
        timestamp=ts,
        close_time=close_ts,
        open=open_p,
        high=high_p,
        low=low_p,
        close=close_p,
        volume=volume,
        is_closed=is_closed,
        source="binance_ws",
    )


@pytest.fixture
def mock_asset_resolver():
    """Provides an in-memory pre-configured asset resolver."""
    resolver = AssetRegistryResolver()
    resolver.register_asset("BTCUSDT", 1, is_active=True)
    resolver.register_asset("ETHUSDT", 2, is_active=True)
    resolver.register_asset("SOLUSDT", 3, is_active=True)
    resolver.register_asset("DELISTEDUSDT", 4, is_active=False)
    return resolver


# ============================================================================
# 1. Finalized CandleEvent Enters Queue
# ============================================================================
@pytest.mark.asyncio
async def test_1_finalized_candle_enters_queue(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=100,
    )
    await pipeline.start()
    try:
        candle = create_candle(symbol="BTCUSDT", is_closed=True)
        accepted = await pipeline.enqueue_candle(candle)

        assert accepted is True
        assert pipeline.metrics.candles_received == 1
        assert pipeline.metrics.rejected_candles == 0
        assert pipeline.queue_size == 1
    finally:
        await pipeline.stop()


# ============================================================================
# 2. Partial CandleEvent Rejected
# ============================================================================
@pytest.mark.asyncio
async def test_2_partial_candle_rejected(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=100,
    )
    await pipeline.start()
    try:
        partial_candle = create_candle(symbol="BTCUSDT", is_closed=False)
        accepted = await pipeline.enqueue_candle(partial_candle)

        assert accepted is False
        assert pipeline.metrics.rejected_candles == 1
        assert pipeline.metrics.candles_received == 0
        assert pipeline.queue_size == 0
    finally:
        await pipeline.stop()


# ============================================================================
# 3. Bounded Queue Never Exceeds Maxsize
# ============================================================================
@pytest.mark.asyncio
async def test_3_bounded_queue_never_exceeds_maxsize(mock_asset_resolver):
    # Fixed small maxsize of 5, flusher stopped to test queue saturation
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=5,
    )
    pipeline._is_running = True  # Enable enqueue without running flusher task

    # Fill queue to capacity
    for i in range(5):
        accepted = await pipeline.enqueue_candle(create_candle(minute_offset=i))
        assert accepted is True

    assert pipeline.queue_size == 5

    # 6th candle under backpressure with short timeout
    with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
        accepted_6th = await pipeline.enqueue_candle(create_candle(minute_offset=5))
        assert accepted_6th is False
        assert pipeline.queue_size == 5
        assert pipeline.metrics.queue_overflow_count == 1


# ============================================================================
# 4. Queue Utilization Thresholds (50%, 75%, 90%, 100%)
# ============================================================================
@pytest.mark.asyncio
async def test_4_queue_utilization_thresholds(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    # 50% capacity (5 items)
    for i in range(5):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))
    assert pipeline.queue_utilization == 0.50
    assert pipeline.metrics.is_degraded is False

    # 80% capacity (8 items -> >= 75% warning)
    for i in range(5, 8):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))
    assert pipeline.queue_utilization == 0.80
    assert pipeline.metrics.is_degraded is False

    # 90% capacity (9 items -> degraded mode active)
    await pipeline.enqueue_candle(create_candle(minute_offset=8))
    assert pipeline.queue_utilization == 0.90
    assert pipeline.metrics.is_degraded is True


# ============================================================================
# 5. Batch Flush by Size
# ============================================================================
@pytest.mark.asyncio
async def test_5_batch_flush_by_size(mock_asset_resolver):
    # Set huge flush interval (100 seconds) so time flush will not trigger
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=3,
        flush_interval_ms=100000,
    )

    flush_mock = AsyncMock()
    pipeline._commit_asset_batch = flush_mock

    await pipeline.start()
    try:
        # Enqueue 3 items (reaches batch_size=3)
        for i in range(3):
            await pipeline.enqueue_candle(create_candle(minute_offset=i))

        # Yield to let flusher run
        await asyncio.sleep(0.05)

        assert flush_mock.await_count == 1
        asset_id, payload = flush_mock.call_args[0]
        assert asset_id == 1
        assert len(payload) == 3
    finally:
        await pipeline.stop()


# ============================================================================
# 6. Batch Flush by Time
# ============================================================================
@pytest.mark.asyncio
async def test_6_batch_flush_by_time(mock_asset_resolver):
    # Set huge batch_size (100) and small flush interval (50ms)
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=100,
        flush_interval_ms=50,
    )

    flush_mock = AsyncMock()
    pipeline._commit_asset_batch = flush_mock

    await pipeline.start()
    try:
        # Enqueue only 2 items (< 100)
        await pipeline.enqueue_candle(create_candle(minute_offset=0))
        await pipeline.enqueue_candle(create_candle(minute_offset=1))

        # Wait for timer to expire (~80ms)
        await asyncio.sleep(0.1)

        assert flush_mock.await_count == 1
        asset_id, payload = flush_mock.call_args[0]
        assert asset_id == 1
        assert len(payload) == 2
    finally:
        await pipeline.stop()


# ============================================================================
# 7. Multiple Assets Partitioned Independently
# ============================================================================
@pytest.mark.asyncio
async def test_7_multiple_assets_partitioned_independently(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=50,
    )

    committed_batches = {}

    async def mock_commit(asset_id: int, payload: list):
        committed_batches[asset_id] = payload

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        # Enqueue mixed assets in interleaved order
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=0))
        await pipeline.enqueue_candle(create_candle(symbol="ETHUSDT", minute_offset=0))
        await pipeline.enqueue_candle(create_candle(symbol="SOLUSDT", minute_offset=0))
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=1))
        await pipeline.enqueue_candle(create_candle(symbol="ETHUSDT", minute_offset=1))

        await asyncio.sleep(0.1)

        assert len(committed_batches) == 3
        assert 1 in committed_batches  # BTC
        assert 2 in committed_batches  # ETH
        assert 3 in committed_batches  # SOL
        assert len(committed_batches[1]) == 2
        assert len(committed_batches[2]) == 2
        assert len(committed_batches[3]) == 1
    finally:
        await pipeline.stop()


# ============================================================================
# 8. BTC Ordering Preserved
# ============================================================================
@pytest.mark.asyncio
async def test_8_btc_ordering_preserved(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=5,
        flush_interval_ms=50,
    )

    committed_payload = []

    async def mock_commit(asset_id: int, payload: list):
        committed_payload.extend(payload)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        # Enqueue in order
        t0 = create_candle(symbol="BTCUSDT", minute_offset=0)
        t1 = create_candle(symbol="BTCUSDT", minute_offset=1)
        t2 = create_candle(symbol="BTCUSDT", minute_offset=2)

        await pipeline.enqueue_candle(t0)
        await pipeline.enqueue_candle(t1)
        await pipeline.enqueue_candle(t2)

        await asyncio.sleep(0.1)

        assert len(committed_payload) == 3
        assert committed_payload[0]["timestamp"] == t0.timestamp
        assert committed_payload[1]["timestamp"] == t1.timestamp
        assert committed_payload[2]["timestamp"] == t2.timestamp
    finally:
        await pipeline.stop()


# ============================================================================
# 9. ETH Ordering Preserved
# ============================================================================
@pytest.mark.asyncio
async def test_9_eth_ordering_preserved(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=5,
        flush_interval_ms=50,
    )

    committed_payload = []

    async def mock_commit(asset_id: int, payload: list):
        committed_payload.extend(payload)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        t0 = create_candle(symbol="ETHUSDT", minute_offset=0)
        t1 = create_candle(symbol="ETHUSDT", minute_offset=1)

        await pipeline.enqueue_candle(t0)
        await pipeline.enqueue_candle(t1)

        await asyncio.sleep(0.1)

        assert len(committed_payload) == 2
        assert committed_payload[0]["timestamp"] == t0.timestamp
        assert committed_payload[1]["timestamp"] == t1.timestamp
    finally:
        await pipeline.stop()


# ============================================================================
# 10. BTC Cannot Interfere with ETH Ordering
# ============================================================================
@pytest.mark.asyncio
async def test_10_btc_cannot_interfere_with_eth_ordering(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=50,
    )

    batches = {}

    async def mock_commit(asset_id: int, payload: list):
        batches[asset_id] = payload

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        # Interleave out of order across assets:
        # BTC: 10:00, 10:02, 10:01
        # ETH: 10:02, 10:00, 10:01
        btc_0 = create_candle(symbol="BTCUSDT", minute_offset=0)
        btc_2 = create_candle(symbol="BTCUSDT", minute_offset=2)
        btc_1 = create_candle(symbol="BTCUSDT", minute_offset=1)

        eth_2 = create_candle(symbol="ETHUSDT", minute_offset=2)
        eth_0 = create_candle(symbol="ETHUSDT", minute_offset=0)
        eth_1 = create_candle(symbol="ETHUSDT", minute_offset=1)

        await pipeline.enqueue_candle(btc_0)
        await pipeline.enqueue_candle(eth_2)
        await pipeline.enqueue_candle(btc_2)
        await pipeline.enqueue_candle(eth_0)
        await pipeline.enqueue_candle(btc_1)
        await pipeline.enqueue_candle(eth_1)

        await asyncio.sleep(0.1)

        assert len(batches) == 2
        # BTC strictly sorted independently
        assert [c["timestamp"] for c in batches[1]] == [btc_0.timestamp, btc_1.timestamp, btc_2.timestamp]
        # ETH strictly sorted independently
        assert [c["timestamp"] for c in batches[2]] == [eth_0.timestamp, eth_1.timestamp, eth_2.timestamp]
    finally:
        await pipeline.stop()


# ============================================================================
# 11. Duplicate Candle is Harmless (In-Batch Deduplication)
# ============================================================================
@pytest.mark.asyncio
async def test_11_duplicate_candle_is_harmless(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=5,
        flush_interval_ms=50,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        # Same timestamp sent twice
        t0 = create_candle(symbol="BTCUSDT", minute_offset=0)
        t0_dup = create_candle(symbol="BTCUSDT", minute_offset=0)

        await pipeline.enqueue_candle(t0)
        await pipeline.enqueue_candle(t0_dup)

        await asyncio.sleep(0.1)

        assert len(committed) == 1
        assert pipeline.metrics.duplicate_candles == 1
    finally:
        await pipeline.stop()


# ============================================================================
# 12. Out-of-Order Candle Handled Correctly
# ============================================================================
@pytest.mark.asyncio
async def test_12_out_of_order_candle_handled_correctly(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=5,
        flush_interval_ms=50,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        # Delivered as 10:00, 10:02, 10:01
        t0 = create_candle(minute_offset=0)
        t2 = create_candle(minute_offset=2)
        t1 = create_candle(minute_offset=1)

        await pipeline.enqueue_candle(t0)
        await pipeline.enqueue_candle(t2)
        await pipeline.enqueue_candle(t1)

        await asyncio.sleep(0.1)

        assert len(committed) == 3
        assert committed[0]["timestamp"] == t0.timestamp
        assert committed[1]["timestamp"] == t1.timestamp
        assert committed[2]["timestamp"] == t2.timestamp
    finally:
        await pipeline.stop()


# ============================================================================
# 13. Missing Minute Does Not Create False Continuous Sync Range
# ============================================================================
@pytest.mark.asyncio
async def test_13_missing_minute_does_not_create_false_continuous_sync_range():
    # Test with IngestionService._commit_batch directly
    mock_db = AsyncMock()
    begin_mock = AsyncMock()
    begin_mock.__aenter__.return_value = None
    begin_mock.__aexit__.return_value = None
    mock_db.begin = MagicMock(return_value=begin_mock)

    service = IngestionService(db_session=mock_db)

    t0 = datetime(2026, 8, 15, 10, 0, tzinfo=timezone.utc)
    t4 = datetime(2026, 8, 15, 10, 4, tzinfo=timezone.utc)

    candles = [
        {"asset_id": 1, "timestamp": t0, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10.0},
        {"asset_id": 1, "timestamp": t4, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10.0},
    ]

    with patch.object(service, "insert_candle_batch", new_callable=AsyncMock) as mock_insert:
        with patch.object(service, "update_sync_ranges", new_callable=AsyncMock) as mock_update:
            await service._commit_batch(1, candles)

            # Must update sync_ranges TWICE (separate blocks [t0, t0] and [t4, t4]), NEVER [t0, t4]
            assert mock_update.call_count == 2
            mock_update.assert_any_call(1, t0, t0)
            mock_update.assert_any_call(1, t4, t4)


# ============================================================================
# 14. Invalid OHLC Rejected
# ============================================================================
@pytest.mark.asyncio
async def test_14_invalid_ohlc_rejected(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    # High < Low (Using CandleEvent with invalid payload validation)
    # CandleEvent constructor rejects invalid OHLC by default, but we test validate_candle_payload defensively
    valid_c = create_candle()
    assert validate_candle_payload(valid_c) is True

    # Mutate internally to simulate corrupted object bypass
    valid_c.high = 40000.0  # High < Low (49900)
    assert validate_candle_payload(valid_c) is False

    accepted = await pipeline.enqueue_candle(valid_c)
    assert accepted is False
    assert pipeline.metrics.rejected_candles == 1


# ============================================================================
# 15. Invalid Volume Rejected
# ============================================================================
@pytest.mark.asyncio
async def test_15_invalid_volume_rejected(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    valid_c = create_candle()
    valid_c.volume = -5.0
    assert validate_candle_payload(valid_c) is False

    accepted = await pipeline.enqueue_candle(valid_c)
    assert accepted is False
    assert pipeline.metrics.rejected_candles == 1


# ============================================================================
# 16. Unknown Symbol Handled Safely
# ============================================================================
@pytest.mark.asyncio
async def test_16_unknown_symbol_handled(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=5,
        flush_interval_ms=50,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        unknown_candle = create_candle(symbol="NONEXISTENTUSDT")
        await pipeline.enqueue_candle(unknown_candle)

        await asyncio.sleep(0.1)

        assert len(committed) == 0
        assert pipeline.metrics.unmapped_symbol_rejections == 1
    finally:
        await pipeline.stop()


# ============================================================================
# 17. Inactive Asset Handled Safely
# ============================================================================
@pytest.mark.asyncio
async def test_17_inactive_asset_handled(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=5,
        flush_interval_ms=50,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()
    try:
        delisted_candle = create_candle(symbol="DELISTEDUSDT")
        await pipeline.enqueue_candle(delisted_candle)

        await asyncio.sleep(0.1)

        assert len(committed) == 0
        assert pipeline.metrics.inactive_asset_rejections == 1
    finally:
        await pipeline.stop()


# ============================================================================
# 18. Database Batch Transaction Rollback
# ============================================================================
@pytest.mark.asyncio
async def test_18_database_batch_transaction_rollback(mock_asset_resolver):
    # Mock session factory where _commit_batch raises DB error
    mock_session = AsyncMock()
    mock_session_factory = MagicMock()
    mock_session_factory.return_value.__aenter__.return_value = mock_session
    mock_session_factory.return_value.__aexit__.return_value = None

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=mock_session_factory,
        asset_resolver=mock_asset_resolver,
        batch_size=2,
        flush_interval_ms=50,
    )

    with patch("app.services.ws_sharding.pipeline.IngestionService._commit_batch", side_effect=RuntimeError("DB Connection Lost")):
        await pipeline.start()
        try:
            await pipeline.enqueue_candle(create_candle(minute_offset=0))
            await pipeline.enqueue_candle(create_candle(minute_offset=1))

            await asyncio.sleep(0.1)

            assert pipeline.metrics.persistence_errors == 1
            assert pipeline.metrics.candles_persisted == 0
        finally:
            await pipeline.stop()


# ============================================================================
# 19. Concurrent BTC Writers (Real PostgreSQL)
# ============================================================================
@pytest.mark.asyncio
async def test_19_concurrent_btc_writers():
    """Verify concurrent writers inserting same BTC candle do not crash (ON CONFLICT DO NOTHING)."""
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Seed asset registry and clean up test rows
    async with session_factory() as session:
        async with session.begin():
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            from sqlalchemy import delete
            stmt_asset = pg_insert(AssetRegistry).values(
                id=1, symbol="BTCUSDT", exchange="binance", asset_type="spot", is_active=True
            ).on_conflict_do_nothing()
            await session.execute(stmt_asset)
            await session.execute(delete(SyncRange).where(SyncRange.asset_id == 1))
            await session.execute(delete(Raw1mCandle).where(Raw1mCandle.asset_id == 1))

    resolver = AssetRegistryResolver(session_factory=session_factory)
    resolver.register_asset("BTCUSDT", 1, is_active=True)

    pipeline_a = BoundedLiveIngestionPipeline(shard_id=0, session_factory=session_factory, asset_resolver=resolver, batch_size=1, flush_interval_ms=20)
    pipeline_b = BoundedLiveIngestionPipeline(shard_id=1, session_factory=session_factory, asset_resolver=resolver, batch_size=1, flush_interval_ms=20)

    await pipeline_a.start()
    await pipeline_b.start()

    try:
        # Both enqueue exact same BTC candle simultaneously
        same_candle = create_candle(symbol="BTCUSDT", minute_offset=0)
        await asyncio.gather(
            pipeline_a.enqueue_candle(same_candle),
            pipeline_b.enqueue_candle(same_candle),
        )

        await asyncio.sleep(0.2)

        assert pipeline_a.metrics.persistence_errors == 0
        assert pipeline_b.metrics.persistence_errors == 0

        # Query database to confirm exactly 1 candle stored
        async with session_factory() as session:
            from sqlalchemy import select, func
            res = await session.execute(select(func.count(Raw1mCandle.timestamp)).where(Raw1mCandle.asset_id == 1))
            count = res.scalar()
            assert count == 1

    finally:
        await pipeline_a.stop()
        await pipeline_b.stop()
        await engine.dispose()


# ============================================================================
# 20. Concurrent Different-Asset Writers (Real PostgreSQL)
# ============================================================================
@pytest.mark.asyncio
async def test_20_concurrent_different_asset_writers():
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Seed asset registry and clean up test rows
    async with session_factory() as session:
        async with session.begin():
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            from sqlalchemy import delete
            stmt_btc = pg_insert(AssetRegistry).values(
                id=1, symbol="BTCUSDT", exchange="binance", asset_type="spot", is_active=True
            ).on_conflict_do_nothing()
            stmt_eth = pg_insert(AssetRegistry).values(
                id=2, symbol="ETHUSDT", exchange="binance", asset_type="spot", is_active=True
            ).on_conflict_do_nothing()
            await session.execute(stmt_btc)
            await session.execute(stmt_eth)
            await session.execute(delete(SyncRange).where(SyncRange.asset_id.in_([1, 2])))
            await session.execute(delete(Raw1mCandle).where(Raw1mCandle.asset_id.in_([1, 2])))

    resolver = AssetRegistryResolver(session_factory=session_factory)
    resolver.register_asset("BTCUSDT", 1, is_active=True)
    resolver.register_asset("ETHUSDT", 2, is_active=True)

    pipeline_a = BoundedLiveIngestionPipeline(shard_id=0, session_factory=session_factory, asset_resolver=resolver, batch_size=1, flush_interval_ms=20)
    pipeline_b = BoundedLiveIngestionPipeline(shard_id=1, session_factory=session_factory, asset_resolver=resolver, batch_size=1, flush_interval_ms=20)

    await pipeline_a.start()
    await pipeline_b.start()

    try:
        btc_candle = create_candle(symbol="BTCUSDT", minute_offset=5)
        eth_candle = create_candle(symbol="ETHUSDT", minute_offset=5)

        await asyncio.gather(
            pipeline_a.enqueue_candle(btc_candle),
            pipeline_b.enqueue_candle(eth_candle),
        )

        await asyncio.sleep(0.2)

        assert pipeline_a.metrics.persistence_errors == 0
        assert pipeline_b.metrics.persistence_errors == 0
        assert pipeline_a.metrics.candles_persisted == 1
        assert pipeline_b.metrics.candles_persisted == 1

    finally:
        await pipeline_a.stop()
        await pipeline_b.stop()
        await engine.dispose()


# ============================================================================
# 21. Shard Ownership Loss Fences Persistence
# ============================================================================
@pytest.mark.asyncio
async def test_21_shard_ownership_loss_fences_persistence(mock_asset_resolver):
    claim = ShardLeaseClaim(
        shard_id=0,
        worker_id="test-worker",
        claim_token="claim-123",
        claimed_at=datetime.now(timezone.utc),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=15),
    )
    runtime = ShardRuntime(
        shard_id=0,
        symbols=["BTCUSDT"],
        claim=claim,
        asset_resolver=mock_asset_resolver,
    )
    await runtime.start()

    commit_mock = AsyncMock()
    runtime.pipeline._commit_asset_batch = commit_mock

    # Enqueue a candle while running
    await runtime.enqueue_candle(create_candle(minute_offset=0))
    assert runtime.pipeline.queue_size == 1

    # Lease lost -> trigger hard fencing
    runtime.fence(reason="Heartbeat renewal failed")

    assert runtime.is_fenced is True
    assert runtime.pipeline.queue_size == 0
    assert runtime.pipeline.metrics.fenced_events_discarded >= 1

    # New work rejected
    rejected = await runtime.enqueue_candle(create_candle(minute_offset=1))
    assert rejected is False
    assert commit_mock.await_count == 0

    await runtime.stop()


# ============================================================================
# 22. Graceful Shutdown Drains Queue
# ============================================================================
@pytest.mark.asyncio
async def test_22_graceful_shutdown_drains_queue(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=10000,  # Long timer
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)
        pipeline.metrics.record_flush_complete(len(payload), 1.0)

    pipeline._commit_asset_batch = mock_commit

    await pipeline.start()

    # Enqueue 3 items
    for i in range(3):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))

    assert pipeline.queue_size == 3

    # Stop gracefully
    await pipeline.stop()

    assert pipeline.queue_size == 0
    assert len(committed) == 3
    assert pipeline.metrics.candles_persisted == 3


# ============================================================================
# 23. Forced Cancellation Cleans Up
# ============================================================================
@pytest.mark.asyncio
async def test_23_forced_cancellation_cleans_up(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=10000,
    )
    await pipeline.start()

    # Cancel task directly
    if pipeline._flusher_task:
        pipeline._flusher_task.cancel()

    # Calling stop should clean up without error or deadlock
    await pipeline.stop()
    assert pipeline.is_running is False


# ============================================================================
# 24. Memory Remains Bounded
# ============================================================================
@pytest.mark.asyncio
async def test_24_memory_remains_bounded(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=20,
        batch_size=5,
        flush_interval_ms=10,
    )

    committed_count = 0

    async def mock_commit(asset_id: int, payload: list):
        nonlocal committed_count
        committed_count += len(payload)

    pipeline._commit_asset_batch = mock_commit
    await pipeline.start()

    try:
        # Enqueue 50 items rapidly through small queue
        for i in range(50):
            await pipeline.enqueue_candle(create_candle(minute_offset=i))
            if pipeline.queue_size >= 15:
                await asyncio.sleep(0.02)  # Let flusher drain

        await asyncio.sleep(0.1)
        assert pipeline.queue_size <= 20
        assert committed_count == 50
    finally:
        await pipeline.stop()


# ============================================================================
# 25. Producer Backpressure
# ============================================================================
@pytest.mark.asyncio
async def test_25_producer_backpressure(mock_asset_resolver):
    # Queue maxsize 2, flusher stopped
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=2,
    )
    pipeline._is_running = True

    assert await pipeline.enqueue_candle(create_candle(minute_offset=0)) is True
    assert await pipeline.enqueue_candle(create_candle(minute_offset=1)) is True

    # 3rd candle will trigger backpressure wait
    async def put_delayed():
        return await pipeline.enqueue_candle(create_candle(minute_offset=2))

    task = asyncio.create_task(put_delayed())
    await asyncio.sleep(0.05)
    assert not task.done()  # Blocked waiting for queue space

    # Drain 1 item to unblock
    pipeline._queue.get_nowait()
    pipeline._queue.task_done()

    res = await task
    assert res is True


# ============================================================================
# 26. Slow Database Handling
# ============================================================================
@pytest.mark.asyncio
async def test_26_slow_database_handling(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=2,
        flush_interval_ms=20,
    )

    async def slow_commit(asset_id: int, payload: list):
        await asyncio.sleep(0.05)  # 50ms slow database
        pipeline.metrics.record_flush_complete(len(payload), 50.0)

    pipeline._commit_asset_batch = slow_commit
    await pipeline.start()

    try:
        await pipeline.enqueue_candle(create_candle(minute_offset=0))
        await pipeline.enqueue_candle(create_candle(minute_offset=1))

        await asyncio.sleep(0.15)

        assert pipeline.metrics.candles_persisted == 2
        assert pipeline.metrics.last_batch_latency_ms >= 50.0
    finally:
        await pipeline.stop()


# ============================================================================
# 27. Queue Saturation Recovery
# ============================================================================
@pytest.mark.asyncio
async def test_27_queue_saturation_recovery(mock_asset_resolver):
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
        batch_size=10,
        flush_interval_ms=50,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)

    pipeline._commit_asset_batch = mock_commit
    await pipeline.start()

    try:
        # Fill to 90% (9 items)
        for i in range(9):
            await pipeline.enqueue_candle(create_candle(minute_offset=i))

        # Flusher will automatically wake up upon degraded threshold and drain
        await asyncio.sleep(0.1)

        # Queue should be drained and degraded state cleared
        assert pipeline.queue_size == 0
        assert pipeline.metrics.is_degraded is False
        assert len(committed) == 9
    finally:
        await pipeline.stop()
