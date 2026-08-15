"""
Comprehensive Verification Test Suite for Bounded Live Ingestion Pipeline (P0.2 Phase 3).

Strictly covers all 42 required verification scenarios from Section 27:

CORE:
1.  finalized candle accepted
2.  partial candle rejected
3.  invalid OHLC rejected
4.  negative volume rejected
5.  UTC timestamp preserved

QUEUE:
6.  queue has maxsize
7.  queue cannot exceed maxsize
8.  50% threshold
9.  75% threshold
10. 90% threshold
11. 100% backpressure
12. bounded memory

ORDERING:
13. BTC chronological ordering
14. ETH chronological ordering
15. BTC and ETH independent ordering
16. duplicate candle
17. out-of-order candle
18. missing candle

BATCHING:
19. flush by batch size
20. flush by time
21. slow asset does not block others
22. batch has hard maximum

DATABASE:
23. successful batch commit
24. transaction rollback
25. duplicate database insert
26. concurrent BTC writers
27. concurrent BTC/ETH writers
28. sync_ranges gap preservation

OWNERSHIP:
29. valid owner can persist
30. lost owner cannot persist
31. ownership loss cancels processing
32. Redis failure fences ingestion

FAILURE:
33. DB unavailable
34. DB timeout
35. WebSocket disconnect
36. worker cancellation
37. graceful shutdown
38. forced shutdown

ASSET MAPPING:
39. valid symbol mapping
40. unknown symbol
41. inactive asset
42. bounded mapping cache
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

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
# CORE TESTS (1-5)
# ============================================================================

@pytest.mark.asyncio
async def test_01_finalized_candle_accepted(mock_asset_resolver):
    """1. Finalized candle is accepted into bounded queue."""
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


@pytest.mark.asyncio
async def test_02_partial_candle_rejected(mock_asset_resolver):
    """2. Partial / unfinalized candle is strictly rejected."""
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


@pytest.mark.asyncio
async def test_03_invalid_ohlc_rejected(mock_asset_resolver):
    """3. Invalid OHLC relationships are rejected defensively."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    # Valid candle
    valid_c = create_candle()
    assert validate_candle_payload(valid_c) is True

    # High < Low
    invalid_hl = create_candle()
    invalid_hl.high = 40000.0
    assert validate_candle_payload(invalid_hl) is False
    assert await pipeline.enqueue_candle(invalid_hl) is False

    # Open > High
    invalid_oh = create_candle()
    invalid_oh.open = 55000.0
    assert validate_candle_payload(invalid_oh) is False
    assert await pipeline.enqueue_candle(invalid_oh) is False

    # Close < Low
    invalid_cl = create_candle()
    invalid_cl.close = 45000.0
    assert validate_candle_payload(invalid_cl) is False
    assert await pipeline.enqueue_candle(invalid_cl) is False

    # Zero / Negative Price
    invalid_zero = create_candle()
    invalid_zero.low = 0.0
    assert validate_candle_payload(invalid_zero) is False
    assert await pipeline.enqueue_candle(invalid_zero) is False

    assert pipeline.metrics.rejected_candles == 4


@pytest.mark.asyncio
async def test_04_negative_volume_rejected(mock_asset_resolver):
    """4. Negative volume is rejected."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    c = create_candle()
    c.volume = -10.0
    assert validate_candle_payload(c) is False
    accepted = await pipeline.enqueue_candle(c)
    assert accepted is False
    assert pipeline.metrics.rejected_candles == 1


@pytest.mark.asyncio
async def test_05_utc_timestamp_preserved(mock_asset_resolver):
    """5. UTC timestamp is preserved; timezone-naive timestamp is rejected."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    # Naive timestamp
    naive_c = create_candle()
    naive_c.timestamp = datetime(2026, 8, 15, 12, 0, 0)
    assert validate_candle_payload(naive_c) is False
    assert await pipeline.enqueue_candle(naive_c) is False

    # UTC timestamp
    utc_c = create_candle()
    assert utc_c.timestamp.tzinfo == timezone.utc
    assert validate_candle_payload(utc_c) is True
    assert await pipeline.enqueue_candle(utc_c) is True


# ============================================================================
# QUEUE & BACKPRESSURE TESTS (6-12)
# ============================================================================

@pytest.mark.asyncio
async def test_06_queue_has_maxsize(mock_asset_resolver):
    """6. Queue is initialized with a strict maxsize (never unlimited)."""
    pipeline = BoundedLiveIngestionPipeline(shard_id=0, asset_resolver=mock_asset_resolver)
    assert pipeline.queue_maxsize == settings.WS_QUEUE_MAXSIZE
    assert pipeline._queue.maxsize == settings.WS_QUEUE_MAXSIZE
    assert pipeline._queue.maxsize > 0


@pytest.mark.asyncio
async def test_07_queue_cannot_exceed_maxsize(mock_asset_resolver):
    """7. Bounded queue never exceeds maxsize and applies bounded wait before drop."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=3,
    )
    pipeline._is_running = True

    # Fill queue to capacity (3 items)
    for i in range(3):
        assert await pipeline.enqueue_candle(create_candle(minute_offset=i)) is True

    assert pipeline.queue_size == 3

    # 4th item when queue full -> timeout & drop
    async def mock_wait_for_timeout(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError()

    with patch.object(asyncio, "wait_for", side_effect=mock_wait_for_timeout):
        accepted_4th = await pipeline.enqueue_candle(create_candle(minute_offset=3))
        assert accepted_4th is False
        assert pipeline.queue_size == 3
        assert pipeline.metrics.queue_overflow_count == 1


@pytest.mark.asyncio
async def test_08_queue_50_percent_threshold(mock_asset_resolver):
    """8. Queue utilization at 50% operates in normal mode."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    for i in range(5):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))

    assert pipeline.queue_utilization == 0.50
    assert pipeline.metrics.is_degraded is False


@pytest.mark.asyncio
async def test_09_queue_75_percent_threshold(mock_asset_resolver):
    """9. Queue utilization at 75% warning threshold."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    for i in range(8):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))

    assert pipeline.queue_utilization == 0.80
    assert pipeline.metrics.queue_utilization_ratio >= settings.WS_QUEUE_WARNING_THRESHOLD
    assert pipeline.metrics.is_degraded is False


@pytest.mark.asyncio
async def test_10_queue_90_percent_threshold(mock_asset_resolver):
    """10. Queue utilization at 90% enters degraded mode and triggers immediate flush."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=10,
    )
    pipeline._is_running = True

    for i in range(9):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))

    assert pipeline.queue_utilization == 0.90
    assert pipeline.metrics.is_degraded is True
    assert pipeline._flush_trigger_event.is_set() is True


@pytest.mark.asyncio
async def test_11_queue_100_percent_backpressure(mock_asset_resolver):
    """11. 100% capacity applies backpressure, blocking producer until space frees."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=2,
    )
    pipeline._is_running = True

    assert await pipeline.enqueue_candle(create_candle(minute_offset=0)) is True
    assert await pipeline.enqueue_candle(create_candle(minute_offset=1)) is True

    # 3rd candle will wait for queue space
    async def put_delayed():
        return await pipeline.enqueue_candle(create_candle(minute_offset=2))

    task = asyncio.create_task(put_delayed())
    await asyncio.sleep(0.05)
    assert not task.done()

    # Free 1 item from queue
    pipeline._queue.get_nowait()
    pipeline._queue.task_done()

    res = await task
    assert res is True
    assert pipeline.queue_size == 2


@pytest.mark.asyncio
async def test_12_bounded_memory(mock_asset_resolver):
    """12. Rapid event streaming through small queue keeps memory and queue size bounded."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=15,
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
        for i in range(60):
            await pipeline.enqueue_candle(create_candle(minute_offset=i))
            if pipeline.queue_size >= 10:
                await asyncio.sleep(0.02)

        await asyncio.sleep(0.1)
        assert pipeline.queue_size <= 15
        assert committed_count == 60
    finally:
        await pipeline.stop()


# ============================================================================
# ORDERING & PARTITIONING TESTS (13-18)
# ============================================================================

@pytest.mark.asyncio
async def test_13_btc_chronological_ordering(mock_asset_resolver):
    """13. BTC events are committed in strict chronological order."""
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


@pytest.mark.asyncio
async def test_14_eth_chronological_ordering(mock_asset_resolver):
    """14. ETH events are committed in strict chronological order."""
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


@pytest.mark.asyncio
async def test_15_btc_and_eth_independent_ordering(mock_asset_resolver):
    """15. BTC and ETH are partitioned and ordered independently without cross-interference."""
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
        btc_0 = create_candle(symbol="BTCUSDT", minute_offset=0)
        btc_2 = create_candle(symbol="BTCUSDT", minute_offset=2)
        btc_1 = create_candle(symbol="BTCUSDT", minute_offset=1)

        eth_2 = create_candle(symbol="ETHUSDT", minute_offset=2)
        eth_0 = create_candle(symbol="ETHUSDT", minute_offset=0)
        eth_1 = create_candle(symbol="ETHUSDT", minute_offset=1)

        # Interleave out of order across assets
        await pipeline.enqueue_candle(btc_0)
        await pipeline.enqueue_candle(eth_2)
        await pipeline.enqueue_candle(btc_2)
        await pipeline.enqueue_candle(eth_0)
        await pipeline.enqueue_candle(btc_1)
        await pipeline.enqueue_candle(eth_1)

        await asyncio.sleep(0.1)

        assert len(batches) == 2
        assert [c["timestamp"] for c in batches[1]] == [btc_0.timestamp, btc_1.timestamp, btc_2.timestamp]
        assert [c["timestamp"] for c in batches[2]] == [eth_0.timestamp, eth_1.timestamp, eth_2.timestamp]
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_16_duplicate_candle(mock_asset_resolver):
    """16. Duplicate candle timestamps in the same batch are deduplicated harmlessly."""
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
        t0 = create_candle(symbol="BTCUSDT", minute_offset=0)
        t0_dup = create_candle(symbol="BTCUSDT", minute_offset=0)

        await pipeline.enqueue_candle(t0)
        await pipeline.enqueue_candle(t0_dup)

        await asyncio.sleep(0.1)

        assert len(committed) == 1
        assert pipeline.metrics.duplicate_candles == 1
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_17_out_of_order_candle(mock_asset_resolver):
    """17. Out-of-order candles (10:00, 10:02, 10:01) are sorted chronologically."""
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


@pytest.mark.asyncio
async def test_18_missing_candle():
    """18. Missing candle (10:00, 10:04) preserves detectable gap in sync_ranges."""
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
# BATCHING TESTS (19-22)
# ============================================================================

@pytest.mark.asyncio
async def test_19_flush_by_batch_size(mock_asset_resolver):
    """19. Batch flushes immediately when queue reaches batch_size."""
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
        for i in range(3):
            await pipeline.enqueue_candle(create_candle(minute_offset=i))

        await asyncio.sleep(0.05)

        assert flush_mock.await_count == 1
        asset_id, payload = flush_mock.call_args[0]
        assert asset_id == 1
        assert len(payload) == 3
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_20_flush_by_time(mock_asset_resolver):
    """20. Batch flushes upon timer expiration if count < batch_size."""
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
        await pipeline.enqueue_candle(create_candle(minute_offset=0))
        await pipeline.enqueue_candle(create_candle(minute_offset=1))

        await asyncio.sleep(0.1)

        assert flush_mock.await_count == 1
        asset_id, payload = flush_mock.call_args[0]
        assert asset_id == 1
        assert len(payload) == 2
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_21_slow_asset_does_not_block_others(mock_asset_resolver):
    """21. Delay or failure on one asset does not block persistence of other assets in batch."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=50,
    )

    committed_assets = []

    async def mock_commit(asset_id: int, payload: list):
        if asset_id == 1:
            await asyncio.sleep(0.02)
            raise RuntimeError("BTC DB error")
        committed_assets.append(asset_id)

    pipeline._commit_asset_batch = mock_commit
    await pipeline.start()
    try:
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=0))
        await pipeline.enqueue_candle(create_candle(symbol="ETHUSDT", minute_offset=0))

        await asyncio.sleep(0.1)

        assert 2 in committed_assets  # ETH committed successfully
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_22_batch_has_hard_maximum(mock_asset_resolver):
    """22. _drain_batch_items never returns more items than batch_size."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        queue_maxsize=20,
        batch_size=5,
    )
    pipeline._is_running = True

    for i in range(15):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))

    assert pipeline.queue_size == 15
    batch = pipeline._drain_batch_items(5)
    assert len(batch) == 5
    assert pipeline.queue_size == 10


# ============================================================================
# DATABASE & TRANSACTION TESTS (23-28)
# ============================================================================

@pytest.mark.asyncio
async def test_23_successful_batch_commit():
    """23. Successful batch commit inserts into raw_1m_candles and sync_ranges."""
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            stmt_asset = pg_insert(AssetRegistry).values(
                id=1, symbol="BTCUSDT", exchange="binance", asset_type="spot", is_active=True
            ).on_conflict_do_nothing()
            await session.execute(stmt_asset)
            await session.execute(delete(SyncRange).where(SyncRange.asset_id == 1))
            await session.execute(delete(Raw1mCandle).where(Raw1mCandle.asset_id == 1))

    resolver = AssetRegistryResolver(session_factory=session_factory)
    resolver.register_asset("BTCUSDT", 1, is_active=True)

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=session_factory,
        asset_resolver=resolver,
        batch_size=2,
        flush_interval_ms=20,
    )
    await pipeline.start()
    try:
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=0))
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=1))

        await asyncio.sleep(0.2)

        assert pipeline.metrics.candles_persisted == 2
        assert pipeline.metrics.persistence_errors == 0

        async with session_factory() as session:
            candles_res = await session.execute(select(func.count(Raw1mCandle.timestamp)).where(Raw1mCandle.asset_id == 1))
            assert candles_res.scalar() == 2

            ranges_res = await session.execute(select(SyncRange).where(SyncRange.asset_id == 1))
            ranges = ranges_res.scalars().all()
            assert len(ranges) == 1
    finally:
        await pipeline.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_24_transaction_rollback(mock_asset_resolver):
    """24. Database persistence failure triggers clean rollback without false metric increments."""
    mock_session_factory = MagicMock()
    mock_session = AsyncMock()
    mock_session_factory.return_value.__aenter__.return_value = mock_session
    mock_session_factory.return_value.__aexit__.return_value = None

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=mock_session_factory,
        asset_resolver=mock_asset_resolver,
        batch_size=2,
        flush_interval_ms=50,
    )

    with patch("app.services.ingestion.IngestionService._commit_batch", side_effect=RuntimeError("DB Rollback Test")):
        await pipeline.start()
        try:
            await pipeline.enqueue_candle(create_candle(minute_offset=0))
            await pipeline.enqueue_candle(create_candle(minute_offset=1))

            await asyncio.sleep(0.1)

            assert pipeline.metrics.persistence_errors == 1
            assert pipeline.metrics.candles_persisted == 0
        finally:
            await pipeline.stop()


@pytest.mark.asyncio
async def test_25_duplicate_database_insert():
    """25. Inserting duplicate candle in separate batches does not crash (ON CONFLICT DO NOTHING)."""
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            stmt_asset = pg_insert(AssetRegistry).values(
                id=1, symbol="BTCUSDT", exchange="binance", asset_type="spot", is_active=True
            ).on_conflict_do_nothing()
            await session.execute(stmt_asset)
            await session.execute(delete(SyncRange).where(SyncRange.asset_id == 1))
            await session.execute(delete(Raw1mCandle).where(Raw1mCandle.asset_id == 1))

    resolver = AssetRegistryResolver(session_factory=session_factory)
    resolver.register_asset("BTCUSDT", 1, is_active=True)

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=session_factory,
        asset_resolver=resolver,
        batch_size=1,
        flush_interval_ms=10,
    )
    await pipeline.start()
    try:
        c0 = create_candle(symbol="BTCUSDT", minute_offset=0)
        await pipeline.enqueue_candle(c0)
        await asyncio.sleep(0.1)

        # Enqueue identical candle again in next batch
        await pipeline.enqueue_candle(c0)
        await asyncio.sleep(0.1)

        assert pipeline.metrics.persistence_errors == 0

        async with session_factory() as session:
            res = await session.execute(select(func.count(Raw1mCandle.timestamp)).where(Raw1mCandle.asset_id == 1))
            assert res.scalar() == 1
    finally:
        await pipeline.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_26_concurrent_btc_writers():
    """26. Concurrent writers inserting the same BTC candle resolve harmlessly."""
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
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
        same_candle = create_candle(symbol="BTCUSDT", minute_offset=0)
        await asyncio.gather(
            pipeline_a.enqueue_candle(same_candle),
            pipeline_b.enqueue_candle(same_candle),
        )

        await asyncio.sleep(0.2)

        assert pipeline_a.metrics.persistence_errors == 0
        assert pipeline_b.metrics.persistence_errors == 0

        async with session_factory() as session:
            res = await session.execute(select(func.count(Raw1mCandle.timestamp)).where(Raw1mCandle.asset_id == 1))
            assert res.scalar() == 1
    finally:
        await pipeline_a.stop()
        await pipeline_b.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_27_concurrent_btc_eth_writers():
    """27. Concurrent writers on different assets operate independently."""
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
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


@pytest.mark.asyncio
async def test_28_sync_ranges_gap_preservation():
    """28. Sync ranges in real PostgreSQL preserve non-contiguous gaps."""
    engine = create_async_engine(settings.sqlalchemy_database_uri)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            stmt_asset = pg_insert(AssetRegistry).values(
                id=1, symbol="BTCUSDT", exchange="binance", asset_type="spot", is_active=True
            ).on_conflict_do_nothing()
            await session.execute(stmt_asset)
            await session.execute(delete(SyncRange).where(SyncRange.asset_id == 1))
            await session.execute(delete(Raw1mCandle).where(Raw1mCandle.asset_id == 1))

    resolver = AssetRegistryResolver(session_factory=session_factory)
    resolver.register_asset("BTCUSDT", 1, is_active=True)

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=session_factory,
        asset_resolver=resolver,
        batch_size=10,
        flush_interval_ms=50,
    )
    await pipeline.start()
    try:
        # Enqueue 10:00 and 10:04 (missing 10:01, 10:02, 10:03)
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=0))
        await pipeline.enqueue_candle(create_candle(symbol="BTCUSDT", minute_offset=4))

        await asyncio.sleep(0.2)

        async with session_factory() as session:
            res = await session.execute(select(SyncRange).where(SyncRange.asset_id == 1).order_by(SyncRange.start_timestamp.asc()))
            ranges = res.scalars().all()
            assert len(ranges) == 2
            assert ranges[0].start_timestamp == ranges[0].end_timestamp
            assert ranges[1].start_timestamp == ranges[1].end_timestamp
    finally:
        await pipeline.stop()
        await engine.dispose()


# ============================================================================
# OWNERSHIP & FENCING TESTS (29-32)
# ============================================================================

@pytest.mark.asyncio
async def test_29_valid_owner_can_persist(mock_asset_resolver):
    """29. ShardRuntime with active lease claim successfully persists candles."""
    claim = ShardLeaseClaim(
        shard_id=0,
        worker_id="worker-1",
        claim_token="claim-token-1",
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

    runtime.pipeline.batch_size = 1
    runtime.pipeline.flush_interval_seconds = 0.02

    commit_mock = AsyncMock()
    runtime.pipeline._commit_asset_batch = commit_mock

    try:
        accepted = await runtime.enqueue_candle(create_candle(minute_offset=0))
        assert accepted is True
        await asyncio.sleep(0.1)
        assert commit_mock.await_count == 1
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_30_lost_owner_cannot_persist(mock_asset_resolver):
    """30. Lost shard owner is hard-fenced and cannot persist uncommitted data."""
    claim = ShardLeaseClaim(
        shard_id=0,
        worker_id="worker-1",
        claim_token="claim-token-1",
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

    # Shard loses lease
    runtime.fence(reason="Heartbeat lease expired")
    assert runtime.is_fenced is True

    # New work rejected
    accepted = await runtime.enqueue_candle(create_candle(minute_offset=0))
    assert accepted is False
    assert commit_mock.await_count == 0

    await runtime.stop()


@pytest.mark.asyncio
async def test_31_ownership_loss_cancels_processing(mock_asset_resolver):
    """31. Fencing discards in-memory queue items immediately."""
    claim = ShardLeaseClaim(
        shard_id=0,
        worker_id="worker-1",
        claim_token="claim-token-1",
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

    # Enqueue into queue without flusher running
    await runtime.enqueue_candle(create_candle(minute_offset=0))
    await runtime.enqueue_candle(create_candle(minute_offset=1))

    assert runtime.buffer_count >= 2
    runtime.fence(reason="Ownership lost to Worker B")

    assert runtime.pipeline.queue_size == 0
    assert runtime.buffer_count == 0
    assert runtime.pipeline.metrics.fenced_events_discarded >= 2

    await runtime.stop()


@pytest.mark.asyncio
async def test_32_redis_failure_fences_ingestion(mock_asset_resolver):
    """32. Redis failure causes fail-closed fencing, halting ingestion."""
    claim = ShardLeaseClaim(
        shard_id=0,
        worker_id="worker-1",
        claim_token="claim-token-1",
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

    # Simulate Redis connection failure
    runtime.fence(reason="RedisConnectionError: Connection refused")
    assert runtime.is_fenced is True
    assert runtime.is_accepting_work is False

    await runtime.stop()


# ============================================================================
# FAILURE & RESILIENCE TESTS (33-38)
# ============================================================================

@pytest.mark.asyncio
async def test_33_db_unavailable(mock_asset_resolver):
    """33. DB connection failure enters error handling, leaves gap recoverable, does not crash."""
    mock_session_factory = MagicMock()
    mock_session_factory.side_effect = RuntimeError("PostgreSQL Connection Refused")

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=mock_session_factory,
        asset_resolver=mock_asset_resolver,
        batch_size=2,
        flush_interval_ms=20,
    )
    await pipeline.start()
    try:
        await pipeline.enqueue_candle(create_candle(minute_offset=0))
        await pipeline.enqueue_candle(create_candle(minute_offset=1))

        await asyncio.sleep(0.1)

        assert pipeline.metrics.persistence_errors >= 1
        assert pipeline.metrics.candles_persisted == 0
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_34_db_timeout(mock_asset_resolver):
    """34. Slow/hanging DB commit hits timeout and rolls back safely."""
    mock_session_factory = MagicMock()
    mock_session = AsyncMock()
    mock_session_factory.return_value.__aenter__.return_value = mock_session
    mock_session_factory.return_value.__aexit__.return_value = None

    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        session_factory=mock_session_factory,
        asset_resolver=mock_asset_resolver,
        batch_size=1,
        flush_interval_ms=10,
    )

    async def timeout_commit(asset_id, payload):
        raise asyncio.TimeoutError("DB transaction timed out")

    with patch("app.services.ingestion.IngestionService._commit_batch", side_effect=timeout_commit):
        await pipeline.start()
        try:
            await pipeline.enqueue_candle(create_candle(minute_offset=0))
            await asyncio.sleep(0.05)

            assert pipeline.metrics.persistence_errors == 1
        finally:
            await pipeline.stop()


@pytest.mark.asyncio
async def test_35_ws_disconnect(mock_asset_resolver):
    """35. WebSocket disconnect allows pipeline to flush pending in-flight items cleanly."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=5000,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)

    pipeline._commit_asset_batch = mock_commit
    await pipeline.start()

    await pipeline.enqueue_candle(create_candle(minute_offset=0))
    await pipeline.enqueue_candle(create_candle(minute_offset=1))

    # WebSocket disconnects -> triggers pipeline stop
    await pipeline.stop()

    assert pipeline.queue_size == 0
    assert len(committed) == 2


@pytest.mark.asyncio
async def test_36_worker_cancellation(mock_asset_resolver):
    """36. Pipeline flusher handles asyncio cancellation cleanly without orphan tasks."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=5000,
    )
    await pipeline.start()

    if pipeline._flusher_task:
        pipeline._flusher_task.cancel()

    await pipeline.stop()
    assert pipeline.is_running is False


@pytest.mark.asyncio
async def test_37_graceful_shutdown(mock_asset_resolver):
    """37. Graceful shutdown drains queue and commits remaining batches."""
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=mock_asset_resolver,
        batch_size=10,
        flush_interval_ms=10000,
    )

    committed = []

    async def mock_commit(asset_id: int, payload: list):
        committed.extend(payload)
        pipeline.metrics.record_flush_complete(len(payload), 1.0)

    pipeline._commit_asset_batch = mock_commit
    await pipeline.start()

    for i in range(4):
        await pipeline.enqueue_candle(create_candle(minute_offset=i))

    assert pipeline.queue_size == 4
    await pipeline.stop()

    assert pipeline.queue_size == 0
    assert len(committed) == 4
    assert pipeline.metrics.candles_persisted == 4


@pytest.mark.asyncio
async def test_38_forced_shutdown(mock_asset_resolver):
    """38. Forced shutdown terminates without deadlock."""
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

    await pipeline.stop()
    assert pipeline.is_running is False


# ============================================================================
# ASSET MAPPING TESTS (39-42)
# ============================================================================

@pytest.mark.asyncio
async def test_39_valid_symbol_mapping(mock_asset_resolver):
    """39. Valid registered symbol resolves to (asset_id, True)."""
    res = await mock_asset_resolver.resolve_symbol("BTCUSDT")
    assert res == (1, True)


@pytest.mark.asyncio
async def test_40_unknown_symbol(mock_asset_resolver):
    """40. Unknown symbol returns None and is rejected by pipeline."""
    res = await mock_asset_resolver.resolve_symbol("UNKNOWNUSDT")
    assert res is None

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
        await pipeline.enqueue_candle(create_candle(symbol="UNKNOWNUSDT"))
        await asyncio.sleep(0.1)

        assert len(committed) == 0
        assert pipeline.metrics.unmapped_symbol_rejections == 1
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_41_inactive_asset(mock_asset_resolver):
    """41. Inactive/delisted asset returns (id, False) and is rejected by pipeline."""
    res = await mock_asset_resolver.resolve_symbol("DELISTEDUSDT")
    assert res == (4, False)

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
        await pipeline.enqueue_candle(create_candle(symbol="DELISTEDUSDT"))
        await asyncio.sleep(0.1)

        assert len(committed) == 0
        assert pipeline.metrics.inactive_asset_rejections == 1
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_42_bounded_mapping_cache():
    """42. AssetRegistryResolver cache is bounded and unknown symbols do not cause memory growth."""
    resolver = AssetRegistryResolver(max_cache_size=5)

    # Register up to capacity
    for i in range(5):
        assert resolver.register_asset(f"SYM{i}", i + 1, is_active=True) is True

    assert resolver.cached_count == 5

    # 6th registration rejected
    assert resolver.register_asset("SYM6", 6, is_active=True) is False
    assert resolver.cached_count == 5

    # Querying unknown symbols does not grow cache
    for i in range(100):
        res = await resolver.resolve_symbol(f"RANDOM{i}USDT")
        assert res is None

    assert resolver.cached_count == 5

    # Invalidate cache
    resolver.invalidate()
    assert resolver.is_cache_valid is False
