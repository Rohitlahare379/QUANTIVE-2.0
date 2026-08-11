import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.ingestion import IngestionService
from app.models.sync_ranges import SyncRange
from app.connectors.exceptions import PayloadCorruptionError

@pytest.fixture
def mock_db():
    db = AsyncMock()
    # Mock commit, rollback, execute, flush
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.execute = AsyncMock()
    db.flush = AsyncMock()
    db.add = MagicMock()
    
    # Mock begin() context manager
    begin_mock = AsyncMock()
    begin_mock.__aenter__.return_value = None
    begin_mock.__aexit__.return_value = None
    db.begin.return_value = begin_mock
    
    return db

@pytest.fixture
def mock_binance():
    client = AsyncMock()
    
    # Create an async generator mock for get_klines
    async def mock_get_klines(*args, **kwargs):
        yield {
            "timestamp": datetime(2023, 1, 15, tzinfo=timezone.utc),
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 101.0,
            "volume": 1000.0
        }
    
    client.get_klines = mock_get_klines
    return client

@pytest.fixture
def service(mock_db, mock_binance):
    return IngestionService(mock_db, mock_binance)

@pytest.mark.asyncio
async def test_detect_missing_ranges_empty(service, mock_db):
    # Mock empty result from DB
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = []
    mock_db.execute.return_value = mock_result
    
    req_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    req_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    gaps = await service.detect_missing_ranges(1, req_start, req_end)
    assert len(gaps) == 1
    assert gaps[0] == (req_start, req_end)

@pytest.mark.asyncio
async def test_detect_missing_ranges_partial(service, mock_db):
    mock_range = SyncRange(
        asset_id=1,
        start_timestamp=datetime(2023, 1, 10, tzinfo=timezone.utc),
        end_timestamp=datetime(2023, 1, 20, tzinfo=timezone.utc)
    )
    
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = [mock_range]
    mock_db.execute.return_value = mock_result
    
    req_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    req_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    gaps = await service.detect_missing_ranges(1, req_start, req_end)
    
    assert len(gaps) == 2
    assert gaps[0] == (req_start, mock_range.start_timestamp)
    assert gaps[1] == (mock_range.end_timestamp, req_end)

@pytest.mark.asyncio
async def test_update_sync_ranges_merge(service, mock_db):
    # Case A: 1 Jan to 10 Jan exists. New is 11 Jan to 20 Jan.
    mock_range = SyncRange(
        id=1,
        asset_id=1,
        start_timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end_timestamp=datetime(2023, 1, 10, tzinfo=timezone.utc)
    )
    
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = [mock_range]
    mock_db.execute.return_value = mock_result
    
    new_start = datetime(2023, 1, 11, tzinfo=timezone.utc)
    new_end = datetime(2023, 1, 20, tzinfo=timezone.utc)
    
    await service.update_sync_ranges(1, new_start, new_end)
    
    # Verify the new range added encompasses both
    service.db.add.assert_called_once()
    added_range = service.db.add.call_args[0][0]
    assert added_range.start_timestamp == mock_range.start_timestamp
    assert added_range.end_timestamp == new_end

@pytest.mark.asyncio
async def test_commit_batch_fragments_gaps(service, mock_db):
    # Create payload with a gap
    # 10:00, 10:01, (gap 10:02), 10:03, 10:04
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2023, 1, 1, 10, 1, tzinfo=timezone.utc)
    t3 = datetime(2023, 1, 1, 10, 3, tzinfo=timezone.utc)
    t4 = datetime(2023, 1, 1, 10, 4, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t1},
        {"timestamp": t3},
        {"timestamp": t4},
    ]
    
    with patch.object(service, 'insert_candle_batch', new_callable=AsyncMock) as mock_insert:
        with patch.object(service, 'update_sync_ranges', new_callable=AsyncMock) as mock_update:
            await service._commit_batch(1, candles)
            
            # Should bulk insert exactly once with all 4 candles
            mock_insert.assert_awaited_once_with(candles)
            
            # Should update sync ranges exactly TWICE because of the 10:02 gap
            assert mock_update.call_count == 2
            mock_update.assert_any_call(1, t0, t1)
            mock_update.assert_any_call(1, t3, t4)

@pytest.mark.asyncio
async def test_commit_batch_duplicate_timestamp(service, mock_db):
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t0},  # Duplicate!
    ]
    
    with pytest.raises(PayloadCorruptionError, match="Payload corruption detected"):
        await service._commit_batch(1, candles)

@pytest.mark.asyncio
async def test_commit_batch_out_of_order(service, mock_db):
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2023, 1, 1, 10, 1, tzinfo=timezone.utc)
    t2 = datetime(2023, 1, 1, 10, 2, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t2},  # 10:02
        {"timestamp": t1},  # 10:01 (Out of order!)
    ]
    
    with pytest.raises(PayloadCorruptionError, match="Payload corruption detected"):
        await service._commit_batch(1, candles)

@pytest.mark.asyncio
async def test_commit_batch_normal_contiguous(service, mock_db):
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2023, 1, 1, 10, 1, tzinfo=timezone.utc)
    t2 = datetime(2023, 1, 1, 10, 2, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t1},
        {"timestamp": t2},
    ]
    
    with patch.object(service, 'insert_candle_batch', new_callable=AsyncMock) as mock_insert:
        with patch.object(service, 'update_sync_ranges', new_callable=AsyncMock) as mock_update:
            await service._commit_batch(1, candles)
            
            mock_insert.assert_awaited_once_with(candles)
            mock_update.assert_called_once_with(1, t0, t2)
