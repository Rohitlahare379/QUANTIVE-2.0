import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.workers.tasks import (
    _run_sync,
    gap_repair_job,
    full_historical_sync_job,
    RetryableError,
    PermanentError
)
from app.connectors.exceptions import RateLimitError, APIError

@pytest.fixture
def mock_redis():
    with patch("app.workers.tasks.get_async_redis") as mock:
        redis_instance = AsyncMock()
        lock_instance = AsyncMock()
        
        lock_instance.acquire.return_value = True
        redis_instance.lock = MagicMock(return_value=lock_instance)
        
        mock.return_value = redis_instance
        yield redis_instance, lock_instance

@pytest.fixture
def mock_ingestion_service():
    with patch("app.workers.tasks.IngestionService") as mock:
        service_instance = AsyncMock()
        mock.return_value = service_instance
        yield service_instance

@pytest.mark.asyncio
async def test_run_sync_acquires_lock(mock_redis, mock_ingestion_service):
    _, lock = mock_redis
    
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    await _run_sync(1, "BTCUSDT", start, end)
    
    lock.acquire.assert_awaited_once_with(blocking=False)
    mock_ingestion_service.sync_asset.assert_awaited_once_with(1, "BTCUSDT", start, end)
    lock.release.assert_awaited_once()

@pytest.mark.asyncio
async def test_run_sync_skips_if_locked(mock_redis, mock_ingestion_service):
    _, lock = mock_redis
    lock.acquire.return_value = False
    
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    await _run_sync(1, "BTCUSDT", start, end)
    
    lock.acquire.assert_awaited_once_with(blocking=False)
    mock_ingestion_service.sync_asset.assert_not_awaited()
    # Should not release a lock it didn't acquire
    lock.release.assert_not_awaited()

@pytest.mark.asyncio
async def test_run_sync_retries_on_rate_limit(mock_redis, mock_ingestion_service):
    mock_ingestion_service.sync_asset.side_effect = RateLimitError("Too many reqs", retry_after=5)
    
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    with pytest.raises(RetryableError):
        await _run_sync(1, "BTCUSDT", start, end)

@pytest.mark.asyncio
async def test_run_sync_permanent_error(mock_redis, mock_ingestion_service):
    mock_ingestion_service.sync_asset.side_effect = APIError("Invalid symbol")
    
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    with pytest.raises(PermanentError):
        await _run_sync(1, "BTCUSDT", start, end)

def test_full_historical_sync_chunking():
    with patch("app.workers.tasks.sync_asset_job") as mock_job:
        # Note: the job is not async, it's a dramatiq actor
        full_historical_sync_job(1, "BTCUSDT", 2024)
        
        # It should have chunked from Jan 2024 to present day.
        # Present day is mocked or just verified by call count.
        assert mock_job.send.call_count >= 1
