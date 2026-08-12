import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.query import CandleQueryService, AssetQueryService
from app.services.exceptions import (
    AssetNotFoundError,
    UnsupportedTimeframeError,
    InvalidDateRangeError
)

@pytest.fixture
def mock_db():
    return AsyncMock()

@pytest.fixture
def mock_asset_repo():
    with patch("app.services.query.AssetRepository") as mock:
        repo = AsyncMock()
        mock.return_value = repo
        yield repo

@pytest.fixture
def mock_candle_repo():
    with patch("app.services.query.CandleRepository") as mock:
        repo = AsyncMock()
        mock.return_value = repo
        yield repo

@pytest.mark.asyncio
async def test_get_asset_by_symbol(mock_db, mock_asset_repo):
    mock_asset = AsyncMock()
    mock_asset.id = 1
    mock_asset.symbol = "BTCUSDT"
    mock_asset.exchange = "BINANCE"
    mock_asset.asset_type = "SPOT"
    mock_asset.is_active = True
    
    mock_asset_repo.get_by_symbol.return_value = mock_asset
    
    service = AssetQueryService(mock_db)
    result = await service.get_asset_by_symbol("BTCUSDT")
    
    assert result["id"] == 1
    assert result["symbol"] == "BTCUSDT"

@pytest.mark.asyncio
async def test_get_asset_not_found(mock_db, mock_asset_repo):
    mock_asset_repo.get_by_symbol.return_value = None
    service = AssetQueryService(mock_db)
    
    with pytest.raises(AssetNotFoundError):
        await service.get_asset_by_symbol("INVALID")

@pytest.mark.asyncio
async def test_timeframe_routing_invalid(mock_db):
    service = CandleQueryService(mock_db)
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 2, tzinfo=timezone.utc)
    
    with pytest.raises(UnsupportedTimeframeError):
        generator = service.get_candles(1, "2m", start, end)
        await generator.asend(None)

@pytest.mark.asyncio
async def test_invalid_date_range(mock_db):
    service = CandleQueryService(mock_db)
    start = datetime(2023, 1, 2, tzinfo=timezone.utc)
    end = datetime(2023, 1, 1, tzinfo=timezone.utc)
    
    with pytest.raises(InvalidDateRangeError):
        generator = service.get_candles(1, "1h", start, end)
        await generator.asend(None)

@pytest.mark.asyncio
async def test_missing_asset(mock_db, mock_asset_repo):
    mock_asset_repo.get_by_id.return_value = None
    service = CandleQueryService(mock_db)
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 2, tzinfo=timezone.utc)
    
    with pytest.raises(AssetNotFoundError):
        generator = service.get_candles(999, "1h", start, end)
        await generator.asend(None)

@pytest.mark.asyncio
async def test_stream_normalization(mock_db, mock_asset_repo, mock_candle_repo):
    mock_asset_repo.get_by_id.return_value = AsyncMock()
    
    # Mock stream generator
    async def mock_stream(*args, **kwargs):
        class MockRow:
            def __init__(self):
                self._mapping = {
                    "timestamp": datetime(2023, 1, 1, tzinfo=timezone.utc),
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 102.0,
                    "volume": 10.5
                }
        yield MockRow()
        yield MockRow()
        
    mock_candle_repo.get_candles_stream = MagicMock(side_effect=mock_stream)
    
    service = CandleQueryService(mock_db)
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 1, 2, tzinfo=timezone.utc)
    
    results = []
    async for candle in service.get_candles(1, "1h", start, end):
        results.append(candle)
        
    assert len(results) == 2
    assert results[0]["open"] == 100.0
    assert "timestamp" in results[0]

@pytest.mark.asyncio
async def test_window_limit_exceeded(mock_db):
    service = CandleQueryService(mock_db)
    # Requesting 31 days for 1m (limit is 30)
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end = datetime(2023, 2, 2, tzinfo=timezone.utc)
    
    with pytest.raises(InvalidDateRangeError, match="exceeds maximum allowed of 30 days"):
        generator = service.get_candles(1, "1m", start, end)
        await generator.asend(None)
