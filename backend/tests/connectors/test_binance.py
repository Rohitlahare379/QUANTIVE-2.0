import pytest
import respx
import httpx
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from app.connectors.binance import BinanceClient
from app.connectors.exceptions import APIError, RateLimitError, TemporaryBanError, NetworkError
from app.core.config import settings

@pytest.fixture(autouse=True)
def mock_global_rate_limiter():
    """Ensure GlobalRateLimiter tokens are available during connector unit tests."""
    with patch("app.connectors.rate_limiter.GlobalRateLimiter.acquire", new_callable=AsyncMock) as mock_acq:
        mock_acq.return_value = True
        yield mock_acq

@pytest.fixture
def binance_client():
    return BinanceClient()

@pytest.mark.asyncio
async def test_ping(binance_client):
    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/ping").respond(200, json={})
        async with binance_client as client:
            res = await client.ping()
            assert res == {}

@pytest.mark.asyncio
async def test_rate_limit_retry(binance_client):
    async with respx.mock:
        route = respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/ping")
        # Fail first with 429, then succeed
        route.side_effect = [
            httpx.Response(429, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"success": True})
        ]
        
        async with binance_client as client:
            res = await client.ping()
            assert res == {"success": True}
            assert route.call_count == 2

@pytest.mark.asyncio
async def test_temporary_ban_raises_after_max_retries():
    # Force max retries to 1 for quick testing
    settings.BINANCE_MAX_RETRIES = 1
    settings.BINANCE_RETRY_DELAY_SECONDS = 0.0
    
    async with respx.mock:
        route = respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/ping").respond(418)
        
        client = BinanceClient()
        async with client:
            with pytest.raises(TemporaryBanError):
                await client.ping()
        
        assert route.call_count == 2 # Initial try + 1 retry

@pytest.mark.asyncio
async def test_get_klines_pagination_and_normalization(binance_client):
    start_time = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 1, 0, tzinfo=timezone.utc)
    
    start_ms = int(start_time.timestamp() * 1000)
    
    mock_kline = [
        start_ms,
        "100.0", "105.0", "95.0", "101.0", "1000.0",
        start_ms + 59999, "100000.0", 500, "500.0", "50000.0", "0"
    ]
    
    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/klines").respond(200, json=[mock_kline])
        
        async with binance_client as client:
            candles = []
            async for candle in client.get_klines("BTCUSDT", "1m", start_time, end_time):
                candles.append(candle)
                
            assert len(candles) == 1
            assert candles[0]["open"] == 100.0
            assert candles[0]["volume"] == 1000.0
            assert candles[0]["timestamp"].timestamp() == start_time.timestamp()
