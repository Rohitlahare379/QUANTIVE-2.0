"""
Test Matrix — REST Reconciliation & Validation (Tests 9 to 16).
Verifies successful REST repair, malformed response handling, OHLC price invariants,
invalid timestamps, timeouts, HTTP 500, HTTP 429 backoff, and rate limiter exhaustion.
"""

import pytest
import respx
import httpx
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch

from app.core.config import settings
from app.connectors.binance import BinanceClient
from app.connectors.exceptions import (
    APIError,
    NetworkError,
    RateLimitError,
    PayloadCorruptionError,
)
from app.connectors.rate_limiter import GlobalRateLimiter


@pytest.fixture(autouse=True)
def mock_rate_limiter_acquire():
    with patch("app.connectors.rate_limiter.GlobalRateLimiter.acquire", new_callable=AsyncMock) as mock_acq:
        mock_acq.return_value = True
        yield mock_acq


@pytest.mark.asyncio
async def test_9_successful_rest_repair():
    """
    Test 9: Successful REST repair fetching and validating klines from Binance.
    """
    start_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 1, 1, 10, 3, tzinfo=timezone.utc)
    start_ms = int(start_time.timestamp() * 1000)

    # 3 valid 1m klines
    mock_klines = [
        [start_ms, "100.0", "105.0", "99.0", "104.0", "10.5", start_ms + 59999, "1000.0", 10, "5.0", "500.0", "0"],
        [start_ms + 60000, "104.0", "106.0", "103.0", "105.0", "12.0", start_ms + 119999, "1200.0", 12, "6.0", "600.0", "0"],
        [start_ms + 120000, "105.0", "108.0", "104.0", "107.0", "15.0", start_ms + 179999, "1500.0", 15, "8.0", "800.0", "0"],
    ]

    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/klines").respond(200, json=mock_klines)

        client = BinanceClient()
        async with client:
            candles = []
            async for c in client.get_klines("BTCUSDT", "1m", start_time, end_time):
                candles.append(c)

        assert len(candles) == 3
        assert candles[0]["open"] == 100.0
        assert candles[0]["high"] == 105.0
        assert candles[0]["low"] == 99.0
        assert candles[0]["close"] == 104.0
        assert candles[0]["volume"] == 10.5
        assert candles[0]["timestamp"] == start_time


@pytest.mark.asyncio
async def test_10_malformed_response():
    """
    Test 10: Binance returns malformed JSON or invalid array structure -> raises PayloadCorruptionError.
    """
    start_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 1, 1, 10, 2, tzinfo=timezone.utc)

    async with respx.mock:
        # Returns a dict instead of a list of kline arrays
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/klines").respond(200, json={"unexpected": "format"})

        client = BinanceClient()
        async with client:
            with pytest.raises(PayloadCorruptionError, match="Expected klines list"):
                async for _ in client.get_klines("BTCUSDT", "1m", start_time, end_time):
                    pass


@pytest.mark.asyncio
async def test_11_invalid_ohlc_prices():
    """
    Test 11: Rejects invalid OHLC invariants (e.g. High < Low, negative prices, Open outside [Low, High]).
    """
    start_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 1, 1, 10, 2, tzinfo=timezone.utc)
    start_ms = int(start_time.timestamp() * 1000)

    # High (90.0) is LESS than Low (100.0)
    bad_kline = [start_ms, "95.0", "90.0", "100.0", "92.0", "10.0", start_ms + 59999, "1000", 1, "1", "1", "0"]

    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/klines").respond(200, json=[bad_kline])

        client = BinanceClient()
        async with client:
            with pytest.raises(PayloadCorruptionError, match="High price .* < Low price"):
                async for _ in client.get_klines("BTCUSDT", "1m", start_time, end_time):
                    pass


@pytest.mark.asyncio
async def test_12_invalid_negative_price():
    """
    Test 12: Rejects strictly non-positive prices (e.g. price <= 0).
    """
    start_time = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    end_time = datetime(2026, 1, 1, 10, 2, tzinfo=timezone.utc)
    start_ms = int(start_time.timestamp() * 1000)

    # Open price is -5.0
    bad_kline = [start_ms, "-5.0", "100.0", "50.0", "80.0", "10.0", start_ms + 59999, "1000", 1, "1", "1", "0"]

    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/klines").respond(200, json=[bad_kline])

        client = BinanceClient()
        async with client:
            with pytest.raises(PayloadCorruptionError, match="Price must be strictly positive"):
                async for _ in client.get_klines("BTCUSDT", "1m", start_time, end_time):
                    pass


@pytest.mark.asyncio
async def test_13_rest_timeout():
    """
    Test 13: HTTP client timeout raises NetworkError after retries.
    """
    settings.BINANCE_MAX_RETRIES = 1
    settings.BINANCE_RETRY_DELAY_SECONDS = 0.0

    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/ping").side_effect = httpx.TimeoutException("Read timed out")

        client = BinanceClient()
        async with client:
            with pytest.raises(NetworkError, match="Network error after"):
                await client.ping()


@pytest.mark.asyncio
async def test_14_http_500_server_error():
    """
    Test 14: Binance returns HTTP 500 Internal Server Error -> raises APIError.
    """
    settings.BINANCE_MAX_RETRIES = 1
    settings.BINANCE_RETRY_DELAY_SECONDS = 0.0

    async with respx.mock:
        respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/ping").respond(500, text="Internal Server Error")

        client = BinanceClient()
        async with client:
            with pytest.raises(APIError, match="API Error 500"):
                await client.ping()


@pytest.mark.asyncio
async def test_15_http_429_rate_limit_with_retry_after():
    """
    Test 15: Binance returns HTTP 429 with Retry-After header, client backs off and retries.
    """
    settings.BINANCE_MAX_RETRIES = 2
    settings.BINANCE_RETRY_DELAY_SECONDS = 0.01

    async with respx.mock:
        route = respx.get(f"{settings.BINANCE_BASE_URL}/api/v3/ping")
        # First return 429 with Retry-After: 0, then 200 OK
        route.side_effect = [
            httpx.Response(429, headers={"Retry-After": "0"}, text="Too Many Requests"),
            httpx.Response(200, json={"status": "ok"})
        ]

        client = BinanceClient()
        async with client:
            res = await client.ping()
            assert res == {"status": "ok"}
            assert route.call_count == 2


@pytest.mark.asyncio
async def test_16_rate_limiter_exhaustion():
    """
    Test 16: Global token bucket rate limiter is exhausted -> raises RateLimitError.
    """
    settings.BINANCE_MAX_RETRIES = 1
    settings.BINANCE_RETRY_DELAY_SECONDS = 0.01

    with patch("app.connectors.rate_limiter.GlobalRateLimiter.acquire", new_callable=AsyncMock) as mock_acq:
        mock_acq.return_value = False # Token bucket empty

        client = BinanceClient()
        async with client:
            with pytest.raises(RateLimitError, match="Internal global rate limit exceeded"):
                await client.ping()
