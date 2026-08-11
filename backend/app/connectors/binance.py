import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any, Optional

import httpx

from app.core.config import settings
from app.connectors.exceptions import (
    ConnectorError,
    NetworkError,
    APIError,
    RateLimitError,
    TemporaryBanError,
)
from app.connectors.rate_limiter import GlobalRateLimiter

logger = logging.getLogger(__name__)

class BinanceClient:
    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        self.base_url = settings.BINANCE_BASE_URL
        self._client = client
        self._owns_client = client is None
        self.rate_limiter = GlobalRateLimiter()

    async def __aenter__(self):
        if self._owns_client:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=settings.BINANCE_TIMEOUT_SECONDS
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._owns_client and self._client:
            await self._client.aclose()

    async def _request(self, method: str, endpoint: str, weight: int = 1, **kwargs) -> Any:
        if not self._client:
            raise ConnectorError("Client is not initialized. Use 'async with BinanceClient() as client:'.")

        retries = 0
        delay = settings.BINANCE_RETRY_DELAY_SECONDS

        while retries <= settings.BINANCE_MAX_RETRIES:
            try:
                # Intercept HTTP call with Global Redis Token Bucket
                has_tokens = await self.rate_limiter.acquire(weight)
                if not has_tokens:
                    # Throw internally so exponential backoff logic triggers natively
                    raise RateLimitError("Internal global rate limit exceeded", retry_after=delay)

                response = await self._client.request(method, endpoint, **kwargs)
                
                if response.status_code == 200:
                    return response.json()
                
                retry_after = response.headers.get("Retry-After")
                retry_after_sec = int(retry_after) if retry_after else None

                if response.status_code == 429:
                    raise RateLimitError("Rate limit exceeded.", retry_after=retry_after_sec)
                elif response.status_code == 418:
                    raise TemporaryBanError("Temporary IP ban.", retry_after=retry_after_sec)
                else:
                    raise APIError(f"API Error {response.status_code}: {response.text}")

            except httpx.RequestError as e:
                logger.warning(f"Network error on {endpoint}: {str(e)}")
                if retries == settings.BINANCE_MAX_RETRIES:
                    raise NetworkError(f"Network error after {retries} retries: {str(e)}") from e
            
            except (RateLimitError, TemporaryBanError) as e:
                logger.warning(f"Rate limited or banned on {endpoint}. Retry after: {e.retry_after}s")
                if retries == settings.BINANCE_MAX_RETRIES:
                    raise
                # Use provided Retry-After or exponential backoff
                sleep_time = e.retry_after if e.retry_after else delay
                await asyncio.sleep(sleep_time)
                retries += 1
                delay *= 2
                continue

            except APIError:
                raise

            # Exponential backoff for network errors
            await asyncio.sleep(delay)
            retries += 1
            delay *= 2

        raise ConnectorError("Max retries exceeded.")

    async def ping(self) -> dict:
        """Test connectivity to the Rest API."""
        return await self._request("GET", "/api/v3/ping")

    async def get_server_time(self) -> dict:
        """Test connectivity to the Rest API and get the current server time."""
        return await self._request("GET", "/api/v3/time")

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fetch historical klines (candles) using automatic pagination.
        Yields normalized candle dictionaries.
        """
        current_start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        limit = 1000

        while current_start_ms < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start_ms,
                "endTime": end_ms,
                "limit": limit
            }
            
            # Klines weight is 2 per Binance docs
            klines = await self._request("GET", "/api/v3/klines", weight=2, params=params)
            
            if not klines:
                break
                
            for kline in klines:
                kline_time_ms = kline[0]
                
                # Yield normalized candle
                yield {
                    "timestamp": datetime.fromtimestamp(kline_time_ms / 1000.0, tz=timezone.utc),
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5])
                }
            
            last_kline_time_ms = klines[-1][0]
            
            # If we received fewer items than the limit, we've hit the end of the available data
            if len(klines) < limit:
                break
                
            # Advance start time to prevent duplicate fetching
            current_start_ms = last_kline_time_ms + 1
