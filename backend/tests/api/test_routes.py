import pytest
from httpx import AsyncClient
from datetime import datetime, timezone
from unittest.mock import patch

from app.main import app
from app.api.auth import verify_api_key

async def mock_auth():
    return True

@pytest.fixture
async def async_client():
    from httpx import ASGITransport
    app.dependency_overrides[verify_api_key] = mock_auth
    orig_enabled = app.state.limiter.enabled
    app.state.limiter.enabled = False
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.state.limiter.enabled = orig_enabled
    app.dependency_overrides.clear()

@pytest.mark.asyncio
async def test_health_check(async_client):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

@pytest.mark.asyncio
async def test_get_assets(async_client):
    # We patch AssetQueryService.list_assets
    with patch("app.api.routes.assets.AssetQueryService.list_assets") as mock_list:
        mock_list.return_value = [
            {"id": 1, "symbol": "BTCUSDT", "exchange": "BINANCE", "asset_type": "SPOT", "is_active": True}
        ]
        response = await async_client.get("/assets")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["symbol"] == "BTCUSDT"

@pytest.mark.asyncio
async def test_get_candles_validation_error(async_client):
    # Invalid asset_id (string instead of int) triggers FastAPI automatic validation
    response = await async_client.get(
        "/candles",
        params={"asset_id": "abc", "timeframe": "1h", "start_time": "2023-01-01T00:00:00Z", "end_time": "2023-01-02T00:00:00Z"}
    )
    assert response.status_code == 422 # Unprocessable Entity (FastAPI standard)

@pytest.mark.asyncio
async def test_get_candles_unsupported_timeframe(async_client):
    from app.services.exceptions import UnsupportedTimeframeError
    
    with patch("app.api.routes.candles.CandleQueryService.get_candles") as mock_get:
        mock_get.side_effect = UnsupportedTimeframeError("Unsupported timeframe: 2m")
        response = await async_client.get(
            "/candles",
            params={"asset_id": 1, "timeframe": "2m", "start_time": "2023-01-01T00:00:00Z", "end_time": "2023-01-02T00:00:00Z"}
        )
        assert response.status_code == 400
        assert "Unsupported timeframe" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_candles_stream(async_client):
    # Mock the AsyncGenerator
    async def mock_generator():
        yield {
            "timestamp": datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 101.0,
            "volume": 1000.0
        }
        
    with patch("app.api.routes.candles.CandleQueryService.get_candles") as mock_get:
        mock_get.return_value = mock_generator()
        response = await async_client.get(
            "/candles",
            params={"asset_id": 1, "timeframe": "1h", "start_time": "2023-01-01T00:00:00Z", "end_time": "2023-01-02T00:00:00Z"}
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/x-ndjson"
        
        # Read the raw stream content
        content = response.content.decode("utf-8")
        assert "100.0" in content
        assert "2023-01-01T10:00:00+00:00" in content
        assert content.endswith("\n")

def test_proxy_ip_extraction():
    from app.api.dependencies import get_trusted_client_ip
    from fastapi import Request
    
    # 1. Test CF-Connecting-IP
    scope = {
        "type": "http",
        "headers": [(b"cf-connecting-ip", b"203.0.113.1")]
    }
    req = Request(scope)
    assert get_trusted_client_ip(req) == "203.0.113.1"
    
    # 2. Test X-Real-IP
    scope = {
        "type": "http",
        "headers": [(b"x-real-ip", b"198.51.100.1")]
    }
    req = Request(scope)
    assert get_trusted_client_ip(req) == "198.51.100.1"
    
    # 3. Test ASGI client host fallback
    scope = {
        "type": "http",
        "headers": [],
        "client": ("192.0.2.1", 12345)
    }
    req = Request(scope)
    assert get_trusted_client_ip(req) == "192.0.2.1"

def test_limiter_redis_backend():
    from app.api.dependencies import limiter
    from limits.storage.redis import RedisStorage
    
    assert isinstance(limiter._storage, RedisStorage)
    assert limiter._storage.storage.connection_pool.connection_kwargs["socket_connect_timeout"] == 2
