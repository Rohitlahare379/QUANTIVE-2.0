import pytest
import hashlib
from unittest.mock import patch, AsyncMock, MagicMock

from app.main import app
from app.api.dependencies import get_db
from app.models.api_keys import ApiKey
from app.core.config import settings


@pytest.mark.asyncio
async def test_auth_missing_header(async_client):
    response = await async_client.get("/assets")
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing X-API-Key header"


@pytest.mark.asyncio
async def test_auth_invalid_key(async_client):
    async def override_get_db():
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        session.execute.return_value = mock_result
        yield session

    app.dependency_overrides[get_db] = override_get_db

    response = await async_client.get("/assets", headers={"X-API-Key": "non_existent_key"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or inactive API Key"

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_auth_inactive_key(async_client):
    async def override_get_db():
        session = AsyncMock()
        mock_result = MagicMock()
        peppered_hash = hashlib.sha256((settings.API_KEY_PEPPER + "inactive_key").encode()).hexdigest()
        mock_key = ApiKey(id=1, key_hash=peppered_hash, owner_name="Tester", is_active=False)
        mock_result.scalars.return_value.first.return_value = mock_key
        session.execute.return_value = mock_result
        yield session

    app.dependency_overrides[get_db] = override_get_db

    response = await async_client.get("/assets", headers={"X-API-Key": "inactive_key"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or inactive API Key"

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_auth_success_legacy(async_client):
    async def override_get_db():
        session = AsyncMock()
        mock_result = MagicMock()
        legacy_hash = hashlib.sha256(b"legacy_good_key").hexdigest()
        mock_key = ApiKey(id=1, key_hash=legacy_hash, owner_name="LegacyOwner", is_active=True)
        mock_result.scalars.return_value.first.return_value = mock_key
        session.execute.return_value = mock_result
        yield session

    app.dependency_overrides[get_db] = override_get_db

    with patch("app.api.routes.assets.AssetQueryService.list_assets") as mock_list:
        mock_list.return_value = []
        response = await async_client.get("/assets", headers={"X-API-Key": "legacy_good_key"})
        assert response.status_code == 200

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_auth_success_peppered(async_client):
    async def override_get_db():
        session = AsyncMock()
        mock_result = MagicMock()
        peppered_hash = hashlib.sha256((settings.API_KEY_PEPPER + "peppered_good_key").encode()).hexdigest()
        mock_key = ApiKey(id=2, key_hash=peppered_hash, owner_name="PepperedOwner", is_active=True)
        mock_result.scalars.return_value.first.return_value = mock_key
        session.execute.return_value = mock_result
        yield session

    app.dependency_overrides[get_db] = override_get_db

    with patch("app.api.routes.assets.AssetQueryService.list_assets") as mock_list:
        mock_list.return_value = []
        response = await async_client.get("/assets", headers={"X-API-Key": "peppered_good_key"})
        assert response.status_code == 200

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_auth_pepper_change_invalidates_hash(async_client):
    async def override_get_db():
        session = AsyncMock()
        mock_result = MagicMock()
        # Hash generated with an old/different pepper
        old_peppered_hash = hashlib.sha256(("WRONG_PEPPER" + "good_key").encode()).hexdigest()
        mock_key = ApiKey(id=3, key_hash=old_peppered_hash, owner_name="OldOwner", is_active=True)
        mock_result.scalars.return_value.first.return_value = mock_key
        session.execute.return_value = mock_result
        yield session

    app.dependency_overrides[get_db] = override_get_db

    response = await async_client.get("/assets", headers={"X-API-Key": "good_key"})
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or inactive API Key"

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_health_endpoint_public_access(async_client):
    """Health check must be accessible without API key."""
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_metrics_endpoint_public_access(async_client):
    """Metrics endpoint must be accessible without API key for Prometheus."""
    response = await async_client.get("/metrics")
    assert response.status_code == 200
