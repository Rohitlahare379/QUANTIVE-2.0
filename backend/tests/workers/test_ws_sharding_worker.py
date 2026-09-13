"""Lifecycle regressions for the WebSocket shard supervisor process."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.workers import ws_sharding


class _FailingSessionContext:
    async def __aenter__(self):
        raise RuntimeError("PostgreSQL unavailable")

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_symbol_discovery_fails_closed_and_disposes_engine_on_database_error(monkeypatch):
    """A zero-symbol supervisor must never acquire leases after DB startup failure."""
    engine = MagicMock()
    engine.dispose = AsyncMock()
    session_factory = MagicMock(return_value=_FailingSessionContext())
    monkeypatch.setattr(ws_sharding, "create_async_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(ws_sharding, "async_sessionmaker", lambda *args, **kwargs: session_factory)

    with pytest.raises(RuntimeError, match="PostgreSQL unavailable"):
        await ws_sharding.fetch_active_symbols_from_db()

    engine.dispose.assert_awaited_once()
