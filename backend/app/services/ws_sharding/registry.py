"""
Cached Asset Registry Resolver for WebSocket Ingestion (P0.2 Phase 3).

Resolves Binance ticker symbols to database asset_ids and active status using
an in-memory TTL cache to eliminate per-candle database queries while preventing
unbounded cache growth.
"""

import asyncio
import logging
import time
from typing import Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.services.ws_sharding.assignment import normalize_symbol

logger = logging.getLogger(__name__)


MAX_REGISTRY_CACHE_SIZE = 10000


class AssetRegistryResolver:
    """
    In-memory cached resolver for AssetRegistry records.
    Provides fast O(1) symbol -> (asset_id, is_active) lookups for the ingestion pipeline.
    """

    def __init__(
        self,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        cache_ttl_seconds: Optional[float] = None,
        max_cache_size: int = MAX_REGISTRY_CACHE_SIZE,
    ):
        self.session_factory = session_factory
        self.cache_ttl_seconds = cache_ttl_seconds or settings.WS_REGISTRY_CACHE_TTL_SECONDS
        self.max_cache_size = max_cache_size
        self._cache: Dict[str, Tuple[int, bool]] = {}
        self._last_loaded_at: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def is_cache_valid(self) -> bool:
        """Returns True if cache has been populated and has not exceeded TTL."""
        if not self._cache or self._last_loaded_at == 0.0:
            return False
        return (time.time() - self._last_loaded_at) < self.cache_ttl_seconds

    @property
    def cached_count(self) -> int:
        """Returns the number of cached symbols."""
        return len(self._cache)

    def register_asset(self, symbol: str, asset_id: int, is_active: bool = True) -> bool:
        """
        Manually registers an asset mapping in cache.
        Useful for unit testing and deterministic verification.
        Returns False if max_cache_size is reached.
        """
        clean_symbol = normalize_symbol(symbol)
        if clean_symbol not in self._cache and len(self._cache) >= self.max_cache_size:
            logger.warning(f"AssetRegistryResolver cache reached max size ({self.max_cache_size}).")
            return False
        self._cache[clean_symbol] = (asset_id, is_active)
        if self._last_loaded_at == 0.0:
            self._last_loaded_at = time.time()
        return True

    def invalidate(self) -> None:
        """Invalidates the cached mappings."""
        self._last_loaded_at = 0.0

    async def load_cache(self, session: Optional[AsyncSession] = None) -> int:
        """
        Populates or refreshes the cache from PostgreSQL AssetRegistry table.
        """
        if session is not None:
            return await self._execute_load(session)

        if self.session_factory is None:
            logger.debug("AssetRegistryResolver has no session factory; skipping database load.")
            return len(self._cache)

        async with self.session_factory() as sess:
            return await self._execute_load(sess)

    async def _execute_load(self, session: AsyncSession) -> int:
        async with self._lock:
            try:
                stmt = select(AssetRegistry.id, AssetRegistry.symbol, AssetRegistry.is_active)
                result = await session.execute(stmt)
                rows = result.fetchall()

                new_cache: Dict[str, Tuple[int, bool]] = {}
                for asset_id, sym, is_active in rows:
                    if len(new_cache) >= self.max_cache_size:
                        logger.warning(f"Asset registry table exceeds cache capacity ({self.max_cache_size}). Truncating cache.")
                        break
                    if sym:
                        new_cache[normalize_symbol(sym)] = (asset_id, bool(is_active))

                self._cache = new_cache
                self._last_loaded_at = time.time()
                logger.info(f"Loaded {len(self._cache)} asset symbols into AssetRegistryResolver cache.")
                return len(self._cache)
            except Exception as e:
                logger.warning(f"Failed to load asset registry cache from database: {e}")
                return len(self._cache)

    async def resolve_symbol(self, symbol: str) -> Optional[Tuple[int, bool]]:
        """
        Resolves a symbol to (asset_id, is_active).
        Returns None if symbol is not found in registry.
        """
        if not symbol or not symbol.strip():
            return None

        clean_symbol = normalize_symbol(symbol)

        # Fast path: cache hit
        if self.is_cache_valid and clean_symbol in self._cache:
            return self._cache[clean_symbol]

        # Slow path: refresh cache if expired or missing
        if not self.is_cache_valid and self.session_factory is not None:
            await self.load_cache()

        return self._cache.get(clean_symbol)
