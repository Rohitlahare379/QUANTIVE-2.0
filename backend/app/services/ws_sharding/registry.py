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
BINANCE_EXCHANGE = "BINANCE"


class AssetRegistryUnavailableError(RuntimeError):
    """The authoritative asset mapping could not be refreshed safely."""


def normalize_exchange(exchange: str) -> str:
    if not isinstance(exchange, str) or not exchange.strip():
        raise ValueError("Exchange must be a non-empty string")
    return exchange.strip().upper()


class AssetRegistryResolver:
    """
    In-memory cached resolver for AssetRegistry records.
    Provides fast O(1) (exchange, symbol) -> (asset_id, is_active) lookups for
    the ingestion pipeline.  Binance transport events must never be resolved to
    a same-symbol row belonging to a different exchange.
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
        self._cache: Dict[Tuple[str, str], Tuple[int, bool]] = {}
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

    def register_asset(
        self,
        symbol: str,
        asset_id: int,
        is_active: bool = True,
        exchange: str = BINANCE_EXCHANGE,
    ) -> bool:
        """
        Manually registers an asset mapping in cache.
        Useful for unit testing and deterministic verification.
        Returns False if max_cache_size is reached.
        """
        cache_key = (normalize_exchange(exchange), normalize_symbol(symbol))
        if cache_key not in self._cache and len(self._cache) >= self.max_cache_size:
            logger.warning(f"AssetRegistryResolver cache reached max size ({self.max_cache_size}).")
            return False
        self._cache[cache_key] = (asset_id, is_active)
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

        try:
            async with self.session_factory() as sess:
                return await self._execute_load(sess)
        except AssetRegistryUnavailableError:
            raise
        except Exception as exc:
            # Connection acquisition itself happens before _execute_load's
            # query boundary.  It must be subject to the same fail-closed
            # semantics as an unsuccessful SELECT.
            logger.exception("Failed to open authoritative asset registry session")
            raise AssetRegistryUnavailableError(
                "Asset registry refresh failed; refusing stale symbol attribution"
            ) from exc

    async def _execute_load(self, session: AsyncSession) -> int:
        async with self._lock:
            try:
                stmt = select(
                    AssetRegistry.id,
                    AssetRegistry.symbol,
                    AssetRegistry.exchange,
                    AssetRegistry.is_active,
                )
                result = await session.execute(stmt)
                rows = result.fetchall()

                new_cache: Dict[Tuple[str, str], Tuple[int, bool]] = {}
                for asset_id, sym, exchange, is_active in rows:
                    if len(new_cache) >= self.max_cache_size:
                        logger.warning(f"Asset registry table exceeds cache capacity ({self.max_cache_size}). Truncating cache.")
                        break
                    if sym and exchange:
                        new_cache[(normalize_exchange(exchange), normalize_symbol(sym))] = (
                            asset_id,
                            bool(is_active),
                        )

                self._cache = new_cache
                self._last_loaded_at = time.time()
                logger.info(f"Loaded {len(self._cache)} asset symbols into AssetRegistryResolver cache.")
                return len(self._cache)
            except Exception as exc:
                # Returning a stale cache after its TTL has elapsed can write a
                # Binance candle to an asset that was disabled, reassigned, or
                # moved to another exchange.  Fail closed instead: a missing
                # candle is recoverable by REST reconciliation; incorrect
                # attribution is not.
                logger.exception("Failed to refresh authoritative asset registry cache")
                raise AssetRegistryUnavailableError(
                    "Asset registry refresh failed; refusing stale symbol attribution"
                ) from exc

    async def resolve_symbol(
        self, symbol: str, exchange: str = BINANCE_EXCHANGE
    ) -> Optional[Tuple[int, bool]]:
        """
        Resolves a symbol to (asset_id, is_active).
        Returns None if symbol is not found in registry.
        """
        if not symbol or not symbol.strip():
            return None

        cache_key = (normalize_exchange(exchange), normalize_symbol(symbol))

        # Fast path: cache hit
        if self.is_cache_valid and cache_key in self._cache:
            return self._cache[cache_key]

        # Slow path: refresh cache if expired or missing
        if not self.is_cache_valid and self.session_factory is not None:
            await self.load_cache()

        return self._cache.get(cache_key)
