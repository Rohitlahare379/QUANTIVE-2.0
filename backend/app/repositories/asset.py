from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.asset_registry import AssetRegistry

class AssetRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_symbol(
        self, symbol: str, exchange: str = "BINANCE"
    ) -> Optional[AssetRegistry]:
        """Resolve an asset using its real transport identity, not symbol alone."""
        stmt = select(AssetRegistry).where(
            AssetRegistry.symbol == symbol.strip().upper(),
            AssetRegistry.exchange == exchange.strip().upper(),
        )
        result = await self.db.execute(stmt)
        return result.scalars().first()
        
    async def get_by_id(self, asset_id: int) -> Optional[AssetRegistry]:
        stmt = select(AssetRegistry).where(AssetRegistry.id == asset_id)
        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def list_assets(
        self,
        *,
        exchange: Optional[str] = None,
        asset_type: Optional[str] = None,
        active_only: bool = True,
        limit: int,
        offset: int = 0,
    ) -> List[AssetRegistry]:
        """Return one SQL-bounded page; never materialize the asset universe."""
        if limit <= 0 or offset < 0:
            raise ValueError("limit must be positive and offset must be non-negative")
        stmt = select(AssetRegistry)
        if active_only:
            stmt = stmt.where(AssetRegistry.is_active.is_(True))
        if exchange:
            stmt = stmt.where(AssetRegistry.exchange == exchange.strip().upper())
        if asset_type:
            stmt = stmt.where(AssetRegistry.asset_type == asset_type.strip().upper())
        stmt = stmt.order_by(AssetRegistry.id.asc()).limit(limit).offset(offset)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
