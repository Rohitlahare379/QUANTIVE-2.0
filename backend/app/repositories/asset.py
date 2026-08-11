from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.asset_registry import AssetRegistry

class AssetRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_symbol(self, symbol: str) -> Optional[AssetRegistry]:
        stmt = select(AssetRegistry).where(AssetRegistry.symbol == symbol)
        result = await self.db.execute(stmt)
        return result.scalars().first()
        
    async def get_by_id(self, asset_id: int) -> Optional[AssetRegistry]:
        stmt = select(AssetRegistry).where(AssetRegistry.id == asset_id)
        result = await self.db.execute(stmt)
        return result.scalars().first()

    async def list_assets(self, active_only: bool = True) -> List[AssetRegistry]:
        stmt = select(AssetRegistry)
        if active_only:
            stmt = stmt.where(AssetRegistry.is_active == True)
        result = await self.db.execute(stmt)
        return list(result.scalars().all())
