from typing import List, Optional
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db
from app.api.schemas import AssetResponse
from app.services.query import AssetQueryService

router = APIRouter(prefix="/assets", tags=["Assets"])

@router.get("", response_model=List[AssetResponse])
async def list_assets(
    exchange: Optional[str] = None,
    asset_type: Optional[str] = None,
    active_only: bool = True,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    service = AssetQueryService(db)
    assets = await service.list_assets()
    
    # Simple Python-side filtering for MVP
    # In production, push limit/offset and filters down to AssetRepository
    if exchange:
        assets = [a for a in assets if a["exchange"] == exchange]
    if asset_type:
        assets = [a for a in assets if a["asset_type"] == asset_type]
    
    return assets[offset : offset + limit]

@router.get("/{symbol}", response_model=AssetResponse)
async def get_asset(symbol: str, db: AsyncSession = Depends(get_db)):
    service = AssetQueryService(db)
    return await service.get_asset_by_symbol(symbol)
