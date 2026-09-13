from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db
from app.api.schemas import AssetResponse
from app.services.query import AssetQueryService
from app.core.config import settings

router = APIRouter(prefix="/assets", tags=["Assets"])

@router.get("", response_model=List[AssetResponse])
async def list_assets(
    exchange: Optional[str] = None,
    asset_type: Optional[str] = None,
    active_only: bool = True,
    limit: int = Query(settings.API_DEFAULT_PAGE_SIZE, ge=1, le=settings.API_MAX_ASSET_PAGE_SIZE),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    service = AssetQueryService(db)
    return await service.list_assets(
        exchange=exchange,
        asset_type=asset_type,
        active_only=active_only,
        limit=limit,
        offset=offset,
    )

@router.get("/{symbol}", response_model=AssetResponse)
async def get_asset(
    symbol: str,
    exchange: str = "BINANCE",
    db: AsyncSession = Depends(get_db),
):
    service = AssetQueryService(db)
    return await service.get_asset_by_symbol(symbol, exchange=exchange)
