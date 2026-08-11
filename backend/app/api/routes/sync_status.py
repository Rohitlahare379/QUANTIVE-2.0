from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.dependencies import get_db
from app.api.schemas import SyncStatusResponse, SyncRangeResponse
from app.models.sync_ranges import SyncRange

router = APIRouter(prefix="/sync-status", tags=["Sync Status"])

@router.get("/{asset_id}", response_model=SyncStatusResponse)
async def get_sync_status(asset_id: int, db: AsyncSession = Depends(get_db)):
    """
    Returns the verified contiguous ranges that have been synchronized for this asset.
    """
    stmt = select(SyncRange).where(SyncRange.asset_id == asset_id).order_by(SyncRange.start_timestamp.asc())
    result = await db.execute(stmt)
    ranges = result.scalars().all()
    
    return SyncStatusResponse(
        asset_id=asset_id,
        synced_ranges=[
            SyncRangeResponse(start_timestamp=r.start_timestamp, end_timestamp=r.end_timestamp) 
            for r in ranges
        ]
    )
