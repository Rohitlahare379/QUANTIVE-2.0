from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.dependencies import get_db
from app.api.schemas import SyncStatusResponse, SyncRangeResponse
from app.models.sync_ranges import SyncRange
from app.core.config import settings

router = APIRouter(prefix="/sync-status", tags=["Sync Status"])

@router.get("/{asset_id}", response_model=SyncStatusResponse)
async def get_sync_status(
    asset_id: int,
    limit: int = Query(settings.API_DEFAULT_PAGE_SIZE, ge=1, le=settings.API_MAX_SYNC_RANGE_PAGE_SIZE),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns the verified contiguous ranges that have been synchronized for this asset.
    """
    # Fetch a single sentinel row to expose pagination without loading all
    # fragmented coverage metadata into an API worker.
    stmt = (
        select(SyncRange)
        .where(SyncRange.asset_id == asset_id)
        .order_by(SyncRange.start_timestamp.asc(), SyncRange.id.asc())
        .limit(limit + 1)
        .offset(offset)
    )
    result = await db.execute(stmt)
    ranges = result.scalars().all()
    page = ranges[:limit]
    
    return SyncStatusResponse(
        asset_id=asset_id,
        synced_ranges=[
            SyncRangeResponse(start_timestamp=r.start_timestamp, end_timestamp=r.end_timestamp)
            for r in page
        ],
        next_offset=offset + limit if len(ranges) > limit else None,
    )
