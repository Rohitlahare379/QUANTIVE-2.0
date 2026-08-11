import uuid
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.dependencies import get_db, get_api_key
from app.models.export_jobs import ExportJob
from app.services.s3_storage import S3StorageService
from app.workers.export import process_export_job
from app.services.query import TIMEFRAME_TABLE_MAP

router = APIRouter(prefix="/exports", tags=["Historical Exports"])

class ExportRequest(BaseModel):
    asset_id: int
    timeframe: str
    start_time: datetime
    end_time: datetime

class ExportResponse(BaseModel):
    id: uuid.UUID
    status: str
    download_url: Optional[str] = None
    expires_at: Optional[datetime] = None
    error_message: Optional[str] = None

@router.post("", response_model=ExportResponse, status_code=202)
async def create_export(
    req: ExportRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Initiates an asynchronous historical data export.
    The worker fetches the data, completely decouples the database from network latency,
    and uploads a highly compressed Parquet file to S3.
    """
    if req.timeframe not in TIMEFRAME_TABLE_MAP:
        raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {req.timeframe}")
        
    if req.start_time >= req.end_time:
        raise HTTPException(status_code=400, detail="start_time must be before end_time")
        
    # Force UTC
    start = req.start_time if req.start_time.tzinfo else req.start_time.replace(tzinfo=timezone.utc)
    end = req.end_time if req.end_time.tzinfo else req.end_time.replace(tzinfo=timezone.utc)

    # 1. Register Job
    job = ExportJob(
        asset_id=req.asset_id,
        timeframe=req.timeframe,
        start_time=start,
        end_time=end
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    
    # 2. Enqueue Worker
    process_export_job.send(str(job.id))
    
    return ExportResponse(
        id=job.id,
        status=job.status.value
    )

@router.get("/{job_id}", response_model=ExportResponse)
async def get_export_status(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    api_key: str = Depends(get_api_key)
):
    """
    Check the status of an export. If completed, returns a presigned S3 download URL.
    """
    job = await db.get(ExportJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
        
    download_url = None
    
    # If completed and not expired, generate presigned URL
    if job.status.value == "completed" and job.s3_key:
        if job.expires_at and job.expires_at > datetime.now(timezone.utc):
            s3 = S3StorageService()
            download_url = s3.generate_presigned_url(job.s3_key)
        else:
            raise HTTPException(status_code=410, detail="Export has expired. Please request a new export.")
            
    return ExportResponse(
        id=job.id,
        status=job.status.value,
        download_url=download_url,
        expires_at=job.expires_at,
        error_message=job.error_message
    )
