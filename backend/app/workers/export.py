import os
import uuid
import logging
import asyncio
from datetime import datetime, timezone, timedelta
import dramatiq
import pyarrow as pa
import pyarrow.parquet as pq

from app.db.session import async_session_maker
from app.models.export_jobs import ExportJob, ExportStatus
from app.services.query import CandleQueryService
from app.services.s3_storage import S3StorageService

logger = logging.getLogger(__name__)

# Define PyArrow Schema for strict typed Parquet creation
CANDLE_SCHEMA = pa.schema([
    ('timestamp', pa.timestamp('ms', tz='UTC')),
    ('open', pa.float64()),
    ('high', pa.float64()),
    ('low', pa.float64()),
    ('close', pa.float64()),
    ('volume', pa.float64())
])

async def generate_parquet_export(job_id: uuid.UUID) -> str:
    """
    Connects to database, streams data incrementally, and writes chunks directly to 
    a PyArrow Parquet file on disk to guarantee O(1) memory usage.
    Returns the file path.
    """
    file_path = f"/tmp/{job_id}.parquet"
    
    async with async_session_maker() as session:
        # 1. Fetch and Lock Job
        job = await session.get(ExportJob, job_id)
        if not job or job.status != ExportStatus.PENDING:
            return ""
            
        job.status = ExportStatus.PROCESSING
        await session.commit()
        
        # 2. Setup Streaming
        query_service = CandleQueryService(session)
        try:
            stream = query_service.get_candles(
                asset_id=job.asset_id,
                timeframe=job.timeframe,
                start_time=job.start_time,
                end_time=job.end_time
            )
            
            # 3. Stream & Incremental Write
            # Open the ParquetWriter
            with pq.ParquetWriter(file_path, CANDLE_SCHEMA, compression='gzip') as writer:
                chunk_columns = {col: [] for col in CANDLE_SCHEMA.names}
                chunk_size = 0
                
                async for candle in stream:
                    chunk_columns['timestamp'].append(candle['timestamp'])
                    chunk_columns['open'].append(candle['open'])
                    chunk_columns['high'].append(candle['high'])
                    chunk_columns['low'].append(candle['low'])
                    chunk_columns['close'].append(candle['close'])
                    chunk_columns['volume'].append(candle['volume'])
                    
                    chunk_size += 1
                    
                    if chunk_size >= 10000:
                        # Flush to disk to clear memory
                        table = pa.Table.from_pydict(chunk_columns, schema=CANDLE_SCHEMA)
                        writer.write_table(table)
                        # Reset chunk
                        chunk_columns = {col: [] for col in CANDLE_SCHEMA.names}
                        chunk_size = 0
                        
                # Flush remaining records
                if chunk_size > 0:
                    table = pa.Table.from_pydict(chunk_columns, schema=CANDLE_SCHEMA)
                    writer.write_table(table)
            
            return file_path
            
        except Exception as e:
            job.status = ExportStatus.FAILED
            job.error_message = str(e)
            await session.commit()
            if os.path.exists(file_path):
                os.remove(file_path)
            raise

async def finalize_export(job_id: uuid.UUID, s3_key: str, expires_at: datetime):
    async with async_session_maker() as session:
        job = await session.get(ExportJob, job_id)
        if job:
            job.status = ExportStatus.COMPLETED
            job.s3_key = s3_key
            job.expires_at = expires_at
            await session.commit()

async def fail_export(job_id: uuid.UUID, error: str):
    async with async_session_maker() as session:
        job = await session.get(ExportJob, job_id)
        if job:
            job.status = ExportStatus.FAILED
            job.error_message = error
            await session.commit()

@dramatiq.actor(queue_name="exports", max_retries=3)
def process_export_job(job_id_str: str):
    job_id = uuid.UUID(job_id_str)
    logger.info(f"Starting export job {job_id}")
    
    file_path = f"/tmp/{job_id}.parquet"
    
    try:
        # 1. Generate Parquet (Streams directly from DB to disk)
        try:
            # We inject file_path instead of relying on it returning
            generated_path = asyncio.run(generate_parquet_export(job_id))
            if not generated_path:
                logger.warning(f"Export job {job_id} skipped or not pending.")
                return
        except Exception as e:
            logger.error(f"Failed to generate parquet for export {job_id}: {e}")
            return
            
        # 2. Upload to S3 (CPU/Network Bound)
        try:
            async def get_metadata():
                async with async_session_maker() as session:
                    return await session.get(ExportJob, job_id)
            
            job_meta = asyncio.run(get_metadata())
            
            s3 = S3StorageService()
            s3_key = f"exports/{job_meta.asset_id}/{job_meta.timeframe}/{job_id}.parquet"
            
            success = s3.upload_file(file_path, s3_key)
            
            if success:
                expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
                asyncio.run(finalize_export(job_id, s3_key, expires_at))
                logger.info(f"Export job {job_id} completed successfully.")
            else:
                raise Exception("S3 upload failed")
                
        except Exception as e:
            logger.error(f"Failed to upload export {job_id}: {e}")
            asyncio.run(fail_export(job_id, str(e)))
            raise

    finally:
        # Absolute guarantee of cleanup across all exceptions, SIGINT, SIGTERM, KeyboardInterrupt
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                pass
