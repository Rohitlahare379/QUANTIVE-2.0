import os
import uuid
import pytest
from unittest.mock import patch, MagicMock
from app.workers.export import process_export_job

# We simulate the process_export_job logic and inject exceptions at different layers

@pytest.fixture
def mock_generate(monkeypatch):
    # Mock the asyncio.run to return a file path
    # and mock the generation logic itself to just create an empty file
    def fake_run(coro):
        return "/tmp/fake.parquet"
        
    monkeypatch.setattr("app.workers.export.asyncio.run", fake_run)

def test_file_cleanup_on_success(tmp_path, monkeypatch):
    job_id = uuid.uuid4()
    file_path = f"/tmp/{job_id}.parquet"
    
    # Create the fake file to simulate generate_parquet_export success
    with open(file_path, 'w') as f:
        f.write("test")
        
    assert os.path.exists(file_path)
    
    # Mock upload success
    monkeypatch.setattr("app.workers.export.asyncio.run", lambda c: MagicMock())
    monkeypatch.setattr("app.services.s3_storage.S3StorageService.upload_file", lambda self, f, k: True)
    
    process_export_job(str(job_id))
    
    # File MUST be removed
    assert not os.path.exists(file_path)

def test_file_cleanup_on_upload_failure(tmp_path, monkeypatch):
    job_id = uuid.uuid4()
    file_path = f"/tmp/{job_id}.parquet"
    
    with open(file_path, 'w') as f:
        f.write("test")
        
    monkeypatch.setattr("app.workers.export.asyncio.run", lambda c: MagicMock())
    
    # Mock S3 failing
    def fail_upload(*args, **kwargs):
        raise Exception("AWS S3 Down")
        
    monkeypatch.setattr("app.services.s3_storage.S3StorageService.upload_file", fail_upload)
    
    # Dramatiq catches the raise at the worker level, so process_export_job raises
    with pytest.raises(Exception, match="AWS S3 Down"):
        process_export_job(str(job_id))
        
    # File MUST be removed
    assert not os.path.exists(file_path)

def test_file_cleanup_on_worker_interruption(tmp_path, monkeypatch):
    job_id = uuid.uuid4()
    file_path = f"/tmp/{job_id}.parquet"
    
    with open(file_path, 'w') as f:
        f.write("test")
        
    # Simulate KeyboardInterrupt gracefully exiting the worker thread
    def raise_interrupt(*args, **kwargs):
        raise KeyboardInterrupt("Worker shutting down")
        
    monkeypatch.setattr("app.workers.export.asyncio.run", raise_interrupt)
    
    with pytest.raises(KeyboardInterrupt):
        process_export_job(str(job_id))
        
    # File MUST be removed despite the interrupt bypassing standard exception handlers
    assert not os.path.exists(file_path)

def test_file_cleanup_on_generation_exception(monkeypatch):
    job_id = uuid.uuid4()
    file_path = f"/tmp/{job_id}.parquet"
    
    # Simulate generate_parquet_export crashing halfway through AND leaving the file behind
    with open(file_path, 'w') as f:
        f.write("partial")
        
    def crash_generation(*args, **kwargs):
        raise Exception("Database Connection Lost during streaming")
        
    monkeypatch.setattr("app.workers.export.asyncio.run", crash_generation)
    
    # process_export_job catches the generation error and returns early
    process_export_job(str(job_id))
    
    # The file must STILL be cleaned up by the top-level finally
    assert not os.path.exists(file_path)
