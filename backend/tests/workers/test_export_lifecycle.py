"""No-infrastructure regressions for fenced export worker lifecycle behavior."""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.export_jobs import ExportLeaseLostError
from app.workers.export import (
    _artifact_file_path,
    _artifact_object_key,
    _execute_claimed_export,
    _generate_parquet_export,
    _run_available_exports,
    _run_export_job,
    process_export_job,
)


def _job(*, attempt_count: int = 1, lease_token: str = "a" * 64):
    return SimpleNamespace(
        id=uuid.uuid4(),
        asset_id=1,
        timeframe="1m",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
        attempt_count=attempt_count,
        lease_token=lease_token,
    )


def _lease_service(*, complete: bool = True):
    service = MagicMock()
    service.heartbeat_loop = AsyncMock(return_value=None)
    service.assert_owned = AsyncMock(return_value=None)
    service.complete_if_owned = AsyncMock(return_value=complete)
    service.record_failure_if_owned = AsyncMock(return_value=True)
    return service


@pytest.mark.asyncio
async def test_successful_claim_removes_its_token_scoped_temp_file_and_fences_completion(monkeypatch):
    job = _job()
    file_path = _artifact_file_path(job.id, job.lease_token)
    service = _lease_service()

    async def generate(**kwargs):
        with open(kwargs["file_path"], "w") as file:
            file.write("partial export")

    monkeypatch.setattr("app.workers.export._generate_parquet_export", generate)
    monkeypatch.setattr("app.workers.export._upload_artifact_with_fence", AsyncMock(return_value=True))

    result = await _execute_claimed_export(
        job=job,
        session_factory=MagicMock(),
        lease_service=service,
        worker_id="worker-a",
        lease_duration=timedelta(minutes=5),
        heartbeat_interval=60,
    )

    assert result is True
    assert not os.path.exists(file_path)
    service.complete_if_owned.assert_awaited_once()
    completion = service.complete_if_owned.await_args.kwargs
    assert completion["job_id"] == job.id
    assert completion["worker_id"] == "worker-a"
    assert completion["lease_token"] == job.lease_token
    assert completion["s3_key"] == _artifact_object_key(job, job.lease_token)
    service.record_failure_if_owned.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_claim_records_failure_with_its_token_and_removes_temp_file(monkeypatch):
    job = _job()
    file_path = _artifact_file_path(job.id, job.lease_token)
    service = _lease_service()

    async def generate(**kwargs):
        with open(kwargs["file_path"], "w") as file:
            file.write("partial export")
        raise RuntimeError("database stream failed")

    monkeypatch.setattr("app.workers.export._generate_parquet_export", generate)

    with pytest.raises(RuntimeError, match="database stream failed"):
        await _execute_claimed_export(
            job=job,
            session_factory=MagicMock(),
            lease_service=service,
            worker_id="worker-a",
            lease_duration=timedelta(minutes=5),
            heartbeat_interval=60,
        )

    assert not os.path.exists(file_path)
    failure = service.record_failure_if_owned.await_args.kwargs
    assert failure["job_id"] == job.id
    assert failure["worker_id"] == "worker-a"
    assert failure["lease_token"] == job.lease_token


@pytest.mark.asyncio
async def test_cancelled_export_cleans_its_temp_file_without_mutating_failure_state(monkeypatch):
    job = _job()
    file_path = _artifact_file_path(job.id, job.lease_token)
    service = _lease_service()

    async def generate(**kwargs):
        with open(kwargs["file_path"], "w") as file:
            file.write("partial export")
        raise asyncio.CancelledError()

    monkeypatch.setattr("app.workers.export._generate_parquet_export", generate)

    with pytest.raises(asyncio.CancelledError):
        await _execute_claimed_export(
            job=job,
            session_factory=MagicMock(),
            lease_service=service,
            worker_id="worker-a",
            lease_duration=timedelta(minutes=5),
            heartbeat_interval=60,
        )

    assert not os.path.exists(file_path)
    service.record_failure_if_owned.assert_not_awaited()


@pytest.mark.asyncio
async def test_ownership_loss_never_starts_upload_or_terminal_write(monkeypatch):
    job = _job()
    service = _lease_service()
    upload = AsyncMock(return_value=True)

    async def lose_ownership(**kwargs):
        kwargs["ownership_lost"].set()
        raise ExportLeaseLostError("lease lost")

    monkeypatch.setattr("app.workers.export._generate_parquet_export", lose_ownership)
    monkeypatch.setattr("app.workers.export._upload_artifact_with_fence", upload)

    result = await _execute_claimed_export(
        job=job,
        session_factory=MagicMock(),
        lease_service=service,
        worker_id="worker-a",
        lease_duration=timedelta(minutes=5),
        heartbeat_interval=60,
    )

    assert result is False
    upload.assert_not_awaited()
    service.complete_if_owned.assert_not_awaited()
    service.record_failure_if_owned.assert_not_awaited()


@pytest.mark.asyncio
async def test_parquet_generation_never_accumulates_more_than_one_bounded_chunk(monkeypatch):
    """A large export must flush chunks rather than retain the full candle window."""
    job = _job()
    service = _lease_service()
    chunk_sizes = []

    class FakeWriter:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def write_table(self, _table):
            return None

    class FakeQueryService:
        def __init__(self, _session):
            pass

        async def get_candles(self, **_kwargs):
            for index in range(20_001):
                yield {
                    "timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=index),
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1.0,
                }

    class FakeSessionContext:
        async def __aenter__(self):
            return MagicMock()

        async def __aexit__(self, *_args):
            return False

    def table_from_pydict(columns, **_kwargs):
        chunk_sizes.append(len(columns["timestamp"]))
        return object()

    monkeypatch.setattr("app.workers.export.CandleQueryService", FakeQueryService)
    monkeypatch.setattr("app.workers.export.pq.ParquetWriter", lambda *_args, **_kwargs: FakeWriter())
    monkeypatch.setattr(
        "app.workers.export.pa",
        SimpleNamespace(Table=SimpleNamespace(from_pydict=table_from_pydict)),
    )

    await _generate_parquet_export(
        job=job,
        file_path="/tmp/not-written-by-fake-writer.parquet",
        session_factory=lambda: FakeSessionContext(),
        lease_service=service,
        worker_id="worker-a",
        lease_token=job.lease_token,
        ownership_lost=asyncio.Event(),
    )

    assert chunk_sizes == [10_000, 10_000, 1]
    assert max(chunk_sizes) == 10_000


def test_attempt_scoped_artifact_names_cannot_collide_after_reclaim():
    job = _job(attempt_count=1, lease_token="a" * 64)
    successor = _job(attempt_count=2, lease_token="b" * 64)
    successor.id = job.id
    successor.asset_id = job.asset_id
    successor.timeframe = job.timeframe

    assert _artifact_file_path(job.id, job.lease_token) != _artifact_file_path(
        successor.id, successor.lease_token
    )
    assert _artifact_object_key(job, job.lease_token) != _artifact_object_key(
        successor, successor.lease_token
    )


def test_sync_actor_uses_exactly_one_asyncio_run(monkeypatch):
    calls = []

    def close_and_return(coroutine):
        calls.append(coroutine)
        coroutine.close()
        return False

    monkeypatch.setattr("app.workers.export.asyncio.run", close_and_return)
    process_export_job(str(uuid.uuid4()))

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_known_job_runner_disposes_its_loop_local_engine(monkeypatch):
    job_id = uuid.uuid4()
    engine = AsyncMock()
    factory = MagicMock()
    service = MagicMock()
    service.claim_job = AsyncMock(return_value=None)

    monkeypatch.setattr("app.workers.export._session_factory_for_actor", lambda: (factory, engine))
    monkeypatch.setattr("app.workers.export.ExportJobService", lambda _factory: service)

    assert await _run_export_job(job_id) is False
    engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_recovery_runner_is_bounded_and_continues_after_one_failed_job(monkeypatch):
    first = _job()
    second = _job()
    service = MagicMock()
    service.reap_expired_exhausted_jobs = AsyncMock(return_value=0)
    service.claim_next_job = AsyncMock(side_effect=[first, second, None])
    execute = AsyncMock(side_effect=[RuntimeError("bad export"), True])

    monkeypatch.setattr("app.workers.export.ExportJobService", lambda _factory: service)
    monkeypatch.setattr("app.workers.export._execute_claimed_export", execute)

    processed = await _run_available_exports(max_jobs=2, session_factory=MagicMock())

    assert processed == 2
    assert service.reap_expired_exhausted_jobs.await_args.kwargs["max_jobs"] == 2
    assert service.claim_next_job.await_count == 2
    assert execute.await_count == 2
