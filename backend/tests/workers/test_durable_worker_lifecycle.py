"""No-infrastructure regressions for bounded durable actor lifecycles."""

import asyncio

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.workers.cagg_refresh import _run_cagg_refresh
from app.workers.gap_repair import _run_gap_repair_worker
from app.workers.historical_merge import _run_historical_merge


@pytest.mark.asyncio
async def test_gap_actor_stops_at_configured_work_bound():
    factory = MagicMock()
    service = MagicMock()
    service.process_next_job = AsyncMock(side_effect=[True, True, True])
    with patch("app.workers.gap_repair.GapRepairService", return_value=service):
        processed = await _run_gap_repair_worker(max_jobs=2, session_factory=factory)

    assert processed == 2
    assert service.process_next_job.await_count == 2
    assert all(call.kwargs["respect_retry_schedule"] for call in service.process_next_job.await_args_list)
    assert all(not call.kwargs["raise_on_attempt_error"] for call in service.process_next_job.await_args_list)


@pytest.mark.asyncio
async def test_cagg_actor_stops_at_configured_work_bound():
    factory = MagicMock()
    service = MagicMock()
    service.process_pending_jobs = AsyncMock(side_effect=[True, True, True])
    with patch("app.workers.cagg_refresh.CaggRefreshService", return_value=service):
        processed = await _run_cagg_refresh(max_jobs=2, session_factory=factory)

    assert processed == 2
    assert service.process_pending_jobs.await_count == 2
    assert all(call.kwargs["respect_retry_schedule"] for call in service.process_pending_jobs.await_args_list)
    assert all(not call.kwargs["raise_on_attempt_error"] for call in service.process_pending_jobs.await_args_list)


@pytest.mark.asyncio
async def test_historical_actor_passes_bounded_page_and_job_limits():
    factory = MagicMock()
    service = MagicMock()
    service.process_available_jobs = AsyncMock(return_value=2)
    with patch("app.workers.historical_merge.HistoricalMergeService", return_value=service):
        processed = await _run_historical_merge(
            max_jobs=2,
            max_schedule_days=3,
            page_size=17,
            session_factory=factory,
        )

    assert processed == 2
    service.process_available_jobs.assert_awaited_once_with(
        max_jobs=2,
        max_schedule_days=3,
        page_size=17,
        respect_retry_schedule=True,
        raise_on_attempt_error=False,
    )


@pytest.mark.asyncio
async def test_cagg_actor_disposes_loop_local_engine_on_normal_exit():
    factory = MagicMock()
    engine = AsyncMock()
    service = MagicMock()
    service.process_pending_jobs = AsyncMock(return_value=False)
    with (
        patch("app.workers.cagg_refresh._session_factory_for_actor", return_value=(factory, engine)),
        patch("app.workers.cagg_refresh.CaggRefreshService", return_value=service),
    ):
        assert await _run_cagg_refresh(max_jobs=1) == 0

    engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_gap_actor_disposes_loop_local_engine_when_cancelled():
    factory = MagicMock()
    engine = AsyncMock()
    service = MagicMock()
    service.process_next_job = AsyncMock(side_effect=asyncio.CancelledError())
    with (
        patch("app.workers.gap_repair._session_factory_for_actor", return_value=(factory, engine)),
        patch("app.workers.gap_repair.GapRepairService", return_value=service),
        patch("app.workers.gap_repair.close_async_redis_for_current_loop", new_callable=AsyncMock),
    ):
        with pytest.raises(asyncio.CancelledError):
            await _run_gap_repair_worker(max_jobs=1)

    engine.dispose.assert_awaited_once()
