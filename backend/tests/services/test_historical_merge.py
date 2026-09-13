"""Focused unit regressions for fenced, bounded staged-data promotion."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.historical_merge import HistoricalMergeService


def _session_factory(session: AsyncMock) -> MagicMock:
    factory = MagicMock()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    factory.return_value = context
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=None)
    transaction.__aexit__ = AsyncMock(return_value=False)
    session.begin = MagicMock(return_value=transaction)
    return factory


@pytest.mark.asyncio
async def test_staged_page_is_promoted_before_exact_rows_are_deleted():
    """Regression: a failed raw promotion must leave its staging row intact."""
    day = datetime(2026, 1, 5, tzinfo=timezone.utc)
    row = SimpleNamespace(
        asset_id=7,
        timestamp=day,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=3.0,
        source="binance_rest",
        source_event_time=day,
        source_received_at=day,
    )
    session = AsyncMock()
    factory = _session_factory(session)
    owned = MagicMock()
    owned.scalar_one_or_none.return_value = 1
    staged = MagicMock()
    staged.scalars.return_value.all.return_value = [row]
    session.execute.side_effect = [owned, staged, MagicMock()]
    service = HistoricalMergeService(factory)

    with patch(
        "app.services.historical_merge.IngestionService.commit_raw_batch_in_transaction",
        new_callable=AsyncMock,
    ) as promote:
        count = await service._merge_one_page(
            day=day,
            next_day=datetime(2026, 1, 6, tzinfo=timezone.utc),
            page_size=10,
            job_id=1,
            worker_id="worker-a",
            lease_token="claim-a",
            ownership_lost=asyncio.Event(),
        )

    assert count == 1
    promote.assert_awaited_once()
    # The third statement is the exact composite-key delete, reached only after
    # the transactional canonical promotion completed.
    assert session.execute.await_count == 3


@pytest.mark.asyncio
async def test_failed_raw_promotion_never_reaches_staging_delete():
    day = datetime(2026, 1, 5, tzinfo=timezone.utc)
    row = SimpleNamespace(
        asset_id=7,
        timestamp=day,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=3.0,
        source="binance_rest",
        source_event_time=day,
        source_received_at=day,
    )
    session = AsyncMock()
    factory = _session_factory(session)
    owned = MagicMock()
    owned.scalar_one_or_none.return_value = 1
    staged = MagicMock()
    staged.scalars.return_value.all.return_value = [row]
    session.execute.side_effect = [owned, staged]
    service = HistoricalMergeService(factory)

    with patch(
        "app.services.historical_merge.IngestionService.commit_raw_batch_in_transaction",
        new_callable=AsyncMock,
        side_effect=RuntimeError("raw insert failed"),
    ):
        with pytest.raises(RuntimeError, match="raw insert failed"):
            await service._merge_one_page(
                day=day,
                next_day=datetime(2026, 1, 6, tzinfo=timezone.utc),
                page_size=10,
                job_id=1,
                worker_id="worker-a",
                lease_token="claim-a",
                ownership_lost=asyncio.Event(),
            )

    # Ownership + page select only; no delete statement was issued.
    assert session.execute.await_count == 2


@pytest.mark.asyncio
async def test_available_work_is_bounded_to_requested_job_limit():
    service = HistoricalMergeService(MagicMock())
    service.schedule_staged_days = AsyncMock(return_value=20)
    service.process_next_job = AsyncMock(side_effect=[True, True, False])

    processed = await service.process_available_jobs(max_jobs=2, max_schedule_days=20)

    assert processed == 2
    service.schedule_staged_days.assert_awaited_once_with(max_days=20)
    assert service.process_next_job.await_count == 2
