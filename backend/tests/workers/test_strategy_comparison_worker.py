"""Unit coverage for coalesced, restart-safe comparison production dispatch."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.workers.strategy_comparison import (
    AsyncSessionMaker,
    _DISPATCH_LEASE_KEY,
    _RELEASE_DISPATCH_LEASE,
    _release_dispatch_lease,
    _run_strategy_comparison_production,
    enqueue_strategy_comparison_production,
)


@pytest.mark.asyncio
async def test_comparison_worker_delegates_to_bounded_producer(monkeypatch):
    process_active = AsyncMock(return_value=2)
    monkeypatch.setattr(
        "app.workers.strategy_comparison.StrategyComparisonProducerService.process_active",
        process_active,
    )

    produced = await _run_strategy_comparison_production()

    assert produced == 2
    process_active.assert_awaited_once_with(AsyncSessionMaker)


def test_comparison_dispatch_lease_is_fail_closed_and_token_owned(monkeypatch):
    send = MagicMock()
    redis_set = MagicMock(return_value=True)
    redis_eval = MagicMock()
    monkeypatch.setattr("app.workers.strategy_comparison.redis_client.set", redis_set)
    monkeypatch.setattr("app.workers.strategy_comparison.redis_client.eval", redis_eval)
    monkeypatch.setattr("app.workers.strategy_comparison.process_strategy_comparisons.send", send)

    assert enqueue_strategy_comparison_production() is True
    token = send.call_args.args[0]
    assert redis_set.call_args.args[0] == _DISPATCH_LEASE_KEY
    assert redis_set.call_args.args[1] == token
    assert redis_set.call_args.kwargs["nx"] is True

    _release_dispatch_lease(token)
    redis_eval.assert_called_once_with(_RELEASE_DISPATCH_LEASE, 1, _DISPATCH_LEASE_KEY, token)


def test_comparison_dispatch_redis_failure_never_enqueues_unbounded_work(monkeypatch):
    send = MagicMock()
    monkeypatch.setattr(
        "app.workers.strategy_comparison.redis_client.set",
        MagicMock(side_effect=ConnectionError("redis down")),
    )
    monkeypatch.setattr("app.workers.strategy_comparison.process_strategy_comparisons.send", send)

    assert enqueue_strategy_comparison_production() is False
    send.assert_not_called()
