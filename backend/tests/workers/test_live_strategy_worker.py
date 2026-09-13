from unittest.mock import AsyncMock, MagicMock

import pytest

from app.workers.live_strategy import (
    AsyncSessionMaker,
    _DISPATCH_LEASE_KEY,
    _RELEASE_DISPATCH_LEASE,
    _release_dispatch_lease,
    _run_live_strategy_evaluation,
    enqueue_live_strategy_evaluation,
)


@pytest.mark.asyncio
async def test_live_strategy_worker_delegates_to_bounded_runtime_service(monkeypatch):
    process_active = AsyncMock(return_value=4)
    monkeypatch.setattr(
        "app.workers.live_strategy.LiveStrategyEvaluatorService.process_active",
        process_active,
    )

    processed = await _run_live_strategy_evaluation()

    assert processed == 4
    process_active.assert_awaited_once_with(AsyncSessionMaker)


def test_dispatch_lease_coalesces_maintenance_cycles_without_queueing_another_message(monkeypatch):
    send = MagicMock()
    monkeypatch.setattr("app.workers.live_strategy.process_live_strategies.send", send)
    monkeypatch.setattr("app.workers.live_strategy.redis_client.set", lambda *args, **kwargs: False)

    enqueued = enqueue_live_strategy_evaluation()

    assert enqueued is False
    send.assert_not_called()


def test_dispatch_lease_is_owned_and_released_by_its_token(monkeypatch):
    sent_tokens = []
    redis_set = MagicMock(return_value=True)
    redis_eval = MagicMock()
    monkeypatch.setattr("app.workers.live_strategy.redis_client.set", redis_set)
    monkeypatch.setattr("app.workers.live_strategy.redis_client.eval", redis_eval)
    monkeypatch.setattr(
        "app.workers.live_strategy.process_live_strategies.send",
        lambda token: sent_tokens.append(token),
    )

    assert enqueue_live_strategy_evaluation() is True
    assert len(sent_tokens) == 1
    redis_set.assert_called_once()
    set_args, set_kwargs = redis_set.call_args
    assert set_args[0] == _DISPATCH_LEASE_KEY
    assert set_args[1] == sent_tokens[0]
    assert set_kwargs["nx"] is True

    _release_dispatch_lease(sent_tokens[0])
    redis_eval.assert_called_once_with(_RELEASE_DISPATCH_LEASE, 1, _DISPATCH_LEASE_KEY, sent_tokens[0])


def test_actor_releases_its_dispatch_lease_after_processing(monkeypatch):
    async def _processed():
        return 4

    release = MagicMock()
    monkeypatch.setattr("app.workers.live_strategy._run_live_strategy_evaluation", _processed)
    monkeypatch.setattr("app.workers.live_strategy._release_dispatch_lease", release)

    from app.workers.live_strategy import process_live_strategies

    process_live_strategies("dispatch-token")

    release.assert_called_once_with("dispatch-token")
