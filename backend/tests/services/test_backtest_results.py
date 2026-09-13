"""Canonical result persistence tests; no backtest calculations are performed here."""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from app.backtests.contracts import (
    BacktestResultInput,
    BacktestStrategySnapshot,
    CanonicalBacktestResult,
)
from app.models.backtest import BacktestResult
from app.models.strategy import Strategy, StrategyVersion
from app.services.backtest_results import BacktestResultService
from app.services.exceptions import BacktestResultConflictError, BacktestResultError
from app.strategies.contracts import (
    CanonicalStrategyDefinition,
    InstrumentSnapshot,
    StrategyDefinitionInput,
)


def _definition() -> CanonicalStrategyDefinition:
    input_definition = StrategyDefinitionInput.model_validate(
        {
            "timeframe": "1h",
            "universe_asset_ids": [2, 1],
            "parameters": {"fast_window": 10, "slow_window": 30},
            "signals": [
                {
                    "signal_id": "enter-long",
                    "role": "entry",
                    "direction": "long",
                    "definition": {"kind": "cross_above"},
                },
                {
                    "signal_id": "exit-long",
                    "role": "exit",
                    "direction": "flat",
                    "definition": {"kind": "cross_below"},
                },
            ],
            "position": {"mode": "long_only", "sizing": {"kind": "fixed_notional", "value": 100}},
            "execution": {
                "timing": "next_bar_open",
                "order_type": "market",
                "fee_bps": 10,
                "slippage_bps": 2,
            },
            "implementation": {
                "kind": "declarative",
                "identifier": "moving-average-cross",
                "revision": "2026-09-13",
            },
            "benchmark_asset_id": 1,
        }
    )
    return CanonicalStrategyDefinition.from_input(
        input_definition,
        {
            1: InstrumentSnapshot(asset_id=1, symbol="BTCUSDT", exchange="BINANCE", asset_type="SPOT"),
            2: InstrumentSnapshot(asset_id=2, symbol="ETHUSDT", exchange="BINANCE", asset_type="SPOT"),
        },
    )


def _strategy_version() -> tuple[Strategy, StrategyVersion, CanonicalStrategyDefinition]:
    definition = _definition()
    strategy = Strategy(id=uuid.uuid4(), strategy_key="moving-average-cross", name="MA Cross")
    version = StrategyVersion(
        id=uuid.uuid4(),
        strategy_id=strategy.id,
        version_number=3,
        schema_version="1",
        definition_hash=definition.definition_hash(),
        canonical_definition=definition.canonical_payload(),
    )
    return strategy, version, definition


def _result_input(strategy_version_id: uuid.UUID, **overrides) -> BacktestResultInput:
    payload = {
        "strategy_version_id": strategy_version_id,
        "data": {
            "source": "quantive.raw_1m",
            "version": "ingestion-revision-2026-09-13",
            "coverage_fingerprint": "7c34d4e5",
        },
        "start_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2026, 2, 1, tzinfo=timezone.utc),
        "initial_capital": Decimal("1000.00"),
        "final_equity": Decimal("1010.00"),
        "metrics": {
            "total_return": Decimal("0.01"),
            "max_drawdown": Decimal("0.04"),
            "annualized_volatility": Decimal("0.12"),
            "sharpe_ratio": Decimal("1.2"),
            "exposure": Decimal("0.5"),
            "benchmark_return": Decimal("0.005"),
            "benchmark_excess_return": Decimal("0.005"),
            "trade_count": 2,
            "winning_trade_count": 1,
            "losing_trade_count": 1,
            "breakeven_trade_count": 0,
            "fees_paid": Decimal("2.00"),
            "slippage_paid": Decimal("1.00"),
        },
        "trades": [
            {
                "sequence": 2,
                "asset_id": 2,
                "direction": "long",
                "entry_time": datetime(2026, 1, 10, tzinfo=timezone.utc),
                "exit_time": datetime(2026, 1, 12, tzinfo=timezone.utc),
                "entry_price": Decimal("100"),
                "exit_price": Decimal("95"),
                "quantity": Decimal("1"),
                "gross_pnl": Decimal("-5"),
                "net_pnl": Decimal("-6.5"),
                "fees_paid": Decimal("1"),
                "slippage_paid": Decimal("0.5"),
                "entry_signal_id": "enter-long",
                "exit_signal_id": "exit-long",
            },
            {
                "sequence": 1,
                "asset_id": 1,
                "direction": "long",
                "entry_time": datetime(2026, 1, 2, tzinfo=timezone.utc),
                "exit_time": datetime(2026, 1, 4, tzinfo=timezone.utc),
                "entry_price": Decimal("100"),
                "exit_price": Decimal("120"),
                "quantity": Decimal("1"),
                "gross_pnl": Decimal("20"),
                "net_pnl": Decimal("18.5"),
                "fees_paid": Decimal("1"),
                "slippage_paid": Decimal("0.5"),
                "entry_signal_id": "enter-long",
                "exit_signal_id": "exit-long",
            },
        ],
        "signals": [
            {
                "sequence": 2,
                "asset_id": 1,
                "event_type": "exit",
                "event_time": datetime(2026, 1, 4, tzinfo=timezone.utc),
                "observed_at": datetime(2026, 1, 4, 0, 0, 1, tzinfo=timezone.utc),
                "signal_id": "exit-long",
                "position_state": "flat",
            },
            {
                "sequence": 1,
                "asset_id": 1,
                "event_type": "entry",
                "event_time": datetime(2026, 1, 2, tzinfo=timezone.utc),
                "observed_at": datetime(2026, 1, 2, 0, 0, 1, tzinfo=timezone.utc),
                "signal_id": "enter-long",
                "position_state": "long",
            },
        ],
        "generated_at": datetime(2026, 2, 1, 3, tzinfo=timezone.utc),
        "reproducibility": {
            "evaluator_id": "quantive-reference-evaluator",
            "evaluator_revision": "git:abcdef",
            "random_seed": 7,
            "runtime_config": {"warmup_bars": 30},
        },
    }
    payload.update(overrides)
    return BacktestResultInput.model_validate(payload)


def _snapshot(
    strategy: Strategy,
    version: StrategyVersion,
    definition: CanonicalStrategyDefinition,
) -> BacktestStrategySnapshot:
    return BacktestStrategySnapshot(
        strategy_id=strategy.id,
        strategy_version_id=version.id,
        strategy_version_number=version.version_number,
        strategy_definition_hash=version.definition_hash,
        definition=definition,
    )


def _transactional_db():
    db = AsyncMock()
    transaction = AsyncMock()
    transaction.__aenter__.return_value = None
    transaction.__aexit__.return_value = None
    db.begin = MagicMock(return_value=transaction)
    db.begin_nested = MagicMock(return_value=transaction)
    db.add = MagicMock()
    db.flush = AsyncMock()
    return db


def _row_result(strategy: Strategy, version: StrategyVersion):
    result = MagicMock()
    result.first.return_value = (strategy, version)
    return result


def _scalar_result(value):
    result = MagicMock()
    result.scalars.return_value.first.return_value = value
    return result


def test_run_identity_is_deterministic_and_ignores_presentation_order():
    strategy, version, definition = _strategy_version()
    original = CanonicalBacktestResult(_result_input(version.id), _snapshot(strategy, version, definition))
    reordered_input = _result_input(
        version.id,
        trades=list(reversed(_result_input(version.id).trades)),
        signals=list(reversed(_result_input(version.id).signals)),
    )
    reordered = CanonicalBacktestResult(reordered_input, _snapshot(strategy, version, definition))
    regenerated = CanonicalBacktestResult(
        _result_input(version.id, generated_at=datetime(2026, 2, 2, tzinfo=timezone.utc)),
        _snapshot(strategy, version, definition),
    )
    different_window = CanonicalBacktestResult(
        _result_input(version.id, end_time=datetime(2026, 2, 2, tzinfo=timezone.utc)),
        _snapshot(strategy, version, definition),
    )
    different_data_version = CanonicalBacktestResult(
        _result_input(
            version.id,
            data={"source": "quantive.raw_1m", "version": "ingestion-revision-2026-09-14"},
        ),
        _snapshot(strategy, version, definition),
    )

    assert original.run_id() == reordered.run_id()
    assert original.result_hash() == reordered.result_hash()
    assert original.run_id() == regenerated.run_id()
    assert original.result_hash() == regenerated.result_hash()
    assert original.run_id() != different_window.run_id()
    assert original.run_id() != different_data_version.run_id()


@pytest.mark.asyncio
async def test_persistence_snapshots_strategy_version_parameters_window_and_forensic_rows():
    strategy, version, definition = _strategy_version()
    result_input = _result_input(version.id)
    db = _transactional_db()
    db.execute.side_effect = [_row_result(strategy, version), _scalar_result(None)]

    persisted = await BacktestResultService(db).persist(result_input)

    assert persisted.run_id == CanonicalBacktestResult(
        result_input, _snapshot(strategy, version, definition)
    ).run_id()
    assert persisted.strategy_id == strategy.id
    assert persisted.strategy_version_id == version.id
    assert persisted.strategy_version_number == 3
    assert persisted.parameters == {"fast_window": 10, "slow_window": 30}
    assert persisted.execution_assumptions["fee_bps"] == 10.0
    assert persisted.timeframe == "1h"
    assert persisted.start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert persisted.end_time == datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert persisted.data_version == "ingestion-revision-2026-09-13"
    assert persisted.trade_count == 2
    assert db.add.call_count == 5  # result + 2 trades + 2 signals
    persisted_rows = [call.args[0] for call in db.add.call_args_list]
    assert [row.sequence for row in persisted_rows[1:3]] == [1, 2]
    assert [row.sequence for row in persisted_rows[3:]] == [1, 2]
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_duplicate_run_returns_identical_result_without_persisting_more_rows():
    strategy, version, definition = _strategy_version()
    result_input = _result_input(version.id)
    canonical = CanonicalBacktestResult(result_input, _snapshot(strategy, version, definition))
    existing = BacktestResult(run_id=canonical.run_id(), result_hash=canonical.result_hash())
    db = _transactional_db()
    db.execute.side_effect = [_row_result(strategy, version), _scalar_result(existing)]

    returned = await BacktestResultService(db).persist(result_input)

    assert returned is existing
    db.add.assert_not_called()
    db.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_run_with_different_output_is_rejected():
    strategy, version, definition = _strategy_version()
    result_input = _result_input(version.id)
    canonical = CanonicalBacktestResult(result_input, _snapshot(strategy, version, definition))
    existing = BacktestResult(run_id=canonical.run_id(), result_hash="0" * 64)
    db = _transactional_db()
    db.execute.side_effect = [_row_result(strategy, version), _scalar_result(existing)]

    with pytest.raises(BacktestResultConflictError, match="different persisted result"):
        await BacktestResultService(db).persist(result_input)


@pytest.mark.asyncio
async def test_concurrent_first_writer_race_returns_the_winning_idempotent_result():
    """Regression for an absent-row FOR UPDATE race on deterministic run ids."""
    from sqlalchemy.exc import IntegrityError

    strategy, version, definition = _strategy_version()
    result_input = _result_input(version.id)
    canonical = CanonicalBacktestResult(result_input, _snapshot(strategy, version, definition))
    winner = BacktestResult(run_id=canonical.run_id(), result_hash=canonical.result_hash())
    db = _transactional_db()
    db.execute.side_effect = [_row_result(strategy, version), _scalar_result(None), _scalar_result(winner)]
    db.flush.side_effect = IntegrityError("duplicate run", {}, Exception("unique"))

    returned = await BacktestResultService(db).persist(result_input)

    assert returned is winner
    db.begin_nested.assert_called_once()


@pytest.mark.asyncio
async def test_service_rejects_unknown_strategy_version_and_forensic_references():
    db = _transactional_db()
    missing = MagicMock()
    missing.first.return_value = None
    db.execute.return_value = missing
    with pytest.raises(BacktestResultError, match="not found"):
        await BacktestResultService(db).persist(_result_input(uuid.uuid4()))

    strategy, version, _ = _strategy_version()
    db = _transactional_db()
    invalid_trade = _result_input(
        version.id,
        trades=[
            {
                **_result_input(version.id).trades[0].model_dump(),
                "asset_id": 999,
            },
            _result_input(version.id).trades[1].model_dump(),
        ],
    )
    db.execute.side_effect = [_row_result(strategy, version)]
    with pytest.raises(BacktestResultError, match="outside the strategy universe"):
        await BacktestResultService(db).persist(invalid_trade)
    db.add.assert_not_called()


def test_result_validation_preserves_utc_window_and_rejects_inconsistent_trade_statistics():
    _, version, _ = _strategy_version()
    result = _result_input(
        version.id,
        start_time=datetime(2026, 1, 1, 5, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        end_time=datetime(2026, 2, 1, 5, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    )
    assert result.start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert result.end_time == datetime(2026, 2, 1, tzinfo=timezone.utc)

    bad_metrics = _result_input(version.id).metrics.model_dump()
    bad_metrics["winning_trade_count"] = 2
    bad_metrics["losing_trade_count"] = 0
    with pytest.raises(ValidationError, match="win/loss statistics"):
        _result_input(version.id, metrics=bad_metrics)


def test_result_validation_does_not_invent_unavailable_metrics():
    _, version, _ = _strategy_version()
    metrics = _result_input(version.id).metrics.model_dump()
    for field_name in (
        "total_return",
        "max_drawdown",
        "annualized_volatility",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "value_at_risk",
        "benchmark_return",
        "benchmark_excess_return",
    ):
        metrics[field_name] = None

    result = _result_input(version.id, metrics=metrics)

    assert result.metrics.annualized_volatility is None
    assert result.metrics.benchmark_return is None


def test_forensic_rows_are_window_bound_and_input_is_memory_bounded():
    _, version, _ = _strategy_version()
    out_of_window_trade = _result_input(version.id).trades[0].model_dump()
    out_of_window_trade["entry_time"] = datetime(2025, 12, 31, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="wholly contained"):
        _result_input(version.id, trades=[out_of_window_trade, _result_input(version.id).trades[1]])

    out_of_window_signal = _result_input(version.id).signals[0].model_dump()
    out_of_window_signal["event_time"] = datetime(2026, 2, 2, tzinfo=timezone.utc)
    out_of_window_signal["observed_at"] = datetime(2026, 2, 2, 0, 0, 1, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="event_time must be contained"):
        _result_input(version.id, signals=[out_of_window_signal, _result_input(version.id).signals[1]])

    # The bound is enforced during Pydantic parsing, before persistence can
    # allocate a result-sized ORM graph or open a write transaction.
    oversized_signals = [
        {
            "sequence": index + 1,
            "asset_id": 1,
            "event_type": "signal",
            "event_time": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "observed_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "signal_id": "enter-long",
        }
        for index in range(20_001)
    ]
    with pytest.raises(ValidationError, match="at most 20000"):
        _result_input(version.id, signals=oversized_signals)


def test_decimal_values_are_canonicalized_to_database_precision_without_rounding():
    _, version, _ = _strategy_version()
    canonical = _result_input(version.id, initial_capital=Decimal("1000.0000"))
    equivalent = _result_input(version.id, initial_capital=Decimal("1000"))

    assert canonical.initial_capital == equivalent.initial_capital
    with pytest.raises(ValidationError, match="persistence precision"):
        _result_input(version.id, final_equity=Decimal("1010.000000001"))
