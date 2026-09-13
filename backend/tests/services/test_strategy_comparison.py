"""Deterministic forensic comparison tests; no transport or metric fabrication."""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.divergence.contracts import (
    CandlePoint,
    ComparisonInput,
    ComparisonPolicy,
    ComparisonStatus,
    DataIssue,
    DivergenceSeverity,
    DivergenceType,
    ExecutionPoint,
    PerformancePoint,
    PositionPoint,
    SignalPoint,
    StrategyVersionIdentity,
    compare,
)
from app.models.backtest import BacktestResult
from app.models.divergence import StrategyComparisonRun
from app.models.live_strategy import LiveStrategyActivation
from app.services.strategy_comparison import ComparisonSupplement, StrategyComparisonService


UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64


def _policy(**overrides) -> ComparisonPolicy:
    payload = {
        "policy_id": "forensic-default",
        "revision": "1",
        "signal_timing_tolerance_seconds": 0,
        "market_timestamp_tolerance_seconds": 0,
        "ohlc_tolerance_bps": "0",
        "volume_tolerance": "0",
        "execution_delay_tolerance_seconds": 0,
        "execution_price_tolerance_bps": "0",
        "fee_tolerance": "0",
        "slippage_tolerance": "0",
        "exposure_tolerance": "0",
        "return_tolerance": "0",
        "drawdown_tolerance": "0",
        "volatility_tolerance": "0",
        "trade_count_tolerance": 0,
        "win_rate_tolerance": "0",
        "benchmark_relative_tolerance": "0",
        "critical_types": ["strategy_version_mismatch"],
    }
    payload.update(overrides)
    return ComparisonPolicy.model_validate(payload)


def _identity(version_number: int = 1, definition_hash: str = HASH_A) -> StrategyVersionIdentity:
    return StrategyVersionIdentity(
        strategy_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        strategy_version_id=uuid.UUID("00000000-0000-0000-0000-000000000002"),
        strategy_version_number=version_number,
        strategy_definition_hash=definition_hash,
    )


def _input(**overrides) -> ComparisonInput:
    identity = _identity()
    payload = {
        "strategy": identity,
        "reference_strategy": identity,
        "policy": _policy(),
        "window_start": START,
        "window_end": START + timedelta(hours=2),
        "reference_source": {"run_id": "reference"},
        "observed_source": {"activation_id": "live"},
        "reference_complete": True,
        "observed_complete": True,
    }
    payload.update(overrides)
    return ComparisonInput.model_validate(payload)


def _signal(*, timestamp=START, position="long") -> SignalPoint:
    return SignalPoint(asset_id=1, event_type="signal", event_time=timestamp, signal_id="enter-long", position_state=position)


def _candle(*, timestamp=START, close="100", revision=None) -> CandlePoint:
    close = Decimal(close)
    return CandlePoint(asset_id=1, timestamp=timestamp, open=Decimal("100"), high=max(Decimal("101"), close), low=min(Decimal("99"), close), close=close, volume=Decimal("10"), revision=revision)


def _execution(*, side="entry", timestamp=START, price="100", fees="1", slippage="0") -> ExecutionPoint:
    return ExecutionPoint(asset_id=1, side=side, direction="long", execution_time=timestamp, price=Decimal(price), quantity=Decimal("1"), fees_paid=Decimal(fees), slippage_paid=Decimal(slippage), signal_id="enter-long")


def test_identical_complete_live_and_reference_behavior_is_healthy():
    signal = _signal()
    result = compare(_input(expected_signals=[signal], observed_signals=[signal]))

    assert result.status is ComparisonStatus.HEALTHY
    assert result.findings == ()


def test_signal_mismatch_and_timing_mismatch_are_distinguished():
    missing = compare(_input(expected_signals=[_signal()]))
    timing = compare(_input(expected_signals=[_signal()], observed_signals=[_signal(timestamp=START + timedelta(seconds=1))]))
    unexpected = compare(_input(observed_signals=[_signal()]))

    assert {finding.divergence_type for finding in missing.findings} == {DivergenceType.MISSING_SIGNAL}
    assert {finding.divergence_type for finding in timing.findings} == {DivergenceType.SIGNAL_TIMING_DIFFERENCE}
    assert {finding.divergence_type for finding in unexpected.findings} == {DivergenceType.UNEXPECTED_SIGNAL}


def test_missing_market_data_never_reports_a_healthy_comparison():
    result = compare(_input(expected_candles=[_candle()]))

    assert result.status is ComparisonStatus.DIVERGENT
    assert result.findings[0].divergence_type is DivergenceType.MARKET_DATA_MISSING_CANDLE


def test_corrected_ohlc_and_volume_data_are_separate_forensic_findings():
    result = compare(_input(
        expected_candles=[_candle(revision="source-v1")],
        observed_candles=[CandlePoint(asset_id=1, timestamp=START, open=Decimal("100"), high=Decimal("103"), low=Decimal("98"), close=Decimal("102"), volume=Decimal("12"), revision="source-v2")],
    ))

    assert {finding.divergence_type for finding in result.findings} == {
        DivergenceType.MARKET_DATA_CORRECTED_CANDLE,
        DivergenceType.MARKET_DATA_OHLC_DISCREPANCY,
        DivergenceType.MARKET_DATA_VOLUME_DISCREPANCY,
    }


def test_market_timestamp_mismatch_and_strategy_state_failures_remain_explicit():
    timestamp = compare(_input(expected_candles=[_candle()], observed_candles=[_candle(timestamp=START + timedelta(seconds=1))]))
    state = compare(_input(
        expected_configuration_hash=HASH_A,
        observed_configuration_hash=HASH_B,
        observed_data_issues=[DataIssue(
            divergence_type=DivergenceType.EVALUATOR_FAILURE,
            timestamp=START,
            observed={"error": "evaluation_error"},
            explanation="Live evaluator failed.",
        )],
    ))

    assert {finding.divergence_type for finding in timestamp.findings} == {DivergenceType.MARKET_DATA_TIMESTAMP_MISMATCH}
    assert {finding.divergence_type for finding in state.findings} == {
        DivergenceType.CONFIGURATION_MISMATCH,
        DivergenceType.EVALUATOR_FAILURE,
    }


def test_execution_slippage_fee_and_delay_divergences_use_configured_tolerances():
    result = compare(_input(
        expected_executions=[_execution()],
        observed_executions=[_execution(timestamp=START + timedelta(seconds=2), price="101", fees="2", slippage="1")],
    ))

    assert {finding.divergence_type for finding in result.findings} == {
        DivergenceType.EXECUTION_DELAY,
        DivergenceType.EXECUTION_SLIPPAGE,
        DivergenceType.FEE_DIFFERENCE,
    }


def test_missing_and_unexpected_entries_are_not_collapsed_into_price_divergence():
    missing = compare(_input(expected_executions=[_execution(side="entry")]))
    unexpected = compare(_input(observed_executions=[_execution(side="exit")]))

    assert missing.findings[0].divergence_type is DivergenceType.MISSING_EXPECTED_ENTRY
    assert unexpected.findings[0].divergence_type is DivergenceType.UNEXPECTED_EXIT


def test_position_and_exposure_divergence_are_separate():
    result = compare(_input(
        expected_positions=[PositionPoint(asset_id=1, timestamp=START, position_state="long", exposure=Decimal("0.5"))],
        observed_positions=[PositionPoint(asset_id=1, timestamp=START, position_state="flat", exposure=Decimal("0.1"))],
    ))

    assert {finding.divergence_type for finding in result.findings} == {
        DivergenceType.POSITION_MISMATCH,
        DivergenceType.EXPOSURE_MISMATCH,
    }


def test_multiple_simultaneous_divergence_causes_remain_independent():
    result = compare(_input(
        expected_signals=[_signal()],
        expected_candles=[_candle()],
        expected_positions=[PositionPoint(asset_id=1, timestamp=START, position_state="long")],
        observed_positions=[PositionPoint(asset_id=1, timestamp=START, position_state="flat")],
    ))

    assert {finding.divergence_type for finding in result.findings} == {
        DivergenceType.MISSING_SIGNAL,
        DivergenceType.MARKET_DATA_MISSING_CANDLE,
        DivergenceType.POSITION_MISMATCH,
    }


def test_version_mismatch_is_critical_and_blocks_direct_comparability():
    result = compare(_input(reference_strategy=_identity(version_number=2, definition_hash=HASH_B)))

    finding = result.findings[0]
    assert result.status is ComparisonStatus.DIVERGENT
    assert finding.divergence_type is DivergenceType.STRATEGY_VERSION_MISMATCH
    assert finding.severity is DivergenceSeverity.CRITICAL


def test_incomplete_reference_or_observed_coverage_is_never_healthy():
    result = compare(_input(reference_complete=False, observed_complete=False))

    assert result.status is ComparisonStatus.INSUFFICIENT_REFERENCE
    assert result.findings[0].divergence_type is DivergenceType.INSUFFICIENT_REFERENCE_DATA


def test_comparison_rejects_out_of_window_or_unbounded_direct_evidence():
    with pytest.raises(ValueError, match="timestamp must be contained"):
        _input(expected_signals=[_signal(timestamp=START + timedelta(hours=3))])

    # This is an invariant test rather than a timing-sensitive benchmark: the
    # input contract refuses an unbounded direct-integration allocation before
    # the O(n) pairing routine receives it.
    oversized = [
        SignalPoint(
            asset_id=1,
            event_type="signal",
            event_time=START,
            signal_id=f"s-{index}",
            position_state="flat",
        )
        for index in range(10_001)
    ]
    with pytest.raises(ValueError, match="bounded limit"):
        _input(expected_signals=oversized)


def test_large_same_identity_stream_pairs_in_order_without_quadratic_candidate_scans():
    # The old matcher built a compatible-list for every expected point.  This
    # regression uses a realistically large same-key stream and verifies the
    # exact monotonic matching result under the hard input bound.
    expected = tuple(_signal(timestamp=START + timedelta(seconds=index)) for index in range(5_000))
    observed = tuple(_signal(timestamp=START + timedelta(seconds=index)) for index in range(5_000))
    result = compare(_input(expected_signals=expected, observed_signals=observed))

    assert result.status is ComparisonStatus.HEALTHY
    assert result.findings == ()


def test_performance_metrics_are_compared_only_when_explicitly_supplied():
    result = compare(_input(
        expected_performance=PerformancePoint(total_return=Decimal("0.1"), max_drawdown=Decimal("0.1"), annualized_volatility=Decimal("0.2"), trade_count=2, winning_trade_count=2, benchmark_return=Decimal("0.05")),
        observed_performance=PerformancePoint(total_return=Decimal("0.02"), max_drawdown=Decimal("0.2"), annualized_volatility=Decimal("0.3"), trade_count=1, winning_trade_count=0, benchmark_return=Decimal("0.01")),
    ))

    assert {finding.divergence_type for finding in result.findings} == {
        DivergenceType.RETURN_DIFFERENCE,
        DivergenceType.DRAWDOWN_DIFFERENCE,
        DivergenceType.VOLATILITY_DIFFERENCE,
        DivergenceType.TRADE_FREQUENCY_DIFFERENCE,
        DivergenceType.WIN_RATE_DIFFERENCE,
        DivergenceType.BENCHMARK_RELATIVE_DIFFERENCE,
    }


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


def _scalar_result(value):
    result = MagicMock()
    result.scalars.return_value.first.return_value = value
    return result


def _all_result(values):
    result = MagicMock()
    result.scalars.return_value.all.return_value = values
    return result


def _activation(identity: StrategyVersionIdentity) -> LiveStrategyActivation:
    return LiveStrategyActivation(
        id=uuid.uuid4(), strategy_id=identity.strategy_id, strategy_version_id=identity.strategy_version_id,
        strategy_version_number=identity.strategy_version_number, strategy_definition_hash=identity.strategy_definition_hash,
        configuration={"evaluator_id": "threshold-v1"}, configuration_hash=HASH_A, status="active", health="healthy",
        activated_at=START, open_position_count=0,
    )


def _reference(identity: StrategyVersionIdentity) -> BacktestResult:
    return BacktestResult(
        run_id="c" * 64, result_hash="d" * 64, strategy_id=identity.strategy_id, strategy_version_id=identity.strategy_version_id,
        strategy_version_number=identity.strategy_version_number, strategy_definition_hash=identity.strategy_definition_hash,
        universe=[], timeframe="1h", parameters={}, execution_assumptions={}, data_source="canonical", data_version="v1",
        data_coverage_fingerprint=None, start_time=START, end_time=START + timedelta(hours=2), initial_capital=Decimal("100"), final_equity=Decimal("101"),
        total_return=Decimal("0.01"), max_drawdown=Decimal("0"), annualized_volatility=Decimal("0"), sharpe_ratio=None, sortino_ratio=None,
        calmar_ratio=None, value_at_risk=None, exposure=Decimal("0"), benchmark_return=None, benchmark_excess_return=None, trade_count=0,
        winning_trade_count=0, losing_trade_count=0, breakeven_trade_count=0, fees_paid=Decimal("0"), slippage_paid=Decimal("0"),
        evaluator_id="reference", evaluator_revision="1", random_seed=None, reproducibility_metadata={}, generated_at=START,
    )


@pytest.mark.asyncio
async def test_persistence_captures_policy_sources_and_each_independent_finding():
    identity = _identity()
    activation, reference = _activation(identity), _reference(identity)
    comparison_input = _input(expected_signals=[_signal()], reference_complete=False, observed_complete=False)
    db = _transactional_db()
    db.execute.side_effect = [_scalar_result(activation), _scalar_result(reference), _scalar_result(None)]

    run = await StrategyComparisonService(db).persist(
        activation_id=activation.id, reference_run_id=reference.run_id, comparison_input=comparison_input
    )

    rows = [call.args[0] for call in db.add.call_args_list]
    assert isinstance(run, StrategyComparisonRun)
    assert run.status == ComparisonStatus.INSUFFICIENT_REFERENCE.value
    missing_signal = next(row for row in rows[1:] if row.divergence_type == DivergenceType.MISSING_SIGNAL.value)
    assert missing_signal.source_reference["reference"]["run_id"] == "reference"
    assert missing_signal.resolution_status == "open"
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_duplicate_deterministic_comparison_returns_existing_run():
    identity = _identity()
    activation, reference = _activation(identity), _reference(identity)
    existing = StrategyComparisonRun(run_id="e" * 64)
    db = _transactional_db()
    db.execute.side_effect = [_scalar_result(activation), _scalar_result(reference), _scalar_result(existing)]

    returned = await StrategyComparisonService(db).persist(
        activation_id=activation.id,
        reference_run_id=reference.run_id,
        comparison_input=_input(reference_complete=False, observed_complete=False),
    )

    assert returned is existing
    db.add.assert_not_called()
    db.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_first_writer_comparison_race_returns_the_winning_run():
    """Regression for the absent-row comparison idempotency race."""
    from sqlalchemy.exc import IntegrityError

    identity = _identity()
    activation, reference = _activation(identity), _reference(identity)
    comparison_input = _input(reference_complete=False, observed_complete=False)
    winning = StrategyComparisonRun(run_id=StrategyComparisonService._run_id(activation, reference, comparison_input))
    db = _transactional_db()
    db.execute.side_effect = [
        _scalar_result(activation), _scalar_result(reference), _scalar_result(None), _scalar_result(winning)
    ]
    db.flush.side_effect = IntegrityError("duplicate comparison", {}, Exception("unique"))

    returned = await StrategyComparisonService(db).persist(
        activation_id=activation.id, reference_run_id=reference.run_id, comparison_input=comparison_input
    )

    assert returned is winning
    db.begin_nested.assert_called_once()


@pytest.mark.asyncio
async def test_direct_persistence_refuses_caller_attested_healthy_coverage():
    identity = _identity()
    activation, reference = _activation(identity), _reference(identity)
    db = _transactional_db()
    db.execute.side_effect = [_scalar_result(activation), _scalar_result(reference)]

    with pytest.raises(Exception, match="cannot attest complete canonical coverage"):
        await StrategyComparisonService(db).persist(
            activation_id=activation.id,
            reference_run_id=reference.run_id,
            comparison_input=_input(),
        )


@pytest.mark.asyncio
async def test_activation_comparison_reads_live_gap_as_market_data_evidence():
    identity = _identity()
    activation, reference = _activation(identity), _reference(identity)
    gap = MagicMock(
        id=uuid.uuid4(), status="gap_detected", candle_timestamp=START + timedelta(hours=1), asset_id=1,
        error_code="missing_candles", error_message="missing",
    )
    db = _transactional_db()
    db.execute.side_effect = [
        _scalar_result(activation), _scalar_result(reference), _all_result([]), _all_result([]),
        _all_result([gap]), _all_result([]), _scalar_result(None),
    ]
    supplement = ComparisonSupplement(
        policy=_policy(), window_start=START, window_end=START + timedelta(hours=2), observed_complete=True,
        observed_performance=PerformancePoint(total_return=Decimal("0.01"), max_drawdown=Decimal("0"), annualized_volatility=Decimal("0"), trade_count=0, winning_trade_count=0),
    )

    run = await StrategyComparisonService(db).compare_activation(
        activation_id=activation.id, reference_run_id=reference.run_id, supplement=supplement
    )

    rows = [call.args[0] for call in db.add.call_args_list]
    assert run.status == ComparisonStatus.INSUFFICIENT_REFERENCE.value
    assert any(row.divergence_type == DivergenceType.MARKET_DATA_MISSING_CANDLE.value for row in rows[1:])
    assert any(row.divergence_type == DivergenceType.INSUFFICIENT_REFERENCE_DATA.value for row in rows[1:])


@pytest.mark.asyncio
async def test_activation_comparison_never_treats_caller_reference_candles_as_trusted_coverage():
    """A nonempty caller payload is not a reproducible backtest data snapshot."""
    identity = _identity()
    activation, reference = _activation(identity), _reference(identity)
    reference.data_coverage_fingerprint = "immutable-reference-coverage-v1"

    state_rows = MagicMock()
    state_rows.all.return_value = [(1, START + timedelta(hours=2))]
    observed_counts = MagicMock()
    observed_counts.all.return_value = [(1, 1)]
    db = _transactional_db()
    db.execute.side_effect = [
        _scalar_result(activation),
        _scalar_result(reference),
        _all_result([]),  # reference signals
        _all_result([]),  # live events
        _all_result([]),  # latest failed observations
        state_rows,
        observed_counts,
        _all_result([]),  # reference executions
        _scalar_result(None),  # deterministic comparison run does not exist
    ]
    supplement = ComparisonSupplement(
        policy=_policy(),
        window_start=START,
        window_end=START + timedelta(hours=2),
        observed_complete=True,
        reference_candles=[_candle()],
        observed_candles=[_candle()],
    )

    run = await StrategyComparisonService(db).compare_activation(
        activation_id=activation.id,
        reference_run_id=reference.run_id,
        supplement=supplement,
    )

    assert run.status == ComparisonStatus.INSUFFICIENT_REFERENCE.value
    findings = [call.args[0] for call in db.add.call_args_list[1:]]
    assert any(
        finding.divergence_type == DivergenceType.INSUFFICIENT_REFERENCE_DATA.value
        for finding in findings
    )


@pytest.mark.asyncio
async def test_repaired_evaluation_uses_only_the_latest_attempt_for_active_data_issues():
    """A corrected replay must not leave its earlier failed attempt active forever."""
    identity = _identity()
    activation = _activation(identity)
    db = _transactional_db()
    db.execute.return_value = _all_result([])
    service = StrategyComparisonService(db)
    supplement = ComparisonSupplement(
        policy=_policy(),
        window_start=START,
        window_end=START + timedelta(hours=2),
    )

    await service._live_observations(activation.id, supplement)

    statement = service.db.execute.call_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))
    assert "max(live_strategy_observations.evaluation_attempt)" in compiled
    assert "live_strategy_observations.evaluation_attempt = anon_1.evaluation_attempt" in compiled
