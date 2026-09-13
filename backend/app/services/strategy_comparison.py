"""Persistence boundary for deterministic live-versus-reference comparison."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.divergence.contracts import (
    CandlePoint,
    ComparisonInput,
    ComparisonPolicy,
    ComparisonResult,
    DataIssue,
    DivergenceType,
    ExecutionPoint,
    PerformancePoint,
    PositionPoint,
    SignalPoint,
    StrategyVersionIdentity,
    compare,
)
from app.models.backtest import BacktestResult, BacktestSignalRecord, BacktestTrade
from app.models.divergence import StrategyComparisonRun, StrategyDivergenceFinding
from app.models.live_strategy import (
    LiveStrategyActivation,
    LiveStrategyEvent,
    LiveStrategyObservation,
    LiveStrategyState,
)
from app.services.exceptions import StrategyComparisonError
from app.strategies.contracts import canonicalize_json


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ComparisonSupplement(_StrictModel):
    """Evidence not currently produced by Quantive's signal-only live evaluator.

    The caller may provide bounded forensic evidence, but a caller-supplied
    candle list is not a trusted data snapshot.  The service verifies live
    evaluator coverage from durable state and currently fails reference
    coverage closed until an immutable backtest-candle snapshot adapter exists.
    """

    policy: ComparisonPolicy
    window_start: datetime
    window_end: datetime
    observed_complete: bool = False
    expected_configuration_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    reference_candles: tuple[CandlePoint, ...] = ()
    observed_candles: tuple[CandlePoint, ...] = ()
    observed_executions: tuple[ExecutionPoint, ...] = ()
    observed_positions: tuple[PositionPoint, ...] = ()
    observed_performance: PerformancePoint | None = None
    observed_source: dict[str, Any] = Field(default_factory=dict)

    @field_validator("observed_source")
    @classmethod
    def canonical_observed_source(cls, value: dict[str, Any]) -> dict[str, Any]:
        return canonicalize_json(value, path="comparison.observed_source")

    @model_validator(mode="after")
    def validate_window(self) -> "ComparisonSupplement":
        if self.window_start.tzinfo is None or self.window_end.tzinfo is None:
            raise ValueError("comparison window must be timezone-aware")
        if self.window_end <= self.window_start:
            raise ValueError("window_end must be after window_start")
        return self


class StrategyComparisonService:
    """Builds/persists immutable comparison evidence without any market transport."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def compare_activation(
        self,
        *,
        activation_id: uuid.UUID,
        reference_run_id: str,
        supplement: ComparisonSupplement,
    ) -> StrategyComparisonRun:
        """Load canonical live/reference records, compare them, and persist the result."""
        async with self.db.begin():
            activation = await self._activation(activation_id)
            reference = await self._reference(reference_run_id)
            self._validate_activation_reference(activation, reference)
            comparison_input = await self._build_input(activation, reference, supplement)
            return await self._persist_loaded(activation, reference, comparison_input)

    async def persist(
        self,
        *,
        activation_id: uuid.UUID,
        reference_run_id: str,
        comparison_input: ComparisonInput,
    ) -> StrategyComparisonRun:
        """Persist a caller-built deterministic comparison after identity validation.

        This supports integrations that retain their own immutable market/fill
        evidence while preserving the same strategy-version forensic boundary.
        """
        async with self.db.begin():
            activation = await self._activation(activation_id)
            reference = await self._reference(reference_run_id)
            self._validate_activation_reference(activation, reference)
            if comparison_input.strategy != self._activation_identity(activation):
                raise StrategyComparisonError("comparison input does not match the live activation version")
            if comparison_input.reference_strategy != self._reference_identity(reference):
                raise StrategyComparisonError("comparison input does not match the reference backtest version")
            if comparison_input.reference_complete or comparison_input.observed_complete:
                # This method accepts caller-built forensic evidence.  It has
                # no database path to prove that the caller's claimed coverage
                # matches canonical candles, so it must not mint a healthy
                # result from attestation alone.  Use compare_activation (or a
                # future trusted evidence adapter) for complete coverage.
                raise StrategyComparisonError(
                    "direct comparison persistence cannot attest complete canonical coverage; use compare_activation"
                )
            return await self._persist_loaded(activation, reference, comparison_input)

    async def _persist_loaded(
        self,
        activation: LiveStrategyActivation,
        reference: BacktestResult,
        comparison_input: ComparisonInput,
    ) -> StrategyComparisonRun:
        result = compare(comparison_input)
        run_id = self._run_id(activation, reference, comparison_input)
        existing_result = await self.db.execute(
            select(StrategyComparisonRun).where(StrategyComparisonRun.run_id == run_id).with_for_update()
        )
        existing = existing_result.scalars().first()
        if existing is not None:
            return existing

        run = StrategyComparisonRun(
            run_id=run_id,
            strategy_id=activation.strategy_id,
            strategy_version_id=activation.strategy_version_id,
            strategy_version_number=activation.strategy_version_number,
            strategy_definition_hash=activation.strategy_definition_hash,
            activation_id=activation.id,
            reference_run_id=reference.run_id,
            reference_result_hash=reference.result_hash,
            policy_id=comparison_input.policy.policy_id,
            policy_revision=comparison_input.policy.revision,
            policy_hash=comparison_input.policy.policy_hash(),
            policy=comparison_input.policy.model_dump(mode="json"),
            reference_source=comparison_input.reference_source,
            observed_source=comparison_input.observed_source,
            window_start=comparison_input.window_start,
            window_end=comparison_input.window_end,
            status=result.status.value,
        )
        try:
            # The initial SELECT cannot lock an absent run.  A savepoint makes
            # concurrent deterministic writes idempotent without poisoning the
            # outer comparison transaction on the unique-key race.
            async with self.db.begin_nested():
                self.db.add(run)
                for finding in result.findings:
                    self.db.add(self._finding_model(run_id, finding))
                await self.db.flush()
        except IntegrityError:
            raced_result = await self._existing_run(run_id)
            if raced_result is None:
                raise
            return raced_result
        return run

    async def _existing_run(self, run_id: str) -> StrategyComparisonRun | None:
        result = await self.db.execute(
            select(StrategyComparisonRun).where(StrategyComparisonRun.run_id == run_id).with_for_update()
        )
        return result.scalars().first()

    async def _build_input(
        self,
        activation: LiveStrategyActivation,
        reference: BacktestResult,
        supplement: ComparisonSupplement,
    ) -> ComparisonInput:
        # ``reference_candles`` is caller-supplied comparison evidence, not a
        # durable snapshot linked to ``reference.data_coverage_fingerprint``.
        # A nonempty list therefore cannot prove that the whole backtest
        # window is represented by the immutable data version used for that
        # run.  Retain it to surface discrepancies, but fail coverage closed
        # until a trusted snapshot adapter exists.  Without this boundary an
        # arbitrary one-candle supplement could mint a false healthy result.
        reference_complete = False

        expected_signals = await self._backtest_signals(reference.run_id, supplement)
        observed_signals = await self._live_events(activation.id, supplement)
        observations = await self._live_observations(activation.id, supplement)
        observed_issues = tuple(_issue_from_observation(observation) for observation in observations if _issue_from_observation(observation))
        live_has_gap_or_error = bool(observed_issues)
        observed_complete = (
            supplement.observed_complete
            and not live_has_gap_or_error
            and await self._canonical_observed_coverage_complete(activation.id, supplement)
        )
        expected_positions = tuple(
            PositionPoint(
                asset_id=signal.asset_id,
                timestamp=signal.event_time,
                position_state=signal.position_state,
            )
            for signal in expected_signals
            if signal.position_state is not None
        )
        observed_positions = supplement.observed_positions or tuple(
            PositionPoint(
                asset_id=event.asset_id,
                timestamp=event.event_time,
                position_state=event.position_state,
            )
            for event in observed_signals
            if event.event_type in {"entry", "exit"}
        )
        expected_executions = await self._backtest_executions(reference.run_id, supplement)
        # A full-run metric is not a metric for an arbitrary live subwindow.
        # Omitting it is safer than generating a false performance divergence.
        expected_performance = (
            _reference_performance(reference)
            if supplement.window_start == reference.start_time and supplement.window_end == reference.end_time
            else None
        )
        return ComparisonInput(
            strategy=self._activation_identity(activation),
            reference_strategy=self._reference_identity(reference),
            policy=supplement.policy,
            window_start=supplement.window_start,
            window_end=supplement.window_end,
            reference_source={
                "backtest_run_id": reference.run_id,
                "result_hash": reference.result_hash,
                "data_source": reference.data_source,
                "data_version": reference.data_version,
                "data_coverage_fingerprint": reference.data_coverage_fingerprint,
            },
            observed_source={
                "activation_id": str(activation.id),
                "configuration_hash": activation.configuration_hash,
                **supplement.observed_source,
            },
            reference_complete=reference_complete,
            observed_complete=observed_complete,
            expected_configuration_hash=supplement.expected_configuration_hash,
            observed_configuration_hash=activation.configuration_hash,
            expected_signals=expected_signals,
            observed_signals=observed_signals,
            expected_candles=supplement.reference_candles,
            observed_candles=supplement.observed_candles,
            expected_executions=expected_executions,
            observed_executions=supplement.observed_executions,
            expected_positions=expected_positions,
            observed_positions=observed_positions,
            expected_performance=expected_performance,
            observed_performance=supplement.observed_performance,
            observed_data_issues=observed_issues,
        )

    async def _activation(self, activation_id: uuid.UUID) -> LiveStrategyActivation:
        result = await self.db.execute(select(LiveStrategyActivation).where(LiveStrategyActivation.id == activation_id))
        activation = result.scalars().first()
        if activation is None:
            raise StrategyComparisonError(f"live activation {activation_id} not found")
        return activation

    async def _reference(self, reference_run_id: str) -> BacktestResult:
        result = await self.db.execute(select(BacktestResult).where(BacktestResult.run_id == reference_run_id))
        reference = result.scalars().first()
        if reference is None:
            raise StrategyComparisonError(f"reference backtest {reference_run_id} not found")
        return reference

    @staticmethod
    def _validate_activation_reference(activation: LiveStrategyActivation, reference: BacktestResult) -> None:
        if activation.strategy_id != reference.strategy_id:
            raise StrategyComparisonError("activation and reference belong to different strategies")

    async def _backtest_signals(self, run_id: str, supplement: ComparisonSupplement) -> tuple[SignalPoint, ...]:
        records = await self._bounded_rows(
            select(BacktestSignalRecord)
            .where(
                BacktestSignalRecord.run_id == run_id,
                BacktestSignalRecord.event_time >= supplement.window_start,
                BacktestSignalRecord.event_time <= supplement.window_end,
            )
            .order_by(BacktestSignalRecord.event_time.asc(), BacktestSignalRecord.sequence.asc())
        )
        return tuple(
            SignalPoint(
                asset_id=row.asset_id,
                event_type=row.event_type,
                event_time=row.event_time,
                signal_id=row.signal_id,
                position_state=row.position_state,
                source_id=f"backtest:{run_id}:{row.sequence}",
            )
            for row in records
        )

    async def _live_events(self, activation_id: uuid.UUID, supplement: ComparisonSupplement) -> tuple[SignalPoint, ...]:
        records = await self._bounded_rows(
            select(LiveStrategyEvent)
            .where(
                LiveStrategyEvent.activation_id == activation_id,
                LiveStrategyEvent.event_time >= supplement.window_start,
                LiveStrategyEvent.event_time <= supplement.window_end,
            )
            .order_by(LiveStrategyEvent.event_time.asc(), LiveStrategyEvent.id.asc())
        )
        return tuple(
            SignalPoint(
                asset_id=row.asset_id,
                event_type=row.event_type,
                event_time=row.event_time,
                signal_id=row.signal_id,
                position_state=row.position_state,
                source_id=f"live-event:{row.id}",
            )
            for row in records
        )

    async def _live_observations(self, activation_id: uuid.UUID, supplement: ComparisonSupplement) -> tuple[LiveStrategyObservation, ...]:
        latest_attempts = self._latest_observation_attempts(activation_id, supplement)
        records = await self._bounded_rows(
            select(LiveStrategyObservation)
            .join(
                latest_attempts,
                and_(
                    LiveStrategyObservation.activation_id == latest_attempts.c.activation_id,
                    LiveStrategyObservation.asset_id == latest_attempts.c.asset_id,
                    LiveStrategyObservation.candle_timestamp == latest_attempts.c.candle_timestamp,
                    LiveStrategyObservation.evaluation_attempt == latest_attempts.c.evaluation_attempt,
                ),
            )
            .where(
                LiveStrategyObservation.status != "evaluated",
            )
            .order_by(LiveStrategyObservation.candle_timestamp.asc(), LiveStrategyObservation.id.asc())
        )
        return tuple(records)

    async def _backtest_executions(self, run_id: str, supplement: ComparisonSupplement) -> tuple[ExecutionPoint, ...]:
        records = await self._bounded_rows(
            select(BacktestTrade)
            .where(
                BacktestTrade.run_id == run_id,
                (BacktestTrade.entry_time >= supplement.window_start)
                & (BacktestTrade.entry_time <= supplement.window_end)
                | (BacktestTrade.exit_time >= supplement.window_start)
                & (BacktestTrade.exit_time <= supplement.window_end),
            )
            .order_by(BacktestTrade.sequence.asc())
        )
        points: list[ExecutionPoint] = []
        for row in records:
            if supplement.window_start <= row.entry_time <= supplement.window_end:
                points.append(
                    ExecutionPoint(asset_id=row.asset_id, side="entry", direction=row.direction, execution_time=row.entry_time,
                        price=row.entry_price, quantity=row.quantity, fees_paid=None, slippage_paid=None,
                        signal_id=row.entry_signal_id, source_id=f"backtest:{run_id}:{row.sequence}:entry")
                )
            if supplement.window_start <= row.exit_time <= supplement.window_end:
                points.append(
                    ExecutionPoint(asset_id=row.asset_id, side="exit", direction=row.direction, execution_time=row.exit_time,
                        price=row.exit_price, quantity=row.quantity, fees_paid=None, slippage_paid=None,
                        signal_id=row.exit_signal_id, source_id=f"backtest:{run_id}:{row.sequence}:exit")
                )
        return tuple(points)

    async def _canonical_observed_coverage_complete(
        self,
        activation_id: uuid.UUID,
        supplement: ComparisonSupplement,
    ) -> bool:
        """Derive observed completeness from durable evaluator state, not a flag.

        Every configured asset must have advanced through the requested window
        and have at least one successful observation in it.  This intentionally
        yields insufficient-reference for a quiet/unobserved window rather than
        a false healthy comparison.
        """
        states_result = await self.db.execute(
            select(LiveStrategyState.asset_id, LiveStrategyState.last_candle_timestamp)
            .where(LiveStrategyState.activation_id == activation_id)
        )
        states = states_result.all()
        if not states:
            return False
        state_asset_ids = []
        for row in states:
            try:
                asset_id, last_timestamp = row.asset_id, row.last_candle_timestamp
            except AttributeError:
                asset_id, last_timestamp = row
            if last_timestamp is None or last_timestamp < supplement.window_end:
                return False
            state_asset_ids.append(asset_id)
        latest_attempts = self._latest_observation_attempts(activation_id, supplement)
        counts_result = await self.db.execute(
            select(LiveStrategyObservation.asset_id, func.count(LiveStrategyObservation.id))
            .join(
                latest_attempts,
                and_(
                    LiveStrategyObservation.activation_id == latest_attempts.c.activation_id,
                    LiveStrategyObservation.asset_id == latest_attempts.c.asset_id,
                    LiveStrategyObservation.candle_timestamp == latest_attempts.c.candle_timestamp,
                    LiveStrategyObservation.evaluation_attempt == latest_attempts.c.evaluation_attempt,
                ),
            )
            .where(
                LiveStrategyObservation.status == "evaluated",
            )
            .group_by(LiveStrategyObservation.asset_id)
        )
        observed_asset_ids = {row[0] for row in counts_result.all() if row[1] > 0}
        return set(state_asset_ids).issubset(observed_asset_ids)

    @staticmethod
    def _latest_observation_attempts(
        activation_id: uuid.UUID,
        supplement: ComparisonSupplement,
    ):
        """Select exactly the effective immutable outcome per live candle.

        A corrected replay deliberately leaves a failed first attempt in the
        forensic ledger.  Comparison must surface the *latest* attempt rather
        than treating repaired historical evidence as a still-active outage.
        """
        return (
            select(
                LiveStrategyObservation.activation_id.label("activation_id"),
                LiveStrategyObservation.asset_id.label("asset_id"),
                LiveStrategyObservation.candle_timestamp.label("candle_timestamp"),
                func.max(LiveStrategyObservation.evaluation_attempt).label("evaluation_attempt"),
            )
            .where(
                LiveStrategyObservation.activation_id == activation_id,
                LiveStrategyObservation.candle_timestamp >= supplement.window_start,
                LiveStrategyObservation.candle_timestamp <= supplement.window_end,
            )
            .group_by(
                LiveStrategyObservation.activation_id,
                LiveStrategyObservation.asset_id,
                LiveStrategyObservation.candle_timestamp,
            )
            .subquery()
        )

    async def _bounded_rows(self, statement):
        limit = settings.STRATEGY_COMPARISON_MAX_RECORDS
        result = await self.db.execute(statement.limit(limit + 1))
        rows = list(result.scalars().all())
        if len(rows) > limit:
            raise StrategyComparisonError(
                f"comparison evidence exceeds bounded limit of {limit}; narrow the comparison window"
            )
        return rows

    @staticmethod
    def _activation_identity(activation: LiveStrategyActivation) -> StrategyVersionIdentity:
        return StrategyVersionIdentity(
            strategy_id=activation.strategy_id,
            strategy_version_id=activation.strategy_version_id,
            strategy_version_number=activation.strategy_version_number,
            strategy_definition_hash=activation.strategy_definition_hash,
        )

    @staticmethod
    def _reference_identity(reference: BacktestResult) -> StrategyVersionIdentity:
        return StrategyVersionIdentity(
            strategy_id=reference.strategy_id,
            strategy_version_id=reference.strategy_version_id,
            strategy_version_number=reference.strategy_version_number,
            strategy_definition_hash=reference.strategy_definition_hash,
        )

    @staticmethod
    def _run_id(
        activation: LiveStrategyActivation,
        reference: BacktestResult,
        comparison_input: ComparisonInput,
    ) -> str:
        payload = {
            "activation_id": str(activation.id),
            "reference_run_id": reference.run_id,
            "reference_result_hash": reference.result_hash,
            "comparison": comparison_input.model_dump(mode="json"),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _finding_model(run_id: str, finding) -> StrategyDivergenceFinding:
        asset_id = finding.expected_value.get("asset_id") or finding.observed_value.get("asset_id")
        return StrategyDivergenceFinding(
            id=uuid.uuid4(),
            comparison_run_id=run_id,
            fingerprint=finding.fingerprint(),
            strategy_id=finding.strategy.strategy_id,
            strategy_version_id=finding.strategy.strategy_version_id,
            strategy_version_number=finding.strategy.strategy_version_number,
            strategy_definition_hash=finding.strategy.strategy_definition_hash,
            asset_id=asset_id,
            event_timestamp=finding.timestamp,
            window_start=finding.window_start,
            window_end=finding.window_end,
            divergence_type=finding.divergence_type.value,
            severity=finding.severity.value,
            category=finding.category,
            expected_value=finding.expected_value,
            observed_value=finding.observed_value,
            source_reference=finding.source_reference,
            explanation=finding.explanation,
            resolution_status=finding.resolution_status.value,
        )


def _issue_from_observation(observation: LiveStrategyObservation) -> DataIssue | None:
    observed = {
        "observation_id": str(observation.id),
        "status": observation.status,
        "error_code": observation.error_code,
        "error_message": observation.error_message,
        "asset_id": observation.asset_id,
    }
    if observation.status == "gap_detected":
        return DataIssue(
            divergence_type=DivergenceType.MARKET_DATA_MISSING_CANDLE,
            timestamp=observation.candle_timestamp,
            asset_id=observation.asset_id,
            observed=observed,
            explanation="Live evaluator detected a missing canonical candle; comparison remains incomplete.",
        )
    if observation.status == "invalid" and observation.error_code == "timestamp_not_aligned":
        return DataIssue(
            divergence_type=DivergenceType.MARKET_DATA_TIMESTAMP_MISMATCH,
            timestamp=observation.candle_timestamp,
            asset_id=observation.asset_id,
            observed=observed,
            explanation="Live evaluator rejected a canonical candle timestamp as misaligned.",
        )
    if observation.status in {"invalid", "error"}:
        return DataIssue(
            divergence_type=DivergenceType.EVALUATOR_FAILURE,
            timestamp=observation.candle_timestamp,
            asset_id=observation.asset_id,
            observed=observed,
            explanation="Live evaluator recorded an invalid/error outcome; no causal attribution is made.",
        )
    return None


def _reference_performance(reference: BacktestResult) -> PerformancePoint:
    return PerformancePoint(
        total_return=reference.total_return,
        max_drawdown=reference.max_drawdown,
        annualized_volatility=reference.annualized_volatility,
        trade_count=reference.trade_count,
        winning_trade_count=reference.winning_trade_count,
        benchmark_return=reference.benchmark_return,
    )
