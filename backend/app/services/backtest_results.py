"""Persistence boundary for normalized, reproducible backtest outputs."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.backtests.contracts import (
    BacktestResultInput,
    BacktestStrategySnapshot,
    CanonicalBacktestResult,
)
from app.models.backtest import BacktestResult, BacktestSignalRecord, BacktestTrade
from app.models.strategy import Strategy, StrategyVersion
from app.services.exceptions import BacktestResultConflictError, BacktestResultError
from app.strategies.contracts import CanonicalStrategyDefinition


class BacktestResultService:
    """Stores one immutable normalized result for one deterministic run input."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def persist(self, result_input: BacktestResultInput) -> BacktestResult:
        """Persist a completed evaluator output or return its identical prior result.

        Metrics arrive from an evaluator and are never calculated here.  The
        strategy snapshot is loaded from the immutable version, so callers
        cannot substitute parameters, assumptions, instruments, or timeframe.
        """
        async with self.db.begin():
            snapshot = await self._load_strategy_snapshot(result_input.strategy_version_id)
            canonical_result = CanonicalBacktestResult(input=result_input, strategy=snapshot)
            self._validate_forensic_references(canonical_result)

            run_id = canonical_result.run_id()
            result_hash = canonical_result.result_hash()
            existing_result = await self.db.execute(
                select(BacktestResult).where(BacktestResult.run_id == run_id).with_for_update()
            )
            existing = existing_result.scalars().first()
            if existing is not None:
                if existing.result_hash != result_hash:
                    raise BacktestResultConflictError(
                        "deterministic run inputs already have a different persisted result"
                    )
                return existing

            result = self._build_result_model(canonical_result, run_id, result_hash)
            try:
                # A FOR UPDATE lookup cannot lock an absent deterministic run.
                # Insert behind a savepoint so a concurrent winner is converted
                # into the same idempotent result rather than a raw unique-key
                # failure that aborts the caller's outer transaction.
                async with self.db.begin_nested():
                    self._add_result_rows(result, result_input)
                    await self.db.flush()
            except IntegrityError:
                raced_result = await self._existing_run(run_id)
                if raced_result is None:
                    raise
                if raced_result.result_hash != result_hash:
                    raise BacktestResultConflictError(
                        "deterministic run inputs already have a different persisted result"
                    )
                return raced_result
            return result

    async def _existing_run(self, run_id: str) -> BacktestResult | None:
        result = await self.db.execute(
            select(BacktestResult).where(BacktestResult.run_id == run_id).with_for_update()
        )
        return result.scalars().first()

    def _add_result_rows(self, result: BacktestResult, result_input: BacktestResultInput) -> None:
        self.db.add(result)
        for trade in sorted(result_input.trades, key=lambda item: item.sequence):
            self.db.add(
                BacktestTrade(
                    run_id=result.run_id,
                    sequence=trade.sequence,
                    asset_id=trade.asset_id,
                    direction=trade.direction.value,
                    entry_time=trade.entry_time,
                    exit_time=trade.exit_time,
                    entry_price=trade.entry_price,
                    exit_price=trade.exit_price,
                    quantity=trade.quantity,
                    gross_pnl=trade.gross_pnl,
                    net_pnl=trade.net_pnl,
                    fees_paid=trade.fees_paid,
                    slippage_paid=trade.slippage_paid,
                    entry_signal_id=trade.entry_signal_id,
                    exit_signal_id=trade.exit_signal_id,
                    metadata_json=trade.metadata,
                )
            )
        for signal in sorted(result_input.signals, key=lambda item: item.sequence):
            self.db.add(
                BacktestSignalRecord(
                    run_id=result.run_id,
                    sequence=signal.sequence,
                    asset_id=signal.asset_id,
                    event_type=signal.event_type.value,
                    event_time=signal.event_time,
                    observed_at=signal.observed_at,
                    signal_id=signal.signal_id,
                    position_state=signal.position_state.value if signal.position_state else None,
                    payload=signal.payload,
                )
            )

    async def _load_strategy_snapshot(self, strategy_version_id) -> BacktestStrategySnapshot:
        db_result = await self.db.execute(
            select(Strategy, StrategyVersion)
            .join(StrategyVersion, StrategyVersion.strategy_id == Strategy.id)
            .where(StrategyVersion.id == strategy_version_id)
        )
        row = db_result.first()
        if row is None:
            raise BacktestResultError(f"strategy version {strategy_version_id} not found")
        strategy, version = row

        try:
            definition = CanonicalStrategyDefinition.model_validate(version.canonical_definition)
        except ValueError as exc:
            raise BacktestResultError("stored strategy definition is invalid") from exc
        if definition.definition_hash() != version.definition_hash:
            raise BacktestResultError("stored strategy definition does not match its definition hash")

        return BacktestStrategySnapshot(
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            strategy_version_number=version.version_number,
            strategy_definition_hash=version.definition_hash,
            definition=definition,
        )

    @staticmethod
    def _validate_forensic_references(canonical_result: CanonicalBacktestResult) -> None:
        definition = canonical_result.strategy.definition
        universe_asset_ids = {instrument.asset_id for instrument in definition.universe}
        signal_ids = {signal.signal_id for signal in definition.signals}

        for trade in canonical_result.input.trades:
            if trade.asset_id not in universe_asset_ids:
                raise BacktestResultError(f"trade {trade.sequence} references an asset outside the strategy universe")
            for signal_id in (trade.entry_signal_id, trade.exit_signal_id):
                if signal_id is not None and signal_id not in signal_ids:
                    raise BacktestResultError(
                        f"trade {trade.sequence} references unknown strategy signal {signal_id!r}"
                    )
        for signal in canonical_result.input.signals:
            if signal.asset_id not in universe_asset_ids:
                raise BacktestResultError(f"signal {signal.sequence} references an asset outside the strategy universe")
            if signal.signal_id is not None and signal.signal_id not in signal_ids:
                raise BacktestResultError(
                    f"signal {signal.sequence} references unknown strategy signal {signal.signal_id!r}"
                )

    @staticmethod
    def _build_result_model(
        canonical_result: CanonicalBacktestResult,
        run_id: str,
        result_hash: str,
    ) -> BacktestResult:
        result_input = canonical_result.input
        strategy = canonical_result.strategy
        definition = strategy.definition
        metrics = result_input.metrics
        reproducibility = result_input.reproducibility
        return BacktestResult(
            run_id=run_id,
            strategy_id=strategy.strategy_id,
            strategy_version_id=strategy.strategy_version_id,
            strategy_version_number=strategy.strategy_version_number,
            strategy_definition_hash=strategy.strategy_definition_hash,
            universe=[instrument.model_dump(mode="json") for instrument in definition.universe],
            benchmark=definition.benchmark.model_dump(mode="json") if definition.benchmark else None,
            timeframe=definition.timeframe.value,
            parameters=definition.parameters,
            execution_assumptions=definition.execution.model_dump(mode="json"),
            data_source=result_input.data.source,
            data_version=result_input.data.version,
            data_coverage_fingerprint=result_input.data.coverage_fingerprint,
            start_time=result_input.start_time,
            end_time=result_input.end_time,
            initial_capital=result_input.initial_capital,
            final_equity=result_input.final_equity,
            total_return=metrics.total_return,
            max_drawdown=metrics.max_drawdown,
            annualized_volatility=metrics.annualized_volatility,
            sharpe_ratio=metrics.sharpe_ratio,
            sortino_ratio=metrics.sortino_ratio,
            calmar_ratio=metrics.calmar_ratio,
            value_at_risk=metrics.value_at_risk,
            exposure=metrics.exposure,
            benchmark_return=metrics.benchmark_return,
            benchmark_excess_return=metrics.benchmark_excess_return,
            trade_count=metrics.trade_count,
            winning_trade_count=metrics.winning_trade_count,
            losing_trade_count=metrics.losing_trade_count,
            breakeven_trade_count=metrics.breakeven_trade_count,
            fees_paid=metrics.fees_paid,
            slippage_paid=metrics.slippage_paid,
            evaluator_id=reproducibility.evaluator_id,
            evaluator_revision=reproducibility.evaluator_revision,
            random_seed=reproducibility.random_seed,
            reproducibility_metadata=reproducibility.model_dump(mode="json"),
            result_hash=result_hash,
            generated_at=result_input.generated_at,
        )
