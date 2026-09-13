"""Normalized, immutable persistence for completed backtest results."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    Numeric,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PostgreSQLUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


_AMOUNT = Numeric(24, 8)
_RATIO = Numeric(24, 12)


class BacktestResult(Base):
    """One completed result, identified by its deterministic run-id hash."""

    __tablename__ = "backtest_results"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_id: Mapped[UUID] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    strategy_version_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), nullable=False, index=True)
    strategy_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    universe: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    benchmark: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    execution_assumptions: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    data_source: Mapped[str] = mapped_column(String(128), nullable=False)
    data_version: Mapped[str] = mapped_column(String(256), nullable=False)
    data_coverage_fingerprint: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    initial_capital: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    final_equity: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    total_return: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    max_drawdown: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    annualized_volatility: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    sharpe_ratio: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    sortino_ratio: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    calmar_ratio: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    value_at_risk: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    exposure: Mapped[Decimal] = mapped_column(_RATIO, nullable=False)
    benchmark_return: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    benchmark_excess_return: Mapped[Optional[Decimal]] = mapped_column(_RATIO, nullable=True)
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    winning_trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    losing_trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    breakeven_trade_count: Mapped[int] = mapped_column(Integer, nullable=False)
    fees_paid: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    slippage_paid: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)

    evaluator_id: Mapped[str] = mapped_column(String(128), nullable=False)
    evaluator_revision: Mapped[str] = mapped_column(String(256), nullable=False)
    random_seed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reproducibility_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    result_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_backtest_results_strategy_version_strategy",
        ),
        CheckConstraint("char_length(run_id) = 64", name="ck_backtest_results_run_id_length"),
        CheckConstraint("run_id ~ '^[0-9a-f]{64}$'", name="ck_backtest_results_run_id_format"),
        CheckConstraint("result_hash ~ '^[0-9a-f]{64}$'", name="ck_backtest_results_result_hash_format"),
        CheckConstraint("end_time > start_time", name="ck_backtest_results_valid_window"),
        CheckConstraint("initial_capital > 0", name="ck_backtest_results_positive_initial_capital"),
        CheckConstraint("final_equity > 0", name="ck_backtest_results_positive_final_equity"),
        CheckConstraint("exposure >= 0 AND exposure <= 1", name="ck_backtest_results_exposure_range"),
        CheckConstraint(
            "max_drawdown IS NULL OR (max_drawdown >= 0 AND max_drawdown <= 1)",
            name="ck_backtest_results_drawdown_range",
        ),
        CheckConstraint(
            "trade_count >= 0 AND winning_trade_count >= 0 AND losing_trade_count >= 0 "
            "AND breakeven_trade_count >= 0",
            name="ck_backtest_results_nonnegative_trade_counts",
        ),
        CheckConstraint(
            "winning_trade_count + losing_trade_count + breakeven_trade_count = trade_count",
            name="ck_backtest_results_trade_count_breakdown",
        ),
        CheckConstraint("fees_paid >= 0 AND slippage_paid >= 0", name="ck_backtest_results_nonnegative_costs"),
    )


class BacktestTrade(Base):
    """A closed trade persisted in order for forensic replay/comparison."""

    __tablename__ = "backtest_trades"

    run_id: Mapped[str] = mapped_column(ForeignKey("backtest_results.run_id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id"), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    entry_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    exit_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    exit_price: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    gross_pnl: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    net_pnl: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    fees_paid: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    slippage_paid: Mapped[Decimal] = mapped_column(_AMOUNT, nullable=False)
    entry_signal_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    exit_signal_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, nullable=False)

    __table_args__ = (
        CheckConstraint("sequence > 0", name="ck_backtest_trades_positive_sequence"),
        CheckConstraint("direction IN ('long', 'short')", name="ck_backtest_trades_direction"),
        CheckConstraint("exit_time >= entry_time", name="ck_backtest_trades_valid_timeline"),
        CheckConstraint(
            "entry_price > 0 AND exit_price > 0 AND quantity > 0",
            name="ck_backtest_trades_positive_execution_values",
        ),
        CheckConstraint("fees_paid >= 0 AND slippage_paid >= 0", name="ck_backtest_trades_nonnegative_costs"),
    )


class BacktestSignalRecord(Base):
    """A normalized historical signal/event generated during a backtest."""

    __tablename__ = "backtest_signals"

    run_id: Mapped[str] = mapped_column(ForeignKey("backtest_results.run_id"), primary_key=True)
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(16), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    signal_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    position_state: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        CheckConstraint("sequence > 0", name="ck_backtest_signals_positive_sequence"),
        CheckConstraint(
            "event_type IN ('signal', 'entry', 'exit', 'position')",
            name="ck_backtest_signals_event_type",
        ),
        CheckConstraint("observed_at >= event_time", name="ck_backtest_signals_observation_time"),
        CheckConstraint(
            "position_state IS NULL OR position_state IN ('long', 'short', 'flat')",
            name="ck_backtest_signals_position_state",
        ),
    )
