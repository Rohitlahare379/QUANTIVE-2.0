"""Validation and canonicalization for backtest outputs, not backtest execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.strategies.contracts import (
    CanonicalStrategyDefinition,
    SignalDirection,
    StrategyEventType,
    canonicalize_json,
)


# These records are kept in one immutable result transaction.  The persistence
# service is intentionally not a bulk data lake, so accepting an unbounded
# caller-supplied tuple would turn one backtest request into an unbounded
# allocation/transaction.  Larger results must be split at the evaluator
# boundary with a separately reproducible run identity.
MAX_BACKTEST_TRADES = 10_000
MAX_BACKTEST_SIGNALS = 20_000


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _normalize_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _normalize_decimal(
    value: Decimal,
    *,
    field_name: str,
    max_digits: int,
    decimal_places: int,
) -> Decimal:
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    normalized = value.normalize()
    if normalized == 0:
        normalized = Decimal("0")
    fractional_digits = max(-normalized.as_tuple().exponent, 0)
    integer_digits = max(normalized.adjusted() + 1, 0)
    if fractional_digits > decimal_places or integer_digits + fractional_digits > max_digits:
        raise ValueError(
            f"{field_name} exceeds NUMERIC({max_digits}, {decimal_places}) persistence precision"
        )
    return normalized


class BacktestDataReference(_StrictModel):
    """A caller-supplied, versioned reference to the exact market data used."""

    source: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=1, max_length=256)
    coverage_fingerprint: str | None = Field(default=None, min_length=1, max_length=256)

    @field_validator("source", "version", "coverage_fingerprint")
    @classmethod
    def normalize_reference_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("data reference fields cannot be blank")
        return normalized


class ReproducibilityMetadata(_StrictModel):
    """Evaluator and runtime facts needed to reproduce a run's inputs."""

    evaluator_id: str = Field(min_length=1, max_length=128)
    evaluator_revision: str = Field(min_length=1, max_length=256)
    random_seed: int | None = None
    runtime_config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("evaluator_id", "evaluator_revision")
    @classmethod
    def normalize_evaluator_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("evaluator fields cannot be blank")
        return normalized

    @field_validator("runtime_config")
    @classmethod
    def validate_runtime_config(cls, value: dict[str, Any]) -> dict[str, Any]:
        return canonicalize_json(value, path="reproducibility.runtime_config")


class BacktestMetrics(_StrictModel):
    """Reported metrics only; this layer never derives or annualizes metrics."""

    total_return: Decimal | None = None
    max_drawdown: Decimal | None = Field(default=None, ge=Decimal("0"), le=Decimal("1"))
    annualized_volatility: Decimal | None = Field(default=None, ge=Decimal("0"))
    sharpe_ratio: Decimal | None = None
    sortino_ratio: Decimal | None = None
    calmar_ratio: Decimal | None = None
    value_at_risk: Decimal | None = None
    exposure: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    benchmark_return: Decimal | None = None
    benchmark_excess_return: Decimal | None = None
    trade_count: int = Field(ge=0)
    winning_trade_count: int = Field(ge=0)
    losing_trade_count: int = Field(ge=0)
    breakeven_trade_count: int = Field(ge=0)
    fees_paid: Decimal = Field(ge=Decimal("0"))
    slippage_paid: Decimal = Field(ge=Decimal("0"))

    @field_validator(
        "total_return",
        "max_drawdown",
        "annualized_volatility",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "value_at_risk",
        "exposure",
        "benchmark_return",
        "benchmark_excess_return",
    )
    @classmethod
    def normalize_ratio_metric(cls, value: Decimal | None, info) -> Decimal | None:
        if value is None:
            return None
        return _normalize_decimal(
            value,
            field_name=info.field_name,
            max_digits=24,
            decimal_places=12,
        )

    @field_validator("fees_paid", "slippage_paid")
    @classmethod
    def normalize_cost_metric(cls, value: Decimal, info) -> Decimal:
        return _normalize_decimal(
            value,
            field_name=info.field_name,
            max_digits=24,
            decimal_places=8,
        )

    @model_validator(mode="after")
    def validate_trade_statistics(self) -> "BacktestMetrics":
        classified_trade_count = (
            self.winning_trade_count + self.losing_trade_count + self.breakeven_trade_count
        )
        if classified_trade_count != self.trade_count:
            raise ValueError("win/loss/breakeven counts must equal trade_count")
        return self


class BacktestTrade(_StrictModel):
    """One closed trade reported by a backtest evaluator."""

    sequence: int = Field(ge=1)
    asset_id: int = Field(gt=0)
    direction: SignalDirection
    entry_time: datetime
    exit_time: datetime
    entry_price: Decimal = Field(gt=Decimal("0"))
    exit_price: Decimal = Field(gt=Decimal("0"))
    quantity: Decimal = Field(gt=Decimal("0"))
    gross_pnl: Decimal
    net_pnl: Decimal
    fees_paid: Decimal = Field(ge=Decimal("0"))
    slippage_paid: Decimal = Field(ge=Decimal("0"))
    entry_signal_id: str | None = Field(default=None, min_length=1, max_length=64)
    exit_signal_id: str | None = Field(default=None, min_length=1, max_length=64)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "entry_price",
        "exit_price",
        "quantity",
        "gross_pnl",
        "net_pnl",
        "fees_paid",
        "slippage_paid",
    )
    @classmethod
    def normalize_trade_amount(cls, value: Decimal, info) -> Decimal:
        return _normalize_decimal(
            value,
            field_name=info.field_name,
            max_digits=24,
            decimal_places=8,
        )

    @field_validator("direction")
    @classmethod
    def validate_trade_direction(cls, value: SignalDirection) -> SignalDirection:
        if value is SignalDirection.FLAT:
            raise ValueError("trades must have long or short direction")
        return value

    @field_validator("entry_time", "exit_time")
    @classmethod
    def normalize_trade_time(cls, value: datetime, info) -> datetime:
        return _normalize_utc(value, field_name=info.field_name)

    @field_validator("entry_signal_id", "exit_signal_id")
    @classmethod
    def normalize_signal_id(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        return canonicalize_json(value, path="trade.metadata")

    @model_validator(mode="after")
    def validate_trade_timeline(self) -> "BacktestTrade":
        if self.exit_time < self.entry_time:
            raise ValueError("exit_time cannot precede entry_time")
        return self


class BacktestSignal(_StrictModel):
    """One generated strategy signal/event from the historical evaluator."""

    sequence: int = Field(ge=1)
    asset_id: int = Field(gt=0)
    event_type: StrategyEventType
    event_time: datetime
    observed_at: datetime
    signal_id: str | None = Field(default=None, min_length=1, max_length=64)
    position_state: SignalDirection | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_time", "observed_at")
    @classmethod
    def normalize_signal_time(cls, value: datetime, info) -> datetime:
        return _normalize_utc(value, field_name=info.field_name)

    @field_validator("signal_id")
    @classmethod
    def normalize_signal_id(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        return canonicalize_json(value, path="signal.payload")

    @model_validator(mode="after")
    def validate_signal_shape(self) -> "BacktestSignal":
        if self.observed_at < self.event_time:
            raise ValueError("observed_at cannot precede event_time")
        if self.event_type is StrategyEventType.SIGNAL and self.signal_id is None:
            raise ValueError("signal events require signal_id")
        if self.event_type is StrategyEventType.ENTRY:
            if self.signal_id is None or self.position_state not in {SignalDirection.LONG, SignalDirection.SHORT}:
                raise ValueError("entry events require signal_id and long or short position_state")
        if self.event_type is StrategyEventType.EXIT and self.position_state is not SignalDirection.FLAT:
            raise ValueError("exit events require flat position_state")
        if self.event_type is StrategyEventType.POSITION and self.position_state is None:
            raise ValueError("position events require position_state")
        return self


class BacktestResultInput(_StrictModel):
    """Completed backtest output accepted by the persistence layer."""

    strategy_version_id: UUID
    data: BacktestDataReference
    start_time: datetime
    end_time: datetime
    initial_capital: Decimal = Field(gt=Decimal("0"))
    final_equity: Decimal = Field(gt=Decimal("0"))
    metrics: BacktestMetrics
    trades: tuple[BacktestTrade, ...] = Field(default=(), max_length=MAX_BACKTEST_TRADES)
    signals: tuple[BacktestSignal, ...] = Field(default=(), max_length=MAX_BACKTEST_SIGNALS)
    generated_at: datetime
    reproducibility: ReproducibilityMetadata

    @field_validator("initial_capital", "final_equity")
    @classmethod
    def normalize_capital(cls, value: Decimal, info) -> Decimal:
        return _normalize_decimal(
            value,
            field_name=info.field_name,
            max_digits=24,
            decimal_places=8,
        )

    @field_validator("start_time", "end_time", "generated_at")
    @classmethod
    def normalize_result_time(cls, value: datetime, info) -> datetime:
        return _normalize_utc(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_result_consistency(self) -> "BacktestResultInput":
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")

        trade_sequences = [trade.sequence for trade in self.trades]
        if len(set(trade_sequences)) != len(trade_sequences):
            raise ValueError("trade sequences must be unique within a result")
        signal_sequences = [signal.sequence for signal in self.signals]
        if len(set(signal_sequences)) != len(signal_sequences):
            raise ValueError("signal sequences must be unique within a result")
        if self.metrics.trade_count != len(self.trades):
            raise ValueError("trade_count must equal the number of persisted trades")

        for trade in self.trades:
            if trade.entry_time < self.start_time or trade.exit_time > self.end_time:
                raise ValueError(
                    f"trade {trade.sequence} must be wholly contained in the backtest data window"
                )
        for signal in self.signals:
            if not self.start_time <= signal.event_time <= self.end_time:
                raise ValueError(
                    f"signal {signal.sequence} event_time must be contained in the backtest data window"
                )

        fees_paid = sum((trade.fees_paid for trade in self.trades), Decimal("0"))
        slippage_paid = sum((trade.slippage_paid for trade in self.trades), Decimal("0"))
        if self.metrics.fees_paid != fees_paid:
            raise ValueError("fees_paid must equal the sum of persisted trade fees")
        if self.metrics.slippage_paid != slippage_paid:
            raise ValueError("slippage_paid must equal the sum of persisted trade slippage")

        winning = sum(trade.net_pnl > 0 for trade in self.trades)
        losing = sum(trade.net_pnl < 0 for trade in self.trades)
        breakeven = sum(trade.net_pnl == 0 for trade in self.trades)
        if (
            self.metrics.winning_trade_count,
            self.metrics.losing_trade_count,
            self.metrics.breakeven_trade_count,
        ) != (winning, losing, breakeven):
            raise ValueError("win/loss statistics must match persisted trade net_pnl values")
        return self


@dataclass(frozen=True)
class BacktestStrategySnapshot:
    strategy_id: UUID
    strategy_version_id: UUID
    strategy_version_number: int
    strategy_definition_hash: str
    definition: CanonicalStrategyDefinition


@dataclass(frozen=True)
class CanonicalBacktestResult:
    """Canonical result plus its deterministic input and output identities."""

    input: BacktestResultInput
    strategy: BacktestStrategySnapshot

    def run_identity_payload(self) -> dict[str, Any]:
        definition = self.strategy.definition
        return {
            "strategy": {
                "strategy_id": str(self.strategy.strategy_id),
                "strategy_version_id": str(self.strategy.strategy_version_id),
                "strategy_version_number": self.strategy.strategy_version_number,
                "definition_hash": self.strategy.strategy_definition_hash,
                "timeframe": definition.timeframe.value,
                "universe": [instrument.model_dump(mode="json") for instrument in definition.universe],
                "parameters": definition.parameters,
                "execution": definition.execution.model_dump(mode="json"),
            },
            "data": self.input.data.model_dump(mode="json"),
            "window": {
                "start_time": self.input.start_time.isoformat(),
                "end_time": self.input.end_time.isoformat(),
            },
            "initial_capital": str(self.input.initial_capital),
            "reproducibility": self.input.reproducibility.model_dump(mode="json"),
        }

    def run_id(self) -> str:
        return _hash_payload(self.run_identity_payload())

    def result_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id(),
            "final_equity": str(self.input.final_equity),
            "metrics": self.input.metrics.model_dump(mode="json"),
            "trades": [
                trade.model_dump(mode="json")
                for trade in sorted(self.input.trades, key=lambda item: item.sequence)
            ],
            "signals": [
                signal.model_dump(mode="json")
                for signal in sorted(self.input.signals, key=lambda item: item.sequence)
            ],
        }

    def result_hash(self) -> str:
        return _hash_payload(self.result_payload())


def _hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        canonicalize_json(payload, path="backtest"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
