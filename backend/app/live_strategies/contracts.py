"""Strict contracts and deterministic threshold evaluation for live strategies."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.strategies.contracts import (
    CanonicalStrategyDefinition,
    ExecutionTiming,
    SignalDirection,
    SignalRole,
    StrategyEventType,
    canonicalize_json,
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PositionState(str, Enum):
    FLAT = "flat"
    LONG = "long"
    SHORT = "short"


class LiveStrategyActivationStatus(str, Enum):
    ACTIVE = "active"
    DEACTIVATED = "deactivated"
    ERROR = "error"


class LiveStrategyHealth(str, Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    ERROR = "error"
    STOPPED = "stopped"


class LiveEvaluationStatus(str, Enum):
    EVALUATED = "evaluated"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    GAP_DETECTED = "gap_detected"
    INVALID = "invalid"
    ERROR = "error"


class LiveStrategyConfiguration(_StrictModel):
    """Bounded runtime configuration for the supported deterministic evaluator."""

    evaluator_id: Literal["threshold-v1"]
    evaluator_revision: str = Field(min_length=1, max_length=128)
    max_candles_per_cycle: int = Field(default=100, ge=1, le=1000)
    runtime_config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("evaluator_revision")
    @classmethod
    def normalize_revision(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("evaluator_revision cannot be blank")
        return normalized

    @field_validator("runtime_config")
    @classmethod
    def validate_runtime_config(cls, value: dict[str, Any]) -> dict[str, Any]:
        return canonicalize_json(value, path="live_strategy.runtime_config")

    def configuration_hash(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class LiveStrategyActivationInput(_StrictModel):
    strategy_version_id: UUID
    configuration: LiveStrategyConfiguration


class LiveCandle(_StrictModel):
    """A validated canonical candle read from persisted market-data tables."""

    asset_id: int = Field(gt=0)
    timestamp: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("candle timestamp must be timezone-aware")
        return value.astimezone(timezone.utc)

    @field_validator("open", "high", "low", "close", "volume")
    @classmethod
    def validate_finite_values(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("candle values must be finite")
        return value

    @model_validator(mode="after")
    def validate_ohlcv(self) -> "LiveCandle":
        epsilon = 1e-9
        if self.high < self.low - epsilon:
            raise ValueError("high cannot be below low")
        if not self.low - epsilon <= self.open <= self.high + epsilon:
            raise ValueError("open must lie within high/low")
        if not self.low - epsilon <= self.close <= self.high + epsilon:
            raise ValueError("close must lie within high/low")
        return self


@dataclass(frozen=True)
class GeneratedLiveEvent:
    event_type: StrategyEventType
    signal_id: str
    position_state: PositionState


@dataclass(frozen=True)
class LiveEvaluationOutcome:
    status: LiveEvaluationStatus
    position_before: PositionState
    position_after: PositionState
    signal_id: str | None = None
    events: tuple[GeneratedLiveEvent, ...] = ()
    error_code: str | None = None
    error_message: str | None = None


_TIMEFRAME_INTERVALS = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}


def validate_live_definition(
    definition: CanonicalStrategyDefinition,
    configuration: LiveStrategyConfiguration,
) -> None:
    """Reject unsupported live definitions before an activation is created."""
    if configuration.evaluator_id != "threshold-v1":
        raise ValueError(f"unsupported live evaluator {configuration.evaluator_id!r}")
    if definition.implementation.kind != "declarative":
        raise ValueError("live threshold evaluator requires a declarative strategy")
    if definition.implementation.identifier != configuration.evaluator_id:
        raise ValueError("strategy implementation identifier must match the live evaluator")
    if definition.implementation.revision != configuration.evaluator_revision:
        raise ValueError("strategy implementation revision must match the live evaluator revision")
    if definition.timeframe.value not in _TIMEFRAME_INTERVALS:
        raise ValueError(f"unsupported strategy timeframe {definition.timeframe.value!r}")
    if definition.execution.timing is not ExecutionTiming.BAR_CLOSE:
        raise ValueError("live threshold evaluator only supports bar_close execution timing")
    for signal in definition.signals:
        _threshold_condition(signal.definition)


def evaluate_live_candle(
    definition: CanonicalStrategyDefinition,
    candle: LiveCandle,
    *,
    position_state: PositionState,
    last_candle_timestamp: datetime | None,
    can_open_position: bool = True,
) -> LiveEvaluationOutcome:
    """Evaluate one persisted candle without side effects or transport access."""
    if candle.asset_id not in {instrument.asset_id for instrument in definition.universe}:
        return LiveEvaluationOutcome(
            status=LiveEvaluationStatus.INVALID,
            position_before=position_state,
            position_after=position_state,
            error_code="asset_outside_universe",
            error_message="canonical candle asset is not part of the strategy universe",
        )

    if not _is_timeframe_aligned(candle.timestamp, definition.timeframe.value):
        return LiveEvaluationOutcome(
            status=LiveEvaluationStatus.INVALID,
            position_before=position_state,
            position_after=position_state,
            error_code="timestamp_not_aligned",
            error_message=(
                f"canonical candle timestamp is not aligned to the {definition.timeframe.value} bucket"
            ),
        )

    if last_candle_timestamp is not None:
        if candle.timestamp == last_candle_timestamp:
            return LiveEvaluationOutcome(
                status=LiveEvaluationStatus.DUPLICATE,
                position_before=position_state,
                position_after=position_state,
            )
        if candle.timestamp < last_candle_timestamp:
            return LiveEvaluationOutcome(
                status=LiveEvaluationStatus.OUT_OF_ORDER,
                position_before=position_state,
                position_after=position_state,
            )
        expected_timestamp = last_candle_timestamp + _TIMEFRAME_INTERVALS[definition.timeframe.value]
        if candle.timestamp > expected_timestamp:
            return LiveEvaluationOutcome(
                status=LiveEvaluationStatus.GAP_DETECTED,
                position_before=position_state,
                position_after=position_state,
                error_code="missing_candles",
                error_message=(
                    f"expected canonical candle at {expected_timestamp.isoformat()} before "
                    f"{candle.timestamp.isoformat()}"
                ),
            )

    try:
        matching_signal = next(
            (
                signal
                for signal in definition.signals
                if _matches_threshold(signal.definition, candle.close)
            ),
            None,
        )
    except ValueError as exc:
        return LiveEvaluationOutcome(
            status=LiveEvaluationStatus.INVALID,
            position_before=position_state,
            position_after=position_state,
            error_code="invalid_signal_definition",
            error_message=str(exc),
        )

    if matching_signal is None:
        return LiveEvaluationOutcome(
            status=LiveEvaluationStatus.EVALUATED,
            position_before=position_state,
            position_after=position_state,
        )

    events: list[GeneratedLiveEvent] = [
        GeneratedLiveEvent(
            event_type=StrategyEventType.SIGNAL,
            signal_id=matching_signal.signal_id,
            position_state=position_state,
        )
    ]
    position_after = position_state

    if matching_signal.role is SignalRole.EXIT:
        if position_state is not PositionState.FLAT:
            position_after = PositionState.FLAT
            events.append(
                GeneratedLiveEvent(
                    event_type=StrategyEventType.EXIT,
                    signal_id=matching_signal.signal_id,
                    position_state=position_after,
                )
            )
    elif position_state is PositionState.FLAT and can_open_position:
        selected_state = _position_from_direction(matching_signal.direction)
        position_after = selected_state
        events.append(
            GeneratedLiveEvent(
                event_type=StrategyEventType.ENTRY,
                signal_id=matching_signal.signal_id,
                position_state=position_after,
            )
        )
    elif (
        position_state is not _position_from_direction(matching_signal.direction)
        and definition.position.allow_reversal
    ):
        selected_state = _position_from_direction(matching_signal.direction)
        events.append(
            GeneratedLiveEvent(
                event_type=StrategyEventType.EXIT,
                signal_id=matching_signal.signal_id,
                position_state=PositionState.FLAT,
            )
        )
        position_after = selected_state
        events.append(
            GeneratedLiveEvent(
                event_type=StrategyEventType.ENTRY,
                signal_id=matching_signal.signal_id,
                position_state=position_after,
            )
        )

    return LiveEvaluationOutcome(
        status=LiveEvaluationStatus.EVALUATED,
        position_before=position_state,
        position_after=position_after,
        signal_id=matching_signal.signal_id,
        events=tuple(events),
    )


def _threshold_condition(definition: dict[str, Any]) -> tuple[str, float]:
    if set(definition) != {"kind", "operator", "threshold"}:
        raise ValueError("threshold-v1 signals require exactly kind, operator, and threshold")
    if definition["kind"] != "close_threshold":
        raise ValueError("threshold-v1 only supports close_threshold signals")
    operator = definition["operator"]
    if operator not in {"gt", "gte", "lt", "lte"}:
        raise ValueError("threshold-v1 operator must be gt, gte, lt, or lte")
    threshold = definition["threshold"]
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise ValueError("threshold-v1 threshold must be numeric")
    threshold_value = float(threshold)
    if not math.isfinite(threshold_value) or threshold_value <= 0:
        raise ValueError("threshold-v1 threshold must be finite and positive")
    return operator, threshold_value


def _matches_threshold(definition: dict[str, Any], close: float) -> bool:
    operator, threshold = _threshold_condition(definition)
    return {
        "gt": close > threshold,
        "gte": close >= threshold,
        "lt": close < threshold,
        "lte": close <= threshold,
    }[operator]


def _is_timeframe_aligned(timestamp: datetime, timeframe: str) -> bool:
    interval = _TIMEFRAME_INTERVALS[timeframe]
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed_microseconds = int((timestamp - epoch).total_seconds() * 1_000_000)
    interval_microseconds = int(interval.total_seconds() * 1_000_000)
    return elapsed_microseconds % interval_microseconds == 0


def _position_from_direction(direction: SignalDirection) -> PositionState:
    if direction is SignalDirection.LONG:
        return PositionState.LONG
    if direction is SignalDirection.SHORT:
        return PositionState.SHORT
    raise ValueError("entry signal cannot target flat position")
