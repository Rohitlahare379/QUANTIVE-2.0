"""Stable strategy-definition and evaluation-event contracts.

This module intentionally does not evaluate strategies.  It defines the exact,
serializable inputs that a historical evaluator and a live evaluator must share
before their outputs can be compared.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


STRATEGY_DEFINITION_SCHEMA_VERSION = "1"
SUPPORTED_STRATEGY_TIMEFRAMES = frozenset({"1m", "5m", "15m", "1h", "4h", "1d"})
_IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class StrategyTimeframe(str, Enum):
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"


class SignalRole(str, Enum):
    ENTRY = "entry"
    EXIT = "exit"


class SignalDirection(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class PositionMode(str, Enum):
    LONG_ONLY = "long_only"
    SHORT_ONLY = "short_only"
    LONG_SHORT = "long_short"


class ExecutionTiming(str, Enum):
    BAR_CLOSE = "bar_close"
    NEXT_BAR_OPEN = "next_bar_open"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class StrategyEventType(str, Enum):
    SIGNAL = "signal"
    ENTRY = "entry"
    EXIT = "exit"
    POSITION = "position"


class StrategyEvaluationMode(str, Enum):
    HISTORICAL = "historical"
    LIVE = "live"


def _canonicalize_json(value: Any, *, path: str = "$") -> Any:
    """Return JSON-only data, rejecting ambiguous or non-deterministic values."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or infinity")
        return value
    if isinstance(value, list | tuple):
        return [_canonicalize_json(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError(f"{path} object keys must be strings")
        return {
            key: _canonicalize_json(value[key], path=f"{path}.{key}")
            for key in sorted(value)
        }
    raise ValueError(f"{path} must contain JSON values only, not {type(value).__name__}")


def canonicalize_json(value: Any, *, path: str = "$") -> Any:
    """Public JSON canonicalizer shared by strategy-result persistence contracts."""
    return _canonicalize_json(value, path=path)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InstrumentSnapshot(_StrictModel):
    """The immutable instrument identity embedded into a strategy version."""

    asset_id: int = Field(gt=0)
    symbol: str = Field(min_length=1, max_length=50)
    exchange: str = Field(min_length=1, max_length=50)
    asset_type: str = Field(min_length=1, max_length=50)

    @field_validator("symbol", "exchange", "asset_type")
    @classmethod
    def normalize_instrument_identifier(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("instrument identifiers cannot be empty")
        return normalized


class StrategyImplementation(_StrictModel):
    """Stable reference to the code/configuration that interprets the signals."""

    kind: Literal["declarative", "external"]
    identifier: str = Field(min_length=1, max_length=64)
    revision: str = Field(min_length=1, max_length=128)

    @field_validator("identifier")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        normalized = value.strip()
        if not _IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError("identifier must be lowercase kebab/snake-case")
        return normalized

    @field_validator("revision")
    @classmethod
    def validate_revision(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("revision cannot be empty")
        return normalized


class StrategyMetadata(_StrictModel):
    """Human-facing catalog fields that do not affect executable identity."""

    strategy_key: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)

    @field_validator("strategy_key")
    @classmethod
    def validate_strategy_key(cls, value: str) -> str:
        normalized = value.strip()
        if not _IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError("strategy_key must be lowercase kebab/snake-case")
        return normalized

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("name cannot be empty")
        return normalized

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip() or None


class SignalDefinition(_StrictModel):
    """Declarative signal metadata; its contents are opaque to this catalog."""

    signal_id: str = Field(min_length=1, max_length=64)
    role: SignalRole
    direction: SignalDirection
    priority: int = Field(default=100, ge=0, le=1_000_000)
    definition: dict[str, Any] = Field(min_length=1)

    @field_validator("signal_id")
    @classmethod
    def validate_signal_id(cls, value: str) -> str:
        normalized = value.strip()
        if not _IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError("signal_id must be lowercase kebab/snake-case")
        return normalized

    @field_validator("definition")
    @classmethod
    def validate_json_definition(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _canonicalize_json(value, path="signal.definition")

    @model_validator(mode="after")
    def validate_role_direction(self) -> "SignalDefinition":
        if self.role is SignalRole.EXIT and self.direction is not SignalDirection.FLAT:
            raise ValueError("exit signals must target the flat position state")
        if self.role is SignalRole.ENTRY and self.direction is SignalDirection.FLAT:
            raise ValueError("entry signals must target long or short")
        return self


class PositionRules(_StrictModel):
    mode: PositionMode
    max_open_positions: int = Field(default=1, ge=1, le=10_000)
    allow_reversal: bool = False
    sizing: dict[str, Any] = Field(min_length=1)

    @field_validator("sizing")
    @classmethod
    def validate_sizing(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _canonicalize_json(value, path="position.sizing")


class ExecutionAssumptions(_StrictModel):
    timing: ExecutionTiming
    order_type: OrderType
    fee_bps: float = Field(default=0.0, ge=0.0)
    slippage_bps: float = Field(default=0.0, ge=0.0)
    latency_ms: int = Field(default=0, ge=0)

    @field_validator("fee_bps", "slippage_bps")
    @classmethod
    def validate_finite_cost(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("fees and slippage must be finite")
        return value


class StrategyDefinitionInput(_StrictModel):
    """Authoring input; asset ids are resolved to immutable snapshots on save."""

    timeframe: StrategyTimeframe
    universe_asset_ids: tuple[int, ...] = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    signals: tuple[SignalDefinition, ...] = Field(min_length=1)
    position: PositionRules
    execution: ExecutionAssumptions
    implementation: StrategyImplementation
    benchmark_asset_id: int | None = Field(default=None, gt=0)

    @field_validator("universe_asset_ids")
    @classmethod
    def validate_universe_asset_ids(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if any(asset_id <= 0 for asset_id in value):
            raise ValueError("universe asset ids must be positive")
        if len(set(value)) != len(value):
            raise ValueError("universe asset ids must be unique")
        return value

    @field_validator("parameters")
    @classmethod
    def validate_parameters(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _canonicalize_json(value, path="parameters")

    @model_validator(mode="after")
    def validate_strategy_consistency(self) -> "StrategyDefinitionInput":
        signal_ids = [signal.signal_id for signal in self.signals]
        if len(set(signal_ids)) != len(signal_ids):
            raise ValueError("signal ids must be unique")

        allowed_entry_directions = {
            PositionMode.LONG_ONLY: {SignalDirection.LONG},
            PositionMode.SHORT_ONLY: {SignalDirection.SHORT},
            PositionMode.LONG_SHORT: {SignalDirection.LONG, SignalDirection.SHORT},
        }[self.position.mode]
        for signal in self.signals:
            if signal.role is SignalRole.ENTRY and signal.direction not in allowed_entry_directions:
                raise ValueError(
                    f"entry signal {signal.signal_id!r} is incompatible with {self.position.mode.value}"
                )
        return self


class CanonicalStrategyDefinition(_StrictModel):
    """The persisted, hashable definition for one exact strategy version."""

    schema_version: Literal[STRATEGY_DEFINITION_SCHEMA_VERSION] = STRATEGY_DEFINITION_SCHEMA_VERSION
    timeframe: StrategyTimeframe
    universe: tuple[InstrumentSnapshot, ...] = Field(min_length=1)
    parameters: dict[str, Any]
    signals: tuple[SignalDefinition, ...] = Field(min_length=1)
    position: PositionRules
    execution: ExecutionAssumptions
    implementation: StrategyImplementation
    benchmark: InstrumentSnapshot | None = None

    @model_validator(mode="after")
    def validate_canonical_universe(self) -> "CanonicalStrategyDefinition":
        asset_ids = [instrument.asset_id for instrument in self.universe]
        if len(set(asset_ids)) != len(asset_ids):
            raise ValueError("canonical universe asset ids must be unique")
        if tuple(asset_ids) != tuple(sorted(asset_ids)):
            raise ValueError("canonical universe must be sorted by asset_id")
        signal_order = [(signal.priority, signal.signal_id) for signal in self.signals]
        if signal_order != sorted(signal_order):
            raise ValueError("canonical signals must be sorted by priority then signal_id")
        return self

    @classmethod
    def from_input(
        cls,
        definition: StrategyDefinitionInput,
        assets_by_id: dict[int, InstrumentSnapshot],
    ) -> "CanonicalStrategyDefinition":
        required_asset_ids = set(definition.universe_asset_ids)
        if definition.benchmark_asset_id is not None:
            required_asset_ids.add(definition.benchmark_asset_id)
        missing = sorted(required_asset_ids.difference(assets_by_id))
        if missing:
            raise ValueError(f"strategy references unknown assets: {missing}")

        universe = tuple(assets_by_id[asset_id] for asset_id in sorted(definition.universe_asset_ids))
        benchmark = (
            assets_by_id[definition.benchmark_asset_id]
            if definition.benchmark_asset_id is not None
            else None
        )
        return cls(
            timeframe=definition.timeframe,
            universe=universe,
            parameters=definition.parameters,
            signals=tuple(sorted(definition.signals, key=lambda signal: (signal.priority, signal.signal_id))),
            position=definition.position,
            execution=definition.execution,
            implementation=definition.implementation,
            benchmark=benchmark,
        )

    def canonical_payload(self) -> dict[str, Any]:
        return _canonicalize_json(self.model_dump(mode="json"))

    def canonical_json(self) -> str:
        return json.dumps(
            self.canonical_payload(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )

    def definition_hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


class StrategyVersionReference(_StrictModel):
    strategy_key: str = Field(min_length=1, max_length=64)
    version_number: int = Field(ge=1)
    definition_hash: str = Field(min_length=64, max_length=64)

    @field_validator("strategy_key")
    @classmethod
    def validate_strategy_key(cls, value: str) -> str:
        normalized = value.strip()
        if not _IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError("strategy_key must be lowercase kebab/snake-case")
        return normalized

    @field_validator("definition_hash")
    @classmethod
    def validate_hash(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("definition_hash must be a lowercase SHA-256 hex digest")
        return value

    @property
    def identity(self) -> str:
        return f"{self.strategy_key}@v{self.version_number}:{self.definition_hash}"


class StrategyEvent(_StrictModel):
    """Common event envelope for future historical and live evaluators."""

    strategy: StrategyVersionReference
    evaluation_mode: StrategyEvaluationMode
    event_type: StrategyEventType
    event_time: datetime
    observed_at: datetime
    asset_id: int = Field(gt=0)
    signal_id: str | None = Field(default=None, max_length=64)
    position_state: SignalDirection | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_time", "observed_at")
    @classmethod
    def normalize_event_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)

    @field_validator("signal_id")
    @classmethod
    def validate_optional_signal_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not _IDENTIFIER_PATTERN.fullmatch(normalized):
            raise ValueError("signal_id must be lowercase kebab/snake-case")
        return normalized

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _canonicalize_json(value, path="event.payload")

    @model_validator(mode="after")
    def validate_event_shape(self) -> "StrategyEvent":
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
