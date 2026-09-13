"""Pure, versioned strategy-divergence comparison contracts and logic.

The comparison reports observed differences.  It deliberately does not infer a
cause from correlation: e.g. an execution-price difference is a discrepancy,
not proof that slippage caused a performance change.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.strategies.contracts import canonicalize_json


COMPARISON_SCHEMA_VERSION = "1"
# A comparison is an auditable bounded window, not an arbitrary in-memory
# analytics job.  The cap applies to all caller-provided evidence combined,
# including direct integrations that do not use the database query helpers.
MAX_COMPARISON_INPUT_POINTS = 10_000


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal, name: str) -> Decimal:
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value.normalize() if value else Decimal("0")


class ComparisonStatus(str, Enum):
    HEALTHY = "healthy"
    DIVERGENT = "divergent"
    INSUFFICIENT_REFERENCE = "insufficient_reference"


class DivergenceSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ResolutionStatus(str, Enum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    NOT_APPLICABLE = "not_applicable"


class DivergenceType(str, Enum):
    INSUFFICIENT_REFERENCE_DATA = "insufficient_reference_data"
    STRATEGY_VERSION_MISMATCH = "strategy_version_mismatch"
    CONFIGURATION_MISMATCH = "configuration_mismatch"
    EVALUATOR_FAILURE = "evaluator_failure"
    MISSING_SIGNAL = "missing_signal"
    UNEXPECTED_SIGNAL = "unexpected_signal"
    SIGNAL_TIMING_DIFFERENCE = "signal_timing_difference"
    MARKET_DATA_MISSING_CANDLE = "market_data_missing_candle"
    MARKET_DATA_CORRECTED_CANDLE = "market_data_corrected_candle"
    MARKET_DATA_TIMESTAMP_MISMATCH = "market_data_timestamp_mismatch"
    MARKET_DATA_OHLC_DISCREPANCY = "market_data_ohlc_discrepancy"
    MARKET_DATA_VOLUME_DISCREPANCY = "market_data_volume_discrepancy"
    MISSING_EXPECTED_ENTRY = "missing_expected_entry"
    MISSING_EXPECTED_EXIT = "missing_expected_exit"
    UNEXPECTED_ENTRY = "unexpected_entry"
    UNEXPECTED_EXIT = "unexpected_exit"
    EXECUTION_DELAY = "execution_delay"
    EXECUTION_SLIPPAGE = "execution_slippage"
    FEE_DIFFERENCE = "fee_difference"
    POSITION_MISMATCH = "position_mismatch"
    EXPOSURE_MISMATCH = "exposure_mismatch"
    UNEXPECTED_POSITION_PERSISTENCE = "unexpected_position_persistence"
    RETURN_DIFFERENCE = "return_difference"
    DRAWDOWN_DIFFERENCE = "drawdown_difference"
    VOLATILITY_DIFFERENCE = "volatility_difference"
    TRADE_FREQUENCY_DIFFERENCE = "trade_frequency_difference"
    WIN_RATE_DIFFERENCE = "win_rate_difference"
    BENCHMARK_RELATIVE_DIFFERENCE = "benchmark_relative_difference"


class StrategyVersionIdentity(_StrictModel):
    strategy_id: UUID
    strategy_version_id: UUID
    strategy_version_number: int = Field(ge=1)
    strategy_definition_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ComparisonPolicy(_StrictModel):
    """Explicit tolerances captured with every persisted comparison run."""

    schema_version: Literal[COMPARISON_SCHEMA_VERSION] = COMPARISON_SCHEMA_VERSION
    policy_id: str = Field(min_length=1, max_length=128)
    revision: str = Field(min_length=1, max_length=128)
    signal_timing_tolerance_seconds: int = Field(ge=0)
    market_timestamp_tolerance_seconds: int = Field(ge=0)
    ohlc_tolerance_bps: Decimal = Field(ge=Decimal("0"))
    volume_tolerance: Decimal = Field(ge=Decimal("0"))
    execution_delay_tolerance_seconds: int = Field(ge=0)
    execution_price_tolerance_bps: Decimal = Field(ge=Decimal("0"))
    fee_tolerance: Decimal = Field(ge=Decimal("0"))
    slippage_tolerance: Decimal = Field(ge=Decimal("0"))
    exposure_tolerance: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    return_tolerance: Decimal = Field(ge=Decimal("0"))
    drawdown_tolerance: Decimal = Field(ge=Decimal("0"))
    volatility_tolerance: Decimal = Field(ge=Decimal("0"))
    trade_count_tolerance: int = Field(ge=0)
    win_rate_tolerance: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    benchmark_relative_tolerance: Decimal = Field(ge=Decimal("0"))
    default_severity: DivergenceSeverity = DivergenceSeverity.WARNING
    critical_types: tuple[DivergenceType, ...] = ()

    @field_validator(
        "ohlc_tolerance_bps", "volume_tolerance", "execution_price_tolerance_bps",
        "fee_tolerance", "slippage_tolerance", "exposure_tolerance", "return_tolerance",
        "drawdown_tolerance", "volatility_tolerance", "win_rate_tolerance",
        "benchmark_relative_tolerance",
    )
    @classmethod
    def normalize_decimal_tolerance(cls, value: Decimal, info) -> Decimal:
        return _decimal(value, info.field_name)

    @field_validator("policy_id", "revision")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("policy text cannot be blank")
        return normalized

    def policy_hash(self) -> str:
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def severity_for(self, divergence_type: DivergenceType) -> DivergenceSeverity:
        return DivergenceSeverity.CRITICAL if divergence_type in self.critical_types else self.default_severity


class SignalPoint(_StrictModel):
    asset_id: int = Field(gt=0)
    event_type: Literal["signal", "entry", "exit", "position"]
    event_time: datetime
    signal_id: str | None = Field(default=None, max_length=64)
    position_state: Literal["flat", "long", "short"] | None = None
    source_id: str | None = Field(default=None, max_length=128)

    @field_validator("event_time")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value, "event_time")


class CandlePoint(_StrictModel):
    asset_id: int = Field(gt=0)
    timestamp: datetime
    open: Decimal = Field(gt=Decimal("0"))
    high: Decimal = Field(gt=Decimal("0"))
    low: Decimal = Field(gt=Decimal("0"))
    close: Decimal = Field(gt=Decimal("0"))
    volume: Decimal = Field(ge=Decimal("0"))
    revision: str | None = Field(default=None, max_length=256)

    @field_validator("timestamp")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value, "timestamp")

    @field_validator("open", "high", "low", "close", "volume")
    @classmethod
    def normalize_values(cls, value: Decimal, info) -> Decimal:
        return _decimal(value, info.field_name)

    @model_validator(mode="after")
    def validate_ohlc(self) -> "CandlePoint":
        if not self.low <= self.open <= self.high or not self.low <= self.close <= self.high:
            raise ValueError("open and close must be within low/high")
        return self


class ExecutionPoint(_StrictModel):
    asset_id: int = Field(gt=0)
    side: Literal["entry", "exit"]
    direction: Literal["long", "short"]
    execution_time: datetime
    price: Decimal = Field(gt=Decimal("0"))
    quantity: Decimal = Field(gt=Decimal("0"))
    # BacktestTrade records costs for a closed trade, not each fill. Costs are
    # therefore optional here so comparison never invents an entry/exit split.
    fees_paid: Decimal | None = Field(default=None, ge=Decimal("0"))
    slippage_paid: Decimal | None = Field(default=None, ge=Decimal("0"))
    signal_id: str | None = Field(default=None, max_length=64)
    source_id: str | None = Field(default=None, max_length=128)

    @field_validator("execution_time")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value, "execution_time")

    @field_validator("price", "quantity", "fees_paid", "slippage_paid")
    @classmethod
    def normalize_values(cls, value: Decimal | None, info) -> Decimal | None:
        return _decimal(value, info.field_name) if value is not None else None


class PositionPoint(_StrictModel):
    asset_id: int = Field(gt=0)
    timestamp: datetime
    position_state: Literal["flat", "long", "short"]
    exposure: Decimal | None = Field(default=None, ge=Decimal("0"), le=Decimal("1"))

    @field_validator("timestamp")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value, "timestamp")

    @field_validator("exposure")
    @classmethod
    def normalize_exposure(cls, value: Decimal | None) -> Decimal | None:
        return _decimal(value, "exposure") if value is not None else None


class PerformancePoint(_StrictModel):
    total_return: Decimal | None = None
    max_drawdown: Decimal | None = Field(default=None, ge=Decimal("0"))
    annualized_volatility: Decimal | None = Field(default=None, ge=Decimal("0"))
    trade_count: int | None = Field(default=None, ge=0)
    winning_trade_count: int | None = Field(default=None, ge=0)
    benchmark_return: Decimal | None = None

    @field_validator("total_return", "max_drawdown", "annualized_volatility", "benchmark_return")
    @classmethod
    def normalize_metrics(cls, value: Decimal | None, info) -> Decimal | None:
        return _decimal(value, info.field_name) if value is not None else None

    @model_validator(mode="after")
    def validate_winning_trades(self) -> "PerformancePoint":
        if self.winning_trade_count is not None and self.trade_count is not None:
            if self.winning_trade_count > self.trade_count:
                raise ValueError("winning_trade_count cannot exceed trade_count")
        return self


class DataIssue(_StrictModel):
    divergence_type: Literal[
        DivergenceType.MARKET_DATA_MISSING_CANDLE,
        DivergenceType.MARKET_DATA_CORRECTED_CANDLE,
        DivergenceType.MARKET_DATA_TIMESTAMP_MISMATCH,
        DivergenceType.EVALUATOR_FAILURE,
    ]
    timestamp: datetime
    asset_id: int | None = Field(default=None, gt=0)
    observed: dict[str, Any] = Field(default_factory=dict)
    explanation: str = Field(min_length=1, max_length=2000)

    @field_validator("timestamp")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        return _utc(value, "timestamp")

    @field_validator("observed")
    @classmethod
    def normalize_observed(cls, value: dict[str, Any]) -> dict[str, Any]:
        return canonicalize_json(value, path="data_issue.observed")


class ComparisonInput(_StrictModel):
    strategy: StrategyVersionIdentity
    reference_strategy: StrategyVersionIdentity
    policy: ComparisonPolicy
    window_start: datetime
    window_end: datetime
    reference_source: dict[str, Any]
    observed_source: dict[str, Any]
    reference_complete: bool
    observed_complete: bool
    expected_configuration_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    observed_configuration_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    expected_signals: tuple[SignalPoint, ...] = ()
    observed_signals: tuple[SignalPoint, ...] = ()
    expected_candles: tuple[CandlePoint, ...] = ()
    observed_candles: tuple[CandlePoint, ...] = ()
    expected_executions: tuple[ExecutionPoint, ...] = ()
    observed_executions: tuple[ExecutionPoint, ...] = ()
    expected_positions: tuple[PositionPoint, ...] = ()
    observed_positions: tuple[PositionPoint, ...] = ()
    expected_performance: PerformancePoint | None = None
    observed_performance: PerformancePoint | None = None
    observed_data_issues: tuple[DataIssue, ...] = ()

    @field_validator("window_start", "window_end")
    @classmethod
    def normalize_window(cls, value: datetime, info) -> datetime:
        return _utc(value, info.field_name)

    @field_validator("reference_source", "observed_source")
    @classmethod
    def canonical_sources(cls, value: dict[str, Any], info) -> dict[str, Any]:
        return canonicalize_json(value, path=info.field_name)

    @model_validator(mode="after")
    def validate_window(self) -> "ComparisonInput":
        if self.window_end <= self.window_start:
            raise ValueError("window_end must be after window_start")
        evidence = (
            *((point, point.event_time, "signal") for point in self.expected_signals),
            *((point, point.event_time, "signal") for point in self.observed_signals),
            *((point, point.timestamp, "candle") for point in self.expected_candles),
            *((point, point.timestamp, "candle") for point in self.observed_candles),
            *((point, point.execution_time, "execution") for point in self.expected_executions),
            *((point, point.execution_time, "execution") for point in self.observed_executions),
            *((point, point.timestamp, "position") for point in self.expected_positions),
            *((point, point.timestamp, "position") for point in self.observed_positions),
            *((issue, issue.timestamp, "data issue") for issue in self.observed_data_issues),
        )
        if len(evidence) > MAX_COMPARISON_INPUT_POINTS:
            raise ValueError(
                f"comparison evidence exceeds bounded limit of {MAX_COMPARISON_INPUT_POINTS} records"
            )
        for _, timestamp, kind in evidence:
            if not self.window_start <= timestamp <= self.window_end:
                raise ValueError(f"{kind} timestamp must be contained in the comparison window")
        return self


class DivergenceFinding(_StrictModel):
    strategy: StrategyVersionIdentity
    divergence_type: DivergenceType
    severity: DivergenceSeverity
    timestamp: datetime | None
    window_start: datetime
    window_end: datetime
    expected_value: dict[str, Any]
    observed_value: dict[str, Any]
    source_reference: dict[str, Any]
    category: str = Field(min_length=1, max_length=128)
    explanation: str = Field(min_length=1, max_length=2000)
    resolution_status: ResolutionStatus = ResolutionStatus.OPEN

    @field_validator("timestamp", "window_start", "window_end")
    @classmethod
    def normalize_times(cls, value: datetime | None, info) -> datetime | None:
        return _utc(value, info.field_name) if value is not None else None

    @field_validator("expected_value", "observed_value", "source_reference")
    @classmethod
    def canonical_values(cls, value: dict[str, Any], info) -> dict[str, Any]:
        return canonicalize_json(value, path=info.field_name)

    def fingerprint(self) -> str:
        payload = self.model_dump(mode="json", exclude={"severity", "resolution_status"})
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class ComparisonResult(_StrictModel):
    status: ComparisonStatus
    findings: tuple[DivergenceFinding, ...]


def compare(input: ComparisonInput) -> ComparisonResult:
    """Return deterministic difference findings without persistence or I/O."""
    findings: list[DivergenceFinding] = []
    if not input.reference_complete or not input.observed_complete:
        missing = []
        if not input.reference_complete:
            missing.append("reference")
        if not input.observed_complete:
            missing.append("observed")
        findings.append(_finding(
            input, DivergenceType.INSUFFICIENT_REFERENCE_DATA, None,
            {"reference_complete": input.reference_complete},
            {"observed_complete": input.observed_complete},
            "coverage", f"Comparison cannot be healthy: {', '.join(missing)} coverage is incomplete.",
        ))

    if input.strategy != input.reference_strategy:
        findings.append(_finding(
            input, DivergenceType.STRATEGY_VERSION_MISMATCH, None,
            input.reference_strategy.model_dump(mode="json"), input.strategy.model_dump(mode="json"),
            "strategy_identity", "Reference and live strategy identities differ; behavior is not directly comparable.",
        ))

    if (
        input.expected_configuration_hash is not None
        and input.observed_configuration_hash is not None
        and input.expected_configuration_hash != input.observed_configuration_hash
    ):
        findings.append(_finding(
            input, DivergenceType.CONFIGURATION_MISMATCH, None,
            {"configuration_hash": input.expected_configuration_hash},
            {"configuration_hash": input.observed_configuration_hash},
            "strategy_state", "Configuration hashes differ; this detects an input mismatch, not its cause.",
        ))

    for issue in input.observed_data_issues:
        findings.append(_finding(
            input, issue.divergence_type, issue.timestamp, {}, issue.observed,
            "market_data" if issue.divergence_type is not DivergenceType.EVALUATOR_FAILURE else "strategy_state",
            issue.explanation,
        ))

    findings.extend(_compare_signals(input))
    findings.extend(_compare_candles(input))
    findings.extend(_compare_executions(input))
    findings.extend(_compare_positions(input))
    findings.extend(_compare_performance(input))

    deduped = {finding.fingerprint(): finding for finding in findings}
    ordered = tuple(sorted(
        deduped.values(),
        key=lambda finding: (finding.timestamp or input.window_start, finding.divergence_type.value, finding.fingerprint()),
    ))
    status = (
        ComparisonStatus.INSUFFICIENT_REFERENCE
        if any(f.divergence_type is DivergenceType.INSUFFICIENT_REFERENCE_DATA for f in ordered)
        else ComparisonStatus.DIVERGENT if ordered else ComparisonStatus.HEALTHY
    )
    return ComparisonResult(status=status, findings=ordered)


def _finding(
    input: ComparisonInput,
    divergence_type: DivergenceType,
    timestamp: datetime | None,
    expected: dict[str, Any],
    observed: dict[str, Any],
    category: str,
    explanation: str,
) -> DivergenceFinding:
    return DivergenceFinding(
        strategy=input.strategy,
        divergence_type=divergence_type,
        severity=input.policy.severity_for(divergence_type),
        timestamp=timestamp,
        window_start=input.window_start,
        window_end=input.window_end,
        expected_value=_json_value(expected),
        observed_value=_json_value(observed),
        source_reference={"reference": input.reference_source, "observed": input.observed_source},
        category=category,
        explanation=explanation,
    )


def _compare_signals(input: ComparisonInput) -> list[DivergenceFinding]:
    findings: list[DivergenceFinding] = []
    pairs, expected_only, observed_only = _pair_points(input.expected_signals, input.observed_signals, "event_time")
    for expected, observed in pairs:
        difference = abs((observed.event_time - expected.event_time).total_seconds())
        if difference > input.policy.signal_timing_tolerance_seconds:
            findings.append(_finding(
                input, DivergenceType.SIGNAL_TIMING_DIFFERENCE, observed.event_time,
                expected.model_dump(mode="json"), observed.model_dump(mode="json"), "signal",
                "Signal timing differs beyond the explicitly configured tolerance; no cause is inferred.",
            ))
    for expected in expected_only:
        findings.append(_finding(
            input, DivergenceType.MISSING_SIGNAL, expected.event_time,
            expected.model_dump(mode="json"), {}, "signal", "Expected strategy event has no matching live event.",
        ))
    for observed in observed_only:
        findings.append(_finding(
            input, DivergenceType.UNEXPECTED_SIGNAL, observed.event_time,
            {}, observed.model_dump(mode="json"), "signal", "Live strategy event has no matching reference event.",
        ))
    return findings


def _compare_candles(input: ComparisonInput) -> list[DivergenceFinding]:
    findings: list[DivergenceFinding] = []
    pairs, expected_only, observed_only = _pair_points(input.expected_candles, input.observed_candles, "timestamp")
    for expected, observed in pairs:
        seconds = abs((observed.timestamp - expected.timestamp).total_seconds())
        if seconds > input.policy.market_timestamp_tolerance_seconds:
            findings.append(_finding(input, DivergenceType.MARKET_DATA_TIMESTAMP_MISMATCH, observed.timestamp,
                expected.model_dump(mode="json"), observed.model_dump(mode="json"), "market_data",
                "Candle timestamps differ beyond the configured tolerance; this is a data discrepancy, not causal attribution."))
            continue
        if expected.revision is not None and observed.revision is not None and expected.revision != observed.revision:
            findings.append(_finding(input, DivergenceType.MARKET_DATA_CORRECTED_CANDLE, observed.timestamp,
                {"asset_id": expected.asset_id, "revision": expected.revision},
                {"asset_id": observed.asset_id, "revision": observed.revision}, "market_data",
                "Candle revisions differ, indicating a correction or source-version discrepancy."))
        price_difference = max(_bps_difference(getattr(expected, field), getattr(observed, field)) for field in ("open", "high", "low", "close"))
        if price_difference > input.policy.ohlc_tolerance_bps:
            findings.append(_finding(input, DivergenceType.MARKET_DATA_OHLC_DISCREPANCY, observed.timestamp,
                expected.model_dump(mode="json"), observed.model_dump(mode="json"), "market_data",
                "OHLC values differ beyond the configured tolerance; no downstream causal claim is made."))
        if abs(expected.volume - observed.volume) > input.policy.volume_tolerance:
            findings.append(_finding(input, DivergenceType.MARKET_DATA_VOLUME_DISCREPANCY, observed.timestamp,
                {"asset_id": expected.asset_id, "volume": str(expected.volume)},
                {"asset_id": observed.asset_id, "volume": str(observed.volume)}, "market_data",
                "Volume differs beyond the configured absolute tolerance."))
    for expected in expected_only:
        findings.append(_finding(input, DivergenceType.MARKET_DATA_MISSING_CANDLE, expected.timestamp,
            expected.model_dump(mode="json"), {}, "market_data", "Reference candle has no observed canonical counterpart."))
    for observed in observed_only:
        findings.append(_finding(input, DivergenceType.MARKET_DATA_TIMESTAMP_MISMATCH, observed.timestamp,
            {}, observed.model_dump(mode="json"), "market_data", "Observed candle has no reference timestamp counterpart."))
    return findings


def _compare_executions(input: ComparisonInput) -> list[DivergenceFinding]:
    findings: list[DivergenceFinding] = []
    pairs, expected_only, observed_only = _pair_points(input.expected_executions, input.observed_executions, "execution_time")
    for expected, observed in pairs:
        delay = abs((observed.execution_time - expected.execution_time).total_seconds())
        if delay > input.policy.execution_delay_tolerance_seconds:
            findings.append(_finding(input, DivergenceType.EXECUTION_DELAY, observed.execution_time,
                expected.model_dump(mode="json"), observed.model_dump(mode="json"), "execution",
                "Execution timestamps differ beyond the configured tolerance; this does not establish a cause."))
        if (
            _bps_difference(expected.price, observed.price) > input.policy.execution_price_tolerance_bps
            or (
                expected.slippage_paid is not None
                and observed.slippage_paid is not None
                and abs(expected.slippage_paid - observed.slippage_paid) > input.policy.slippage_tolerance
            )
        ):
            findings.append(_finding(input, DivergenceType.EXECUTION_SLIPPAGE, observed.execution_time,
                expected.model_dump(mode="json"), observed.model_dump(mode="json"), "execution",
                "Execution price or recorded slippage differs beyond configured tolerance; this is an observed discrepancy."))
        if (
            expected.fees_paid is not None
            and observed.fees_paid is not None
            and abs(expected.fees_paid - observed.fees_paid) > input.policy.fee_tolerance
        ):
            findings.append(_finding(input, DivergenceType.FEE_DIFFERENCE, observed.execution_time,
                {
                    "asset_id": expected.asset_id,
                    "side": expected.side,
                    "signal_id": expected.signal_id,
                    "fees_paid": str(expected.fees_paid),
                },
                {
                    "asset_id": observed.asset_id,
                    "side": observed.side,
                    "signal_id": observed.signal_id,
                    "fees_paid": str(observed.fees_paid),
                },
                "execution",
                "Recorded fees differ beyond the configured tolerance."))
    for expected in expected_only:
        findings.append(_finding(input, DivergenceType.MISSING_EXPECTED_ENTRY if expected.side == "entry" else DivergenceType.MISSING_EXPECTED_EXIT,
            expected.execution_time, expected.model_dump(mode="json"), {}, "execution", "Expected execution has no actual execution record."))
    for observed in observed_only:
        findings.append(_finding(input, DivergenceType.UNEXPECTED_ENTRY if observed.side == "entry" else DivergenceType.UNEXPECTED_EXIT,
            observed.execution_time, {}, observed.model_dump(mode="json"), "execution", "Actual execution has no expected reference record."))
    return findings


def _compare_positions(input: ComparisonInput) -> list[DivergenceFinding]:
    findings: list[DivergenceFinding] = []
    pairs, expected_only, observed_only = _pair_points(input.expected_positions, input.observed_positions, "timestamp")
    for expected, observed in pairs:
        if expected.position_state != observed.position_state:
            findings.append(_finding(input, DivergenceType.POSITION_MISMATCH, observed.timestamp,
                expected.model_dump(mode="json"), observed.model_dump(mode="json"), "position",
                "Expected and observed position states differ; this is state comparison, not causal attribution."))
        if expected.exposure is not None and observed.exposure is not None and abs(expected.exposure - observed.exposure) > input.policy.exposure_tolerance:
            findings.append(_finding(input, DivergenceType.EXPOSURE_MISMATCH, observed.timestamp,
                {"asset_id": expected.asset_id, "exposure": str(expected.exposure)},
                {"asset_id": observed.asset_id, "exposure": str(observed.exposure)}, "position",
                "Position exposure differs beyond the configured tolerance."))
    for expected in expected_only:
        findings.append(_finding(input, DivergenceType.POSITION_MISMATCH, expected.timestamp,
            expected.model_dump(mode="json"), {}, "position", "Expected position snapshot has no observed snapshot."))
    for observed in observed_only:
        if observed.position_state != "flat":
            findings.append(_finding(input, DivergenceType.UNEXPECTED_POSITION_PERSISTENCE, observed.timestamp,
                {}, observed.model_dump(mode="json"), "position", "Observed non-flat position has no expected snapshot."))
    return findings


def _compare_performance(input: ComparisonInput) -> list[DivergenceFinding]:
    expected, observed = input.expected_performance, input.observed_performance
    if expected is None and observed is None:
        return []
    if expected is None or observed is None:
        return [_finding(input, DivergenceType.INSUFFICIENT_REFERENCE_DATA, None,
            expected.model_dump(mode="json") if expected else {}, observed.model_dump(mode="json") if observed else {},
            "performance", "Performance comparison is incomplete and cannot be considered healthy.")]

    findings: list[DivergenceFinding] = []
    for field, divergence_type, tolerance in (
        ("total_return", DivergenceType.RETURN_DIFFERENCE, input.policy.return_tolerance),
        ("max_drawdown", DivergenceType.DRAWDOWN_DIFFERENCE, input.policy.drawdown_tolerance),
        ("annualized_volatility", DivergenceType.VOLATILITY_DIFFERENCE, input.policy.volatility_tolerance),
        ("benchmark_return", DivergenceType.BENCHMARK_RELATIVE_DIFFERENCE, input.policy.benchmark_relative_tolerance),
    ):
        expected_value, observed_value = getattr(expected, field), getattr(observed, field)
        if expected_value is not None and observed_value is not None and abs(expected_value - observed_value) > tolerance:
            findings.append(_finding(input, divergence_type, None, {field: str(expected_value)}, {field: str(observed_value)}, "performance",
                f"{field} differs beyond the configured tolerance; this is correlation only."))
    if expected.trade_count is not None and observed.trade_count is not None and abs(expected.trade_count - observed.trade_count) > input.policy.trade_count_tolerance:
        findings.append(_finding(input, DivergenceType.TRADE_FREQUENCY_DIFFERENCE, None,
            {"trade_count": expected.trade_count}, {"trade_count": observed.trade_count}, "performance",
            "Trade count differs beyond the configured tolerance."))
    if all(value is not None for value in (expected.trade_count, observed.trade_count, expected.winning_trade_count, observed.winning_trade_count)) and expected.trade_count and observed.trade_count:
        expected_rate = Decimal(expected.winning_trade_count) / Decimal(expected.trade_count)
        observed_rate = Decimal(observed.winning_trade_count) / Decimal(observed.trade_count)
        if abs(expected_rate - observed_rate) > input.policy.win_rate_tolerance:
            findings.append(_finding(input, DivergenceType.WIN_RATE_DIFFERENCE, None,
                {"win_rate": str(expected_rate)}, {"win_rate": str(observed_rate)}, "performance",
                "Win rate differs beyond the configured tolerance."))
    return findings


def _pair_points(expected: tuple[Any, ...], observed: tuple[Any, ...], time_field: str):
    """Pair same-kind records in chronological order with O(n) auxiliary work.

    A previous implementation scanned every remaining observed point for each
    expected point.  A large same-asset window therefore had quadratic CPU and
    allocation behaviour.  Strategy events are temporal streams, so monotonic
    pairing per stable identity is both deterministic and preserves causal
    ordering; timing tolerance is evaluated by the caller after pairing.
    """
    observed_by_key: dict[tuple[Any, ...], deque[Any]] = defaultdict(deque)
    for point in sorted(observed, key=lambda item: (getattr(item, time_field), _point_key(item))):
        observed_by_key[_point_key(point)].append(point)
    pairs: list[tuple[Any, Any]] = []
    expected_only: list[Any] = []
    for point in sorted(expected, key=lambda item: (getattr(item, time_field), _point_key(item))):
        candidates = observed_by_key[_point_key(point)]
        if not candidates:
            expected_only.append(point)
            continue
        pairs.append((point, candidates.popleft()))
    unmatched = [point for candidates in observed_by_key.values() for point in candidates]
    unmatched.sort(key=lambda point: (getattr(point, time_field), _point_key(point)))
    return pairs, expected_only, unmatched


def _point_key(point: Any) -> tuple[Any, ...]:
    if isinstance(point, SignalPoint):
        return point.asset_id, point.event_type, point.signal_id
    if isinstance(point, CandlePoint):
        return (point.asset_id,)
    if isinstance(point, ExecutionPoint):
        return point.asset_id, point.side, point.direction, point.signal_id
    if isinstance(point, PositionPoint):
        return (point.asset_id,)
    raise TypeError(f"unsupported comparison point {type(point).__name__}")


def _bps_difference(expected: Decimal, observed: Decimal) -> Decimal:
    if expected == 0:
        return Decimal("Infinity") if observed != 0 else Decimal("0")
    return abs(observed - expected) / abs(expected) * Decimal("10000")


def _json_value(value: Any) -> dict[str, Any]:
    normalized = _normalize_json_scalars(value)
    if not isinstance(normalized, dict):
        raise ValueError("forensic values must be object-shaped")
    return canonicalize_json(normalized, path="comparison")


def _normalize_json_scalars(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, BaseModel):
        return _normalize_json_scalars(value.model_dump(mode="python"))
    if isinstance(value, dict):
        return {str(key): _normalize_json_scalars(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_normalize_json_scalars(item) for item in value]
    return value
