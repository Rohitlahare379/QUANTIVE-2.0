"""Canonical contracts for reproducible, persisted backtest results."""

from .contracts import (
    BacktestDataReference,
    BacktestMetrics,
    BacktestResultInput,
    BacktestSignal,
    BacktestTrade,
    ReproducibilityMetadata,
)

__all__ = [
    "BacktestDataReference",
    "BacktestMetrics",
    "BacktestResultInput",
    "BacktestSignal",
    "BacktestTrade",
    "ReproducibilityMetadata",
]
