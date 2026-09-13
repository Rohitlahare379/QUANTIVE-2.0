class QueryError(Exception):
    """Base exception for all query errors."""
    pass

class AssetNotFoundError(QueryError):
    """Raised when an requested asset cannot be found."""
    pass

class UnsupportedTimeframeError(QueryError):
    """Raised when an unsupported timeframe string is requested."""
    pass

class InvalidDateRangeError(QueryError):
    """Raised when start_time >= end_time."""
    pass


class StrategyCatalogError(Exception):
    """Base error for immutable strategy catalog operations."""
    pass


class StrategyNotFoundError(StrategyCatalogError):
    """Raised when a logical strategy or one of its versions is absent."""
    pass


class StrategyDefinitionError(StrategyCatalogError):
    """Raised when a definition cannot be resolved or its stored hash is invalid."""
    pass


class BacktestResultError(Exception):
    """Base error for validated, immutable backtest-result persistence."""
    pass


class BacktestResultConflictError(BacktestResultError):
    """Raised when the same deterministic run inputs produce a different result."""
    pass


class LiveStrategyError(Exception):
    """Base error for bounded live strategy activation and evaluation."""
    pass


class LiveStrategyActivationError(LiveStrategyError):
    """Raised when a strategy version cannot safely be activated live."""
    pass


class StrategyComparisonError(Exception):
    """Base error for deterministic strategy-reference comparison."""
    pass


class StrategyMonitoringError(Exception):
    """Base error for durable strategy-divergence monitoring."""
    pass


class StrategyMonitoringRegistrationError(StrategyMonitoringError):
    """Raised when an activation cannot be safely enrolled for monitoring."""
    pass


class StrategyIncidentNotFoundError(StrategyMonitoringError):
    """Raised when an incident lifecycle operation targets no known incident."""
    pass
