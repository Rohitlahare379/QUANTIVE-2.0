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
