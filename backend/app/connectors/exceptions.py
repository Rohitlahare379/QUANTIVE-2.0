class ConnectorError(Exception):
    """Base exception for all connector errors."""
    pass

class NetworkError(ConnectorError):
    """Raised when a network-level error occurs (timeouts, connection drops)."""
    pass

class APIError(ConnectorError):
    """Raised when the API returns a non-200 response that isn't a rate limit."""
    pass

class RateLimitError(ConnectorError):
    """Raised when the API returns an HTTP 429 Too Many Requests."""
    def __init__(self, message: str, retry_after: int = None):
        super().__init__(message)
        self.retry_after = retry_after

class TemporaryBanError(ConnectorError):
    """Raised when the API returns an HTTP 418 I'm a teapot (IP banned)."""
    def __init__(self, message: str, retry_after: int = None):
        super().__init__(message)
        self.retry_after = retry_after

class PayloadCorruptionError(ConnectorError):
    """Raised when the exchange payload is mathematically corrupted (e.g., out of order timestamps)."""
    pass
