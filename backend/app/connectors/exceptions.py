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

class WebSocketConnectorError(ConnectorError):
    """Base exception for all WebSocket-specific connector errors."""
    pass

class WebSocketClosedError(WebSocketConnectorError):
    """Raised when a WebSocket connection closes unexpectedly."""
    pass

class WebSocketReconnectError(WebSocketConnectorError):
    """Raised when reconnection attempts are exhausted or fail critically."""
    pass

class SubscriptionError(WebSocketConnectorError):
    """Raised when exchange subscription request fails or is rejected."""
    pass

class MalformedMessageError(ConnectorError):
    """Raised when an incoming WebSocket frame has invalid JSON, missing fields, or bad types."""
    pass

class UnknownMessageTypeError(ConnectorError):
    """Raised when a message type is not recognized (e.g., system notice, ping response)."""
    pass

class InvalidSymbolError(ConnectorError):
    """Raised when a symbol provided for subscription is malformed or invalid."""
    pass
