"""
Connectors Package for External Market Data Providers.
"""

from app.connectors.binance import BinanceClient
from app.connectors.binance_ws import (
    BinanceWebSocketClient,
    WebSocketConnectionState,
    build_kline_stream_name,
    build_subscription_payload,
    calculate_reconnect_backoff,
)
from app.connectors.exceptions import (
    APIError,
    ConnectorError,
    InvalidSymbolError,
    MalformedMessageError,
    NetworkError,
    PayloadCorruptionError,
    RateLimitError,
    SubscriptionError,
    TemporaryBanError,
    UnknownMessageTypeError,
    WebSocketClosedError,
    WebSocketConnectorError,
    WebSocketReconnectError,
)
from app.connectors.models import CandleEvent
from app.connectors.normalizer import (
    parse_binance_kline_message,
    parse_binance_kline_safe,
)
from app.connectors.rate_limiter import GlobalRateLimiter

__all__ = [
    "BinanceClient",
    "BinanceWebSocketClient",
    "WebSocketConnectionState",
    "build_kline_stream_name",
    "build_subscription_payload",
    "calculate_reconnect_backoff",
    "CandleEvent",
    "parse_binance_kline_message",
    "parse_binance_kline_safe",
    "GlobalRateLimiter",
    "ConnectorError",
    "NetworkError",
    "APIError",
    "RateLimitError",
    "TemporaryBanError",
    "PayloadCorruptionError",
    "WebSocketConnectorError",
    "WebSocketClosedError",
    "WebSocketReconnectError",
    "SubscriptionError",
    "MalformedMessageError",
    "UnknownMessageTypeError",
    "InvalidSymbolError",
]
