"""
Strongly Typed Normalized Candle Event Models.

Represents standardized market data candle events produced by WebSocket feeds.
Pure domain/transport model with zero database dependencies.
"""

from datetime import datetime, timezone
import json
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class CandleEvent(BaseModel):
    """
    Standardized, strongly typed representation of an OHLCV candle event.
    """
    symbol: str = Field(..., description="Normalized uppercase asset ticker symbol, e.g. BTCUSDT")
    interval: str = Field(default="1m", description="Candle timeframe interval, e.g. 1m")
    timestamp: datetime = Field(..., description="UTC start timestamp of the candle window")
    close_time: datetime = Field(..., description="UTC close timestamp of the candle window")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Close price")
    volume: float = Field(..., description="Base asset traded volume")
    quote_volume: Optional[float] = Field(default=None, description="Quote asset traded volume")
    trade_count: Optional[int] = Field(default=None, description="Total number of trades in candle")
    taker_buy_base_volume: Optional[float] = Field(default=None, description="Taker buy base asset volume")
    taker_buy_quote_volume: Optional[float] = Field(default=None, description="Taker buy quote asset volume")
    is_closed: bool = Field(..., description="True if candle interval has completed (authoritative)")
    source: str = Field(default="binance_ws", description="Source connector identifier")
    event_time: Optional[datetime] = Field(default=None, description="Exchange event emission timestamp")
    received_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Local UTC timestamp when message was received by connector"
    )

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError("Symbol must be a non-empty string")
        return v.strip().upper()

    @field_validator("open", "high", "low", "close", "volume")
    @classmethod
    def validate_positive_numbers(cls, v: float) -> float:
        if v is None or not isinstance(v, (int, float)):
            raise ValueError("Price and volume fields must be numeric")
        if v < 0:
            raise ValueError("Price and volume cannot be negative")
        return float(v)

    @model_validator(mode="after")
    def validate_ohlc_invariants(self) -> "CandleEvent":
        # Allow tiny float precision margin
        eps = 1e-9
        if self.high < (self.low - eps):
            raise ValueError(f"High price ({self.high}) cannot be less than low price ({self.low})")
        if self.open < (self.low - eps) or self.open > (self.high + eps):
            raise ValueError(f"Open price ({self.open}) must be within [low ({self.low}), high ({self.high})]")
        if self.close < (self.low - eps) or self.close > (self.high + eps):
            raise ValueError(f"Close price ({self.close}) must be within [low ({self.low}), high ({self.high})]")
        if self.close_time < self.timestamp:
            raise ValueError(f"Close time ({self.close_time}) cannot precede start timestamp ({self.timestamp})")
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Converts CandleEvent to a serializable dictionary."""
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "timestamp": self.timestamp.isoformat(),
            "close_time": self.close_time.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
            "taker_buy_base_volume": self.taker_buy_base_volume,
            "taker_buy_quote_volume": self.taker_buy_quote_volume,
            "is_closed": self.is_closed,
            "source": self.source,
            "event_time": self.event_time.isoformat() if self.event_time else None,
            "received_at": self.received_at.isoformat(),
        }

    def to_json(self) -> str:
        """Serializes CandleEvent to a JSON string."""
        return json.dumps(self.to_dict())
