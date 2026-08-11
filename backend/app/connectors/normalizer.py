"""
Binance WebSocket Message Parser & Kline Normalizer.

Parses raw WebSocket messages from Binance Spot WebSocket streams (single or combined)
and transforms them into strongly-typed `CandleEvent` domain models.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, Optional, Tuple, Union

from app.connectors.exceptions import (
    InvalidSymbolError,
    MalformedMessageError,
    UnknownMessageTypeError,
)
from app.connectors.models import CandleEvent
from app.services.ws_sharding.assignment import normalize_symbol

logger = logging.getLogger(__name__)


def parse_binance_kline_message(raw_msg: Union[str, bytes, Dict[str, Any]]) -> CandleEvent:
    """
    Parses and normalizes a raw Binance WebSocket kline message.

    Supports both:
    1. Combined stream wrapper format:
       {"stream": "btcusdt@kline_1m", "data": {"e": "kline", "E": ..., "s": "BTCUSDT", "k": {...}}}
    2. Direct single stream format:
       {"e": "kline", "E": ..., "s": "BTCUSDT", "k": {...}}

    Args:
        raw_msg: Raw JSON string, bytes, or parsed dictionary.

    Returns:
        CandleEvent: Normalized candle event.

    Raises:
        MalformedMessageError: If JSON decoding fails, required keys are missing, or numeric values are invalid.
        UnknownMessageTypeError: If message is not a kline event (e.g. subscription ACK, system ping).
        InvalidSymbolError: If the asset symbol is invalid or missing.
    """
    # 1. Decode JSON if string or bytes
    if isinstance(raw_msg, (str, bytes)):
        try:
            payload = json.loads(raw_msg)
        except Exception as e:
            raise MalformedMessageError(f"Failed to decode WebSocket JSON message: {e}") from e
    elif isinstance(raw_msg, dict):
        payload = raw_msg
    else:
        raise MalformedMessageError(f"Unsupported message type: {type(raw_msg).__name__}")

    if not isinstance(payload, dict):
        raise MalformedMessageError(f"Expected JSON object payload, got {type(payload).__name__}")

    # 2. Handle Binance subscription response / system messages
    if "result" in payload and "id" in payload:
        # Standard subscription acknowledgement: {"result": null, "id": 1}
        raise UnknownMessageTypeError(f"Received subscription acknowledgment with ID {payload.get('id')}")

    if "error" in payload:
        raise MalformedMessageError(f"Exchange returned error payload: {payload['error']}")

    # 3. Unwrap combined stream wrapper if present
    data = payload.get("data", payload) if "data" in payload and "stream" in payload else payload

    # 4. Validate event type
    event_type = data.get("e")
    if event_type != "kline":
        raise UnknownMessageTypeError(f"Message is not a kline event (type: '{event_type}')")

    # 5. Extract kline inner payload
    k = data.get("k")
    if not isinstance(k, dict):
        raise MalformedMessageError("Missing or invalid 'k' (kline) object in payload")

    # 6. Extract and normalize symbol
    raw_symbol = data.get("s") or k.get("s")
    if not raw_symbol:
        raise InvalidSymbolError("Missing symbol 's' in kline payload")
    try:
        symbol = normalize_symbol(raw_symbol)
    except Exception as e:
        raise InvalidSymbolError(f"Invalid symbol '{raw_symbol}': {e}") from e

    # 7. Extract timestamps
    try:
        start_ms = int(k["t"])
        close_ms = int(k["T"])
        timestamp = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
        close_time = datetime.fromtimestamp(close_ms / 1000.0, tz=timezone.utc)
    except KeyError as e:
        raise MalformedMessageError(f"Missing required timestamp field in kline: {e}") from e
    except (ValueError, TypeError, OSError) as e:
        raise MalformedMessageError(f"Invalid timestamp value in kline: {e}") from e

    event_time = None
    if "E" in data and data["E"] is not None:
        try:
            event_time = datetime.fromtimestamp(int(data["E"]) / 1000.0, tz=timezone.utc)
        except Exception:
            event_time = None

    # 8. Extract numeric OHLCV values
    try:
        open_price = float(k["o"])
        high_price = float(k["h"])
        low_price = float(k["l"])
        close_price = float(k["c"])
        volume = float(k["v"])
    except KeyError as e:
        raise MalformedMessageError(f"Missing required OHLCV field in kline: {e}") from e
    except (ValueError, TypeError) as e:
        raise MalformedMessageError(f"Invalid numeric value for OHLCV in kline: {e}") from e

    # Optional extended fields
    quote_volume = None
    if "q" in k and k["q"] is not None:
        try:
            quote_volume = float(k["q"])
        except (ValueError, TypeError):
            pass

    trade_count = None
    if "n" in k and k["n"] is not None:
        try:
            trade_count = int(k["n"])
        except (ValueError, TypeError):
            pass

    taker_buy_base = None
    if "V" in k and k["V"] is not None:
        try:
            taker_buy_base = float(k["V"])
        except (ValueError, TypeError):
            pass

    taker_buy_quote = None
    if "Q" in k and k["Q"] is not None:
        try:
            taker_buy_quote = float(k["Q"])
        except (ValueError, TypeError):
            pass

    # 9. Extract finality flag
    if "x" not in k:
        raise MalformedMessageError("Missing required 'x' (is_closed) boolean field in kline")
    is_closed = bool(k["x"])

    interval = str(k.get("i", "1m"))

    # 10. Instantiate validated CandleEvent
    try:
        return CandleEvent(
            symbol=symbol,
            interval=interval,
            timestamp=timestamp,
            close_time=close_time,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            quote_volume=quote_volume,
            trade_count=trade_count,
            taker_buy_base_volume=taker_buy_base,
            taker_buy_quote_volume=taker_buy_quote,
            is_closed=is_closed,
            source="binance_ws",
            event_time=event_time,
        )
    except Exception as e:
        raise MalformedMessageError(f"Candle validation invariant failed: {e}") from e


def parse_binance_kline_safe(raw_msg: Union[str, bytes, Dict[str, Any]]) -> Tuple[Optional[CandleEvent], Optional[Exception]]:
    """
    Safely parses a Binance kline message without raising exceptions.

    Returns:
        Tuple[Optional[CandleEvent], Optional[Exception]]:
            - (CandleEvent, None) on success
            - (None, Exception) on parsing/validation error
            - (None, None) if message is a normal non-kline control/ack message
    """
    try:
        event = parse_binance_kline_message(raw_msg)
        return event, None
    except UnknownMessageTypeError:
        # Non-kline control frame (e.g. subscription ack)
        return None, None
    except Exception as e:
        logger.debug(f"Failed to parse WebSocket message: {e}")
        return None, e
