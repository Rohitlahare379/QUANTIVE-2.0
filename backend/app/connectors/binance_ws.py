"""
Binance WebSocket Connector & Stream Client.

Handles WebSocket connection lifecycle, dynamic kline subscriptions, ping/pong heartbeats,
reconnection with exponential backoff & jitter, 24-hour planned connection rotation,
and streaming of normalized finalized candle events.
"""

import asyncio
from datetime import datetime, timezone
import enum
import json
import logging
import random
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set, Union
import websockets
from websockets.exceptions import ConnectionClosed

from app.connectors.exceptions import (
    InvalidSymbolError,
    MalformedMessageError,
    SubscriptionError,
    WebSocketClosedError,
    WebSocketConnectorError,
)
from app.connectors.models import CandleEvent
from app.connectors.normalizer import parse_binance_kline_safe
from app.core.config import settings
from app.services.ws_sharding.assignment import normalize_symbol

logger = logging.getLogger(__name__)


class WebSocketConnectionState(str, enum.Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    SUBSCRIBING = "SUBSCRIBING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


def build_kline_stream_name(symbol: str, interval: str = "1m") -> str:
    """
    Constructs a Binance kline stream name from a symbol and interval.
    Example: 'BTCUSDT', '1m' -> 'btcusdt@kline_1m'
    """
    clean_symbol = normalize_symbol(symbol).lower()
    clean_interval = interval.strip().lower()
    return f"{clean_symbol}@kline_{clean_interval}"


def build_subscription_payload(streams: List[str], req_id: int = 1) -> str:
    """
    Builds the JSON payload for subscribing to combined streams on Binance.
    """
    if not streams:
        raise ValueError("Streams list cannot be empty for subscription")
    
    payload = {
        "method": "SUBSCRIBE",
        "params": sorted(list(set(streams))),
        "id": req_id,
    }
    return json.dumps(payload)


def calculate_reconnect_backoff(
    attempt: int,
    initial_delay: float = settings.BINANCE_WS_RECONNECT_INITIAL_DELAY_SECONDS,
    max_delay: float = settings.BINANCE_WS_RECONNECT_MAX_DELAY_SECONDS,
    backoff_factor: float = settings.BINANCE_WS_RECONNECT_BACKOFF_FACTOR,
    jitter_ratio: float = settings.BINANCE_WS_RECONNECT_JITTER_RATIO,
) -> float:
    """
    Computes exponential backoff delay with uniform jitter.
    Formula: min(max_delay, initial_delay * (backoff_factor ** attempt)) * (1 +/- jitter)
    """
    base_delay = min(max_delay, initial_delay * (backoff_factor ** attempt))
    jitter = base_delay * jitter_ratio * (random.random() * 2 - 1)  # [-jitter_ratio, +jitter_ratio]
    return max(0.01, base_delay + jitter)


class BinanceWebSocketClient:
    """
    Production-grade WebSocket client for streaming Binance 1-minute klines.
    """

    def __init__(
        self,
        symbols: List[str],
        interval: str = "1m",
        ws_base_url: Optional[str] = None,
        max_connection_lifetime_seconds: Optional[float] = None,
        ping_interval_seconds: Optional[float] = None,
        ping_timeout_seconds: Optional[float] = None,
        on_connection_opened: Optional[Callable[[], Any]] = None,
        on_connection_closed: Optional[Callable[[Optional[int], Optional[str]], Any]] = None,
        on_reconnect: Optional[Callable[[int, float], Any]] = None,
        on_subscription_success: Optional[Callable[[List[str]], Any]] = None,
        on_subscription_failure: Optional[Callable[[str], Any]] = None,
        on_message_received: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_final_candle: Optional[Callable[[CandleEvent], Any]] = None,
        on_parser_error: Optional[Callable[[Any, Exception], Any]] = None,
        on_protocol_error: Optional[Callable[[Exception], Any]] = None,
    ):
        if not symbols:
            raise InvalidSymbolError("BinanceWebSocketClient requires at least one symbol")

        self.symbols = [normalize_symbol(s) for s in symbols]
        self.interval = interval.strip().lower()
        self.ws_base_url = (ws_base_url or settings.BINANCE_WS_BASE_URL).rstrip("/")
        self.max_connection_lifetime = (
            max_connection_lifetime_seconds or settings.BINANCE_WS_MAX_CONNECTION_LIFETIME_SECONDS
        )
        self.ping_interval = ping_interval_seconds or settings.BINANCE_WS_PING_INTERVAL_SECONDS
        self.ping_timeout = ping_timeout_seconds or settings.BINANCE_WS_PING_TIMEOUT_SECONDS

        # Observability Hooks
        self.on_connection_opened = on_connection_opened
        self.on_connection_closed = on_connection_closed
        self.on_reconnect = on_reconnect
        self.on_subscription_success = on_subscription_success
        self.on_subscription_failure = on_subscription_failure
        self.on_message_received = on_message_received
        self.on_final_candle = on_final_candle
        self.on_parser_error = on_parser_error
        self.on_protocol_error = on_protocol_error

        # Stream Names
        self.stream_names = [build_kline_stream_name(s, self.interval) for s in self.symbols]

        # State tracking
        self._state = WebSocketConnectionState.DISCONNECTED
        self._websocket: Optional[websockets.WebSocketClientProtocol] = None
        self._stop_requested = False
        self._connected_at: Optional[datetime] = None
        self._reconnect_attempts = 0
        self._active_req_id = 1
        self._lock = asyncio.Lock()

    @property
    def state(self) -> WebSocketConnectionState:
        return self._state

    @property
    def is_running(self) -> bool:
        return self._state in (WebSocketConnectionState.CONNECTED, WebSocketConnectionState.SUBSCRIBING, WebSocketConnectionState.RUNNING)

    @property
    def is_connected(self) -> bool:
        if self._websocket is None:
            return False
        # Supports both legacy websockets (.closed) and modern websockets (.state)
        if hasattr(self._websocket, "closed"):
            return not self._websocket.closed
        if hasattr(self._websocket, "state"):
            return getattr(self._websocket.state, "name", "") == "OPEN" or self._websocket.state == 1
        return True

    def _get_stream_url(self) -> str:
        """
        Constructs the combined stream WebSocket URL.
        """
        return f"{self.ws_base_url}/stream"

    async def connect(self) -> Any:
        """
        Opens a WebSocket connection and issues subscription for all assigned streams.
        """
        self._state = WebSocketConnectionState.CONNECTING
        url = self._get_stream_url()
        logger.info(f"Connecting to Binance WebSocket endpoint: {url}")

        try:
            ws = await websockets.connect(
                url,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                close_timeout=5.0,
                max_size=2**20,  # 1MB max frame
            )
            self._websocket = ws
            self._state = WebSocketConnectionState.CONNECTED
            self._connected_at = datetime.now(timezone.utc)
            self._reconnect_attempts = 0
            logger.info(f"Successfully connected to Binance WebSocket at {url}")

            if self.on_connection_opened:
                try:
                    self.on_connection_opened()
                except Exception as e:
                    logger.debug(f"Error in on_connection_opened hook: {e}")

            # Send subscription payload
            await self._subscribe(ws)

            self._state = WebSocketConnectionState.RUNNING
            return ws

        except Exception as e:
            self._state = WebSocketConnectionState.FAILED
            logger.error(f"Failed to connect to Binance WebSocket: {e}")
            if self.on_protocol_error:
                try:
                    self.on_protocol_error(e)
                except Exception:
                    pass
            raise

    async def _subscribe(self, ws: Any) -> None:
        """
        Sends the subscription payload over the active WebSocket.
        """
        self._state = WebSocketConnectionState.SUBSCRIBING
        self._active_req_id += 1
        payload = build_subscription_payload(self.stream_names, req_id=self._active_req_id)
        logger.info(f"Subscribing to {len(self.stream_names)} Binance kline streams")
        
        try:
            await ws.send(payload)
            if self.on_subscription_success:
                try:
                    self.on_subscription_success(self.stream_names)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed to send subscription payload: {e}")
            if self.on_subscription_failure:
                try:
                    self.on_subscription_failure(str(e))
                except Exception:
                    pass
            raise SubscriptionError(f"Subscription failed: {e}") from e

    async def disconnect(self) -> None:
        """
        Gracefully closes the WebSocket connection without triggering reconnection.
        """
        self._stop_requested = True
        self._state = WebSocketConnectionState.STOPPING

        if self._websocket is not None:
            try:
                await self._websocket.close()
            except Exception as e:
                logger.debug(f"Exception during WebSocket close: {e}")
            finally:
                self._websocket = None

        self._state = WebSocketConnectionState.STOPPED
        logger.info("Binance WebSocket client disconnected cleanly.")

        if self.on_connection_closed:
            try:
                self.on_connection_closed(1000, "Clean shutdown")
            except Exception:
                pass

    def _should_rotate_connection(self) -> bool:
        """
        Determines whether the connection age has exceeded the maximum lifetime limit.
        """
        if not self._connected_at:
            return False
        elapsed = (datetime.now(timezone.utc) - self._connected_at).total_seconds()
        return elapsed >= self.max_connection_lifetime

    async def _rotate_connection(self) -> Any:
        """
        Executes controlled 24-hour connection rotation:
        1. Opens replacement connection.
        2. Subscribes replacement.
        3. Closes old connection.
        """
        logger.info("Initiating planned 24-hour WebSocket connection rotation...")
        old_ws = self._websocket

        # Connect replacement
        new_ws = await websockets.connect(
            self._get_stream_url(),
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            close_timeout=5.0,
        )
        await self._subscribe(new_ws)

        # Cut over
        self._websocket = new_ws
        self._connected_at = datetime.now(timezone.utc)
        self._state = WebSocketConnectionState.RUNNING

        # Close old connection gracefully
        if old_ws:
            try:
                await old_ws.close()
            except Exception:
                pass

        logger.info("Completed planned 24-hour WebSocket connection rotation.")
        return new_ws

    async def stream_all_candles(self) -> AsyncIterator[CandleEvent]:
        """
        Async generator that yields ALL candle updates (both partial ticks and finalized candles).
        Handles automatic reconnection and rotation.
        """
        self._stop_requested = False

        while not self._stop_requested:
            try:
                if not self.is_connected:
                    await self.connect()

                ws = self._websocket
                if ws is None:
                    continue

                while not self._stop_requested and self.is_connected:
                    # Check for 24-hour connection rotation threshold
                    if self._should_rotate_connection():
                        ws = await self._rotate_connection()

                    raw_frame = await ws.recv()
                    if self._stop_requested:
                        break

                    # Parse message
                    candle, error = parse_binance_kline_safe(raw_frame)
                    
                    if error:
                        if self.on_parser_error:
                            try:
                                self.on_parser_error(raw_frame, error)
                            except Exception:
                                pass
                        continue

                    if candle is None:
                        # Non-kline control/system frame
                        continue

                    if self.on_message_received:
                        try:
                            self.on_message_received(candle.to_dict())
                        except Exception:
                            pass

                    if candle.is_closed and self.on_final_candle:
                        try:
                            self.on_final_candle(candle)
                        except Exception:
                            pass

                    yield candle

            except (ConnectionClosed, WebSocketClosedError, OSError, asyncio.TimeoutError) as e:
                if self._stop_requested:
                    break

                close_code = getattr(e, "code", None)
                close_reason = getattr(e, "reason", str(e))
                logger.warning(f"Binance WebSocket connection dropped ({close_code}): {close_reason}")

                if self.on_connection_closed:
                    try:
                        self.on_connection_closed(close_code, close_reason)
                    except Exception:
                        pass

                self._websocket = None
                self._state = WebSocketConnectionState.DISCONNECTED

                # Reconnect with exponential backoff and jitter
                self._reconnect_attempts += 1
                delay = calculate_reconnect_backoff(self._reconnect_attempts)
                logger.info(f"Reconnecting Binance WebSocket in {delay:.2f}s (attempt #{self._reconnect_attempts})...")

                if self.on_reconnect:
                    try:
                        self.on_reconnect(self._reconnect_attempts, delay)
                    except Exception:
                        pass

                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break

            except Exception as e:
                if self._stop_requested:
                    break
                logger.error(f"Unexpected error in Binance WebSocket stream: {e}")
                if self.on_protocol_error:
                    try:
                        self.on_protocol_error(e)
                    except Exception:
                        pass
                
                self._websocket = None
                self._state = WebSocketConnectionState.FAILED
                
                self._reconnect_attempts += 1
                delay = calculate_reconnect_backoff(self._reconnect_attempts)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break

    async def stream_final_candles(self) -> AsyncIterator[CandleEvent]:
        """
        Async generator that yields ONLY finalized 1-minute candles (`is_closed == True`).
        Non-final tick updates are filtered out in streaming mode without memory accumulation.
        """
        async for candle in self.stream_all_candles():
            if candle.is_closed:
                yield candle
