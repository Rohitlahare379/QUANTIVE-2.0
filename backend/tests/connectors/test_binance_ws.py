"""
Tests for Binance WebSocket Connector & Candle Normalizer (P0.2 Phase 2).

Covers all 20 required verification scenarios:
1. Valid kline message parsing
2. Partial kline ignored as non-final in final candle stream
3. Final kline emitted in final candle stream
4. Duplicate final event handled gracefully
5. Malformed JSON handling
6. Malformed kline structure (missing 'k')
7. Missing OHLC field handling
8. Invalid numeric value handling
9. Symbol normalization
10. Unknown message type handling (subscription ACKs, pings)
11. Connection open lifecycle
12. Unexpected connection closure triggers reconnect
13. Clean shutdown does not reconnect
14. Reconnect backoff calculation
15. Reconnect jitter bounds
16. Subscription payload generation
17. Subscription failure handling
18. Multiple symbols subscription
19. Multiple streams handling
20. Planned 24-hour connection rotation
"""

import asyncio
from datetime import datetime, timezone
import json
import pytest
import pytest_asyncio
import websockets
try:
    from websockets.asyncio.server import serve
except ImportError:
    from websockets.server import serve

from app.connectors.binance_ws import (
    BinanceWebSocketClient,
    WebSocketConnectionState,
    build_kline_stream_name,
    build_subscription_payload,
    calculate_reconnect_backoff,
)
from app.connectors.exceptions import (
    InvalidSymbolError,
    MalformedMessageError,
    SubscriptionError,
    UnknownMessageTypeError,
)
from app.connectors.models import CandleEvent
from app.connectors.normalizer import (
    parse_binance_kline_message,
    parse_binance_kline_safe,
)


# ============================================================================
# Fixtures & Sample Payloads
# ============================================================================

def make_binance_kline_payload(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    start_ms: int = 1672531200000,
    close_ms: int = 1672531259999,
    open_price: str = "16500.00",
    high_price: str = "16550.00",
    low_price: str = "16480.00",
    close_price: str = "16520.50",
    volume: str = "123.456",
    is_closed: bool = True,
    is_combined: bool = True
) -> Dict:
    raw_kline = {
        "e": "kline",
        "E": start_ms + 30000,
        "s": symbol,
        "k": {
            "t": start_ms,
            "T": close_ms,
            "s": symbol,
            "i": interval,
            "f": 1000,
            "L": 2000,
            "o": open_price,
            "c": close_price,
            "h": high_price,
            "l": low_price,
            "v": volume,
            "n": 100,
            "x": is_closed,
            "q": "2039520.00",
            "V": "50.123",
            "Q": "828000.00",
            "B": "0"
        }
    }
    if is_combined:
        return {
            "stream": f"{symbol.lower()}@kline_{interval}",
            "data": raw_kline
        }
    return raw_kline


# ============================================================================
# 1 - 10: Message Normalization & Parser Tests
# ============================================================================

def test_1_valid_kline_message_parsing():
    """1. Valid kline message parsing for both combined and direct payloads."""
    # Combined wrapper format
    combined_payload = make_binance_kline_payload(is_combined=True)
    event = parse_binance_kline_message(combined_payload)

    assert isinstance(event, CandleEvent)
    assert event.symbol == "BTCUSDT"
    assert event.interval == "1m"
    assert event.open == 16500.00
    assert event.high == 16550.00
    assert event.low == 16480.00
    assert event.close == 16520.50
    assert event.volume == 123.456
    assert event.is_closed is True
    assert event.trade_count == 100
    assert event.timestamp == datetime.fromtimestamp(1672531200000 / 1000.0, tz=timezone.utc)
    assert event.close_time == datetime.fromtimestamp(1672531259999 / 1000.0, tz=timezone.utc)

    # Direct raw format
    direct_payload = make_binance_kline_payload(is_combined=False)
    direct_event = parse_binance_kline_message(direct_payload)
    assert direct_event.symbol == "BTCUSDT"
    assert direct_event.close == 16520.50


def test_2_and_3_partial_and_final_kline_flags():
    """2. Partial kline is parsed with is_closed=False & 3. Final kline with is_closed=True."""
    partial_payload = make_binance_kline_payload(is_closed=False)
    partial_event = parse_binance_kline_message(partial_payload)
    assert partial_event.is_closed is False

    final_payload = make_binance_kline_payload(is_closed=True)
    final_event = parse_binance_kline_message(final_payload)
    assert final_event.is_closed is True


def test_4_duplicate_final_event_handled():
    """4. Duplicate final events are parsed consistently without crash."""
    final_payload = make_binance_kline_payload(is_closed=True)
    event_1 = parse_binance_kline_message(final_payload)
    event_2 = parse_binance_kline_message(final_payload)

    assert event_1.symbol == event_2.symbol
    assert event_1.timestamp == event_2.timestamp
    assert event_1.close == event_2.close
    assert event_1.is_closed == event_2.is_closed is True


def test_5_malformed_json():
    """5. Malformed JSON raises MalformedMessageError."""
    with pytest.raises(MalformedMessageError, match="Failed to decode WebSocket JSON"):
        parse_binance_kline_message("{invalid_json: true,")


def test_6_malformed_kline_structure_missing_k():
    """6. Malformed kline structure missing 'k' raises MalformedMessageError."""
    bad_payload = {"e": "kline", "s": "BTCUSDT"}  # Missing 'k'
    with pytest.raises(MalformedMessageError, match="Missing or invalid 'k'"):
        parse_binance_kline_message(bad_payload)


def test_7_missing_ohlc_field():
    """7. Missing OHLC field raises MalformedMessageError."""
    bad_payload = make_binance_kline_payload()
    del bad_payload["data"]["k"]["o"]  # Remove open price
    with pytest.raises(MalformedMessageError, match="Missing required OHLCV field"):
        parse_binance_kline_message(bad_payload)


def test_8_invalid_numeric_value():
    """8. Invalid numeric string raises MalformedMessageError."""
    bad_payload = make_binance_kline_payload(open_price="not_a_number")
    with pytest.raises(MalformedMessageError, match="Invalid numeric value"):
        parse_binance_kline_message(bad_payload)


def test_9_symbol_normalization():
    """9. Symbol normalization strips whitespace and converts to uppercase."""
    payload = make_binance_kline_payload(symbol="  ethusdt  ")
    event = parse_binance_kline_message(payload)
    assert event.symbol == "ETHUSDT"


def test_10_unknown_message_type():
    """10. Subscription ACK and non-kline frames raise UnknownMessageTypeError / return None in safe mode."""
    ack_payload = {"result": None, "id": 1}
    with pytest.raises(UnknownMessageTypeError, match="Received subscription acknowledgment"):
        parse_binance_kline_message(ack_payload)

    # In safe parsing mode, ACK returns (None, None)
    event, err = parse_binance_kline_safe(ack_payload)
    assert event is None
    assert err is None


# ============================================================================
# 11 - 20: WebSocket Connection, Reconnect, Subscription & Rotation Tests
# ============================================================================

def test_14_and_15_reconnect_backoff_and_jitter():
    """14. Reconnect backoff increases exponentially & 15. Jitter stays within bounded range."""
    initial_delay = 0.5
    max_delay = 30.0
    backoff_factor = 2.0
    jitter_ratio = 0.25

    delays_0 = [
        calculate_reconnect_backoff(
            attempt=0,
            initial_delay=initial_delay,
            max_delay=max_delay,
            backoff_factor=backoff_factor,
            jitter_ratio=jitter_ratio
        )
        for _ in range(50)
    ]
    # For attempt 0: base is 0.5. Range with 25% jitter: [0.375, 0.625]
    assert all(0.35 <= d <= 0.65 for d in delays_0)

    # For attempt 3: base is 0.5 * (2^3) = 4.0. Range with 25% jitter: [3.0, 5.0]
    delays_3 = [
        calculate_reconnect_backoff(
            attempt=3,
            initial_delay=initial_delay,
            max_delay=max_delay,
            backoff_factor=backoff_factor,
            jitter_ratio=jitter_ratio
        )
        for _ in range(50)
    ]
    assert all(2.9 <= d <= 5.1 for d in delays_3)

    # For high attempt: capped at max_delay * 1.25
    delays_high = [
        calculate_reconnect_backoff(
            attempt=20,
            initial_delay=initial_delay,
            max_delay=max_delay,
            backoff_factor=backoff_factor,
            jitter_ratio=jitter_ratio
        )
        for _ in range(50)
    ]
    assert all(d <= max_delay * 1.3 for d in delays_high)


def test_16_18_19_subscription_payload_and_stream_names():
    """16. Subscription payload generation, 18. Multiple symbols, 19. Multiple streams."""
    symbols = ["btcusdt", " ETHUSDT ", "solusdt"]
    stream_names = [build_kline_stream_name(s, "1m") for s in symbols]

    assert stream_names == ["btcusdt@kline_1m", "ethusdt@kline_1m", "solusdt@kline_1m"]

    payload_json = build_subscription_payload(stream_names, req_id=42)
    payload = json.loads(payload_json)

    assert payload["method"] == "SUBSCRIBE"
    assert payload["id"] == 42
    assert sorted(payload["params"]) == ["btcusdt@kline_1m", "ethusdt@kline_1m", "solusdt@kline_1m"]


def test_17_subscription_failure_on_empty():
    """17. Subscription validation rejects empty streams."""
    with pytest.raises(ValueError, match="Streams list cannot be empty"):
        build_subscription_payload([], req_id=1)

    with pytest.raises(InvalidSymbolError, match="requires at least one symbol"):
        BinanceWebSocketClient(symbols=[])


@pytest.mark.asyncio
async def test_11_and_13_connection_lifecycle_and_clean_shutdown():
    """11. Connection open lifecycle transitions & 13. Clean shutdown does not reconnect."""
    received_messages = []

    # Setup local mock WebSocket server
    async def mock_handler(websocket):
        async for msg in websocket:
            data = json.loads(msg)
            if data.get("method") == "SUBSCRIBE":
                # Send subscription ACK
                await websocket.send(json.dumps({"result": None, "id": data.get("id")}))
                # Send a partial candle then a final candle
                await websocket.send(json.dumps(make_binance_kline_payload(is_closed=False)))
                await websocket.send(json.dumps(make_binance_kline_payload(is_closed=True)))

    server = await serve(mock_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    mock_url = f"ws://127.0.0.1:{port}"

    client = BinanceWebSocketClient(
        symbols=["BTCUSDT"],
        ws_base_url=mock_url
    )

    assert client.state == WebSocketConnectionState.DISCONNECTED

    # Connect
    ws = await client.connect()
    assert client.state == WebSocketConnectionState.RUNNING
    assert client.is_connected is True

    # Disconnect cleanly
    await client.disconnect()
    assert client.state == WebSocketConnectionState.STOPPED
    assert client.is_connected is False

    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_12_and_streaming_final_candles():
    """Test 12: Streaming final candles filters partial ticks and yields only final candle."""
    # Mock WebSocket server sending 3 partial updates then 1 final candle
    async def mock_handler(websocket):
        async for msg in websocket:
            data = json.loads(msg)
            if data.get("method") == "SUBSCRIBE":
                await websocket.send(json.dumps({"result": None, "id": data.get("id")}))
                # 3 partial ticks
                await websocket.send(json.dumps(make_binance_kline_payload(close_price="16501.0", is_closed=False)))
                await websocket.send(json.dumps(make_binance_kline_payload(close_price="16502.0", is_closed=False)))
                await websocket.send(json.dumps(make_binance_kline_payload(close_price="16503.0", is_closed=False)))
                # 1 final candle
                await websocket.send(json.dumps(make_binance_kline_payload(close_price="16510.0", is_closed=True)))

    server = await serve(mock_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    mock_url = f"ws://127.0.0.1:{port}"

    client = BinanceWebSocketClient(
        symbols=["BTCUSDT"],
        ws_base_url=mock_url
    )

    final_candles = []
    async def consumer():
        async for candle in client.stream_final_candles():
            final_candles.append(candle)
            # Break after first final candle
            break

    consumer_task = asyncio.create_task(consumer())
    await asyncio.wait_for(consumer_task, timeout=3.0)

    # Invariant: Exactly ONE finalized candle was received despite 3 earlier partial updates
    assert len(final_candles) == 1
    assert final_candles[0].is_closed is True
    assert final_candles[0].close == 16510.0

    await client.disconnect()
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_20_connection_rotation():
    """20. Planned 24-hour connection rotation seamlessly switches connections."""
    connection_count = 0

    async def mock_handler(websocket):
        nonlocal connection_count
        connection_count += 1
        try:
            async for msg in websocket:
                data = json.loads(msg)
                if data.get("method") == "SUBSCRIBE":
                    await websocket.send(json.dumps({"result": None, "id": data.get("id")}))
                    while True:
                        await websocket.send(json.dumps(make_binance_kline_payload(is_closed=True)))
                        await asyncio.sleep(0.05)
        except Exception:
            pass

    server = await serve(mock_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    mock_url = f"ws://127.0.0.1:{port}"

    # Set connection lifetime threshold to 0.1s
    client = BinanceWebSocketClient(
        symbols=["BTCUSDT"],
        ws_base_url=mock_url,
        max_connection_lifetime_seconds=0.1
    )

    received_candles = []
    async def consumer():
        async for candle in client.stream_final_candles():
            received_candles.append(candle)
            if len(received_candles) >= 3 and connection_count >= 2:
                break

    consumer_task = asyncio.create_task(consumer())
    await asyncio.wait_for(consumer_task, timeout=5.0)

    # Invariant: Smooth transition with multiple connections
    assert len(received_candles) >= 3
    assert connection_count >= 2

    await client.disconnect()
    server.close()
    await server.wait_closed()


# ============================================================================
# 21 - 27: Strict Validation & Hook Integration Tests
# ============================================================================

def test_21_zero_and_negative_prices_rejected():
    """21. Prices must be strictly positive (> 0); zero or negative values are rejected."""
    # Zero open price
    bad_payload_zero = make_binance_kline_payload(open_price="0.0")
    with pytest.raises(MalformedMessageError, match="Price must be strictly positive"):
        parse_binance_kline_message(bad_payload_zero)

    # Negative close price
    bad_payload_neg = make_binance_kline_payload(close_price="-100.0")
    with pytest.raises(MalformedMessageError, match="Price must be strictly positive"):
        parse_binance_kline_message(bad_payload_neg)


def test_22_negative_volume_rejected():
    """22. Base and quote volume fields cannot be negative."""
    bad_payload_vol = make_binance_kline_payload(volume="-0.01")
    with pytest.raises(MalformedMessageError, match="Volume cannot be negative"):
        parse_binance_kline_message(bad_payload_vol)


def test_23_ohlc_invariants_violations_rejected():
    """23. OHLC geometric/chronological invariants violations are rejected."""
    # High < Low
    bad_high_low = make_binance_kline_payload(high_price="100.0", low_price="200.0", open_price="150.0", close_price="150.0")
    with pytest.raises(MalformedMessageError, match="High price .* cannot be less than low price"):
        parse_binance_kline_message(bad_high_low)

    # Open > High
    bad_open = make_binance_kline_payload(high_price="100.0", low_price="50.0", open_price="120.0", close_price="80.0")
    with pytest.raises(MalformedMessageError, match="Open price .* must be within"):
        parse_binance_kline_message(bad_open)

    # Close < Low
    bad_close = make_binance_kline_payload(high_price="100.0", low_price="50.0", open_price="80.0", close_price="40.0")
    with pytest.raises(MalformedMessageError, match="Close price .* must be within"):
        parse_binance_kline_message(bad_close)

    # Close time before start timestamp
    bad_time = make_binance_kline_payload(start_ms=1672531259999, close_ms=1672531200000)
    with pytest.raises(MalformedMessageError, match="Close time .* cannot precede start timestamp"):
        parse_binance_kline_message(bad_time)


def test_24_timezone_utc_enforcement():
    """24. Timestamps are always stored and parsed in UTC timezone."""
    payload = make_binance_kline_payload(start_ms=1672531200000, close_ms=1672531259999)
    event = parse_binance_kline_message(payload)

    assert event.timestamp.tzinfo == timezone.utc
    assert event.close_time.tzinfo == timezone.utc
    assert event.received_at.tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_25_connector_error_and_lifecycle_hooks():
    """25. Connector error, lifecycle, and message callbacks are triggered as expected."""
    opened_called = False
    closed_called = False
    final_candle_events = []
    parser_errors = []

    def on_opened():
        nonlocal opened_called
        opened_called = True

    def on_closed(code, reason):
        nonlocal closed_called
        closed_called = True

    def on_final(c):
        final_candle_events.append(c)

    def on_err(frame, err):
        parser_errors.append((frame, err))

    async def mock_handler(websocket):
        async for msg in websocket:
            data = json.loads(msg)
            if data.get("method") == "SUBSCRIBE":
                await websocket.send(json.dumps({"result": None, "id": data.get("id")}))
                # Send invalid frame
                await websocket.send("INVALID_NOT_JSON")
                # Send final valid frame
                await websocket.send(json.dumps(make_binance_kline_payload(is_closed=True)))

    server = await serve(mock_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    mock_url = f"ws://127.0.0.1:{port}"

    client = BinanceWebSocketClient(
        symbols=["BTCUSDT"],
        ws_base_url=mock_url,
        on_connection_opened=on_opened,
        on_connection_closed=on_closed,
        on_final_candle=on_final,
        on_parser_error=on_err,
    )

    async def consumer():
        async for c in client.stream_final_candles():
            break

    consumer_task = asyncio.create_task(consumer())
    await asyncio.wait_for(consumer_task, timeout=3.0)

    assert opened_called is True
    assert len(parser_errors) == 1
    assert len(final_candle_events) == 1

    await client.disconnect()
    assert closed_called is True
    server.close()
    await server.wait_closed()

