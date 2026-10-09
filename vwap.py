"""Live VWAP for several Binance USD-M futures (BTCUSDT, ETHUSDT, XAUUSDT).

The engine (LiveFeed) keeps its state and lists of events instead of printing, so a
display can follow it. Running this file shows it in the terminal.

One combined stream connection carries every asset's trades. Each asset has its own
AssetTracker (VWAP totals, ID checks, fetches, event log); they share the formulas.

VWAP resets at 00:00 UTC each day. When the program starts mid-session, it fetches
the session's earlier trades from the REST API (1m candles, then aggTrades up to the
first live trade) and shows VWAP in yellow until that fetch has been added.

Every trade's aggregated ID is checked against the highest ID seen: duplicates are
ignored, skipped IDs are fetched from REST, and late arrivals are added. When the
connection ends, it reconnects with backoff and the ID check finds the gap.
"""

import asyncio
import http.client
import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone

import websockets

REST_URL = "https://fapi.binance.com"
# Binance's combined stream: one connection carries every symbol's trades, each
# message wrapped as {"stream": "btcusdt@aggTrade", "data": {...trade, "s": "BTCUSDT"}}.
STREAM_BASE_URL = "wss://fstream.binance.com/market/stream?streams="
SYMBOLS = ["BTCUSDT", "ETHUSDT", "XAUUSDT"]  # adding an asset = adding its symbol here

MINUTE_MS = 60 * 1000
DAY_MS = 24 * 60 * MINUTE_MS
MAX_RETRY_WAIT_S = 60
# Trade times past this are treated as corrupt (datetime can't handle times that far off).
MAX_TRADE_TIME_MS = int(datetime(3000, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
MAX_EVENTS = 500  # the debug console keeps this many of the latest events
# Ping Binance this often; no pong within PING_TIMEOUT_S means the connection is dead.
# A pong proves the connection works even when the market is quiet.
PING_INTERVAL_S = 5
PING_TIMEOUT_S = 5
# When closing (e.g. after a missed pong) the library says goodbye and waits this long
# for Binance to answer. A dead network never answers, so this adds to detection time;
# a live one answers well within it. Worst case to notice a cut: 5 + 5 + 1 = 11 s.
CLOSE_TIMEOUT_S = 1

YELLOW = "\033[33m"
RESET_COLOR = "\033[0m"


def combined_stream_url(symbols):
    return STREAM_BASE_URL + "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)


def next_retry_wait(wait_s):
    """The wait after wait_s: 0 (retry straight away), then 5 s, 10 s, 20 s, ... up to MAX_RETRY_WAIT_S."""
    return min(max(wait_s * 2, 5), MAX_RETRY_WAIT_S)


def now_ms():
    """The PC clock in milliseconds (for when the program noticed something, not trade times)."""
    return int(time.time() * 1000)


def next_midnight_ms(trade_time_ms):
    """Return the first 00:00 UTC after the given time, in milliseconds."""
    trade_time = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc)
    next_day = trade_time.date() + timedelta(days=1)
    midnight = datetime(next_day.year, next_day.month, next_day.day, tzinfo=timezone.utc)
    return int(midnight.timestamp() * 1000)


class VwapTracker:
    """Keeps the running totals for one session and the VWAP derived from them."""

    def __init__(self):
        self.cumulative_pv = 0.0
        self.cumulative_volume = 0.0
        self.vwap = None  # None means no trades yet this session (shown as NA)
        self.next_reset_ms = None  # set by the first trade

    def add_trade(self, price, quantity, trade_time_ms):
        """Add one trade and recalculate VWAP. Returns True if a new session started."""
        new_session = self.next_reset_ms is None or trade_time_ms >= self.next_reset_ms
        if new_session:
            self.cumulative_pv = 0.0
            self.cumulative_volume = 0.0
            self.vwap = None
            self.next_reset_ms = next_midnight_ms(trade_time_ms)

        self.add_totals(price * quantity, quantity)
        return new_session

    def add_totals(self, pv, volume):
        """Add already-summed totals (e.g. from candles) and recalculate VWAP."""
        self.cumulative_pv += pv
        self.cumulative_volume += volume
        if self.cumulative_volume > 0:
            self.vwap = self.cumulative_pv / self.cumulative_volume

    def in_session(self, trade_time_ms):
        """True if a trade time falls in the current session (used for late and fetched trades)."""
        return self.next_reset_ms is not None and trade_time_ms >= self.next_reset_ms - DAY_MS


class TradeIdChecker:
    """Spots skipped, late and repeated aggregated trade IDs.

    Binance's aggregated trade IDs go up by exactly 1, so every live ID is compared
    with the highest ID seen so far.
    """

    def __init__(self):
        self.highest_id = None
        self.missing_ids = set()

    def check(self, trade_id):
        """Classify a live ID as "new", "late" or "duplicate".

        Returns (kind, gap). gap is (first_missing_id, last_missing_id) when this ID
        jumped past IDs that never arrived, otherwise None.
        """
        if self.highest_id is None or trade_id > self.highest_id:
            gap = None
            if self.highest_id is not None and trade_id > self.highest_id + 1:
                gap = (self.highest_id + 1, trade_id - 1)
                self.missing_ids.update(range(gap[0], gap[1] + 1))
            self.highest_id = trade_id
            return "new", gap
        if self.claim(trade_id):
            return "late", None
        return "duplicate", None

    def claim(self, trade_id):
        """Remove an ID from the missing set. True if it was missing (so it should be counted)."""
        if trade_id in self.missing_ids:
            self.missing_ids.remove(trade_id)
            return True
        return False


def fetch_json(path, params):
    """One blocking GET request to Binance's REST API, returning the parsed JSON."""
    url = f"{REST_URL}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.loads(response.read())


def run_in_daemon_thread(function, *args):
    """Run a blocking function in a daemon thread; returns a future to await for its result.

    Used instead of asyncio.to_thread because Python doesn't wait for daemon threads
    when it exits, so Ctrl+C stops the program at once, even mid-request.
    """
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def settle(result, error):
        if not future.done():  # the awaiting task may have been cancelled
            if error is None:
                future.set_result(result)
            else:
                future.set_exception(error)

    def worker():
        try:
            result, error = function(*args), None
        except Exception as caught:
            result, error = None, caught
        try:
            loop.call_soon_threadsafe(settle, result, error)
        except RuntimeError:
            pass  # the event loop already closed (program exiting)

    threading.Thread(target=worker, daemon=True).start()
    return future


async def get_json(path, params, log):
    """Run fetch_json in a thread so the live stream keeps going, retrying until it works.

    log(level, message) records each failure (see EventLog.log).

    Retries once straight away, then waits 5 s, 10 s, 20 s, ... up to MAX_RETRY_WAIT_S.
    If Binance says we're sending too many requests, waits at least as long as it asks.
    """
    wait_s = 0
    while True:
        try:
            return await run_in_daemon_thread(fetch_json, path, params)
        # Network/HTTP errors are OSErrors, a response cut off midway is an HTTPException,
        # and bad JSON is a ValueError.
        except (OSError, http.client.HTTPException, ValueError) as error:
            delay_s = max(wait_s, rate_limit_wait_s(error))
            log("warning", f"REST request {path} failed ({error}); retrying in {delay_s} s")
            await asyncio.sleep(delay_s)
            wait_s = next_retry_wait(wait_s)


def rate_limit_wait_s(error):
    """Seconds Binance asks us to wait, or 0 if the error isn't a rate limit.

    429 means too many requests. 418 means the IP is banned for a while because 429s
    were ignored. Both come with a Retry-After header in seconds; retrying sooner
    makes a ban longer.
    """
    if not isinstance(error, urllib.error.HTTPError) or error.code not in (418, 429):
        return 0
    try:
        return int(error.headers["Retry-After"])
    except (TypeError, KeyError, ValueError):  # header missing or not a number
        return MAX_RETRY_WAIT_S


def is_number(value, minimum):
    """True if value (a number, or a number sent as text) is finite and at least minimum."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number >= minimum


def is_whole_number(value, minimum, maximum):
    # bool counts as int in Python, so JSON true/false would otherwise get through
    return isinstance(value, int) and not isinstance(value, bool) and minimum <= value < maximum


def check_trade(trade):
    """Raise ValueError unless an aggTrade (from the stream or REST) has the fields VWAP needs.

    Called before a trade touches any totals or IDs, so a malformed one changes nothing.
    """
    if not isinstance(trade, dict):
        raise ValueError(f"not a JSON object: {trade!r:.200}")
    if not is_whole_number(trade.get("a"), 0, math.inf):
        raise ValueError(f"field 'a' missing or not a trade ID: {trade!r:.200}")
    if not is_whole_number(trade.get("T"), 0, MAX_TRADE_TIME_MS):
        raise ValueError(f"field 'T' missing or not a possible trade time: {trade!r:.200}")
    for key in ("p", "q"):
        if not is_number(trade.get(key), 0) or float(trade[key]) == 0:
            raise ValueError(f"field {key!r} missing or not a positive number: {trade!r:.200}")


def check_candle(candle):
    """Raise ValueError unless a 1m kline row has the fields VWAP needs.

    A row is [open time, open, high, low, close, volume, close time, quote volume, ...].
    Volume can be 0 in a minute with no trades.
    """
    if not isinstance(candle, list) or len(candle) < 8:
        raise ValueError(f"not a kline row: {candle!r:.200}")
    if not (is_whole_number(candle[0], 0, MAX_TRADE_TIME_MS) and is_whole_number(candle[6], 0, MAX_TRADE_TIME_MS)):
        raise ValueError(f"kline open or close time isn't a possible time: {candle!r:.200}")
    if not (is_number(candle[5], 0) and is_number(candle[7], 0)):
        raise ValueError(f"kline volume or quote volume isn't a number: {candle!r:.200}")


def check_list(response, path):
    """Raise ValueError unless a REST response is a list (Binance sends errors as an object)."""
    if not isinstance(response, list):
        raise ValueError(f"{path} returned {response!r:.200} instead of a list")


async def retry_on_error(name, log, function, *args):
    """Run a background fetch, starting it again if it fails, until it finishes.

    get_json already retries network errors; this catches anything else (such as a
    response in an unexpected shape). Without it the fetch would stop silently and
    VWAP would leave out its trades while no longer being shown in yellow.
    Starting again is safe: backfill only adds its totals once it has finished, and
    fetch_gap only adds trades it can still claim from the missing set.
    """
    wait_s = 0
    while True:
        try:
            return await function(*args)
        except Exception as error:
            log("warning", f"{name} failed ({type(error).__name__}: {error}); starting it again in {wait_s} s")
            await asyncio.sleep(wait_s)
            wait_s = next_retry_wait(wait_s)


async def fetch_earlier_trades(symbol, session_start_ms, first_trade, log):
    """Sum one asset's trades in the session from before its first live trade.

    1m candles cover the session start up to the start of the first live trade's
    minute (not including it). aggTrades cover that minute up to, but not including,
    the first live trade. Returns (pv, volume, candle_count, trade_count).
    """
    first_live_id = first_trade["a"]
    minute_start_ms = first_trade["T"] - first_trade["T"] % MINUTE_MS
    pv = 0.0
    volume = 0.0

    candle_count = 0
    if minute_start_ms > session_start_ms:
        candles = await get_json("/fapi/v1/klines", {
            "symbol": symbol, "interval": "1m", "limit": 1500,
            "startTime": session_start_ms, "endTime": minute_start_ms - 1,
        }, log)
        check_list(candles, "/fapi/v1/klines")
        for candle in candles:
            check_candle(candle)
            open_time_ms, close_time_ms = candle[0], candle[6]
            if open_time_ms < session_start_ms or close_time_ms >= minute_start_ms:
                continue  # outside the session, or the still-open candle
            volume += float(candle[5])
            pv += float(candle[7])  # quote volume = sum of price x quantity
            candle_count += 1

    trade_count = 0
    params = {"symbol": symbol, "startTime": minute_start_ms, "limit": 1000}
    while True:
        trades = await get_json("/fapi/v1/aggTrades", params, log)
        check_list(trades, "/fapi/v1/aggTrades")
        if not trades:
            # REST hasn't caught up with the stream yet; ask again for the same page.
            await asyncio.sleep(1)
            continue
        for trade in trades:
            check_trade(trade)
            if trade["a"] >= first_live_id:
                return pv, volume, candle_count, trade_count
            price = float(trade["p"])
            quantity = float(trade["q"])
            pv += price * quantity
            volume += quantity
            trade_count += 1
        params = {"symbol": symbol, "fromId": trades[-1]["a"] + 1, "limit": 1000}


async def backfill(symbol, tracker, first_trade, log):
    """Fetch the session's earlier trades and add them to the tracker's totals."""
    session_reset_ms = tracker.next_reset_ms
    session_start_ms = session_reset_ms - DAY_MS
    log("info", f"Fetching earlier trades since {format_time(session_start_ms)} UTC")
    pv, volume, candle_count, trade_count = await fetch_earlier_trades(symbol, session_start_ms, first_trade, log)

    if tracker.next_reset_ms != session_reset_ms:
        log("info", "A new session started before the fetch finished; discarding the fetched trades")
        return
    tracker.add_totals(pv, volume)
    log("info", f"Added {candle_count} one-minute candles and {trade_count} trades. "
                f"Full-session VWAP {format_vwap(tracker.vwap)}")


async def fetch_gap(symbol, tracker, checker, first_id, last_id, log):
    """Fetch the trades in one gap from REST and add those still missing to the totals.

    A fetched trade is only counted if its ID is still in the missing set, so a trade
    that also arrives late on the stream is never counted twice. Trades from before
    the current session are dropped.
    """
    added = dropped = 0
    from_id = first_id
    while from_id <= last_id:
        trades = await get_json("/fapi/v1/aggTrades", {"symbol": symbol, "fromId": from_id, "limit": 1000}, log)
        check_list(trades, "/fapi/v1/aggTrades")
        if not trades:
            await asyncio.sleep(1)
            continue
        for trade in trades:
            check_trade(trade)  # before claiming, so a bad row can't claim an ID without adding it
            if trade["a"] > last_id:
                break
            if not checker.claim(trade["a"]):
                continue  # already arrived late on the stream
            if tracker.in_session(trade["T"]):
                quantity = float(trade["q"])
                tracker.add_totals(float(trade["p"]) * quantity, quantity)
                added += 1
            else:
                dropped += 1
        from_id = trades[-1]["a"] + 1

    # IDs that REST never returned don't exist; stop waiting for them.
    never_found = [i for i in range(first_id, last_id + 1) if checker.claim(i)]
    log("info", f"Gap {first_id}-{last_id} filled: {added} trades added, "
                f"{dropped} from the previous session dropped. VWAP {format_vwap(tracker.vwap)}")
    if never_found:
        log("warning", f"Binance has no trades for {len(never_found)} IDs in that gap")


def format_vwap(vwap):
    return "NA" if vwap is None else f"{vwap:.2f}"


def format_time(time_ms):
    time = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
    return time.strftime("%H:%M:%S.") + f"{time.microsecond // 1000:03d}"


class EventLog:
    """One source's events for the debug console: an asset's, or the system's (symbol None).

    time_ms is the PC clock: when the program noticed it, not when Binance traded.
    IDs go up by 1 within one log, so (symbol, id) names an event, and a display can
    tell which events of each log it hasn't shown yet.
    """

    def __init__(self, symbol, on_event=None):
        self.symbol = symbol
        self.on_event = on_event  # called with each new event
        self.events = deque(maxlen=MAX_EVENTS)
        self.next_id = 0

    def log(self, level, message):
        """Record an event ("info" or "warning")."""
        event = {"symbol": self.symbol, "id": self.next_id, "time_ms": now_ms(), "level": level, "message": message}
        self.next_id += 1
        self.events.append(event)
        if self.on_event:
            self.on_event(event)

    def find(self, event_id):
        """Return the event with this ID, or None if it isn't kept."""
        for event in self.events:
            if event["id"] == event_id:
                return event
        return None


class AssetTracker:
    """Everything one asset keeps for itself: VWAP totals, ID checks, fetches, last trade, events.

    Every asset uses the same formulas (VwapTracker, TradeIdChecker, the fetch
    functions) but has its own numbers, so a gap in one asset never touches another.
    It lives in LiveFeed, not inside the connection, so it all survives a reconnect.
    """

    def __init__(self, symbol, on_event=None, on_trade=None):
        self.symbol = symbol
        self.on_trade = on_trade  # called with the symbol after each new live trade is counted
        self.event_log = EventLog(symbol, on_event)
        self.tracker = VwapTracker()
        self.checker = TradeIdChecker()
        self.backfill_task = None
        self.fetch_tasks = set()  # keeps running fetch tasks referenced until they finish
        self.last_trade = None  # the latest new live trade: price, quantity, time_ms, received_ms

    def log(self, level, message):
        self.event_log.log(level, message)

    def history_state(self):
        """Return "waiting" before the first trade, "fetching" during the startup fetch, then "done"."""
        if self.backfill_task is None:
            return "waiting"
        return "done" if self.backfill_task.done() else "fetching"

    def is_complete(self):
        """True if VWAP includes every trade so far: startup fetch done and no IDs missing."""
        return self.history_state() == "done" and not self.checker.missing_ids

    def snapshot(self):
        """This asset's state as plain copied values (no await, so always consistent)."""
        session_start_ms = None
        if self.tracker.next_reset_ms is not None:
            session_start_ms = self.tracker.next_reset_ms - DAY_MS
        return {
            "symbol": self.symbol,
            "history": self.history_state(),
            "complete": self.is_complete(),
            "missing_count": len(self.checker.missing_ids),
            "session_start_ms": session_start_ms,
            "last_trade": dict(self.last_trade) if self.last_trade else None,
            "vwap": self.tracker.vwap,
        }

    def start_task(self, name, function, *args):
        task = asyncio.create_task(retry_on_error(name, self.log, function, *args))
        self.fetch_tasks.add(task)
        task.add_done_callback(self.fetch_tasks.discard)
        return task

    def handle_trade(self, trade):
        """Check one live trade's ID, then add it to VWAP if it should be counted."""
        price = float(trade["p"])
        quantity = float(trade["q"])
        trade_time_ms = trade["T"]

        kind, gap = self.checker.check(trade["a"])
        if kind == "duplicate":
            self.log("warning", f"Duplicate trade {trade['a']} ignored")
            return
        if kind == "late":
            if self.tracker.in_session(trade_time_ms):
                self.tracker.add_totals(price * quantity, quantity)
                self.log("info", f"Late trade {trade['a']} arrived and was added")
            return

        if self.tracker.add_trade(price, quantity, trade_time_ms):
            session_date = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc).date()
            self.log("info", f"New session: {session_date} (UTC)")
        self.last_trade = {"price": price, "quantity": quantity, "time_ms": trade_time_ms, "received_ms": now_ms()}

        # The first live trade sets the session, so the startup fetch can start now.
        if self.backfill_task is None:
            self.backfill_task = self.start_task("Fetching earlier trades", backfill,
                                                 self.symbol, self.tracker, trade, self.log)
        if gap is not None:
            first_id, last_id = gap
            self.log("info", f"Gap: {last_id - first_id + 1} trades missing (IDs {first_id}-{last_id}); fetching from REST")
            self.start_task(f"Fetching gap {first_id}-{last_id}", fetch_gap,
                            self.symbol, self.tracker, self.checker, first_id, last_id, self.log)

        if self.on_trade:
            self.on_trade(self.symbol)


class LiveFeed:
    """The live engine: one Binance connection carrying every asset, reconnecting when it ends.

    It doesn't print or draw anything. A display follows it in two ways:
    snapshot() returns a copy of everything there is to show, and the callbacks
    on_event(event) and on_trade(symbol) are called as things happen.

    Each trade goes to its asset's AssetTracker, picked by the trade's symbol ("s").
    Events about the connection itself go to the system log (symbol None).
    """

    def __init__(self, symbols=SYMBOLS, stream_url=None, on_event=None, on_trade=None,
                 ping_interval_s=PING_INTERVAL_S, ping_timeout_s=PING_TIMEOUT_S, close_timeout_s=CLOSE_TIMEOUT_S):
        self.stream_url = stream_url or combined_stream_url(symbols)  # tests point this at a fake server
        self.ping_interval_s = ping_interval_s  # tests shorten these
        self.ping_timeout_s = ping_timeout_s
        self.close_timeout_s = close_timeout_s
        self.sleep = asyncio.sleep  # tests swap this to record reconnect waits
        # Displays set these after the feed is made, so the logs and assets call through emit_*.
        self.on_event = on_event  # called with each new event, from any log
        self.on_trade = on_trade  # called with the symbol after each new live trade is counted
        self.system = EventLog(None, self.emit_event)
        self.assets = {symbol: AssetTracker(symbol, self.emit_event, self.emit_trade) for symbol in symbols}
        self.connection = {"state": "connecting", "message": "Starting", "retry_at_ms": None}

    def emit_event(self, event):
        if self.on_event:
            self.on_event(event)

    def emit_trade(self, symbol):
        if self.on_trade:
            self.on_trade(symbol)

    def log(self, level, message):
        """Record a system event (about the connection or the whole program, not one asset)."""
        self.system.log(level, message)

    @property
    def fetch_tasks(self):
        """Every asset's running fetch tasks."""
        return set().union(*(asset.fetch_tasks for asset in self.assets.values()))

    def event_log(self, symbol):
        """The log for a symbol (None for the system log), or None if there's no such log."""
        if symbol is None:
            return self.system
        asset = self.assets.get(symbol)
        return asset.event_log if asset else None

    def events(self):
        """Every kept event from every log, oldest first.

        Each log stays in ID order (the sort is stable and each log is already in time
        order), so a display can track the highest ID it has seen per symbol.
        """
        logs = [self.system.events, *(asset.event_log.events for asset in self.assets.values())]
        return sorted((event for log in logs for event in log), key=lambda event: event["time_ms"])

    def set_connection(self, state, level, message):
        """Change the connection state and record it as a system event.

        States: "connecting", "connected", "closed" (Binance ended it normally),
        "dropped", "refused" and "unreachable".
        """
        self.connection = {"state": state, "message": message, "retry_at_ms": None}
        self.log(level, message)

    def snapshot(self):
        """Everything a display needs, as plain values copied in one go.

        There's no await in here, so the engine can't change anything halfway through:
        every asset is read at the same moment, and none shows a new VWAP next to an
        old price. Events aren't included; read them with events().
        """
        return {
            "connection": dict(self.connection),
            "assets": [asset.snapshot() for asset in self.assets.values()],
        }

    def handle_message(self, message):
        """Pass one stream message to its asset, or skip it with a warning if it can't be used.

        If a skipped message was a real trade, its ID shows up as a gap in its asset
        and is fetched from REST.
        """
        try:
            trade = json.loads(message)["data"]
            asset = self.assets[trade["s"]]
        except (ValueError, TypeError, KeyError):  # bad JSON is a ValueError too
            self.log("warning", f"Skipped a message that isn't a trade for a tracked asset: {message!r:.200}")
            return
        try:
            check_trade(trade)
        except ValueError as error:
            asset.log("warning", f"Skipped a message that isn't a valid trade ({error})")
            return
        asset.handle_trade(trade)

    async def run(self):
        """Connect and receive trades forever, reconnecting with backoff when the connection ends.

        A connection that stops answering pings (e.g. Wi-Fi cut) is closed by the
        websockets library and counts as dropped.

        Retries straight away, then waits 5 s, 10 s, 20 s, ... up to MAX_RETRY_WAIT_S.
        The wait goes back to zero once a connection delivers a message.
        """
        wait_s = 0
        while True:
            self.set_connection("connecting", "info", f"Connecting to {self.stream_url}")
            try:
                async with websockets.connect(self.stream_url, ping_interval=self.ping_interval_s,
                                              ping_timeout=self.ping_timeout_s,
                                              close_timeout=self.close_timeout_s) as ws:
                    self.set_connection("connected", "info", "Connected")
                    async for message in ws:
                        wait_s = 0
                        self.handle_message(message)
                # A normal close (e.g. Binance's 24 hour limit) ends the loop without an error.
                self.set_connection("closed", "info", "Binance closed the connection, reconnecting")
            except websockets.exceptions.ConnectionClosedError as error:
                self.set_connection("dropped", "warning", f"Connection dropped ({error}), reconnecting")
            except websockets.exceptions.InvalidHandshake as error:
                self.set_connection("refused", "warning", f"Binance refused the connection ({error})")
            except OSError as error:
                self.set_connection("unreachable", "warning",
                                    f"Can't reach Binance, check the internet connection ({error})")

            if wait_s > 0:
                self.connection["retry_at_ms"] = now_ms() + wait_s * 1000
                self.log("info", f"Retrying in {wait_s} s")
            await self.sleep(wait_s)
            wait_s = next_retry_wait(wait_s)


def run_in_terminal():
    """Show the engine in the terminal: one line per event and per new trade, each led by its source."""
    live = LiveFeed()

    def print_event(event):
        source = event["symbol"] or "SYSTEM"
        print(f"{source:<8} " + ("Warning: " if event["level"] == "warning" else "") + event["message"])

    def print_trade(symbol):
        asset = live.assets[symbol]
        trade = asset.last_trade
        vwap_text = format_vwap(asset.tracker.vwap)
        if not asset.is_complete():  # yellow: VWAP doesn't include every trade yet
            vwap_text = f"{YELLOW}{vwap_text}{RESET_COLOR}"
        print(f"{symbol:<8} {format_time(trade['time_ms'])} UTC  "
              f"price {trade['price']:.2f}  qty {trade['quantity']:g}  VWAP {vwap_text}")

    live.on_event = print_event
    live.on_trade = print_trade
    asyncio.run(live.run())


if __name__ == "__main__":
    try:
        run_in_terminal()
    except KeyboardInterrupt:
        print("Stopped.")
