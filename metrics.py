"""Live VWAP and order flow for several Binance USD-M futures (BTCUSDT, ETHUSDT, XAUUSDT).

The engine (LiveFeed) keeps its state and lists of events instead of printing, so a
display can follow it. Running this file shows it in the terminal.

One combined stream connection carries every asset's trades. Each asset has its own
AssetTracker (session totals, rolling window, ID checks, fetches, event log); they share the formulas.

Each asset has three sets of metrics:
- session (since 00:00 UTC): change of the price since the session's first trade, VWAP,
  distance of the price from it, and buy and sell volume with their difference (delta;
  over the session this is CVD).
- rolling (the last ROLLING_WINDOW_S seconds): VWAP, buy and sell volume, delta,
  trades per second and volume per minute. At startup a separate fetch brings the
  minute before the first live trade, so the window is full within seconds.
- candles: the session's one-minute candles with the session VWAP at each minute's
  end, for the page's price chart. They get every trade the session totals get.
- candle check: each finished minute's candle is compared with Binance's official
  one, so the numbers are proven right while the app runs (see CandleChecker).
- volatility (the last VOLATILITY_WINDOW_MIN minutes): the standard deviation of
  one-minute log returns, annualized (x the square root of the minutes in a 365-day
  year, as crypto trades every day) and shown in percent. It uses each finished
  minute's last price; a minute with no trades keeps the previous one. At startup a
  fetch of 1m candles brings the hour before, so it's full at once.
A trade counts as a buy when the buyer was the taker (hit the ask), as a sell when
the seller was (Binance's "m" flag: buyer is maker). Every volume is kept twice: in
USDT (price x quantity, the quote asset) and in the coin itself (the base asset).

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
QUOTE_ASSET = "USDT"  # every symbol is priced in this; the rest of the name is the coin (base asset)

MINUTE_MS = 60 * 1000
DAY_MS = 24 * 60 * MINUTE_MS
ROLLING_WINDOW_S = 60  # every rolling metric covers this many seconds
VOLATILITY_WINDOW_MIN = 60  # volatility uses this many one-minute returns
MINUTES_PER_YEAR = 365 * 24 * 60  # for annualizing; crypto (and Binance's gold) trades every day
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
# Each finished minute's candle is checked against Binance's official one. Binance's is
# final a moment after the minute ends, so checks wait this long, and look every CHECK_POLL_S.
CHECK_DELAY_S = 10
CHECK_POLL_S = 5
# Binance merges fills at one price and taker side within this many ms into one aggregated
# trade, stamped with its first fill's time. Its candles count each fill in its own minute,
# so a trade that starts this close to a minute's end can be split across the boundary.
AGG_WINDOW_MS = 100

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


def trade_totals(price, quantity, is_buy):
    """One trade as (pv, volume, buy_pv, buy_volume): its value in USDT and in coins, and the buy part of each."""
    pv = price * quantity
    return (pv, quantity, pv, quantity) if is_buy else (pv, quantity, 0.0, 0.0)


class SessionCandles:
    """The session's one-minute candles (open, high, low, close, volume) for the price chart.

    A trade that arrives late or is fetched after a gap still lands in its own minute:
    it becomes the open only if it happened earlier than the open, the close only if later.
    revision goes up whenever a minute before the last two changes (startup fetch, gap,
    new session), so a chart showing the last two live can tell when to load them all again.
    """

    def __init__(self):
        # minute (trade time // MINUTE_MS) -> [open_ms, open, high, low, close_ms, close, pv, volume, close_quantity]
        # close_quantity is the last trade's size (0 for a fetched candle), for CandleChecker.
        self.minutes = {}
        self.previous = {}  # the previous session's minutes, so its last ones can still be checked against Binance
        self.newest_minute = None
        self.revision = 0
        # Running totals, so the state sent every STATE_INTERVAL_S doesn't go through every minute.
        self.total_pv = self.total_volume = 0.0
        self.high = self.low = None

    def clear(self):
        self.previous = self.minutes
        self.minutes = {}
        self.newest_minute = None
        self.revision += 1
        self.total_pv = self.total_volume = 0.0
        self.high = self.low = None

    def get(self, minute):
        """One minute's candle from this session or the previous one, or None (no trades seen in it)."""
        return self.minutes.get(minute) or self.previous.get(minute)

    def straddle_free(self, minute):
        """True if no trade can be split across this minute's start: none came in the AGG_WINDOW_MS before it."""
        before = self.get(minute - 1)
        return before is None or before[4] < minute * MINUTE_MS - AGG_WINDOW_MS

    def add(self, price, quantity, trade_time_ms):
        self.merge(trade_time_ms // MINUTE_MS, trade_time_ms, price, price, price,
                   trade_time_ms, price, price * quantity, quantity, quantity)

    def merge(self, minute, open_ms, open_price, high, low, close_ms, close, pv, volume, close_quantity=0.0):
        """Add part of a minute (one trade, or a whole fetched candle) to that minute's candle."""
        candle = self.minutes.get(minute)
        if candle is None:
            self.minutes[minute] = [open_ms, open_price, high, low, close_ms, close, pv, volume, close_quantity]
        else:
            if open_ms < candle[0]:
                candle[0], candle[1] = open_ms, open_price
            candle[2] = max(candle[2], high)
            candle[3] = min(candle[3], low)
            if close_ms >= candle[4]:
                candle[4], candle[5], candle[8] = close_ms, close, close_quantity
            candle[6] += pv
            candle[7] += volume
        self.total_pv += pv
        self.total_volume += volume
        self.high = high if self.high is None else max(self.high, high)
        self.low = low if self.low is None else min(self.low, low)
        if self.newest_minute is None or minute > self.newest_minute:
            self.newest_minute = minute
        elif minute < self.newest_minute - 1:
            self.revision += 1

    def rows(self):
        """Every candle, oldest first, as the chart takes them.

        time is the minute's start in seconds; vwap is the session VWAP at the end of that minute.
        """
        rows = []
        pv = volume = 0.0
        for minute in sorted(self.minutes):
            _, open_price, high, low, _, close, minute_pv, minute_volume, _ = self.minutes[minute]
            pv += minute_pv
            volume += minute_volume
            rows.append({"time": minute * MINUTE_MS // 1000, "open": open_price, "high": high, "low": low,
                         "close": close, "vwap": pv / volume})
        return rows

    def latest_rows(self):
        """The latest two candles, as rows() gives them, without going through the whole session.

        The VWAP at the end of the newest minute is the running total's; at the end of the
        one before, it's the running total without the newest minute.
        """
        if not self.minutes:
            return []
        newest = self.minutes[self.newest_minute]
        before = [m for m in self.minutes if m < self.newest_minute]
        latest = [(self.newest_minute, newest, self.total_pv, self.total_volume)]
        if before:
            minute = max(before)
            latest.insert(0, (minute, self.minutes[minute], self.total_pv - newest[6], self.total_volume - newest[7]))
        return [{"time": minute * MINUTE_MS // 1000, "open": candle[1], "high": candle[2], "low": candle[3],
                 "close": candle[5], "vwap": pv / volume}
                for minute, candle, pv, volume in latest]

    def high_low(self):
        """The session's highest and lowest trade price, or (None, None) before any trade."""
        return self.high, self.low


class SessionTracker:
    """Keeps the running totals for one session and the VWAP derived from them.

    cumulative_pv is also the session's volume in USDT. Sell volume isn't kept:
    it's the total minus the buy part (cumulative_pv - buy_pv, and the same in coins).
    The session's one-minute candles get every trade the totals get.
    """

    def __init__(self):
        self.candles = SessionCandles()
        self.cumulative_pv = 0.0
        self.cumulative_volume = 0.0
        self.buy_pv = 0.0  # USDT volume of trades where the buyer was the taker
        self.buy_volume = 0.0  # the same in coins
        self.vwap = None  # None means no trades yet this session (shown as NA)
        self.next_reset_ms = None  # set by the first trade
        # The session's first trade: its price is the base for the change since 00:00 UTC.
        # An earlier trade found later (startup fetch, gap, late arrival) replaces it.
        self.open_price = None
        self.open_time_ms = None

    def add_trade(self, price, quantity, trade_time_ms, is_buy):
        """Add one trade and recalculate VWAP. Returns True if a new session started."""
        new_session = self.next_reset_ms is None or trade_time_ms >= self.next_reset_ms
        if new_session:
            self.cumulative_pv = 0.0
            self.cumulative_volume = 0.0
            self.buy_pv = 0.0
            self.buy_volume = 0.0
            self.vwap = None
            self.next_reset_ms = next_midnight_ms(trade_time_ms)
            self.open_price = self.open_time_ms = None
            self.candles.clear()

        self.add_found_trade(price, quantity, trade_time_ms, is_buy)
        return new_session

    def add_found_trade(self, price, quantity, trade_time_ms, is_buy):
        """Add a trade known to be in this session (live, late or fetched) to every total and the candles."""
        self.note_price(price, trade_time_ms)
        self.add_totals(*trade_totals(price, quantity, is_buy))
        self.candles.add(price, quantity, trade_time_ms)

    def note_price(self, price, trade_time_ms):
        """Keep the price of the earliest trade in the session as its opening price."""
        if self.open_time_ms is None or trade_time_ms < self.open_time_ms:
            self.open_price = price
            self.open_time_ms = trade_time_ms

    def add_totals(self, pv, volume, buy_pv, buy_volume):
        """Add already-summed totals (e.g. from candles) and recalculate VWAP."""
        self.cumulative_pv += pv
        self.cumulative_volume += volume
        self.buy_pv += buy_pv
        self.buy_volume += buy_volume
        if self.cumulative_volume > 0:
            self.vwap = self.cumulative_pv / self.cumulative_volume

    def in_session(self, trade_time_ms):
        """True if a trade time falls in the current session (used for late and fetched trades)."""
        return self.next_reset_ms is not None and trade_time_ms >= self.next_reset_ms - DAY_MS


class RollingWindow:
    """Totals of the trades in the last ROLLING_WINDOW_S seconds, by trade time.

    Trades are kept in one-second buckets, so a trade that arrives late or is fetched
    after a gap still lands in its own second, and summing the window stays cheap.
    The window is the current second and the ROLLING_WINDOW_S - 1 before it.
    """

    def __init__(self):
        self.buckets = {}  # trade time in whole seconds -> [pv, volume, buy_pv, buy_volume, trade_count]
        # The window has every trade from this time on (None before the first live trade).
        # It's the first live trade's time, then earlier once the startup fetch has added the minute before it.
        self.complete_from_ms = None
        self.newest_s = None

    def add(self, price, quantity, trade_time_ms, is_buy):
        second = trade_time_ms // 1000
        if self.newest_s is not None and second <= self.newest_s - ROLLING_WINDOW_S:
            return  # already outside the window
        bucket = self.buckets.setdefault(second, [0.0, 0.0, 0.0, 0.0, 0])
        for i, value in enumerate(trade_totals(price, quantity, is_buy)):
            bucket[i] += value
        bucket[4] += 1
        if self.newest_s is None or second > self.newest_s:
            self.newest_s = second
            for old in [s for s in self.buckets if s <= second - ROLLING_WINDOW_S]:
                del self.buckets[old]

    def totals(self, now_ms):
        """Sum the window ending at now_ms (Binance's time): [pv, volume, buy_pv, buy_volume, trade_count]."""
        first_s = now_ms // 1000 - ROLLING_WINDOW_S + 1
        sums = [0.0, 0.0, 0.0, 0.0, 0]
        for second, bucket in self.buckets.items():
            if second >= first_s:
                for i, value in enumerate(bucket):
                    sums[i] += value
        return sums


def order_flow(pv, volume, buy_pv, buy_volume):
    """Buy volume, sell volume and delta (buy minus sell), in USDT ("quote") and in coins ("base")."""
    def one_unit(total, buy):
        sell = total - buy
        return {"buy": buy, "sell": sell, "delta": buy - sell}
    return {"quote": one_unit(pv, buy_pv), "base": one_unit(volume, buy_volume)}


class MinuteCloses:
    """The last price of each recent minute, by trade time, for volatility.

    A minute's close is the price of its latest trade, so a trade that arrives late or
    is fetched after a gap only replaces it if it happened later in that minute.
    """

    def __init__(self):
        self.closes = {}  # minute (trade time // MINUTE_MS) -> (time_ms, price)
        self.newest_minute = None

    def add(self, price, trade_time_ms):
        minute = trade_time_ms // MINUTE_MS
        kept = self.closes.get(minute)
        if kept is None or trade_time_ms >= kept[0]:
            self.closes[minute] = (trade_time_ms, price)
        if self.newest_minute is None or minute > self.newest_minute:
            self.newest_minute = minute
            # One extra minute is kept so the oldest minute in the window can take its close.
            for old in [m for m in self.closes if m < minute - VOLATILITY_WINDOW_MIN - 1]:
                del self.closes[old]

    def series(self, last_minute):
        """Closes of the VOLATILITY_WINDOW_MIN + 1 minutes up to last_minute, oldest first.

        A minute with no trades keeps the previous close (the price didn't move). Minutes
        before the first close known are left out, so the list is shorter while warming up.
        """
        first_minute = last_minute - VOLATILITY_WINDOW_MIN
        earlier = [m for m in self.closes if m < first_minute]
        price = self.closes[max(earlier)][1] if earlier else None
        series = []
        for minute in range(first_minute, last_minute + 1):
            if minute in self.closes:
                price = self.closes[minute][1]
            if price is not None:
                series.append(price)
        return series


def realized_volatility(closes):
    """Annualized volatility in percent from consecutive one-minute closes, or None with fewer than 3.

    The sample standard deviation of the log returns ln(close / previous close), scaled
    from one minute to a year by the square root of MINUTES_PER_YEAR.
    """
    returns = [math.log(now / before) for before, now in zip(closes, closes[1:])]
    if len(returns) < 2:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(variance * MINUTES_PER_YEAR) * 100


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
    """Raise ValueError unless an aggTrade (from the stream or REST) has the fields the metrics need.

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
    if not isinstance(trade.get("m"), bool):
        raise ValueError(f"field 'm' missing or not true/false: {trade!r:.200}")


def is_buy(trade):
    """True if the buyer was the taker. "m" means the buyer was the maker, so the seller took."""
    return not trade["m"]


def check_candle(candle):
    """Raise ValueError unless a 1m kline row has the fields the session totals and volatility need.

    A row is [open time, open, high, low, close, volume, close time, quote volume,
    trade count, taker buy volume, taker buy quote volume, ...]. Volume can be 0 in a
    minute with no trades.
    """
    if not isinstance(candle, list) or len(candle) < 11:
        raise ValueError(f"not a kline row: {candle!r:.200}")
    if not (is_whole_number(candle[0], 0, MAX_TRADE_TIME_MS) and is_whole_number(candle[6], 0, MAX_TRADE_TIME_MS)):
        raise ValueError(f"kline open or close time isn't a possible time: {candle!r:.200}")
    if not all(is_number(candle[i], 0) for i in (5, 7, 9, 10)):
        raise ValueError(f"kline volume, quote volume or taker buy volumes aren't numbers: {candle!r:.200}")
    if not all(is_number(candle[i], 0) and float(candle[i]) > 0 for i in (1, 2, 3, 4)):
        raise ValueError(f"kline open, high, low or close isn't a positive price: {candle!r:.200}")


def check_list(response, path):
    """Raise ValueError unless a REST response is a list (Binance sends errors as an object)."""
    if not isinstance(response, list):
        raise ValueError(f"{path} returned {response!r:.200} instead of a list")


async def retry_on_error(name, log, function, *args):
    """Run a background fetch, starting it again if it fails, until it finishes.

    get_json already retries network errors; this catches anything else (such as a
    response in an unexpected shape). Without it the fetch would stop silently and
    VWAP would leave out its trades while no longer being shown in yellow.
    Starting again is safe: backfill and fill_window only add their trades once they
    have finished, and fetch_gap only adds trades it can still claim from the missing set.
    """
    wait_s = 0
    while True:
        try:
            return await function(*args)
        except Exception as error:
            log("warning", f"{name} failed ({type(error).__name__}: {error}); starting it again in {wait_s} s")
            await asyncio.sleep(wait_s)
            wait_s = next_retry_wait(wait_s)


def window_fetch_start_ms(first_trade_time_ms):
    """Where the startup fetch starts its aggTrades, so the rolling window is full at once.

    A whole second, ROLLING_WINDOW_S before the first live trade's second: the window
    counts whole seconds, so this covers every second it can include.
    """
    return (first_trade_time_ms // 1000 - ROLLING_WINDOW_S) * 1000


async def fetch_trades_before(symbol, start_ms, first_trade, log):
    """One asset's aggTrades from start_ms up to, but not including, the first live trade.

    Returns a list of (price, quantity, time_ms, is_buy).
    """
    trades_before = []
    params = {"symbol": symbol, "startTime": start_ms, "limit": 1000}
    while True:
        trades = await get_json("/fapi/v1/aggTrades", params, log)
        check_list(trades, "/fapi/v1/aggTrades")
        if not trades:
            # REST hasn't caught up with the stream yet; ask again for the same page.
            await asyncio.sleep(1)
            continue
        for trade in trades:
            check_trade(trade)
            if trade["a"] >= first_trade["a"]:
                return trades_before
            trades_before.append((float(trade["p"]), float(trade["q"]), trade["T"], is_buy(trade)))
        params = {"symbol": symbol, "fromId": trades[-1]["a"] + 1, "limit": 1000}


async def fetch_trades_from_join(symbol, session_start_ms, first_trade, log):
    """Pick where the session fetch switches from 1m candles to trades, and fetch the trades from there.

    Returns (join_ms, trades): a minute start no trade can straddle, and the trades from
    it up to, but not including, the first live trade (as fetch_trades_before gives them).
    A trade's fills can run AGG_WINDOW_MS past its time and a candle counts each fill in
    the minute it happened, so at a minute start with a trade in the AGG_WINDOW_MS before
    it, the part of that trade after it would be in neither. The join is the latest
    minute start at or before the first live trade's minute without one, or the session
    start (trades only). Each try fetches from further back (1, 2, 4, ... minutes).
    """
    first_minute = first_trade["T"] // MINUTE_MS
    lookback_min = 0
    while True:
        start_ms = max(session_start_ms, (first_minute - lookback_min) * MINUTE_MS - AGG_WINDOW_MS)
        trades = await fetch_trades_before(symbol, start_ms, first_trade, log)
        unsafe = {time_ms // MINUTE_MS + 1 for _, _, time_ms, _ in trades  # minute starts a trade may straddle
                  if time_ms % MINUTE_MS >= MINUTE_MS - AGG_WINDOW_MS}
        for minute in range(first_minute, first_minute - lookback_min - 1, -1):
            join_ms = max(minute * MINUTE_MS, session_start_ms)
            if join_ms == session_start_ms or minute not in unsafe:
                return join_ms, [trade for trade in trades if trade[2] >= join_ms]
        lookback_min = max(1, lookback_min * 2)


async def fetch_earlier_trades(symbol, session_start_ms, first_trade, log):
    """Sum one asset's trades in the session from before its first live trade.

    1m candles cover the session start up to a minute start no trade straddles, normally
    the first live trade's minute (see fetch_trades_from_join). aggTrades cover the rest,
    up to, but not including, the first live trade.

    Returns (pv, volume, buy_pv, buy_volume, candle_count, trade_count, opening, chart_parts).
    opening is (price, time_ms) of the session's first trade found (a candle's open is
    its first trade), or None if there were no trades before the first live trade.
    chart_parts are the arguments of SessionCandles.merge for each fetched candle and trade.
    """
    join_ms, trades = await fetch_trades_from_join(symbol, session_start_ms, first_trade, log)
    totals = [0.0, 0.0, 0.0, 0.0]  # pv, volume, buy_pv, buy_volume
    opening = None
    chart_parts = []

    candle_count = 0
    if join_ms > session_start_ms:
        candles = await get_json("/fapi/v1/klines", {
            "symbol": symbol, "interval": "1m", "limit": 1500,
            "startTime": session_start_ms, "endTime": join_ms - 1,
        }, log)
        check_list(candles, "/fapi/v1/klines")
        for candle in candles:
            check_candle(candle)
            open_time_ms, close_time_ms = candle[0], candle[6]
            if open_time_ms < session_start_ms or close_time_ms >= join_ms:
                continue  # outside the session, or a minute the trades cover
            # quote volume (= sum of price x quantity), volume, taker buy quote volume, taker buy volume
            for i, column in enumerate((7, 5, 10, 9)):
                totals[i] += float(candle[column])
            candle_count += 1
            # A minute with no trades has an open too (the previous close), so skip those.
            if float(candle[5]) > 0:
                if opening is None:
                    opening = (float(candle[1]), open_time_ms)
                open_price, high, low, close = (float(candle[i]) for i in (1, 2, 3, 4))
                chart_parts.append((open_time_ms // MINUTE_MS, open_time_ms, open_price, high, low,
                                    close_time_ms, close, float(candle[7]), float(candle[5])))

    for price, quantity, time_ms, buy in trades:
        for i, value in enumerate(trade_totals(price, quantity, buy)):
            totals[i] += value
        if opening is None:
            opening = (price, time_ms)
        chart_parts.append((time_ms // MINUTE_MS, time_ms, price, price, price, time_ms, price,
                            price * quantity, quantity, quantity))
    return *totals, candle_count, len(trades), opening, chart_parts


async def backfill(symbol, tracker, first_trade, log):
    """Fetch the session's earlier trades and add them to the session totals.

    Nothing is added until the whole fetch has finished, so starting it again after
    a failure can't add anything twice.
    """
    session_reset_ms = tracker.next_reset_ms
    session_start_ms = session_reset_ms - DAY_MS
    log("info", f"Fetching the session's earlier trades since {format_time(session_start_ms)} UTC")
    *totals, candle_count, trade_count, opening, chart_parts = await fetch_earlier_trades(
        symbol, session_start_ms, first_trade, log)

    if tracker.next_reset_ms != session_reset_ms:
        log("info", "A new session started before the fetch finished; discarding the fetched trades")
        return
    tracker.add_totals(*totals)
    if opening is not None:
        tracker.note_price(*opening)
    for part in chart_parts:
        tracker.candles.merge(*part)
    if chart_parts:
        tracker.candles.revision += 1  # the chart loads them all again, even ones in the last two minutes
    log("info", f"Added {candle_count} one-minute candles and {trade_count} trades. Session metrics calculated")


async def fill_window(symbol, window, first_trade, log):
    """Fetch the trades of the last ROLLING_WINDOW_S before the first live trade into the rolling window.

    It's separate from the session fetch (which needs every candle first), so the
    window is full within seconds. It doesn't depend on the session, so it holds even
    if a new one starts. Nothing is added until the fetch has finished, so starting
    it again after a failure can't add anything twice.
    """
    log("info", f"Fetching trades from the last {ROLLING_WINDOW_S} seconds")
    start_ms = window_fetch_start_ms(first_trade["T"])
    trades = await fetch_trades_before(symbol, start_ms, first_trade, log)
    for trade in trades:
        window.add(*trade)
    window.complete_from_ms = start_ms
    log("info", f"Added {len(trades)} trades. {ROLLING_WINDOW_S} second rolling window metrics calculated")


async def fill_closes(symbol, closes, first_trade, log):
    """Fetch the closes of the VOLATILITY_WINDOW_MIN + 1 minutes before the first live trade's minute.

    That minute's own close comes from live trades, so volatility is full at once.
    Each candle's close counts as a trade at the candle's last millisecond, so a live
    trade can't replace it. Adding the same closes twice changes nothing, so starting
    it again after a failure is safe.
    """
    log("info", f"Fetching one-minute candles from the last {VOLATILITY_WINDOW_MIN} minutes")
    minute_start_ms = first_trade["T"] - first_trade["T"] % MINUTE_MS
    candles = await get_json("/fapi/v1/klines", {
        "symbol": symbol, "interval": "1m", "limit": VOLATILITY_WINDOW_MIN + 1,
        "startTime": minute_start_ms - (VOLATILITY_WINDOW_MIN + 1) * MINUTE_MS, "endTime": minute_start_ms - 1,
    }, log)
    check_list(candles, "/fapi/v1/klines")
    for candle in candles:
        check_candle(candle)  # every row first, so a bad one adds nothing
    for candle in candles:
        closes.add(float(candle[4]), candle[6])
    log("info", f"Added {len(candles)} one-minute candles. Volatility calculated")


async def fetch_gap(symbol, tracker, window, closes, checker, first_id, last_id, log):
    """Fetch the trades in one gap from REST and add those still missing to every metric.

    A fetched trade is only counted if its ID is still in the missing set, so a trade
    that also arrives late on the stream is never counted twice. Trades from before
    the current session are dropped from the session totals; the rolling window takes
    any trade recent enough, whichever session it's in.
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
            price, quantity = float(trade["p"]), float(trade["q"])
            window.add(price, quantity, trade["T"], is_buy(trade))
            closes.add(price, trade["T"])
            if tracker.in_session(trade["T"]):
                tracker.add_found_trade(price, quantity, trade["T"], is_buy(trade))
                added += 1
            else:
                dropped += 1
        from_id = trades[-1]["a"] + 1

    # IDs that REST never returned don't exist; stop waiting for them.
    never_found = [i for i in range(first_id, last_id + 1) if checker.claim(i)]
    log("info", f"Gap {first_id}-{last_id} filled: {added} trades added, "
                f"{dropped} from the previous session dropped. Session and rolling window metrics recalculated")
    if never_found:
        log("warning", f"Binance has no trades for {len(never_found)} IDs in that gap")


def check_minute(minute, ours, official, spill_in):
    """Compare our candle for a minute with Binance's official 1m kline row.

    Returns (differences, spill_out); differences is empty if they match. Prices must be
    equal; volumes are sums of floats, so they only need to agree to 1e-9.

    An aggregated trade has only its first fill's time, but Binance's candle counts each
    fill in the minute it happened, so the last trade before a boundary can be split
    across it (see AGG_WINDOW_MS). spill_in is (quantity, price) of the previous
    minute's last trade that Binance counts in this minute (None if none), spill_out the
    same for this minute's last trade. A split is only accepted if that trade started
    within AGG_WINDOW_MS of the boundary, the part is smaller than the trade, and its
    price accounts for the USDT volume too; the next minute must then open with it.
    ours is a SessionCandles entry, or None if we saw no trades in that minute.
    """
    volume, quote_volume = float(official[5]), float(official[7])
    if ours is None and spill_in is None:
        return ([] if volume == 0 else [f"we saw no trades, Binance has volume {official[5]}"]), None
    if ours is None:  # only the previous minute's split trade
        quantity, price = spill_in
        open_price = high = low = close = price
        our_volume, pv, close_ms, close_quantity = quantity, quantity * price, 0, 0.0
    else:
        _, open_price, high, low, close_ms, close, pv, our_volume, close_quantity = ours
        if spill_in is not None:  # its fills came before any trade of this minute
            quantity, price = spill_in
            open_price, high, low = price, max(high, price), min(low, price)
            our_volume += quantity
            pv += quantity * price

    spill_out = None
    excess = our_volume - volume
    if (1e-9 * max(our_volume, volume) < excess < close_quantity
            and close_ms >= (minute + 1) * MINUTE_MS - AGG_WINDOW_MS):
        spill_out = (excess, close)
        our_volume -= excess
        pv -= excess * close
    differences = [f"{name} {mine} vs Binance {theirs}"
                   for name, mine, theirs in (("open", open_price, official[1]), ("high", high, official[2]),
                                              ("low", low, official[3]), ("close", close, official[4]))
                   if mine != float(theirs)]
    differences += [f"{name} {mine} vs Binance {theirs}"
                    for name, mine, theirs in (("volume", our_volume, volume), ("USDT volume", pv, quote_volume))
                    if not math.isclose(mine, theirs, rel_tol=1e-9)]
    return differences, (None if differences else spill_out)


class CandleChecker:
    """Checks each finished minute's candle against Binance's official 1m candle.

    It proves the numbers are right while the app runs, not just in the backtest. Only
    minutes built from trades are checked: after the first live trade's minute (the
    ones before come from Binance's own candles). A minute is checked once it's final
    on both sides: Binance's candle has had CHECK_DELAY_S to settle, and ours can't
    still be missing a trade. That needs no IDs missing and a live trade after the
    minute (IDs are checked up to it, so a dead connection can't hide a trade).

    Each minute passes the part of its last trade that Binance counts in the next one
    on to it (see check_minute), so checking starts at a boundary no trade can straddle:
    the first one after the first live trade with no trade in the AGG_WINDOW_MS before it.
    """

    def __init__(self):
        self.first_minute = None  # the first live trade's minute; set by that trade
        self.next_minute = None  # the first minute not checked yet, once a clean start is found
        self.spill = None  # the part of the last checked minute's last trade counted in the next one
        self.checked = 0
        self.matched = 0
        self.splits = 0  # trades Binance's candles split across a minute boundary

    def snapshot(self):
        return {"checked": self.checked, "matched": self.matched, "splits": self.splits}

    def last_ready_minute(self, asset):
        """The last minute that's final on both sides, or None before the first live trade or while incomplete."""
        if self.first_minute is None or asset.last_trade is None or not asset.is_complete():
            return None
        after_last_trade = asset.last_trade["time_ms"] // MINUTE_MS - 1
        settled = (asset.binance_now_ms() - CHECK_DELAY_S * 1000) // MINUTE_MS - 1
        return min(after_last_trade, settled)

    def find_start(self, asset, last):
        """The first minute up to last whose start no trade can straddle, or None if none has come yet."""
        for minute in range(self.first_minute + 1, last + 1):
            if asset.tracker.candles.straddle_free(minute):
                return minute
        return None

    async def check(self, asset, log):
        """Check every minute that's ready (at most 1000 at a time); returns how many were checked."""
        last = self.last_ready_minute(asset)
        if last is None:
            return 0
        if self.next_minute is None:
            self.next_minute = self.find_start(asset, last)
            if self.next_minute is None:
                return 0
        first = self.next_minute
        last = min(last, first + 999)
        if last < first:
            return 0
        candles = await get_json("/fapi/v1/klines", {
            "symbol": asset.symbol, "interval": "1m", "limit": 1000,
            "startTime": first * MINUTE_MS, "endTime": (last + 1) * MINUTE_MS - 1,
        }, log)
        check_list(candles, "/fapi/v1/klines")
        for candle in candles:
            check_candle(candle)  # every row first, so a bad response counts nothing
        checked = 0
        for candle in candles:
            minute = candle[0] // MINUTE_MS
            if not first <= minute <= last:
                continue
            differences, self.spill = check_minute(minute, asset.tracker.candles.get(minute), candle, self.spill)
            checked += 1
            if differences:
                log("warning", f"Candle {format_time(candle[0])[:5]} UTC doesn't match Binance's: "
                               + "; ".join(differences))
            else:
                self.matched += 1
                self.splits += self.spill is not None
        self.checked += checked
        self.next_minute = last + 1
        return checked


async def check_candles(asset, log):
    """Check finished minutes against Binance's candles every CHECK_POLL_S, forever."""
    while True:
        await asyncio.sleep(CHECK_POLL_S)
        await asset.candle_checker.check(asset, log)


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
    """Everything one asset keeps for itself: session totals, rolling window, minute closes, ID checks,
    fetches, last trade, events.

    Every asset uses the same formulas (SessionTracker, TradeIdChecker, the fetch
    functions) but has its own numbers, so a gap in one asset never touches another.
    It lives in LiveFeed, not inside the connection, so it all survives a reconnect.
    """

    def __init__(self, symbol, on_event=None, on_trade=None):
        self.symbol = symbol
        self.on_trade = on_trade  # called with the symbol after each new live trade is counted
        self.event_log = EventLog(symbol, on_event)
        self.tracker = SessionTracker()
        self.window = RollingWindow()
        self.closes = MinuteCloses()
        self.checker = TradeIdChecker()
        self.candle_checker = CandleChecker()
        self.backfill_task = None
        self.fetch_tasks = set()  # keeps running fetch tasks referenced until they finish
        self.check_tasks = set()  # the candle check, which runs for as long as the program
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

    def binance_now_ms(self):
        """Binance's time now, estimated as the last trade's time plus how long ago it arrived.

        Comparing trade times with this PC's clock directly would be off by however
        far the two clocks differ. Needs a last trade.
        """
        return self.last_trade["time_ms"] + now_ms() - self.last_trade["received_ms"]

    def rolling_snapshot(self):
        """The rolling window's metrics, or None before the first trade.

        "warmup_s" is how many seconds are left until the window holds every trade of
        the last ROLLING_WINDOW_S (0 once it does). fill_window usually fills it
        within seconds; until then (or if that fetch keeps failing) it fills with live
        trades, and its numbers would be too low.
        """
        if self.last_trade is None:
            return None
        binance_now_ms = self.binance_now_ms()
        pv, volume, buy_pv, buy_volume, trade_count = self.window.totals(binance_now_ms)
        full_at_s = self.window.complete_from_ms // 1000 + ROLLING_WINDOW_S
        return {
            "warmup_s": max(0, full_at_s - binance_now_ms // 1000),
            "vwap": pv / volume if volume > 0 else None,
            "volume_per_min": {"quote": pv * 60 / ROLLING_WINDOW_S, "base": volume * 60 / ROLLING_WINDOW_S},
            "trades_per_s": trade_count / ROLLING_WINDOW_S,
            "flow": order_flow(pv, volume, buy_pv, buy_volume),
        }

    def volatility_snapshot(self):
        """Volatility over the finished minutes, or None before the first trade.

        "minutes" is how many one-minute returns it uses: VOLATILITY_WINDOW_MIN once the
        startup fetch has filled it, fewer while it fills with live trades instead.
        """
        if self.last_trade is None:
            return None
        series = self.closes.series(self.binance_now_ms() // MINUTE_MS - 1)
        return {"annualized_pct": realized_volatility(series), "minutes": max(0, len(series) - 1)}

    def snapshot(self):
        """This asset's state as plain copied values (no await, so always consistent)."""
        session_start_ms = None
        if self.tracker.next_reset_ms is not None:
            session_start_ms = self.tracker.next_reset_ms - DAY_MS
        vwap = self.tracker.vwap
        distance_pct = None
        if vwap is not None and self.last_trade is not None:
            distance_pct = (self.last_trade["price"] - vwap) / vwap * 100
        tracker = self.tracker
        change_pct = None
        if tracker.open_price is not None and self.last_trade is not None:
            change_pct = (self.last_trade["price"] - tracker.open_price) / tracker.open_price * 100
        session_high, session_low = tracker.candles.high_low()
        return {
            "symbol": self.symbol,
            "base_asset": self.symbol.removesuffix(QUOTE_ASSET),  # the coin: BTC, ETH, XAU
            "quote_asset": QUOTE_ASSET,
            "history": self.history_state(),
            "complete": self.is_complete(),
            "missing_count": len(self.checker.missing_ids),
            "session_start_ms": session_start_ms,
            "last_trade": dict(self.last_trade) if self.last_trade else None,
            "session_open": tracker.open_price,
            "session_change_pct": change_pct,  # how far the last price is from the session's first trade
            "session_high": session_high,
            "session_low": session_low,
            # The latest two candles change live; the chart loads the rest again when the revision changes.
            "chart": {"revision": tracker.candles.revision, "latest": tracker.candles.latest_rows()},
            "candle_check": self.candle_checker.snapshot(),
            "vwap": vwap,
            "vwap_distance_pct": distance_pct,  # how far the last price is above (+) or below (-) VWAP
            # delta here is CVD
            "flow": order_flow(tracker.cumulative_pv, tracker.cumulative_volume, tracker.buy_pv, tracker.buy_volume),
            "rolling": self.rolling_snapshot(),
            "volatility": self.volatility_snapshot(),
        }

    def chart(self):
        """Every candle of the session, with the revision they belong to (see SessionCandles)."""
        return {"symbol": self.symbol, "revision": self.tracker.candles.revision, "candles": self.tracker.candles.rows()}

    def start_task(self, name, function, *args, tasks=None):
        """Run a fetch (or the never-ending candle check) in the background, started again if it fails.

        It's kept in fetch_tasks unless another set is given: that set is empty once every fetch is done.
        """
        tasks = self.fetch_tasks if tasks is None else tasks
        task = asyncio.create_task(retry_on_error(name, self.log, function, *args))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        return task

    def handle_trade(self, trade):
        """Check one live trade's ID, then add it to VWAP if it should be counted."""
        price = float(trade["p"])
        quantity = float(trade["q"])
        trade_time_ms = trade["T"]
        buy = is_buy(trade)

        kind, gap = self.checker.check(trade["a"])
        if kind == "duplicate":
            self.log("warning", f"Duplicate trade {trade['a']} ignored")
            return
        if kind == "late":
            self.window.add(price, quantity, trade_time_ms, buy)
            self.closes.add(price, trade_time_ms)
            if self.tracker.in_session(trade_time_ms):
                self.tracker.add_found_trade(price, quantity, trade_time_ms, buy)
                self.log("info", f"Late trade {trade['a']} arrived and was added")
            return

        self.window.add(price, quantity, trade_time_ms, buy)
        self.closes.add(price, trade_time_ms)
        if self.tracker.add_trade(price, quantity, trade_time_ms, buy):
            session_date = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc).date()
            self.log("info", f"New session: {session_date} (UTC)")
        self.last_trade = {"price": price, "quantity": quantity, "time_ms": trade_time_ms, "received_ms": now_ms()}

        # The first live trade sets the session, so the startup fetch can start now.
        if self.backfill_task is None:
            self.window.complete_from_ms = trade_time_ms
            self.backfill_task = self.start_task("Fetching the session's earlier trades", backfill,
                                                 self.symbol, self.tracker, trade, self.log)
            self.start_task(f"Fetching the last {ROLLING_WINDOW_S} seconds", fill_window,
                            self.symbol, self.window, trade, self.log)
            self.start_task(f"Fetching the last {VOLATILITY_WINDOW_MIN} minutes", fill_closes,
                            self.symbol, self.closes, trade, self.log)
            self.candle_checker.first_minute = trade_time_ms // MINUTE_MS
            self.start_task("Checking candles against Binance", check_candles, self, self.log,
                            tasks=self.check_tasks)
        if gap is not None:
            first_id, last_id = gap
            self.log("info", f"Gap: {last_id - first_id + 1} trades missing (IDs {first_id}-{last_id}); fetching from REST")
            self.start_task(f"Fetching gap {first_id}-{last_id}", fetch_gap,
                            self.symbol, self.tracker, self.window, self.closes, self.checker,
                            first_id, last_id, self.log)

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
