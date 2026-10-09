"""Tests for metrics.py. Run with: python -m pytest  (offline only: -m "not live")

Offline tests use test doubles: a fake WebSocket server on this machine stands in
for Binance's stream, and a fake get_json returns trades we wrote ourselves instead
of calling the REST API. The live tests at the bottom talk to the real Binance and
compare our totals with its official 1 minute candles (each takes up to ~1 minute).
"""

import asyncio
import contextlib
import http.client
import json
import math
import urllib.error
from http import HTTPStatus

import pytest
import websockets

import metrics

MIDNIGHT_MS = metrics.next_midnight_ms(1_791_000_000_000)  # 00:00 UTC on 2026-10-04


def make_trade(trade_id, price, trade_time_ms, quantity=1, symbol="XAUUSDT", is_buy=True):
    """A trade shaped like Binance's aggTrade (numbers sent as text, like the real one)."""
    return {"e": "aggTrade", "s": symbol, "a": trade_id, "p": str(price), "q": str(quantity),
            "T": trade_time_ms, "m": not is_buy}


def make_candle(open_time_ms, volume, quote_volume, buy_volume, buy_quote_volume):
    """A 1m kline row shaped like Binance's (numbers sent as text)."""
    return [open_time_ms, "1", "1", "1", "1", str(volume), open_time_ms + metrics.MINUTE_MS - 1,
            str(quote_volume), 0, str(buy_volume), str(buy_quote_volume), "0"]


def fake_rest(trades):
    """A stand-in for metrics.get_json that serves trades as if they were Binance's history.

    trades: a list (all one symbol), or {symbol: list} when several assets are tested.
    """
    async def get_json(path, params, log):
        if path == "/fapi/v1/klines":
            return []  # no earlier candles: every test session starts in its first minute
        rows = trades[params["symbol"]] if isinstance(trades, dict) else trades
        if "fromId" in params:
            rows = [t for t in rows if t["a"] >= params["fromId"]]
        else:
            rows = [t for t in rows if t["T"] >= params["startTime"]]
        return rows[: params["limit"]]
    return get_json


def fail_first(get_json, request_kind, bad_response):
    """Wrap a fake get_json so the first aggTrades request of one kind gets bad_response.

    request_kind is "startTime" (the startup fetch) or "fromId" (a gap fetch).
    """
    failed = []

    async def wrapped(path, params, log):
        if path == "/fapi/v1/aggTrades" and request_kind in params and not failed:
            failed.append(True)
            return bad_response
        return await get_json(path, params, log)
    return wrapped


class FakeBinance:
    """A local WebSocket server that plays out a script of connections.

    connections: one list of trades per accepted connection. Each trade (dict) is
    wrapped like Binance's combined stream ({"stream": ..., "data": trade}); a string
    is sent as-is and anything else as plain JSON (for malformed messages). Every connection but the
    last closes normally after sending its trades, like Binance's 24 hour close.
    refuse_first: how many connection attempts get their handshake refused (HTTP 403).
    deaf_first: how many accepted connections go silent after their trades: no close
    and no pong, like a cut network.
    """

    def __init__(self, connections, refuse_first=0, deaf_first=0):
        self.connections = connections
        self.refuse_first = refuse_first
        self.deaf_first = deaf_first
        self.attempts = 0
        self.accepted = 0

    def process_request(self, connection, request):
        self.attempts += 1
        if self.attempts <= self.refuse_first:
            return connection.respond(HTTPStatus.FORBIDDEN, "refused\n")
        return None  # carry on with the normal handshake

    async def handler(self, ws):
        trades = self.connections[self.accepted]
        self.accepted += 1
        for trade in trades:
            if isinstance(trade, dict):
                trade = combined(trade)
            await ws.send(trade if isinstance(trade, str) else json.dumps(trade))
        if self.accepted <= self.deaf_first:
            ws.transport.pause_reading()  # pings aren't read, so aren't answered
            await asyncio.sleep(1)  # longer than the test's ping settings take to give up
            ws.transport.resume_reading()  # now notice the engine has gone, so shutdown is quick
            await ws.wait_closed()
            return
        if self.accepted < len(self.connections):
            return  # returning closes the connection normally
        await ws.wait_closed()  # keep the last connection open until the server shuts down


def combined(trade):
    """Wrap a trade the way Binance's combined stream does."""
    return {"stream": f"{trade['s'].lower()}@aggTrade", "data": trade}


def xau(live):
    """The XAUUSDT asset: the single-asset tests run a feed with only this symbol."""
    return live.assets["XAUUSDT"]


async def wait_until(condition, timeout_s=5):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


async def play(fake, done, symbols=("XAUUSDT",), **live_settings):
    """Run a LiveFeed against the fake server until done(live) is true, then stop it.

    Returns the LiveFeed and the list of reconnect waits it asked for.
    """
    async with websockets.serve(fake.handler, "127.0.0.1", 0, process_request=fake.process_request) as server:
        port = server.sockets[0].getsockname()[1]
        live = metrics.LiveFeed(symbols, stream_url=f"ws://127.0.0.1:{port}", **live_settings)
        waits = []

        async def record_wait(seconds):
            waits.append(seconds)
            await asyncio.sleep(0)  # don't actually wait, just let other tasks run

        live.sleep = record_wait
        run_task = asyncio.create_task(live.run())
        try:
            await wait_until(lambda: done(live))
        finally:
            for task in [run_task, *live.fetch_tasks]:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task
        return live, waits


# ---------- ID checker (offline) ----------

def test_id_checker_classifies_new_late_and_duplicate():
    c = metrics.TradeIdChecker()
    assert c.check(100) == ("new", None)
    assert c.check(101) == ("new", None)
    assert c.check(101) == ("duplicate", None)
    assert c.check(105) == ("new", (102, 104))
    assert c.missing_ids == {102, 103, 104}
    assert c.check(103) == ("late", None)
    assert c.missing_ids == {102, 104}
    assert c.check(103) == ("duplicate", None)  # a late arrival repeated
    assert c.check(99) == ("duplicate", None)  # older than anything, never missing
    assert c.highest_id == 105


def test_fetch_and_late_arrival_never_count_twice():
    c = metrics.TradeIdChecker()
    c.check(1)
    c.check(5)  # gap 2-4
    assert c.claim(2) is True  # the fetch counts 2...
    assert c.check(2) == ("duplicate", None)  # ...so the stream's copy is ignored
    assert c.check(3) == ("late", None)  # the stream counts 3...
    assert c.claim(3) is False  # ...so the fetch skips it
    assert c.missing_ids == {4}


# ---------- reconnecting (offline, fake server) ----------

def test_normal_close_reconnects_straight_away_and_keeps_state(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(5)]
    monkeypatch.setattr(metrics, "get_json", fake_rest(trades))
    fake = FakeBinance([trades[:3], trades[3:]])  # closes normally after trade 2

    live, waits = asyncio.run(play(fake, lambda live: xau(live).checker.highest_id == 4))

    assert fake.accepted == 2
    assert waits == [0]  # reconnected with no wait
    assert xau(live).checker.missing_ids == set()
    assert xau(live).tracker.cumulative_volume == 5  # trades from both connections kept
    assert math.isclose(xau(live).tracker.vwap, 102)  # (100 + 101 + 102 + 103 + 104) / 5


def test_refused_handshake_backs_off_then_connects(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100, t + i) for i in range(3)]
    monkeypatch.setattr(metrics, "get_json", fake_rest(trades))
    fake = FakeBinance([trades], refuse_first=3)

    live, waits = asyncio.run(play(fake, lambda live: xau(live).checker.highest_id == 2))

    assert fake.attempts == 4
    assert waits == [0, 5, 10]
    assert xau(live).tracker.cumulative_volume == 3
    assert sum("refused" in e["message"] for e in live.events()) == 3
    assert live.connection["state"] == "connected"


def test_connection_that_stops_answering_pings_is_dropped_and_reconnected(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100, t + i) for i in range(3)]
    monkeypatch.setattr(metrics, "get_json", fake_rest(trades))
    fake = FakeBinance([trades[:2], trades[2:]], deaf_first=1)  # first connection goes silent

    live, waits = asyncio.run(play(fake, lambda live: xau(live).checker.highest_id == 2,
                                   ping_interval_s=0.2, ping_timeout_s=0.2, close_timeout_s=0.2))

    assert fake.accepted == 2
    assert any("ping timeout" in e["message"] for e in live.events())
    states = [e["message"] for e in live.events() if e["message"].startswith("Connection dropped")]
    assert len(states) == 1
    assert waits == [0]
    assert xau(live).tracker.cumulative_volume == 3
    assert live.connection["state"] == "connected"


# ---------- gap across midnight (offline, fake server + fake REST) ----------

def test_gap_across_midnight_drops_old_day_and_keeps_new_day(monkeypatch):
    # IDs 0-2 before midnight (prices 1, 2, 3), IDs 3-5 after (prices 4, 5, 6).
    times = [MIDNIGHT_MS - 3000, MIDNIGHT_MS - 2000, MIDNIGHT_MS - 1000,
             MIDNIGHT_MS + 1000, MIDNIGHT_MS + 2000, MIDNIGHT_MS + 3000]
    trades = [make_trade(i, i + 1, times[i]) for i in range(6)]
    monkeypatch.setattr(metrics, "get_json", fake_rest(trades))
    # Live: trade 0, then the connection closes; after reconnecting, trade 5. Gap = 1-4.
    fake = FakeBinance([[trades[0]], [trades[5]]])

    def gap_filled(live):
        return xau(live).checker.highest_id == 5 and not xau(live).checker.missing_ids and not live.fetch_tasks

    live, _ = asyncio.run(play(fake, gap_filled))

    assert xau(live).tracker.next_reset_ms == MIDNIGHT_MS + metrics.DAY_MS  # in the new session
    assert xau(live).tracker.cumulative_volume == 3  # trades 3, 4, 5 only
    assert math.isclose(xau(live).tracker.vwap, 5)  # (4 + 5 + 6) / 3; 4 would mean 1 and 2 leaked in


# ---------- bad messages and failed fetches (offline, fake server + fake REST) ----------

def all_counted(live):
    return not xau(live).checker.missing_ids and not live.fetch_tasks


def test_bad_stream_messages_are_skipped_without_reconnecting(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(3)]
    monkeypatch.setattr(metrics, "get_json", fake_rest(trades))
    # Trade 1 arrives broken (price "nan"), so it's skipped and then fetched as a gap.
    # A trade for a symbol this feed doesn't track is skipped too.
    other = make_trade(50, 999, t, symbol="DOGEUSDT")
    fake = FakeBinance([[trades[0], "not json", ["a", "list"], other, {**trades[1], "p": "nan"}, trades[2]]])

    live, waits = asyncio.run(play(fake, lambda live: xau(live).checker.highest_id == 2 and all_counted(live)))

    assert fake.accepted == 1 and waits == []  # the connection was never dropped
    assert xau(live).tracker.cumulative_volume == 3
    assert math.isclose(xau(live).tracker.vwap, 101)  # (100 + 101 + 102) / 3
    # Unreadable or untracked messages go to the system log; a bad XAUUSDT trade to XAUUSDT's.
    system_skips = [e for e in live.system.events if e["message"].startswith("Skipped")]
    asset_skips = [e for e in xau(live).event_log.events if e["message"].startswith("Skipped")]
    assert len(system_skips) == 3 and len(asset_skips) == 1


def test_failed_startup_fetch_is_started_again_not_dropped(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(4)]
    # The first startup aggTrades request gets an error object instead of a list of trades.
    error = {"code": -1003, "msg": "Too many requests"}
    monkeypatch.setattr(metrics, "get_json", fail_first(fake_rest(trades), "startTime", error))
    fake = FakeBinance([[trades[3]]])  # starts mid-session: trades 0-2 come from the fetch

    live, _ = asyncio.run(play(fake, lambda live: xau(live).backfill_task is not None and all_counted(live)
                               and not live.fetch_tasks))

    # Whichever startup fetch (session or rolling window) got the error starts again.
    assert any("failed" in e["message"] and "starting it again" in e["message"] for e in live.events())
    assert xau(live).tracker.cumulative_volume == 4
    assert math.isclose(xau(live).tracker.vwap, 101.5)  # (100 + 101 + 102 + 103) / 4
    assert xau(live).window.totals(t + 3)[1] == 4  # the window has every trade once


def test_failed_gap_fetch_is_started_again_without_counting_twice(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(4)]
    # The first gap response has a good trade 1 then a broken trade 2, so the fetch
    # fails after adding trade 1; starting again must not add trade 1 a second time.
    bad_page = [trades[1], {**trades[2], "q": "abc"}]
    monkeypatch.setattr(metrics, "get_json", fail_first(fake_rest(trades), "fromId", bad_page))
    fake = FakeBinance([[trades[0], trades[3]]])  # gap 1-2

    live, _ = asyncio.run(play(fake, lambda live: xau(live).checker.highest_id == 3 and all_counted(live)))

    assert xau(live).tracker.cumulative_volume == 4
    assert math.isclose(xau(live).tracker.vwap, 101.5)  # (100 + 101 + 102 + 103) / 4


# ---------- state and events for a display (offline) ----------

def test_snapshot_before_the_first_trade_is_empty_and_waiting():
    snapshot = metrics.LiveFeed().snapshot()
    assert [asset["symbol"] for asset in snapshot["assets"]] == metrics.SYMBOLS
    for asset in snapshot["assets"]:
        assert asset["history"] == "waiting"
        assert asset["vwap"] is None and asset["last_trade"] is None and asset["session_start_ms"] is None
        assert asset["vwap_distance_pct"] is None and asset["rolling"] is None
        assert asset["flow"]["quote"] == {"buy": 0, "sell": 0, "delta": 0}
    assert [(a["base_asset"], a["quote_asset"]) for a in snapshot["assets"]] == [
        ("BTC", "USDT"), ("ETH", "USDT"), ("XAU", "USDT")]


def test_rolling_window_counts_the_last_60_seconds_by_trade_time():
    w = metrics.RollingWindow()
    w.add(100, 1, 10_000, True)  # second 10
    w.add(200, 2, 69_500, False)  # second 69
    # [pv, volume, buy_pv, buy_volume, trade_count]
    assert w.totals(69_999) == [500, 3, 100, 1, 2]  # seconds 10-69
    assert w.totals(70_000) == [400, 2, 0, 0, 1]  # seconds 11-70: second 10 has left
    w.add(300, 1, 10_100, True)  # late, but second 10 is still kept
    w.add(150, 2, 69_000, True)  # out of order: lands in its own second
    assert w.totals(69_999) == [1100, 6, 700, 4, 4]
    w.add(100, 1, 71_000, True)  # the window moves past seconds 10 and 11, which are dropped
    assert 10 not in w.buckets
    w.add(999, 1, 11_000, True)  # too old to be in any window from now on: ignored
    assert w.totals(71_000) == [800, 5, 400, 3, 3]
    assert w.totals(200_000) == [0, 0, 0, 0, 0]  # quiet market: the window empties with time


def test_rolling_metrics_warm_up_from_the_first_live_trade_until_the_fetch_fills_them():
    asset = metrics.AssetTracker("XAUUSDT")
    asset.window.complete_from_ms = 1_000_500  # first live trade; the startup fetch hasn't finished
    asset.window.add(100, 1, 1_000_500, True)
    asset.last_trade = {"price": 100, "quantity": 1, "time_ms": 1_020_200, "received_ms": metrics.now_ms()}
    # Binance's time is about 1 020 200: second 1020, and the window is full from second 1060.
    assert asset.rolling_snapshot()["warmup_s"] == 40
    asset.window.complete_from_ms = metrics.window_fetch_start_ms(1_000_500)  # the fetch has finished
    assert asset.rolling_snapshot()["warmup_s"] == 0


def test_startup_fetches_sum_the_session_from_candles_and_fill_the_window_with_trades(monkeypatch):
    first_live = make_trade(12, 100, MIDNIGHT_MS + 2 * metrics.MINUTE_MS + 500)
    candles = [make_candle(MIDNIGHT_MS, 5, 500, 2, 200), make_candle(MIDNIGHT_MS + metrics.MINUTE_MS, 0, 0, 0, 0)]
    trades = [make_trade(9, 100, MIDNIGHT_MS + 90_000, quantity=5),  # in the second candle: window only
              make_trade(10, 100, first_live["T"] - 200, quantity=2, is_buy=False),
              make_trade(11, 100, first_live["T"] - 100, quantity=3), first_live]
    rest = fake_rest(trades)

    async def get_json(path, params, log):
        return candles if path == "/fapi/v1/klines" else await rest(path, params, log)

    monkeypatch.setattr(metrics, "get_json", get_json)
    result = asyncio.run(metrics.fetch_earlier_trades("XAUUSDT", MIDNIGHT_MS, first_live, lambda *_: None))
    # pv, volume, buy pv (200 + 300), buy volume (2 + 3), candles, trades; trade 9 isn't counted twice
    assert result == (1000, 10, 500, 5, 2, 2)

    # The window gets the trades of the last minute before the first live trade, whichever candle they're in.
    window = metrics.RollingWindow()
    logged = []
    asyncio.run(metrics.fill_window("XAUUSDT", window, first_live, lambda *event: logged.append(event)))
    assert window.totals(first_live["T"]) == [1000, 10, 800, 8, 3]
    assert window.complete_from_ms == metrics.window_fetch_start_ms(first_live["T"])
    assert logged[-1] == ("info", "Added 3 trades. 60 second rolling window metrics calculated")


def test_snapshot_after_a_gap_shows_the_filled_state_and_is_a_copy(monkeypatch):
    t = MIDNIGHT_MS + 80_000
    # Quantities 1-4; trades 1 and 3 are sells. Trade 0 is 70 s older than the rest.
    # The fake REST server has only these trades, so the startup fetch adds none to the window.
    trades = [make_trade(i, 100 + i, t + i - (70_000 if i == 0 else 0), quantity=i + 1, is_buy=i % 2 == 0)
              for i in range(4)]
    monkeypatch.setattr(metrics, "get_json", fake_rest(trades))
    fake = FakeBinance([[trades[0]], [trades[3]]])  # gap 1-2 across a reconnect

    live, _ = asyncio.run(play(fake, lambda live: xau(live).checker.highest_id == 3 and all_counted(live)))
    snapshot = live.snapshot()
    asset = snapshot["assets"][0]

    assert snapshot["connection"]["state"] == "connected"
    assert asset["symbol"] == "XAUUSDT"
    assert asset["history"] == "done" and asset["missing_count"] == 0
    assert asset["session_start_ms"] == MIDNIGHT_MS
    assert asset["last_trade"]["price"] == 103 and asset["last_trade"]["time_ms"] == t + 3
    session_vwap = (100 * 1 + 101 * 2 + 102 * 3 + 103 * 4) / 10
    assert math.isclose(asset["vwap"], session_vwap)
    assert math.isclose(asset["vwap_distance_pct"], (103 - session_vwap) / session_vwap * 100)
    # Buys are trades 0 and 2: 100 x 1 + 102 x 3 = 406 USDT, 1 + 3 = 4 coins.
    assert asset["flow"]["quote"] == {"buy": 406, "sell": 1020 - 406, "delta": 406 - 614}
    assert asset["flow"]["base"] == {"buy": 4, "sell": 6, "delta": -2}

    # Trade 0 is outside the last 60 s, so the rolling window has only trades 1-3 (the gap fill
    # included). The startup fetch has finished, so the window is full.
    rolling = asset["rolling"]
    assert rolling["warmup_s"] == 0
    assert math.isclose(rolling["vwap"], (101 * 2 + 102 * 3 + 103 * 4) / 9)
    assert rolling["flow"]["quote"] == {"buy": 306, "sell": 614, "delta": -308}
    assert rolling["flow"]["base"] == {"buy": 3, "sell": 6, "delta": -3}
    assert rolling["volume_per_min"] == {"quote": 920, "base": 9}
    assert rolling["trades_per_s"] == 3 / 60

    # The gap is in the XAUUSDT log; connection events are in the system log.
    messages = [e["message"] for e in xau(live).event_log.events]
    assert any(m.startswith("Gap: 2 trades missing") for m in messages)
    assert any(m.startswith("Gap 1-2 filled") for m in messages)
    assert all(e["symbol"] is None for e in live.system.events)
    assert any(e["message"] == "Connected" for e in live.system.events)
    for log in (live.system, xau(live).event_log):
        ids = [e["id"] for e in log.events]
        assert ids == list(range(len(ids)))  # each log counts its own IDs from 0
    merged = live.events()
    assert [e["time_ms"] for e in merged] == sorted(e["time_ms"] for e in merged)

    snapshot["connection"]["state"] = "changed"
    asset["last_trade"]["price"] = 0
    assert live.connection["state"] == "connected"  # the engine wasn't touched
    assert xau(live).last_trade["price"] == 103


def test_each_log_keeps_only_its_own_latest_events():
    live = metrics.LiveFeed(["BTCUSDT", "XAUUSDT"])
    seen = []
    live.on_event = seen.append  # set after the feed is made, like the server does
    for i in range(metrics.MAX_EVENTS + 100):
        live.assets["BTCUSDT"].log("info", f"event {i}")
    live.assets["XAUUSDT"].log("info", "quiet asset")
    live.log("info", "system event")
    assert len(seen) == metrics.MAX_EVENTS + 102  # the callback sees every event, from every log
    btc = live.assets["BTCUSDT"].event_log.events
    assert len(btc) == metrics.MAX_EVENTS
    assert btc[0]["id"] == 100  # the oldest 100 were dropped
    # A busy asset can't push another log's events out, and each log numbers from 0.
    assert [(e["symbol"], e["id"]) for e in live.assets["XAUUSDT"].event_log.events] == [("XAUUSDT", 0)]
    assert [(e["symbol"], e["id"]) for e in live.system.events] == [(None, 0)]


# ---------- several assets on one connection (offline) ----------

def test_two_assets_on_one_connection_keep_separate_totals_ids_and_gaps(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    btc = [make_trade(i, 1000 + i, t + i, symbol="BTCUSDT") for i in range(4)]
    eth = [make_trade(i, 10 + i, t + i, symbol="ETHUSDT") for i in range(3)]  # same IDs as BTC
    monkeypatch.setattr(metrics, "get_json", fake_rest({"BTCUSDT": btc, "ETHUSDT": eth}))
    # Interleaved on one connection; BTC skips IDs 1-2, ETH has no gap.
    fake = FakeBinance([[btc[0], eth[0], eth[1], btc[3], eth[2]]])

    def done(live):
        return (live.assets["BTCUSDT"].checker.highest_id == 3 and live.assets["ETHUSDT"].checker.highest_id == 2
                and not live.assets["BTCUSDT"].checker.missing_ids and not live.fetch_tasks)

    live, _ = asyncio.run(play(fake, done, symbols=("BTCUSDT", "ETHUSDT")))
    btc_asset, eth_asset = live.assets["BTCUSDT"], live.assets["ETHUSDT"]

    assert btc_asset.tracker.cumulative_volume == 4
    assert math.isclose(btc_asset.tracker.vwap, 1001.5)  # (1000 + 1001 + 1002 + 1003) / 4
    assert eth_asset.tracker.cumulative_volume == 3
    assert math.isclose(eth_asset.tracker.vwap, 11)  # (10 + 11 + 12) / 3; any BTC mixed in would be far off
    # ETH IDs that match BTC IDs weren't taken as duplicates, and the BTC gap stayed in BTC.
    assert not any("Duplicate" in e["message"] for e in live.events())
    assert any(e["message"].startswith("Gap: 2 trades missing") for e in btc_asset.event_log.events)
    assert not any(e["message"].startswith("Gap") for e in eth_asset.event_log.events)


# ---------- checking data and REST retries (offline) ----------

def test_check_trade_rejects_values_that_would_break_vwap():
    good = make_trade(7, 100, MIDNIGHT_MS)
    metrics.check_trade(good)
    bad_fields = [{"a": True}, {"a": -1}, {"a": 7.0}, {"T": -5}, {"T": metrics.MAX_TRADE_TIME_MS},
                  {"p": "0"}, {"p": "inf"}, {"q": "-1"}, {"q": None}, {"m": None}, {"m": "false"}]
    for bad in bad_fields:
        with pytest.raises(ValueError):
            metrics.check_trade({**good, **bad})


def test_check_candle_allows_quiet_minutes_but_not_broken_numbers():
    candle = make_candle(MIDNIGHT_MS, 0, 0, 0, 0)  # no trades that minute
    metrics.check_candle(candle)
    with pytest.raises(ValueError):
        metrics.check_candle(candle[:10])  # no taker buy quote volume
    for index, bad in [(5, "nan"), (7, "abc"), (6, None), (9, "-1"), (10, "x")]:
        with pytest.raises(ValueError):
            metrics.check_candle(candle[:index] + [bad] + candle[index + 1:])
    with pytest.raises(ValueError):
        metrics.check_candle({"code": -1121, "msg": "Invalid symbol."})


def http_error(code, retry_after=None):
    headers = http.client.HTTPMessage()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError("https://fapi.binance.com", code, "error", headers, None)


def test_get_json_waits_as_long_as_binance_asks_when_rate_limited(monkeypatch):
    errors = [http_error(429, "30"), http_error(418, None), http.client.IncompleteRead(b""), http_error(500)]

    def fetch_json(path, params):
        if errors:
            raise errors.pop(0)
        return [1, 2]
    monkeypatch.setattr(metrics, "fetch_json", fetch_json)
    waits = []

    async def record_sleep(seconds):
        waits.append(seconds)
    monkeypatch.setattr(metrics.asyncio, "sleep", record_sleep)

    logged = []
    assert asyncio.run(metrics.get_json("/fapi/v1/aggTrades", {}, lambda *event: logged.append(event))) == [1, 2]
    assert len(logged) == 4 and all(level == "warning" for level, _ in logged)
    # 429 says 30 s; 418 without a header waits the maximum; a cut-off response is
    # retried like a network error; a plain 500 goes back to the normal backoff.
    assert waits == [30, metrics.MAX_RETRY_WAIT_S, 10, 20]


# ---------- live tests (real Binance) ----------
# Run these on your own machine: python -m pytest -m live
# Binance blocks US connections, so from the US connect through a VPN first.
# CI skips them for the same reason (GitHub's runners are in the US).

LIVE_SYMBOL = "XAUUSDT"
LIVE_URL = metrics.combined_stream_url([LIVE_SYMBOL])


def official_totals(session_start_ms, boundary_ms):
    """Sum Binance's closed 1m candles from session start up to (not including) boundary."""
    candles = metrics.fetch_json("/fapi/v1/klines", {
        "symbol": LIVE_SYMBOL, "interval": "1m", "limit": 1500,
        "startTime": session_start_ms, "endTime": boundary_ms - 1})
    candles = [c for c in candles if c[6] < boundary_ms]
    # pv, volume, taker buy quote volume, taker buy volume
    return [sum(float(c[i]) for c in candles) for i in (7, 5, 10, 9)]


def assert_matches_official(tracker, boundary_ms):
    pv, volume, buy_pv, buy_volume = official_totals(tracker.next_reset_ms - metrics.DAY_MS, boundary_ms)
    assert math.isclose(tracker.cumulative_pv, pv, rel_tol=1e-9)
    assert math.isclose(tracker.cumulative_volume, volume, rel_tol=1e-9)
    assert math.isclose(tracker.buy_pv, buy_pv, rel_tol=1e-9)
    assert math.isclose(tracker.buy_volume, buy_volume, rel_tol=1e-9)
    assert math.isclose(tracker.vwap, pv / volume, rel_tol=1e-9)


async def receive_until_boundary(asset, ws, ready):
    """Feed live trades in until ready() is true, then stop at the next minute boundary.

    The trade that crosses the boundary is not added, so our totals cover exactly
    the same span as the closed candles. Returns the boundary time.
    """
    boundary_ms = None
    async for message in ws:
        trade = json.loads(message)["data"]
        if boundary_ms is None and ready():
            boundary_ms = trade["T"] - trade["T"] % metrics.MINUTE_MS + metrics.MINUTE_MS
        if boundary_ms is not None and trade["T"] >= boundary_ms:
            return boundary_ms
        asset.handle_trade(trade)


@pytest.mark.live
def test_live_startup_fetch_matches_official_candles():
    async def scenario():
        live = metrics.AssetTracker(LIVE_SYMBOL)
        async with websockets.connect(LIVE_URL) as ws:
            boundary_ms = await receive_until_boundary(
                live, ws, lambda: live.backfill_task is not None and live.backfill_task.done())
        await asyncio.sleep(3)  # let the last candle close on Binance's side
        return live, boundary_ms

    live, boundary_ms = asyncio.run(scenario())
    assert_matches_official(live.tracker, boundary_ms)


@pytest.mark.live
def test_live_forced_reconnect_fills_gap_and_matches_official_candles():
    async def scenario():
        live = metrics.AssetTracker(LIVE_SYMBOL)
        async with websockets.connect(LIVE_URL) as ws:
            async for message in ws:
                live.handle_trade(json.loads(message)["data"])
                if live.backfill_task.done():
                    break
        highest_before = live.checker.highest_id
        await asyncio.sleep(10)  # offline on purpose; Binance keeps trading

        handled_after = []
        async with websockets.connect(LIVE_URL) as ws:
            def gap_filled():
                handled_after.append(True)
                return len(handled_after) > 1 and not live.checker.missing_ids and not live.fetch_tasks
            boundary_ms = await receive_until_boundary(live, ws, gap_filled)
        await asyncio.sleep(3)
        return live, boundary_ms, highest_before

    live, boundary_ms, highest_before = asyncio.run(scenario())
    assert live.checker.highest_id > highest_before + 1, "expected trades during the 10 s offline"
    assert_matches_official(live.tracker, boundary_ms)
