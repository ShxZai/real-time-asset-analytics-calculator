"""Tests for vwap.py. Run with: python -m pytest

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

import vwap

MIDNIGHT_MS = vwap.next_midnight_ms(1_791_000_000_000)  # 00:00 UTC on 2026-10-04


def make_trade(trade_id, price, trade_time_ms, quantity=1):
    """A message shaped like Binance's aggTrade (numbers sent as text, like the real one)."""
    return {"e": "aggTrade", "a": trade_id, "p": str(price), "q": str(quantity), "T": trade_time_ms}


def fake_rest(trades):
    """A stand-in for vwap.get_json that serves these trades as if they were Binance's history."""
    async def get_json(path, params, log):
        if path == "/fapi/v1/klines":
            return []  # no earlier candles: every test session starts in its first minute
        if "fromId" in params:
            rows = [t for t in trades if t["a"] >= params["fromId"]]
        else:
            rows = [t for t in trades if t["T"] >= params["startTime"]]
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

    connections: one list of trades per accepted connection; a string is sent as-is
    (for malformed messages). Every connection but the
    last closes normally after sending its trades, like Binance's 24 hour close.
    refuse_first: how many connection attempts get their handshake refused (HTTP 403).
    """

    def __init__(self, connections, refuse_first=0):
        self.connections = connections
        self.refuse_first = refuse_first
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
            await ws.send(trade if isinstance(trade, str) else json.dumps(trade))
        if self.accepted < len(self.connections):
            return  # returning closes the connection normally
        await ws.wait_closed()  # keep the last connection open until the server shuts down


async def wait_until(condition, timeout_s=5):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout_s)


async def play(fake, done):
    """Run LiveVwap against the fake server until done(live) is true, then stop it.

    Returns the LiveVwap and the list of reconnect waits it asked for.
    """
    async with websockets.serve(fake.handler, "127.0.0.1", 0, process_request=fake.process_request) as server:
        port = server.sockets[0].getsockname()[1]
        live = vwap.LiveVwap(stream_url=f"ws://127.0.0.1:{port}")
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
    c = vwap.TradeIdChecker()
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
    c = vwap.TradeIdChecker()
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
    monkeypatch.setattr(vwap, "get_json", fake_rest(trades))
    fake = FakeBinance([trades[:3], trades[3:]])  # closes normally after trade 2

    live, waits = asyncio.run(play(fake, lambda live: live.checker.highest_id == 4))

    assert fake.accepted == 2
    assert waits == [0]  # reconnected with no wait
    assert live.checker.missing_ids == set()
    assert live.tracker.cumulative_volume == 5  # trades from both connections kept
    assert math.isclose(live.tracker.vwap, 102)  # (100 + 101 + 102 + 103 + 104) / 5


def test_refused_handshake_backs_off_then_connects(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100, t + i) for i in range(3)]
    monkeypatch.setattr(vwap, "get_json", fake_rest(trades))
    fake = FakeBinance([trades], refuse_first=3)

    live, waits = asyncio.run(play(fake, lambda live: live.checker.highest_id == 2))

    assert fake.attempts == 4
    assert waits == [0, 5, 10]
    assert live.tracker.cumulative_volume == 3
    assert sum("refused" in e["message"] for e in live.events) == 3
    assert live.connection["state"] == "connected"


# ---------- gap across midnight (offline, fake server + fake REST) ----------

def test_gap_across_midnight_drops_old_day_and_keeps_new_day(monkeypatch):
    # IDs 0-2 before midnight (prices 1, 2, 3), IDs 3-5 after (prices 4, 5, 6).
    times = [MIDNIGHT_MS - 3000, MIDNIGHT_MS - 2000, MIDNIGHT_MS - 1000,
             MIDNIGHT_MS + 1000, MIDNIGHT_MS + 2000, MIDNIGHT_MS + 3000]
    trades = [make_trade(i, i + 1, times[i]) for i in range(6)]
    monkeypatch.setattr(vwap, "get_json", fake_rest(trades))
    # Live: trade 0, then the connection closes; after reconnecting, trade 5. Gap = 1-4.
    fake = FakeBinance([[trades[0]], [trades[5]]])

    def gap_filled(live):
        return live.checker.highest_id == 5 and not live.checker.missing_ids and not live.fetch_tasks

    live, _ = asyncio.run(play(fake, gap_filled))

    assert live.tracker.next_reset_ms == MIDNIGHT_MS + vwap.DAY_MS  # in the new session
    assert live.tracker.cumulative_volume == 3  # trades 3, 4, 5 only
    assert math.isclose(live.tracker.vwap, 5)  # (4 + 5 + 6) / 3; 4 would mean 1 and 2 leaked in


# ---------- bad messages and failed fetches (offline, fake server + fake REST) ----------

def all_counted(live):
    return not live.checker.missing_ids and not live.fetch_tasks


def test_bad_stream_messages_are_skipped_without_reconnecting(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(3)]
    monkeypatch.setattr(vwap, "get_json", fake_rest(trades))
    # Trade 1 arrives broken (price "nan"), so it's skipped and then fetched as a gap.
    fake = FakeBinance([[trades[0], "not json", ["a", "list"], {**trades[1], "p": "nan"}, trades[2]]])

    live, waits = asyncio.run(play(fake, lambda live: live.checker.highest_id == 2 and all_counted(live)))

    assert fake.accepted == 1 and waits == []  # the connection was never dropped
    assert live.tracker.cumulative_volume == 3
    assert math.isclose(live.tracker.vwap, 101)  # (100 + 101 + 102) / 3


def test_failed_startup_fetch_is_started_again_not_dropped(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(4)]
    # The first startup aggTrades request gets an error object instead of a list of trades.
    error = {"code": -1003, "msg": "Too many requests"}
    monkeypatch.setattr(vwap, "get_json", fail_first(fake_rest(trades), "startTime", error))
    fake = FakeBinance([[trades[3]]])  # starts mid-session: trades 0-2 come from the fetch

    live, _ = asyncio.run(play(fake, lambda live: live.backfill_task is not None and all_counted(live)))

    assert any("Fetching earlier trades failed" in e["message"] for e in live.events)
    assert live.tracker.cumulative_volume == 4
    assert math.isclose(live.tracker.vwap, 101.5)  # (100 + 101 + 102 + 103) / 4


def test_failed_gap_fetch_is_started_again_without_counting_twice(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(4)]
    # The first gap response has a good trade 1 then a broken trade 2, so the fetch
    # fails after adding trade 1; starting again must not add trade 1 a second time.
    bad_page = [trades[1], {**trades[2], "q": "abc"}]
    monkeypatch.setattr(vwap, "get_json", fail_first(fake_rest(trades), "fromId", bad_page))
    fake = FakeBinance([[trades[0], trades[3]]])  # gap 1-2

    live, _ = asyncio.run(play(fake, lambda live: live.checker.highest_id == 3 and all_counted(live)))

    assert live.tracker.cumulative_volume == 4
    assert math.isclose(live.tracker.vwap, 101.5)  # (100 + 101 + 102 + 103) / 4


# ---------- state and events for a display (offline) ----------

def test_snapshot_before_the_first_trade_is_empty_and_waiting():
    snapshot = vwap.LiveVwap().snapshot()
    assert snapshot["history"] == "waiting"
    assert snapshot["vwap"] is None and snapshot["last_trade"] is None and snapshot["session_start_ms"] is None


def test_snapshot_after_a_gap_shows_the_filled_state_and_is_a_copy(monkeypatch):
    t = MIDNIGHT_MS + 60_000
    trades = [make_trade(i, 100 + i, t + i) for i in range(4)]
    monkeypatch.setattr(vwap, "get_json", fake_rest(trades))
    fake = FakeBinance([[trades[0]], [trades[3]]])  # gap 1-2 across a reconnect

    live, _ = asyncio.run(play(fake, lambda live: live.checker.highest_id == 3 and all_counted(live)))
    snapshot = live.snapshot()

    assert snapshot["connection"]["state"] == "connected"
    assert snapshot["history"] == "done" and snapshot["missing_count"] == 0
    assert snapshot["session_start_ms"] == MIDNIGHT_MS
    assert snapshot["last_trade"]["price"] == 103 and snapshot["last_trade"]["time_ms"] == t + 3
    assert math.isclose(snapshot["vwap"], 101.5)  # (100 + 101 + 102 + 103) / 4
    messages = [e["message"] for e in snapshot["events"]]
    assert any(m.startswith("Gap: 2 trades missing") for m in messages)
    assert any(m.startswith("Gap 1-2 filled") for m in messages)
    ids = [e["id"] for e in snapshot["events"]]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)

    snapshot["connection"]["state"] = "changed"
    snapshot["events"].clear()
    assert live.connection["state"] == "connected" and live.events  # the engine wasn't touched


def test_event_list_keeps_only_the_latest_events():
    live = vwap.LiveVwap()
    seen = []
    live.on_event = seen.append
    for i in range(vwap.MAX_EVENTS + 100):
        live.log("info", f"event {i}")
    assert len(seen) == vwap.MAX_EVENTS + 100  # the callback sees every event
    assert len(live.events) == vwap.MAX_EVENTS
    assert live.events[0]["id"] == 100  # the oldest 100 were dropped


# ---------- checking data and REST retries (offline) ----------

def test_check_trade_rejects_values_that_would_break_vwap():
    good = make_trade(7, 100, MIDNIGHT_MS)
    vwap.check_trade(good)
    bad_fields = [{"a": True}, {"a": -1}, {"a": 7.0}, {"T": -5}, {"T": vwap.MAX_TRADE_TIME_MS},
                  {"p": "0"}, {"p": "inf"}, {"q": "-1"}, {"q": None}]
    for bad in bad_fields:
        with pytest.raises(ValueError):
            vwap.check_trade({**good, **bad})


def test_check_candle_allows_quiet_minutes_but_not_broken_numbers():
    candle = [MIDNIGHT_MS, "1", "1", "1", "1", "0", MIDNIGHT_MS + 59_999, "0", 0]  # no trades that minute
    vwap.check_candle(candle)
    for index, bad in [(5, "nan"), (7, "abc"), (6, None)]:
        with pytest.raises(ValueError):
            vwap.check_candle(candle[:index] + [bad] + candle[index + 1:])
    with pytest.raises(ValueError):
        vwap.check_candle({"code": -1121, "msg": "Invalid symbol."})


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
    monkeypatch.setattr(vwap, "fetch_json", fetch_json)
    waits = []

    async def record_sleep(seconds):
        waits.append(seconds)
    monkeypatch.setattr(vwap.asyncio, "sleep", record_sleep)

    logged = []
    assert asyncio.run(vwap.get_json("/fapi/v1/aggTrades", {}, lambda *event: logged.append(event))) == [1, 2]
    assert len(logged) == 4 and all(level == "warning" for level, _ in logged)
    # 429 says 30 s; 418 without a header waits the maximum; a cut-off response is
    # retried like a network error; a plain 500 goes back to the normal backoff.
    assert waits == [30, vwap.MAX_RETRY_WAIT_S, 10, 20]


# ---------- live tests (real Binance) ----------

def official_totals(session_start_ms, boundary_ms):
    """Sum Binance's closed 1m candles from session start up to (not including) boundary."""
    candles = vwap.fetch_json("/fapi/v1/klines", {
        "symbol": vwap.SYMBOL, "interval": "1m", "limit": 1500,
        "startTime": session_start_ms, "endTime": boundary_ms - 1})
    candles = [c for c in candles if c[6] < boundary_ms]
    return sum(float(c[7]) for c in candles), sum(float(c[5]) for c in candles)


def assert_matches_official(tracker, boundary_ms):
    pv, volume = official_totals(tracker.next_reset_ms - vwap.DAY_MS, boundary_ms)
    assert math.isclose(tracker.cumulative_pv, pv, rel_tol=1e-9)
    assert math.isclose(tracker.cumulative_volume, volume, rel_tol=1e-9)
    assert math.isclose(tracker.vwap, pv / volume, rel_tol=1e-9)


async def receive_until_boundary(live, ws, ready):
    """Feed live trades in until ready() is true, then stop at the next minute boundary.

    The trade that crosses the boundary is not added, so our totals cover exactly
    the same span as the closed candles. Returns the boundary time.
    """
    boundary_ms = None
    async for message in ws:
        trade = json.loads(message)
        if boundary_ms is None and ready():
            boundary_ms = trade["T"] - trade["T"] % vwap.MINUTE_MS + vwap.MINUTE_MS
        if boundary_ms is not None and trade["T"] >= boundary_ms:
            return boundary_ms
        live.handle_trade(trade)


def test_live_startup_fetch_matches_official_candles():
    async def scenario():
        live = vwap.LiveVwap()
        async with websockets.connect(vwap.STREAM_URL) as ws:
            boundary_ms = await receive_until_boundary(
                live, ws, lambda: live.backfill_task is not None and live.backfill_task.done())
        await asyncio.sleep(3)  # let the last candle close on Binance's side
        return live, boundary_ms

    live, boundary_ms = asyncio.run(scenario())
    assert_matches_official(live.tracker, boundary_ms)


def test_live_forced_reconnect_fills_gap_and_matches_official_candles():
    async def scenario():
        live = vwap.LiveVwap()
        async with websockets.connect(vwap.STREAM_URL) as ws:
            async for message in ws:
                live.handle_trade(json.loads(message))
                if live.backfill_task.done():
                    break
        highest_before = live.checker.highest_id
        await asyncio.sleep(10)  # offline on purpose; Binance keeps trading

        handled_after = []
        async with websockets.connect(vwap.STREAM_URL) as ws:
            def gap_filled():
                handled_after.append(True)
                return len(handled_after) > 1 and not live.checker.missing_ids and not live.fetch_tasks
            boundary_ms = await receive_until_boundary(live, ws, gap_filled)
        await asyncio.sleep(3)
        return live, boundary_ms, highest_before

    live, boundary_ms, highest_before = asyncio.run(scenario())
    assert live.checker.highest_id > highest_before + 1, "expected trades during the 10 s offline"
    assert_matches_official(live.tracker, boundary_ms)
