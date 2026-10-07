"""Tests for vwap.py. Run with: python -m pytest

Offline tests use test doubles: a fake WebSocket server on this machine stands in
for Binance's stream, and a fake get_json returns trades we wrote ourselves instead
of calling the REST API. The live tests at the bottom talk to the real Binance and
compare our totals with its official 1 minute candles (each takes up to ~1 minute).
"""

import asyncio
import contextlib
import json
import math
from http import HTTPStatus

import websockets

import vwap

MIDNIGHT_MS = vwap.next_midnight_ms(1_791_000_000_000)  # 00:00 UTC on 2026-10-04


def make_trade(trade_id, price, trade_time_ms, quantity=1):
    """A message shaped like Binance's aggTrade (numbers sent as text, like the real one)."""
    return {"e": "aggTrade", "a": trade_id, "p": str(price), "q": str(quantity), "T": trade_time_ms}


def fake_rest(trades):
    """A stand-in for vwap.get_json that serves these trades as if they were Binance's history."""
    async def get_json(path, params):
        if path == "/fapi/v1/klines":
            return []  # no earlier candles: every test session starts in its first minute
        if "fromId" in params:
            rows = [t for t in trades if t["a"] >= params["fromId"]]
        else:
            rows = [t for t in trades if t["T"] >= params["startTime"]]
        return rows[: params["limit"]]
    return get_json


class FakeBinance:
    """A local WebSocket server that plays out a script of connections.

    connections: one list of trades per accepted connection. Every connection but the
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
            await ws.send(json.dumps(trade))
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
