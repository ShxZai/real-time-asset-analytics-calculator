"""Tests for server.py. Run with: python -m pytest

The server runs for real (uvicorn on a free local port) around an engine that isn't
connected to Binance; each test adds events or trades to the engine itself.
"""

import asyncio
import contextlib
import json
import urllib.error
import urllib.request

import pytest
import uvicorn
import websockets

import server
import vwap


@contextlib.asynccontextmanager
async def running_server(live):
    """Serve create_app(live) on a free port and yield its base address (host:port)."""
    config = uvicorn.Config(server.create_app(live, run_engine=False), host="127.0.0.1", port=0, log_level="warning")
    web = uvicorn.Server(config)
    task = asyncio.create_task(web.serve())
    while not web.started:
        await asyncio.sleep(0.01)
    port = web.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"127.0.0.1:{port}"
    finally:
        web.should_exit = True
        await task


async def receive(ws):
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=5))


def get(url):
    """GET url in a thread (urllib blocks); return (status, JSON body)."""
    def fetch():
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())
    return asyncio.to_thread(fetch)


def test_events_feed_sends_history_then_each_new_event():
    async def scenario():
        live = vwap.LiveFeed()
        for n in range(3):
            live.log("info", f"before {n}")
        async with running_server(live) as address:
            async with websockets.connect(f"ws://{address}/ws/events") as ws:
                history = await receive(ws)
                assert history["type"] == "history"
                assert [e["message"] for e in history["events"]] == ["before 0", "before 1", "before 2"]

                live.log("warning", "after 0")
                live.log("info", "after 1")
                first, second = await receive(ws), await receive(ws)
                assert first == {"type": "event", "event": live.system.events[3]}
                assert second["event"]["id"] == 4 and second["event"]["message"] == "after 1"

    asyncio.run(scenario())


def test_pages_joining_while_events_happen_miss_nothing_and_get_nothing_twice():
    async def scenario():
        live = vwap.LiveFeed()
        total = 300

        async def keep_logging():
            for n in range(total):
                live.log("info", f"event {n}")
                await asyncio.sleep(0.001)

        async def page(join_after_s):
            await asyncio.sleep(join_after_s)
            async with websockets.connect(f"ws://{address}/ws/events") as ws:
                ids = [e["id"] for e in (await receive(ws))["events"]]
                while not ids or ids[-1] < total - 1:
                    ids.append((await receive(ws))["event"]["id"])
                return ids

        async with running_server(live) as address:
            logger = asyncio.create_task(keep_logging())
            pages = await asyncio.gather(*(page(delay) for delay in (0, 0.05, 0.1, 0.2, 0.3)))
            await logger

        for ids in pages:
            assert ids == list(range(ids[0], total))  # consecutive: no gap, no duplicate
        assert pages[0][0] == 0  # the first page joined before anything happened

    asyncio.run(scenario())


def test_state_feed_sends_every_asset_in_one_message_without_events():
    async def scenario():
        live = vwap.LiveFeed(["BTCUSDT", "XAUUSDT"])
        live.log("info", "something happened")
        xau = live.assets["XAUUSDT"]
        xau.tracker.add_trade(100, 2, vwap.next_midnight_ms(0) - 1000)
        xau.last_trade = {"price": 100.0, "quantity": 2.0, "time_ms": 1, "received_ms": 2}
        async with running_server(live) as address:
            async with websockets.connect(f"ws://{address}/ws/state") as ws:
                state = await receive(ws)
                assert state["type"] == "state"
                assert "events" not in state
                assert state["connection"]["state"] == "connecting"  # once, shared by every asset
                btc_state, xau_state = state["assets"]
                assert btc_state["symbol"] == "BTCUSDT" and btc_state["vwap"] is None
                assert xau_state["symbol"] == "XAUUSDT" and xau_state["vwap"] == 100
                assert xau_state["history"] == "waiting" and xau_state["complete"] is False
                assert xau_state["last_trade"]["received_ms"] == 2

                xau.tracker.add_trade(200, 2, vwap.next_midnight_ms(0) - 500)
                assert (await receive(ws))["assets"][1]["vwap"] == 150  # the next tick has the new value

    asyncio.run(scenario())


def test_missing_event_endpoint_returns_kept_events_only():
    async def scenario():
        live = vwap.LiveFeed(["XAUUSDT"])
        for n in range(vwap.MAX_EVENTS + 100):
            live.log("info", f"event {n}")
        async with running_server(live) as address:
            assert await get(f"http://{address}/events/system/550") == (200, live.system.events[450])
            status, body = await get(f"http://{address}/events/system/50")  # older than the latest 500
            assert status == 404 and "isn't kept" in body["detail"]
            status, body = await get(f"http://{address}/events/DOGEUSDT/0")  # not a tracked asset
            assert status == 404 and "No event log" in body["detail"]

    asyncio.run(scenario())


def test_events_with_the_same_id_from_different_logs_are_all_kept_and_found():
    async def scenario():
        live = vwap.LiveFeed(["BTCUSDT", "XAUUSDT"])
        live.log("info", "system 0")
        live.assets["BTCUSDT"].log("info", "btc 0")
        async with running_server(live) as address:
            async with websockets.connect(f"ws://{address}/ws/events") as ws:
                history = (await receive(ws))["events"]
                assert [(e["symbol"], e["id"]) for e in history] == [(None, 0), ("BTCUSDT", 0)]

                live.assets["XAUUSDT"].log("warning", "xau 0")  # ID 0 a third time
                event = (await receive(ws))["event"]
                assert (event["symbol"], event["id"], event["message"]) == ("XAUUSDT", 0, "xau 0")

            # Each (log, id) names a different event.
            assert (await get(f"http://{address}/events/system/0"))[1]["message"] == "system 0"
            assert (await get(f"http://{address}/events/BTCUSDT/0"))[1]["message"] == "btc 0"
            assert (await get(f"http://{address}/events/XAUUSDT/0"))[1]["message"] == "xau 0"

    asyncio.run(scenario())


def test_page_too_far_behind_is_closed_so_it_reconnects():
    async def scenario():
        live = vwap.LiveFeed()
        async with running_server(live) as address:
            async with websockets.connect(f"ws://{address}/ws/events") as ws:
                assert (await receive(ws))["events"] == []
                # More events than the page's queue holds, with no await: the server can't send any in between.
                for n in range(server.PAGE_QUEUE_SIZE + 1):
                    live.log("info", f"event {n}")
                with pytest.raises(websockets.exceptions.ConnectionClosed) as closed:
                    await receive(ws)
                assert closed.value.rcvd.code == server.TRY_AGAIN_LATER

    asyncio.run(scenario())


def test_page_is_served():
    async def scenario():
        async with running_server(vwap.LiveFeed()) as address:
            def fetch(path):
                with urllib.request.urlopen(f"http://{address}{path}", timeout=5) as response:
                    return response.status, response.read().decode()
            status, html = await asyncio.to_thread(fetch, "/")
            assert status == 200 and "/static/app.js" in html
            status, script = await asyncio.to_thread(fetch, "/static/app.js")
            assert status == 200 and "/ws/state" in script

    asyncio.run(scenario())
