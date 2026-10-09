"""Web page for the live VWAP engine (several assets on one Binance connection).

Run `python server.py`, then open http://127.0.0.1:8000 in a browser.

One engine runs for the whole program, in the same asyncio loop as the server, so
every open page shares one Binance connection. A page opens two WebSockets:

- /ws/state: the full state of every asset every STATE_INTERVAL_S, in one message.
  The page compares it with what it shows and changes only what differs.
- /ws/events: the latest events of every log first (history), then each new event
  as it happens. The page rebuilds its console from the history on every connect.

Each asset and the system have their own event log with their own IDs, so an event
is named by (symbol, id). GET /events/{symbol}/{id} returns one event that's still
kept ("system" for the system log), so a page can fetch an ID it didn't receive.
"""

import asyncio
import contextlib
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from vwap import LiveFeed

HOST = "127.0.0.1"  # only this machine can open the page
PORT = 8000
STATE_INTERVAL_S = 0.3
# A page this many events behind is cut off; it reconnects and rebuilds from history.
PAGE_QUEUE_SIZE = 1000
STATIC_DIR = Path(__file__).parent / "static"

TRY_AGAIN_LATER = 1013  # WebSocket close code
SYSTEM_LOG = "system"  # the system log's name in URLs (its events have symbol None)


def state_message(live):
    """The engine's snapshot: the connection and every asset (events go over /ws/events)."""
    return {"type": "state", **live.snapshot()}


class EventFanOut:
    """Passes each new engine event to every connected page's queue.

    The engine has one on_event callback, but several pages can be open.
    """

    def __init__(self, live):
        self.live = live
        self.queues = set()
        live.on_event = self.publish

    def subscribe(self):
        """Return the event history and a queue for every event after it.

        There's no await in here, so no event can happen between copying the
        history and adding the queue: nothing is missed or sent twice.
        """
        queue = asyncio.Queue(maxsize=PAGE_QUEUE_SIZE)
        self.queues.add(queue)
        return self.live.events(), queue

    def unsubscribe(self, queue):
        self.queues.discard(queue)

    def publish(self, event):
        for queue in list(self.queues):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # The page can't keep up: empty its queue and leave None to tell it to close.
                self.queues.discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)


def create_app(live, run_engine=True):
    """Build the web app around an engine. Tests pass run_engine=False and drive it themselves."""
    fan_out = EventFanOut(live)

    @contextlib.asynccontextmanager
    async def lifespan(_app):  # FastAPI passes the app; it isn't needed here
        engine = asyncio.create_task(live.run()) if run_engine else None
        yield
        if engine:
            engine.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await engine

    app = FastAPI(lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def page():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/events/{symbol}/{event_id}")
    async def get_event(symbol: str, event_id: int):
        log = live.event_log(None if symbol == SYSTEM_LOG else symbol)
        if log is None:
            raise HTTPException(404, f"No event log for {symbol}")
        event = log.find(event_id)
        if event is None:
            raise HTTPException(404, f"Event {symbol}/{event_id} isn't kept (only the latest {log.events.maxlen} are)")
        return event

    @app.websocket("/ws/state")
    async def state_feed(ws: WebSocket):
        async def send_states():
            while True:
                await ws.send_json(state_message(live))
                await asyncio.sleep(STATE_INTERVAL_S)

        await ws.accept()
        await send_until_closed(ws, send_states)

    @app.websocket("/ws/events")
    async def event_feed(ws: WebSocket):
        async def send_events():
            await ws.send_json({"type": "history", "events": history})
            while True:
                event = await queue.get()
                if event is None:
                    await ws.close(TRY_AGAIN_LATER, "Too far behind, reconnect")
                    return
                await ws.send_json({"type": "event", "event": event})

        await ws.accept()
        history, queue = fan_out.subscribe()
        try:
            await send_until_closed(ws, send_events)
        finally:
            fan_out.unsubscribe(queue)

    return app


async def send_until_closed(ws, send):
    """Run send() until it returns or the page goes away.

    Pages never send anything, so receive() only returns when the connection closes.
    Without it, a feed waiting for its next event wouldn't notice a closed page and
    would keep the server from shutting down.
    """
    sender = asyncio.create_task(send())
    watcher = asyncio.create_task(ws.receive())
    done, pending = await asyncio.wait({sender, watcher}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect):
            await task
    if sender in done:
        with contextlib.suppress(WebSocketDisconnect):  # the page closed mid-send
            sender.result()


def main():
    app = create_app(LiveFeed())
    print(f"Open http://{HOST}:{PORT} in a browser. Press Ctrl+C to stop.")
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
