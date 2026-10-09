"""Web page for the live VWAP engine (several assets on one Binance connection).

Run `python server.py`, then open the address it prints in a browser. It uses port
8000, or the next free one if another program has it; `--port N` asks for exactly N.

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

import argparse
import asyncio
import contextlib
import socket
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from vwap import LiveFeed

HOST = "127.0.0.1"  # only this machine can open the page
PORT = 8000  # tried first; if it's taken, the next ones up to PORT + PORT_TRIES - 1
PORT_TRIES = 20
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


def bind_first_free(ports):
    """Return a socket listening on the first of these ports that's free, or None if none is.

    The socket is handed to the server as it is, so no other program can take the
    port between checking it and using it. SO_REUSEADDR isn't set: on Windows it
    would let this server share a port another program is already using.
    """
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind((HOST, port))
            sock.listen()
            return sock
        except OSError:  # in use (or not allowed)
            sock.close()
    return None


def main():
    parser = argparse.ArgumentParser(description="Serve the live asset analytics page.")
    parser.add_argument("--port", type=int,
                        help=f"use exactly this port (default: {PORT}, or the next free one)")
    args = parser.parse_args()

    ports = [args.port] if args.port is not None else range(PORT, PORT + PORT_TRIES)
    sock = bind_first_free(ports)
    if sock is None:
        if args.port is not None:
            raise SystemExit(f"Port {args.port} is in use by another program. Pick another with --port N.")
        raise SystemExit(f"Ports {PORT}-{PORT + PORT_TRIES - 1} are all in use. Close a program, or pick a port with --port N.")

    port = sock.getsockname()[1]
    if args.port is None and port != PORT:
        print(f"Port {PORT} is in use by another program, so using {port} instead.")
    print(f"Open http://{HOST}:{port} in a browser. Press Ctrl+C to stop.")
    config = uvicorn.Config(create_app(LiveFeed()), log_level="warning")
    uvicorn.Server(config).run(sockets=[sock])


if __name__ == "__main__":
    main()
