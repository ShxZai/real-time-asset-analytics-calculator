// Page for the live VWAP engine (served by server.py).
//
// Two WebSockets: /ws/state brings the full state of every asset every ~300 ms,
// /ws/events brings the event history on connect and then each new event. The page
// only changes what differs from what it already shows.
//
// Each asset and the system have their own event log with their own IDs, so the
// console tracks the highest ID it has shown per log.

const RECONNECT_WAITS_MS = [1000, 2000, 4000, 5000]; // then 5 s each time
const MAX_CONSOLE_ROWS = 500; // same as the engine keeps

const el = (id) => document.getElementById(id);
const assetRows = el("assets");
const consoleList = el("console");

let state = null; // the latest state message
let stateOpen = false; // is /ws/state connected?
let stateRetryAt = null; // when the page tries /ws/state again
let lastEventIds = new Map(); // highest event ID shown in the console, per log
const rows = new Map(); // symbol -> that asset's table cells
let eventQueue = Promise.resolve(); // handles event messages one after another, in order

// Change an element only when the new value differs from what it shows.
function setText(element, text) {
  if (element.textContent !== text) element.textContent = text;
}

function setClass(element, name, on) {
  if (element.classList.contains(name) !== on) element.classList.toggle(name, on);
}

function setStatus(element, text, kind) {
  setText(element, text);
  for (const name of ["ok", "warn", "bad"]) setClass(element, name, name === kind);
}

function secondsUntil(timeMs) {
  return Math.max(0, Math.ceil((timeMs - Date.now()) / 1000));
}

// ---- State ----

function render() {
  if (stateOpen) {
    setStatus(el("app-status"), "App: connected", "ok");
  } else {
    const wait = stateRetryAt ? ` (retrying in ${secondsUntil(stateRetryAt)} s)` : "";
    setStatus(el("app-status"), "App: lost connection" + wait, "bad");
  }
  if (!state) return;

  // Binance connection (one for every asset). While the app is unreachable, the page can't know it.
  const connection = state.connection;
  if (!stateOpen) {
    setStatus(el("binance-status"), "Binance: unknown", null);
  } else if (connection.state === "connected") {
    setStatus(el("binance-status"), "Binance: live", "ok");
  } else {
    const retry = connection.retry_at_ms ? ` (retrying in ${secondsUntil(connection.retry_at_ms)} s)` : "";
    const cantReach = connection.state === "refused" || connection.state === "unreachable";
    setStatus(el("binance-status"), (cantReach ? "Binance: can't reach" : "Binance: reconnecting") + retry,
              cantReach ? "bad" : "warn");
  }

  // Numbers are frozen (grey) whenever Binance or the app isn't connected.
  const live = stateOpen && connection.state === "connected";
  for (const asset of state.assets) renderAsset(asset, live);
}

// The row for a symbol, made the first time the symbol appears.
function rowFor(symbol) {
  let cells = rows.get(symbol);
  if (!cells) {
    const row = document.createElement("tr");
    const cell = () => {
      const td = document.createElement("td");
      td.textContent = "-";
      row.append(td);
      return td;
    };
    cells = { row, symbol: cell(), price: cell(), vwap: cell(), age: cell(), volume: cell(), pressure: cell() };
    cells.symbol.textContent = symbol;
    assetRows.append(row);
    rows.set(symbol, cells);
  }
  return cells;
}

function renderAsset(asset, live) {
  const cells = rowFor(asset.symbol);
  setClass(cells.row, "frozen", !live);

  // Age of the last trade, worked out with this computer's clock (the server uses the same clock).
  // Just information: the page can't tell a quiet market from a stream that stopped.
  const trade = asset.last_trade;
  setText(cells.age, trade ? `${Math.floor((Date.now() - trade.received_ms) / 1000)} s ago` : "none yet");
  setText(cells.price, trade ? trade.price.toFixed(2) : "-");

  const placeholder = { waiting: "Waiting for first trade", fetching: "Fetching data" }[asset.history];
  if (placeholder || asset.vwap === null) {
    setText(cells.vwap, placeholder || "-");
    setClass(cells.vwap, "placeholder", true);
    setClass(cells.vwap, "gap", false);
  } else {
    setText(cells.vwap, asset.vwap.toFixed(2));
    setClass(cells.vwap, "placeholder", false);
    setClass(cells.vwap, "gap", !asset.complete); // yellow while a gap is being filled
  }
}

// ---- Events (debug console) ----

function formatTime(timeMs) {
  return new Date(timeMs).toLocaleTimeString([], { hour12: false });
}

// The log an event belongs to: its symbol, or "system" (symbol null) for the whole program.
function logName(event) {
  return event.symbol ?? "system";
}

function addConsoleRow(time, log, text, isWarning) {
  const atBottom = consoleList.scrollHeight - consoleList.scrollTop - consoleList.clientHeight < 4;
  const item = document.createElement("li");
  const timeSpan = document.createElement("span");
  timeSpan.className = "time";
  timeSpan.textContent = time + "  ";
  const sourceSpan = document.createElement("span");
  sourceSpan.className = "source";
  sourceSpan.textContent = log.toUpperCase().padEnd(8) + " ";
  const textSpan = document.createElement("span");
  textSpan.textContent = text; // textContent, not innerHTML: messages contain error text
  if (isWarning) textSpan.className = "warning";
  item.append(timeSpan, sourceSpan, textSpan);
  consoleList.append(item);
  while (consoleList.children.length > MAX_CONSOLE_ROWS) consoleList.firstChild.remove();
  setText(el("event-count"), String(consoleList.children.length));
  if (atBottom) consoleList.scrollTop = consoleList.scrollHeight;
}

function addEvent(event) {
  addConsoleRow(formatTime(event.time_ms), logName(event), event.message, event.level === "warning");
  lastEventIds.set(logName(event), event.id);
}

async function fetchEvent(log, id) {
  try {
    const response = await fetch(`/events/${encodeURIComponent(log)}/${id}`);
    return response.ok ? await response.json() : null;
  } catch {
    return null; // the server is unreachable; the console says the event is missing
  }
}

async function handleEventMessage(message) {
  if (message.type === "history") {
    // Rebuild on every connect: this also covers a restarted server, whose IDs start again at 0.
    consoleList.replaceChildren();
    lastEventIds = new Map();
    message.events.forEach(addEvent); // each log's events arrive in ID order
    setText(el("event-count"), String(consoleList.children.length));
    return;
  }

  // Only the same log's IDs are compared: BTCUSDT event 5 and SYSTEM event 5 are different events.
  const event = message.event;
  const log = logName(event);
  const lastId = lastEventIds.get(log);
  if (lastId !== undefined) {
    if (event.id <= lastId) return; // already shown
    // IDs go up by 1 within a log, so anything between was missed: fetch it.
    for (let id = lastId + 1; id < event.id; id++) {
      const missing = await fetchEvent(log, id);
      if (missing) addEvent(missing);
      else addConsoleRow(formatTime(Date.now()), log, `Event ${id} was missed and is no longer available`, true);
    }
  }
  addEvent(event);
}

// ---- Connections ----

// Open a WebSocket to the server and keep reopening it when it closes.
function keepConnected(path, onOpen, onMessage, onClose) {
  let attempt = 0;
  function open() {
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${scheme}://${location.host}${path}`);
    ws.onopen = () => {
      attempt = 0;
      onOpen();
    };
    ws.onmessage = (message) => onMessage(JSON.parse(message.data));
    ws.onclose = () => {
      const wait = RECONNECT_WAITS_MS[Math.min(attempt, RECONNECT_WAITS_MS.length - 1)];
      attempt++;
      onClose(Date.now() + wait);
      setTimeout(open, wait);
    };
  }
  open();
}

keepConnected(
  "/ws/state",
  () => { stateOpen = true; stateRetryAt = null; },
  (message) => { state = message; render(); },
  (retryAt) => { stateOpen = false; stateRetryAt = retryAt; render(); },
);

keepConnected(
  "/ws/events",
  () => setText(el("console-status"), ""),
  (message) => { eventQueue = eventQueue.then(() => handleEventMessage(message)); },
  () => setText(el("console-status"), " - disconnected, reconnecting"),
);

// Ages and countdowns change even when no message arrives.
setInterval(render, 250);
render();
