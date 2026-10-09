// Page for the live VWAP engine (served by server.py).
//
// Two WebSockets: /ws/state brings the full state of every asset every ~300 ms,
// /ws/events brings the event history on connect and then each new event. The page
// only changes what differs from what it already shows.
//
// Each asset and the system have their own event log with their own IDs, so the
// console tracks the highest ID it has shown per log.
//
// Every volume comes in USDT ("quote") and in the coin itself ("base"). A volume cell
// shows one large and the other small underneath; the unit switch swaps them.

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
let mainUnit = loadMainUnit(); // "quote" (USDT) or "base" (the coin): which volume is shown large

// The unit choice is remembered in this browser only; storage can be unavailable (private windows).
function loadMainUnit() {
  try {
    return localStorage.getItem("mainUnit") === "base" ? "base" : "quote";
  } catch {
    return "quote";
  }
}

function setMainUnit(unit) {
  mainUnit = unit;
  try {
    localStorage.setItem("mainUnit", unit);
  } catch {
    // not remembered; the page still switches
  }
  for (const button of document.querySelectorAll("#unit-switch button")) {
    button.setAttribute("aria-pressed", String(button.dataset.unit === unit));
  }
  render();
}

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
    cells = {
      row, symbol: cell(), price: cell(), age: cell(),
      // session (since 00:00 UTC)
      change: cell(), vwap: cell(), distance: cell(), buy: cell(), sell: cell(), delta: cell(),
      // rolling window (last 60 s)
      rollingVwap: cell(), rollingBuy: cell(), rollingSell: cell(), rollingDelta: cell(),
      tradeRate: cell(), volumeRate: cell(),
      // last 60 min
      volatility: cell(),
    };
    cells.symbol.textContent = symbol;
    cells.change.classList.add("start");
    cells.rollingVwap.classList.add("start");
    cells.volatility.classList.add("start");
    assetRows.append(row);
    rows.set(symbol, cells);
  }
  return cells;
}

// A volume in coins: whole numbers once it's large, more decimals when it's small.
function formatCoins(volume) {
  const digits = Math.abs(volume) >= 1000 ? 0 : Math.abs(volume) >= 1 ? 2 : 3;
  return volume.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

// A volume in USDT, shortened: 3.79M, 812.4K.
function formatUsdt(volume) {
  return volume.toLocaleString("en-US", { notation: "compact", minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function signed(text, value) {
  return value > 0 ? "+" + text : text;
}

// Show a value, or a grey italic placeholder when there isn't one yet.
// sign: green when above 0, red when below. gap: yellow (missing trades), which wins over the sign.
function setCell(cell, text, { placeholder = false, sign = 0, gap = false } = {}) {
  setText(cell, text);
  setClass(cell, "placeholder", placeholder);
  setClass(cell, "up", !placeholder && sign > 0);
  setClass(cell, "down", !placeholder && sign < 0);
  setClass(cell, "gap", !placeholder && gap);
}

// A volume cell: the main unit large, the other small underneath.
// values: {quote, base}. isDelta: add a + sign and colour green/red by the sign.
function setVolumeCell(cell, values, asset, { isDelta = false, gap = false } = {}) {
  let main = cell.querySelector(".main");
  let sub = cell.querySelector(".sub");
  if (!main) { // the cell showed a placeholder
    main = document.createElement("span");
    main.className = "main";
    sub = document.createElement("span");
    sub.className = "sub";
    cell.replaceChildren(main, sub);
  }
  const text = {
    quote: `${formatUsdt(values.quote)} ${asset.quote_asset}`,
    base: `${formatCoins(values.base)} ${asset.base_asset}`,
  };
  const otherUnit = mainUnit === "quote" ? "base" : "quote";
  // Coloured by the large number: the two units' deltas can differ in sign when buys and sells were at different prices.
  const sign = isDelta ? values[mainUnit] : 0;
  setText(main, isDelta ? signed(text[mainUnit], values[mainUnit]) : text[mainUnit]);
  setText(sub, isDelta ? signed(text[otherUnit], values[otherUnit]) : text[otherUnit]);
  setClass(cell, "placeholder", false);
  setClass(cell, "up", sign > 0);
  setClass(cell, "down", sign < 0);
  setClass(cell, "gap", gap);
}

function renderAsset(asset, live) {
  const cells = rowFor(asset.symbol);
  setClass(cells.row, "frozen", !live);

  // Age of the last trade, worked out with this computer's clock (the server uses the same clock).
  // Just information: the page can't tell a quiet market from a stream that stopped.
  const trade = asset.last_trade;
  setText(cells.age, trade ? `${Math.floor((Date.now() - trade.received_ms) / 1000)} s ago` : "none yet");
  setText(cells.price, trade ? trade.price.toFixed(2) : "-");

  // Session: not shown until the startup fetch has added the earlier trades.
  const gap = !asset.complete; // yellow while a gap is being filled
  const placeholder = { waiting: "Waiting for first trade", fetching: "Fetching data" }[asset.history];
  if (placeholder || asset.vwap === null) {
    setCell(cells.change, placeholder || "-", { placeholder: true });
    for (const cell of [cells.vwap, cells.distance, cells.buy, cells.sell, cells.delta]) {
      setCell(cell, "-", { placeholder: true });
    }
  } else {
    const change = asset.session_change_pct;
    setCell(cells.change, signed(change.toFixed(2) + "%", change), { sign: change, gap });
    const distance = asset.vwap_distance_pct;
    setCell(cells.vwap, asset.vwap.toFixed(2), { gap });
    setCell(cells.distance, signed(distance.toFixed(2) + "%", distance), { sign: distance, gap });
    const flow = asset.flow;
    setVolumeCell(cells.buy, { quote: flow.quote.buy, base: flow.base.buy }, asset, { gap });
    setVolumeCell(cells.sell, { quote: flow.quote.sell, base: flow.base.sell }, asset, { gap });
    setVolumeCell(cells.delta, { quote: flow.quote.delta, base: flow.base.delta }, asset, { isDelta: true, gap });
  }

  // Last 60 s: not shown until the window holds a full 60 s of trades seen live.
  const rolling = asset.rolling;
  if (!rolling || rolling.warmup_s > 0) {
    setCell(cells.rollingVwap, rolling ? `Warming up (${rolling.warmup_s} s)` : "-", { placeholder: true });
    for (const cell of [cells.rollingBuy, cells.rollingSell, cells.rollingDelta, cells.tradeRate, cells.volumeRate]) {
      setCell(cell, "-", { placeholder: true });
    }
  } else {
    // A quiet minute has no trades, so no VWAP; its volumes and rates are a real 0.
    setCell(cells.rollingVwap, rolling.vwap === null ? "no trades" : rolling.vwap.toFixed(2),
            { placeholder: rolling.vwap === null, gap });
    const flow = rolling.flow;
    setVolumeCell(cells.rollingBuy, { quote: flow.quote.buy, base: flow.base.buy }, asset, { gap });
    setVolumeCell(cells.rollingSell, { quote: flow.quote.sell, base: flow.base.sell }, asset, { gap });
    setVolumeCell(cells.rollingDelta, { quote: flow.quote.delta, base: flow.base.delta }, asset,
                  { isDelta: true, gap });
    setCell(cells.tradeRate, rolling.trades_per_s.toFixed(1), { gap });
    setVolumeCell(cells.volumeRate, rolling.volume_per_min, asset, { gap });
  }

  // Last 60 min: not shown until it has all 60 one-minute returns (the startup fetch
  // brings them within seconds; without it they come from live trades, one a minute).
  const volatility = asset.volatility;
  if (!volatility || volatility.minutes < 60) {
    setCell(cells.volatility, volatility ? `Warming up (${volatility.minutes}/60 min)` : "-", { placeholder: true });
  } else {
    setCell(cells.volatility, volatility.annualized_pct.toFixed(1) + "%", { gap });
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

for (const button of document.querySelectorAll("#unit-switch button")) {
  button.addEventListener("click", () => setMainUnit(button.dataset.unit));
}
setMainUnit(mainUnit); // mark the remembered choice

// Ages and countdowns change even when no message arrives.
setInterval(render, 250);
render();
