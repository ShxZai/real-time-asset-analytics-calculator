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
//
// Clicking an asset's row opens its price chart (Lightweight Charts) and more numbers underneath.
//
// The "Simulate Wi-Fi cut" button cuts the engine's connection to Binance on purpose
// and brings it back. Its label follows the state, so every open page agrees.
//
// A cut link sends no signal, so the page doesn't wait to be told: the server sends
// something on each feed at least every second, and a feed silent for SILENCE_LIMIT_MS
// counts as lost. The "Simulate app disconnect" button cuts this page's link to the
// app (this tab only): nothing from the server is taken in, reconnects and requests
// fail, and the silence check has to notice, as it would for a real cut.

const RECONNECT_WAITS_MS = [1000, 2000, 4000, 5000]; // then 5 s each time
// State comes every 0.3 s and the events feed sends a heartbeat after 1 s without an event.
const SILENCE_LIMIT_MS = 2000;
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
let cutRequestPending = false; // a press of the Wi-Fi cut button is on its way to the server
let appCut = false; // this page's link to the app is cut on purpose ("Simulate app disconnect")
// The page's own events (its links to the app), kept across console rebuilds.
const pageEvents = [];

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

// fetch() to the app, failing the way it does when the app can't be reached during a simulated disconnect.
function appFetch(url, options) {
  return appCut ? Promise.reject(new TypeError("simulated app disconnect")) : fetch(url, options);
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
  renderCutButton();
  renderAppCutButton();
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

  // Candle check, summed over every asset.
  const checked = state.assets.reduce((sum, asset) => sum + asset.candle_check.checked, 0);
  const matched = state.assets.reduce((sum, asset) => sum + asset.candle_check.matched, 0);
  if (checked === 0) {
    setStatus(el("check-status"), "Checked vs Binance: first minute pending", null);
  } else if (matched === checked) {
    setStatus(el("check-status"), `Checked vs Binance: ${checked.toLocaleString("en-US")}/${checked.toLocaleString("en-US")} minutes match`, "ok");
  } else {
    setStatus(el("check-status"), `Checked vs Binance: ${(checked - matched).toLocaleString("en-US")} of ${checked.toLocaleString("en-US")} minutes differ`, "bad");
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
    cells.arrow = document.createElement("span");
    cells.arrow.className = "arrow";
    cells.arrow.textContent = "▸ ";
    cells.symbol.replaceChildren(cells.arrow, symbol);
    row.className = "asset";
    row.tabIndex = 0;
    row.setAttribute("aria-expanded", "false");
    row.addEventListener("click", () => toggleDetail(symbol));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        toggleDetail(symbol);
      }
    });
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

  if (detail && detail.symbol === asset.symbol) renderDetail(asset);
}

// ---- Detail panel (price chart and more numbers under a clicked row) ----
//
// One panel is open at a time. It loads the session's candles from /candles/{symbol},
// then keeps the latest two up to date from each state message. When the state's chart
// revision differs from the loaded one (startup fetch, gap filled, new session), it
// loads them all again.

const CHART_RETRY_MS = 2000;
let detail = null; // the open panel: { symbol, row, chart, candles, vwapLine, stats, revision, ... }

function toggleDetail(symbol) {
  const wasOpen = detail && detail.symbol === symbol;
  closeDetail();
  if (!wasOpen) openDetail(symbol);
  render();
}

function closeDetail() {
  if (!detail) return;
  if (detail.chart) detail.chart.remove();
  detail.row.remove();
  const cells = rows.get(detail.symbol);
  cells.row.setAttribute("aria-expanded", "false");
  setText(cells.arrow, "▸ ");
  detail = null;
}

function cssColor(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function openDetail(symbol) {
  const cells = rows.get(symbol);
  cells.row.setAttribute("aria-expanded", "true");
  setText(cells.arrow, "▾ ");

  const row = document.createElement("tr");
  row.className = "detail";
  const td = document.createElement("td");
  td.colSpan = cells.row.children.length;
  const body = document.createElement("div");
  body.className = "detail-body";
  const chartBox = document.createElement("div");
  chartBox.className = "chart-box";
  const chartElement = document.createElement("div");
  chartElement.className = "chart";
  const note = document.createElement("div");
  note.className = "chart-note";
  note.innerHTML = 'One-minute candles since 00:00 UTC (times in UTC), <span class="vwap-key">session VWAP</span> as a line.';
  chartBox.append(chartElement, note);

  const statList = document.createElement("dl");
  statList.className = "stats";
  const stats = {};
  for (const [key, label] of [["open", "Session open"], ["high", "High"], ["low", "Low"], ["range", "Range"],
                              ["volume", "Volume"], ["buyShare", "Buy share"], ["trades", "Trades (last 60 s)"],
                              ["check", "Minutes vs Binance"]]) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    stats[key] = document.createElement("dd");
    stats[key].textContent = "-";
    statList.append(dt, stats[key]);
  }
  body.append(chartBox, statList);
  td.append(body);
  row.append(td);
  cells.row.after(row);

  detail = { symbol, row, body, stats, chart: null, revision: null, loading: false, nextLoadAt: 0, lastTime: null };
  if (typeof LightweightCharts === "undefined") {
    chartElement.textContent = "The chart library couldn't load (it comes from cdn.jsdelivr.net).";
    chartElement.classList.add("placeholder");
    return;
  }
  const chart = LightweightCharts.createChart(chartElement, {
    autoSize: true,
    layout: { background: { type: "solid", color: cssColor("--bg") }, textColor: cssColor("--muted"),
              fontFamily: getComputedStyle(document.body).fontFamily },
    grid: { vertLines: { color: cssColor("--line") }, horzLines: { color: cssColor("--line") } },
    rightPriceScale: { borderColor: cssColor("--line") },
    timeScale: { borderColor: cssColor("--line"), timeVisible: true, secondsVisible: false },
  });
  const green = cssColor("--green");
  const red = cssColor("--red");
  detail.chart = chart;
  detail.candles = chart.addCandlestickSeries({
    upColor: green, downColor: red, wickUpColor: green, wickDownColor: red, borderVisible: false,
    priceFormat: { type: "price", precision: 2, minMove: 0.01 },
  });
  detail.vwapLine = chart.addLineSeries({
    color: cssColor("--blue"), lineWidth: 2, priceLineVisible: false, crosshairMarkerVisible: false,
    priceFormat: { type: "price", precision: 2, minMove: 0.01 },
  });
}

function toBars(rows) {
  return {
    candles: rows.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })),
    vwap: rows.map(({ time, vwap }) => ({ time, value: vwap })),
  };
}

async function loadCandles(panel) {
  panel.loading = true;
  try {
    const response = await appFetch(`/candles/${encodeURIComponent(panel.symbol)}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (detail !== panel) return; // closed while loading
    const bars = toBars(data.candles);
    panel.candles.setData(bars.candles);
    panel.vwapLine.setData(bars.vwap);
    if (panel.revision === null) panel.chart.timeScale().fitContent(); // first load: show the whole session
    panel.revision = data.revision;
    panel.lastTime = data.candles.length ? data.candles[data.candles.length - 1].time : null;
  } catch {
    panel.nextLoadAt = Date.now() + CHART_RETRY_MS; // the server is unreachable; try again shortly
  } finally {
    panel.loading = false;
  }
}

function setStat(element, text, { sub = null, sign = 0, gap = false } = {}) {
  if (sub === null) {
    setText(element, text);
  } else {
    let main = element.querySelector(".main");
    let small = element.querySelector(".sub");
    if (!main) {
      main = document.createElement("span");
      main.className = "main";
      small = document.createElement("span");
      small.className = "sub";
      element.replaceChildren(main, small);
    }
    setText(main, text);
    setText(small, sub);
  }
  setClass(element, "up", sign > 0);
  setClass(element, "down", sign < 0);
  setClass(element, "gap", gap);
}

function renderDetail(asset) {
  const panel = detail;
  // As wide as the visible part of the table (minus the cell's padding), however wide the table is.
  const width = `${el("table-wrap").clientWidth - 24}px`;
  if (panel.body.style.width !== width) panel.body.style.width = width;
  if (panel.chart && !panel.loading) {
    const chart = asset.chart;
    if (chart.revision !== panel.revision) {
      if (Date.now() >= panel.nextLoadAt) loadCandles(panel);
    } else {
      // The latest two candles change live; update() only takes the last bar or a newer one.
      const bars = toBars(chart.latest.filter((row) => panel.lastTime === null || row.time >= panel.lastTime));
      try {
        bars.candles.forEach((bar) => panel.candles.update(bar));
        bars.vwap.forEach((point) => panel.vwapLine.update(point));
        if (bars.candles.length) panel.lastTime = bars.candles[bars.candles.length - 1].time;
      } catch {
        panel.revision = -1; // out of step with the server: load every candle again (keeping the zoom)
      }
    }
  }

  // Session numbers follow the table: not shown until the startup fetch has added the earlier trades.
  const s = panel.stats;
  const gap = !asset.complete;
  if (asset.history !== "done" || asset.session_open === null || asset.session_high === null) {
    const text = { waiting: "Waiting", fetching: "Fetching data" }[asset.history] || "-";
    for (const key of ["open", "high", "low", "range", "volume", "buyShare"]) setStat(s[key], text);
  } else {
    const open = asset.session_open;
    setStat(s.open, open.toFixed(2), { gap });
    setStat(s.high, asset.session_high.toFixed(2), { gap });
    setStat(s.low, asset.session_low.toFixed(2), { gap });
    setStat(s.range, ((asset.session_high - asset.session_low) / open * 100).toFixed(2) + "%", { gap });
    const flow = asset.flow;
    const otherUnit = mainUnit === "quote" ? "base" : "quote";
    const total = (unit) => flow[unit].buy + flow[unit].sell;
    const volumeText = {
      quote: `${formatUsdt(total("quote"))} ${asset.quote_asset}`,
      base: `${formatCoins(total("base"))} ${asset.base_asset}`,
    };
    setStat(s.volume, volumeText[mainUnit], { sub: volumeText[otherUnit], gap });
    // Share of the session's USDT volume where the buyer was the taker: above 50% means buyers pushed harder.
    const share = total("quote") > 0 ? flow.quote.buy / total("quote") * 100 : 50;
    setStat(s.buyShare, share.toFixed(1) + "%", { sign: share - 50, gap });
  }
  const rolling = asset.rolling;
  setStat(s.trades, rolling && rolling.warmup_s === 0 ? String(Math.round(rolling.trades_per_s * 60)) : "-",
          { gap });
  const check = asset.candle_check;
  setStat(s.check, check.checked === 0 ? "pending" : `${check.matched}/${check.checked} match`,
          { sign: check.checked === 0 ? 0 : check.matched === check.checked ? 1 : -1 });
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

// An event of the page itself (its links to the app). The server can't record these:
// they happen exactly when it can't be reached.
function pageLog(text, isWarning = false) {
  const row = { time_ms: Date.now(), text, isWarning };
  pageEvents.push(row);
  if (pageEvents.length > MAX_CONSOLE_ROWS) pageEvents.shift();
  addConsoleRow(formatTime(row.time_ms), "page", text, isWarning);
}

function addEvent(event) {
  addConsoleRow(formatTime(event.time_ms), logName(event), event.message, event.level === "warning");
  lastEventIds.set(logName(event), event.id);
}

async function fetchEvent(log, id) {
  try {
    const response = await appFetch(`/events/${encodeURIComponent(log)}/${id}`);
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
    // The server's events (each log in ID order) and the page's own, in time order (a stable sort keeps each log's order).
    const rows = [...message.events.map((event) => ({ time_ms: event.time_ms, event })), ...pageEvents]
      .sort((a, b) => a.time_ms - b.time_ms);
    for (const row of rows) {
      if (row.event) addEvent(row.event);
      else addConsoleRow(formatTime(row.time_ms), "page", row.text, row.isWarning);
    }
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

// ---- Simulated Wi-Fi cut ----

function renderCutButton() {
  const button = el("cut-button");
  const cut = Boolean(state && state.simulated_cut);
  setText(button, cut ? "Restore Wi-Fi" : "Simulate Wi-Fi cut");
  setClass(button, "active", cut);
  button.setAttribute("aria-pressed", String(cut));
  const disabled = !stateOpen || !state || cutRequestPending;
  if (button.disabled !== disabled) button.disabled = disabled;
}

async function toggleCut() {
  const action = state.simulated_cut ? "stop" : "start";
  cutRequestPending = true;
  render();
  try {
    await appFetch(`/simulated-cut/${action}`, { method: "POST" });
  } catch {
    // the app is unreachable; the button follows the state once it's back
  }
  // Keep the button disabled until a state message shows the change, so a quick second press can't undo it.
  const wanted = action === "start";
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline && stateOpen && state.simulated_cut !== wanted) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  cutRequestPending = false;
  render();
}

el("cut-button").addEventListener("click", toggleCut);

// ---- Simulated app disconnect (this page only) ----

function renderAppCutButton() {
  const button = el("app-cut-button");
  setText(button, appCut ? "Restore app connection" : "Simulate app disconnect");
  setClass(button, "active", appCut);
  button.setAttribute("aria-pressed", String(appCut));
}

el("app-cut-button").addEventListener("click", () => {
  appCut = !appCut;
  pageLog(appCut ? "Simulated app disconnect: nothing from the app gets through until it's restored"
                 : "Simulated app disconnect over: the next retry can reach the app", appCut);
  render();
});

// ---- Connections ----

// Open a WebSocket to the server and keep reopening it when it closes, or when
// nothing has come for SILENCE_LIMIT_MS (a cut link never closes by itself).
// name labels the feed in the console.
function keepConnected(name, path, onOpen, onMessage, onClose) {
  let attempt = 0;
  let socket = null; // the open (or opening) WebSocket
  let lastHeardAt = 0;

  function retryLater(reason) {
    const wait = RECONNECT_WAITS_MS[Math.min(attempt, RECONNECT_WAITS_MS.length - 1)];
    attempt++;
    pageLog(`${name} feed: ${reason}; retrying in ${wait / 1000} s`, true);
    onClose(Date.now() + wait);
    setTimeout(open, wait);
  }

  function open() {
    if (appCut) {
      retryLater("can't reach the app (simulated app disconnect)");
      return;
    }
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${scheme}://${location.host}${path}`);
    socket = ws;
    lastHeardAt = Date.now();
    ws.onopen = () => {
      attempt = 0;
      lastHeardAt = Date.now();
      if (pageEvents.length) pageLog(`${name} feed: connected`); // not on first load: nothing was lost
      onOpen();
    };
    ws.onmessage = (message) => {
      if (appCut) return; // the link is cut: this never arrived
      lastHeardAt = Date.now();
      onMessage(JSON.parse(message.data));
    };
    ws.onclose = () => {
      if (socket !== ws) return; // already given up on by the silence check
      socket = null;
      retryLater("connection closed");
    };
  }

  setInterval(() => {
    const silentMs = Date.now() - lastHeardAt;
    if (socket && socket.readyState === WebSocket.OPEN && silentMs > SILENCE_LIMIT_MS) {
      const dead = socket;
      socket = null; // so its own close event, whenever it comes, is ignored
      dead.close();
      retryLater(`nothing received for ${(silentMs / 1000).toFixed(1)} s`);
    }
  }, 100);
  open();
}

keepConnected(
  "State",
  "/ws/state",
  () => { stateOpen = true; stateRetryAt = null; },
  (message) => { state = message; render(); },
  (retryAt) => { stateOpen = false; stateRetryAt = retryAt; render(); },
);

keepConnected(
  "Events",
  "/ws/events",
  () => setText(el("console-status"), ""),
  (message) => {
    if (message.type === "heartbeat") return; // only there to show the link is alive
    eventQueue = eventQueue.then(() => handleEventMessage(message));
  },
  () => setText(el("console-status"), " - disconnected, reconnecting"),
);

for (const button of document.querySelectorAll("#unit-switch button")) {
  button.addEventListener("click", () => setMainUnit(button.dataset.unit));
}
setMainUnit(mainUnit); // mark the remembered choice

// Ages and countdowns change even when no message arrives.
setInterval(render, 250);
render();
