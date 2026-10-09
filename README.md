# Real-Time Asset Analytics Calculator

Turns a live market data feed into metrics as trades happen.

Right now it tracks three assets on Binance USDⓈ-M futures, **BTCUSDT**, **ETHUSDT**
and **XAUUSDT** (gold). One combined stream connection carries every asset's trades,
and every metric is updated on every aggregated trade. Each asset keeps its own totals,
trade ID checks and event log, so a gap in one never touches another. You can watch it
in the terminal or on a live web page. Adding an asset means adding its symbol to
`SYMBOLS` in `vwap.py`.

Metrics, per asset:

| | Session (since 00:00 UTC) | Last 60 seconds (rolling) |
|---|---|---|
| **VWAP** (volume-weighted average price) | yes | yes |
| **Distance from VWAP** (last price vs VWAP, in %) | yes | |
| **Buy and sell volume** | yes | yes |
| **Delta** (buy minus sell volume; over the session this is **CVD**) | yes | yes |
| **Trade rate** (aggregated trades per second) | | yes |
| **Volume rate** (volume per minute) | | yes |

A trade is a buy when the buyer was the taker (bought at the ask) and a sell when the
seller was, as given by Binance's `m` flag. Every volume is shown in USDT (price ×
quantity) and in the coin itself (BTC, ETH, XAU = troy ounces); a switch on the page
picks which one is shown large. The rolling window starts filling with the first live trade, so its metrics
show "Warming up" for the first 60 seconds.

It currently also:

- fetches the session's earlier trades when started mid-session (VWAP is shown in
  yellow until they're included)
- checks every aggregated trade ID, ignoring duplicates and fetching any missed trades
- reconnects automatically when the connection drops, with backoff up to one minute
- pings Binance every 5 seconds, so a silently cut connection is noticed within
  about 11 seconds even when the market is quiet
- skips malformed messages (if one was a real trade, it's fetched like any missed trade)
- starts a failed fetch again rather than dropping it, so VWAP never leaves out
  trades without showing in yellow
- waits as long as Binance asks when it rate-limits requests, so a long catch-up
  after an outage doesn't get the IP banned

## Run

Requires Python 3.13+.

```
pip install -r requirements.txt
python server.py
```

Then open the address it prints: http://127.0.0.1:8000, or the next free port
(8001, 8002, ...) if another program is using 8000. To choose the port yourself, run
`python server.py --port 8050`. The page shows one row per asset. It receives the
state of every asset every 300 ms and each event as it happens over WebSockets,
shows when Binance or the app is disconnected and how long ago each asset last
traded, and has a debug console listing gaps, duplicates and reconnects, each
marked with its asset (or SYSTEM for the connection). For terminal output instead, run `python vwap.py`.

Press Ctrl+C to stop.

## Backtest

Recomputes a full day's XAUUSDT VWAP and buy volume from Binance's historical trade file
and checks them against Binance's official daily figures (defaults to yesterday, UTC):

```
python backtest.py [YYYY-MM-DD]
```

## Tests

```
python -m pytest
```

Most tests run offline against fake local servers. Two live tests compare results
with Binance's official 1 minute candles; they need an internet connection and
take about 2 minutes.

## Roadmap

- More metrics (VWAP bands, realized volatility, large-trade alerts, order-book imbalance)
- An equal-weighted index across the assets
- Comparing assets on the page
