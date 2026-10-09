# Real-Time Asset Analytics Calculator

Turns a live market data feed into metrics as trades happen.

Right now it tracks three assets on Binance USDⓈ-M futures, **BTCUSDT**, **ETHUSDT**
and **XAUUSDT** (gold), and one metric: the **VWAP** (volume-weighted average price).
One combined stream connection carries every asset's trades, and VWAP is recalculated
on every aggregated trade. Each asset keeps its own totals, trade ID checks and event
log, so a gap in one never touches another. Each session resets at 00:00 UTC. You can
watch it in the terminal or on a live web page. Adding an asset means adding its
symbol to `SYMBOLS` in `vwap.py`.

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

Then open http://127.0.0.1:8000. The page shows one row per asset. It receives the
state of every asset every 300 ms and each event as it happens over WebSockets,
shows when Binance or the app is disconnected and how long ago each asset last
traded, and has a debug console listing gaps, duplicates and reconnects, each
marked with its asset (or SYSTEM for the connection). For terminal output instead, run `python vwap.py`.

Press Ctrl+C to stop.

## Backtest

Recomputes a full day's XAUUSDT VWAP from Binance's historical trade file and checks it
against Binance's official daily figures (defaults to yesterday, UTC):

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

- More metrics
- An equal-weighted index across the assets
- Comparing assets on the page
