# Real-Time Asset Analytics Calculator

Turns a live market data feed into metrics as trades happen.

Right now it tracks one asset and one metric: the **VWAP** (volume-weighted average
price) of **XAUUSDT**, gold priced in USDT on Binance USDⓈ-M futures. You run it in
the terminal, and it recalculates VWAP on every aggregated trade from Binance's
live stream. Each session resets at 00:00 UTC. You can watch it in the terminal or
on a live web page.

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

Then open http://127.0.0.1:8000. The page receives the state every 300 ms and each
event as it happens over WebSockets, shows when Binance or the app is disconnected
or the feed has stalled, and has a debug console listing gaps, duplicates and
reconnects. For terminal output instead, run `python vwap.py`.

Press Ctrl+C to stop.

## Backtest

Recomputes a full day's VWAP from Binance's historical trade file and checks it
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
- More assets, each shown with its metrics
- Comparing assets on the page
