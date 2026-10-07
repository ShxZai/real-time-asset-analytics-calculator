# Real-Time Asset Analytics Calculator

Turns a live market data feed into metrics as trades happen.

Right now it tracks one asset and one metric: the **VWAP** (volume-weighted average
price) of **XAUUSDT**, gold priced in USDT on Binance USDⓈ-M futures. You run it in
the terminal, and it recalculates VWAP on every aggregated trade from Binance's
live stream. Each session resets at 00:00 UTC.

It currently also:

- fetches the session's earlier trades when started mid-session (VWAP is shown in
  yellow until they're included)
- checks every aggregated trade ID, ignoring duplicates and fetching any missed trades
- reconnects automatically when the connection drops, with backoff up to one minute

## Run

Requires Python 3.13+.

```
pip install -r requirements.txt
python vwap.py
```

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

Most tests run offline against a fake local server. Two live tests compare results
with Binance's official 1 minute candles; they need an internet connection and
take about 2 minutes.

## Roadmap

- More metrics
- More assets, each shown with its metrics
- A display for watching and comparing assets
