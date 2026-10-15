# Real-Time Asset Analytics Calculator

![Tests](https://github.com/ShxZai/real-time-asset-analytics-calculator/actions/workflows/tests.yml/badge.svg)

Turns a live market data feed into metrics as trades happen, and proves they're right
against the exchange's own figures, minute by minute.

## Results

Replaying a full day of trades (`python backtest.py 2026-10-14`) through the same code
the live app runs, then comparing with Binance's official candles:

| Asset | Aggregated trades | Day VWAP vs Binance (relative difference) | One-minute candles matching Binance | Trades split across a minute boundary |
|---|---:|---:|---:|---:|
| BTCUSDT | 1,280,357 | 5.2e-12 | 1,440 / 1,440 | 90 |
| ETHUSDT | 1,300,812 | 2.0e-12 | 1,440 / 1,440 | 89 |
| XAUUSDT | 623,364 | 6.5e-13 | 1,440 / 1,440 | 30 |

A candle matches when its open, high, low and close are equal and its volume (in coins
and in USDT) agrees to 1e-9. The same check runs live on every finished minute.
2026-10-13 gives the same result (every minute matches; 101, 83 and 24 split trades).

**Trades split across a minute boundary.** The first version of the per-minute check
failed on about 12% of BTC and ETH minutes, always in pairs: one minute had a little too
much volume and the next exactly that much too little. Binance merges fills at the same
price and taker side within 100 ms into one *aggregated trade*, stamped with its first
fill's time, but its candles count each fill in the minute it happened. So when such a
trade starts just before a boundary, Binance's candles split it between two minutes,
while the aggregated feed can only put it in one. In the day's data every split trade
started at most 96 ms before the boundary and had several fills. The check now allows
exactly this and nothing else: only the last trade before a boundary, only if it started
within 100 ms of it, only by less than its own size, at its own price, and the next
minute must then open with the rest of it.

The same split matters in two more places:

- **Midnight.** Binance's daily candle is the sum of its one-minute candles, so a trade
  that straddles 00:00 is split between two days. On 2026-10-05, Binance's BTC day
  starts with 0.004 BTC of a buy stamped on 10-04. The backtest moves that part out of
  10-04's totals and into 10-05's, and both days then match to within 5e-12.
- **Startup.** When the app starts mid-session, it takes Binance's one-minute candles up
  to a minute start and the trades after it. It now picks a minute start with no trade
  in the 100 ms before it, so no part of a trade can fall between the two.

Right now it tracks three assets on Binance USDⓈ-M futures, **BTCUSDT**, **ETHUSDT**
and **XAUUSDT** (gold). One combined stream connection carries every asset's trades,
and every metric is updated on every aggregated trade. Each asset keeps its own totals,
trade ID checks and event log, so a gap in one never touches another. You can watch it
in the terminal or on a live web page. Adding an asset means adding its symbol to
`SYMBOLS` in `metrics.py`.

Metrics, per asset:

| | Session (since 00:00 UTC) | Last 60 seconds (rolling) |
|---|---|---|
| **Change** (last price vs the session's first trade, in %) | yes | |
| **VWAP** (volume-weighted average price) | yes | yes |
| **Distance from VWAP** (last price vs VWAP, in %) | yes | |
| **Buy and sell volume** | yes | yes |
| **Delta** (buy minus sell volume; over the session this is **CVD**) | yes | yes |
| **Trade rate** (aggregated trades per second) | | yes |
| **Volume rate** (volume per minute) | | yes |

Plus **realized volatility** over the last 60 minutes: the sample standard deviation of
one-minute log returns (each minute's last price; a quiet minute keeps the previous
one), annualized with √525,600 (minutes in a 365-day year, as these markets trade
every day) and shown in %. The finished minutes only, so it updates once a minute.
At startup the hour before comes from 1m candles, so it's ready at once.
Change uses the same per-asset base (the session's first trade), so a $80,000 asset and
a $2,000 one can be compared side by side; the session's open matches Binance's
official daily open.

A trade is a buy when the buyer was the taker (bought at the ask) and a sell when the
seller was, as given by Binance's `m` flag. Every volume is shown in USDT (price ×
quantity) and in the coin itself (BTC, ETH, XAU = troy ounces); a switch on the page
picks which one is shown large. When the app starts, it also fetches the trades of the 60 seconds before it
connected, so the rolling metrics are ready within seconds (they show "Warming up"
until that fetch is in).

**Checked against Binance while it runs.** Once each minute has finished, its candle
(open, high, low, close and volume), built from the live trades, is compared with
Binance's official one-minute candle, allowing for a trade split across a minute
boundary (see Results). The page shows the running count ("Checked vs Binance:
312/312 minutes match") and any mismatch goes to the debug console. A minute is only
checked once it can't still change: no trade IDs missing, a live trade after it (so a
silently dead connection can't hide a trade), and 10 s for Binance's candle to settle.

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
marked with its asset (or SYSTEM for the connection).

Click a row to open that asset's price chart underneath: one-minute candles since
00:00 UTC with the session VWAP drawn as a line, next to the session's open, high,
low, range, total volume and buy share. The candles are built from the same trades
as every other metric (and from Binance's 1m candles for the part of the session
before the app started), so the chart and the table always agree. The chart uses
TradingView's open-source [Lightweight Charts](https://github.com/tradingview/lightweight-charts) library.

For terminal output instead, run `python metrics.py`.

Press Ctrl+C to stop.

## Backtest

Replays a full day of every asset's trades from Binance's historical trade files and
checks the day's VWAP, volume and buy volume against Binance's official daily candle,
and every one-minute candle against its official one (defaults to yesterday, UTC; the
files, about 40 MB for the three assets, are kept in `data/`). On a day that starts with
part of the previous day's last trade, it also downloads the previous day's file to read
that trade. It exits with code 1 if any check fails.

```
python backtest.py [YYYY-MM-DD] [--symbol BTCUSDT]
```

## Tests

```
python -m pytest                 # everything
python -m pytest -m "not live"   # offline only, a few seconds
```

The tests live in `tests/`. Most run offline against fake local servers. Two live
tests compare results with Binance's official 1 minute candles; they need an
internet connection and take about 2 minutes.

Every push and pull request runs the offline tests on GitHub Actions
(`.github/workflows/tests.yml`). The live tests are left out there because Binance
refuses connections from GitHub's US-based servers.

### Running the live tests

Run the live tests on your own machine with `python -m pytest -m live`. Binance blocks
connections from the US, so if you're in the US, connect through a VPN in another
country first. The live app (`server.py` and `metrics.py`) needs this too.

## Roadmap

- More metrics (VWAP bands, large-trade alerts, order-book imbalance)
