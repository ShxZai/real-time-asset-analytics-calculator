# Real-Time Asset Analytics Calculator

![Tests](https://github.com/ShxZai/real-time-asset-analytics-calculator/actions/workflows/tests.yml/badge.svg)

A live dashboard that streams every trade from Binance and turns it into trading metrics
as it happens: VWAP, buy and sell volume, delta, trade rate, volatility and more, for
several assets at once.

It tracks twelve assets on Binance USDⓈ-M futures, in four groups:

| Group | Assets |
|---|---|
| Crypto | **BTCUSDT**, **ETHUSDT**, **SOLUSDT**, **BNBUSDT** |
| Commodities | **XAUUSDT** (gold), **XAGUSDT** (silver), **CLUSDT** (WTI crude oil) |
| US stock indexes | **SPYUSDT** (S&P 500), **QQQUSDT** (Nasdaq-100) |
| US stocks | **NVDAUSDT** (Nvidia), **TSLAUSDT** (Tesla), **MSTRUSDT** (Strategy, which holds bitcoin) |

The commodity and stock contracts are Binance's own perpetual futures that track those
prices. They trade around the clock like crypto, just more thinly when the real market
is closed.

The page shows one row per asset, updated several times a second. Click a row to open
that asset's price chart underneath: one-minute candles since 00:00 UTC with the session
VWAP drawn as a line, next to the session's open, high, low, range, total volume and buy
share. A switch picks whether volumes are shown in USDT or in units of the asset itself.
At 00:00 UTC the chart and every session metric start over; earlier days aren't kept.

```
pip install -r requirements.txt
python server.py
```

Then open http://127.0.0.1:8000. Requires Python 3.13+.

## Metrics

Every metric is updated on every trade, per asset:

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
one-minute log returns, annualized with √525,600 (minutes in a 365-day year, as these
markets trade every day) and shown in %. It updates once a minute.

A trade is a buy when the buyer was the taker (bought at the ask) and a sell when the
seller was, as given by Binance's `m` flag. Volumes are in USDT (price × quantity) or in
units of the asset: coins for crypto, troy ounces for gold and silver, barrels for oil,
shares for stocks and index ETFs. Change uses each asset's own session base, so an
$80,000 asset and a $2,000 one can be compared side by side.

If the app starts mid-session, it fetches the session's earlier trades and Binance's
one-minute candles so every number covers the whole day (VWAP is shown in yellow until
they're in), plus the last 60 seconds of trades so the rolling metrics are ready within
seconds.

## Staying right when connections fail

The app detects dropped connections, reconnects, and fetches any trades it missed. Every
number on the page is either complete or marked as incomplete (yellow while missed
trades are being fetched, grey while the page has lost its link to the app).

**Noticing a dead connection.** A cut connection usually sends no signal: no error, just
silence. The app pings Binance every second, so a dead connection is noticed within
about 2.5 seconds even when the market is quiet. (A pong takes about 0.3 s to come back,
and Binance closes connections that send more than 10 messages a second, so pings can't
usefully go much faster.)

**Reconnecting.** It reconnects automatically, backing off 5 s, 10 s, 20 s, ... up to one
minute. When Binance rate-limits requests, it waits as long as Binance asks, so a long
catch-up after an outage doesn't get the IP banned.

**Filling the gap.** Every aggregated trade has a sequential ID, and the app checks each
one. After a reconnect, any missing IDs are fetched over REST and added in, so the
totals are exactly what they'd be had the connection never dropped. Duplicates are
ignored, malformed messages are skipped (and fetched like any missed trade if they were
real), and a failed fetch is retried rather than dropped. Until a gap is filled, the
affected numbers are shown in yellow.

**Isolation.** One combined stream carries every asset's trades, but each asset keeps
its own totals, trade ID checks and event log, so a gap in one never touches another.

**The page's own link.** The browser tab's connection to the app can fail too. Both of
its feeds from the app send something at least every second, and a feed silent for 2
seconds counts as lost: the numbers turn grey, the Binance status shows "unknown" (the
page can no longer know), and the page retries after 1, 2, 4 and then every 5 seconds.
Once back, it gets the full state and rebuilds its debug console from the app's
history, so it catches up at once.

A debug console on the page lists every gap, duplicate and reconnect as it happens,
marked with its asset (or SYSTEM for the connection, PAGE for the tab's own steps).

## Checked against Binance

Once each minute has finished, its candle (open, high, low, close and volume), built
from the live trades, is compared with Binance's official one-minute candle. The page
shows the running count ("Checked vs Binance: 312/312 minutes match"), and any mismatch
goes to the debug console. A minute is only checked once it can't still change: no trade
IDs missing, a live trade after it (so a silently dead connection can't hide a trade),
and 10 s for Binance's candle to settle.

Replaying a full day of trades through the same code (`python backtest.py 2026-10-14`)
gives:

| Asset | Aggregated trades | Day VWAP vs Binance (relative difference) | One-minute candles matching Binance | Trades split across a minute boundary |
|---|---:|---:|---:|---:|
| BTCUSDT | 1,280,357 | 5.2e-12 | 1,440 / 1,440 | 90 |
| ETHUSDT | 1,300,812 | 2.0e-12 | 1,440 / 1,440 | 89 |
| SOLUSDT | 286,696 | 3.1e-13 | 1,440 / 1,440 | 43 |
| BNBUSDT | 212,120 | 6.9e-13 | 1,440 / 1,440 | 12 |
| XAUUSDT | 623,364 | 6.5e-13 | 1,440 / 1,440 | 30 |
| XAGUSDT | 102,444 | 4.7e-14 | 1,440 / 1,440 | 3 |
| CLUSDT | 90,566 | 1.3e-14 | 1,440 / 1,440 | 3 |
| SPYUSDT | 27,179 | 6.9e-14 | 1,440 / 1,440 | 1 |
| QQQUSDT | 88,854 | 1.5e-13 | 1,440 / 1,440 | 1 |
| NVDAUSDT | 100,946 | 4.8e-14 | 1,440 / 1,440 | 7 |
| TSLAUSDT | 88,943 | 2.0e-14 | 1,440 / 1,440 | 2 |
| MSTRUSDT | 162,473 | 3.3e-13 | 1,440 / 1,440 | 1 |

A candle matches when its open, high, low and close are equal and its volume (in units
and in USDT) agrees to 1e-9. For BTC, ETH and gold, 2026-10-13 gives the same result
(every minute matches; 101, 83 and 24 split trades).

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
  to a minute start and the trades after it. It picks a minute start with no trade in
  the 100 ms before it, so no part of a trade can fall between the two.

## Break it yourself

Two buttons on the page let you test the recovery yourself, with nothing scripted or
faked:

- **Simulate Wi-Fi cut** cuts the app's connection to Binance. The open connection stops
  reading, so trades and pongs stop arriving, and new connections and REST requests fail
  as they would offline. Within about 2.5 seconds the status shows "Binance: can't
  reach" and retries back off. Press **Restore Wi-Fi** and the next retry reconnects.
  Each asset then finds the trades it missed, fetches them (the numbers are yellow until
  they're in), and those minutes are checked against Binance like any other. The page's
  own link to the app stays up, so it keeps showing what the engine sees.
- **Simulate app disconnect** cuts this browser tab's connection to the app (other tabs
  aren't affected). Within about 2 seconds of silence the page greys out the numbers it
  can no longer trust, retries, and catches up the moment it's back. The app kept running
  the whole time, so nothing was missed.

Watch the debug console while you do it: each step shows as it happens.

## Running it

```
pip install -r requirements.txt
python server.py                 # http://127.0.0.1:8000, or the next free port
python server.py --port 8050     # choose the port yourself
python metrics.py                # terminal output instead of the web page
```

Press Ctrl+C to stop. Binance blocks connections from the US, so if you're in the US,
connect through a VPN in another country first.

To add an asset, add its symbol to `SYMBOLS` in `metrics.py`. The chart uses
TradingView's open-source [Lightweight Charts](https://github.com/tradingview/lightweight-charts) library.

## Tests

```
python -m pytest                 # everything
python -m pytest -m "not live"   # offline only, a few seconds
python -m pytest -m live         # live only, about 2 minutes
```

The tests live in `tests/`. Most run offline against fake local servers that drop
connections, stop answering pings, skip trades and send broken messages. Two live tests compare
results with Binance's official one-minute candles; they need an internet connection
(and the VPN above if you're in the US).

Every push and pull request runs the offline tests on GitHub Actions
(`.github/workflows/tests.yml`). The live tests are left out there because Binance
refuses connections from GitHub's US-based servers.

## Backtest

```
python backtest.py [YYYY-MM-DD] [--symbol BTCUSDT]
```

Replays a full day of every asset's trades from Binance's historical trade files through
the same code the live app runs, and checks the day's VWAP, volume and buy volume
against Binance's official daily candle, and every one-minute candle against its
official one. It defaults to yesterday (UTC); the files, about 60 MB for all twelve
assets, are kept in `data/`. On a day that starts with part of the previous day's last
trade, it also downloads the previous day's file to read that trade. It exits with code
1 if any check fails.

## Roadmap

- More metrics (VWAP bands, large-trade alerts, order-book imbalance)
