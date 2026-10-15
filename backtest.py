"""Backtest the engine on a full day of Binance trades, for every asset the app tracks.

Feeds every trade from the day's aggTrades archive through the same SessionTracker
the live app uses, then checks what it built against Binance's official figures:
- every one-minute candle (open, high, low, close, volume) against Binance's 1m
  klines, with the same comparison the live app runs each minute (check_minute)
- the day's totals (volume in USDT and in coins, VWAP, taker buy volume) against
  the daily kline

Binance counts each fill of an aggregated trade on the day it happened, so a trade
that straddles midnight is split between two days. The day totals take in the part
of the previous day's last trade after 00:00 and leave out the part of this day's
last trade after 24:00; the minute check finds both.

Usage: python backtest.py [YYYY-MM-DD] [--symbol SYMBOL]
(default: yesterday, UTC, and every symbol in metrics.SYMBOLS)
"""

import argparse
import csv
import io
import math
import sys
import urllib.request
import zipfile
from collections import deque
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from metrics import (
    AGG_WINDOW_MS,
    MINUTE_MS,
    QUOTE_ASSET,
    SYMBOLS,
    SessionTracker,
    check_minute,
    format_time,
    trade_totals,
)

ARCHIVE_URL = "https://data.binance.vision/data/futures/um/daily"
DATA_DIR = Path(__file__).parent / "data"
REL_TOL = 1e-9  # one part in a billion


def download(url, path):
    """Download url to path unless the file is already there.

    It's written under another name first, so a download that's cut off never
    leaves a broken file that would be taken as already there next time.
    """
    if path.exists():
        return
    DATA_DIR.mkdir(exist_ok=True)
    print(f"Downloading {url}")
    partial = path.with_name(path.name + ".part")
    urllib.request.urlretrieve(url, partial)
    partial.replace(path)


def fetch_archive(symbol, kind, day):
    """Download one of the day's archives and return its path. kind is "aggTrades" or a kline interval ("1d", "1m")."""
    path = DATA_DIR / f"{symbol}-{kind}-{day}.zip"
    folder = f"aggTrades/{symbol}" if kind == "aggTrades" else f"klines/{symbol}/{kind}"
    download(f"{ARCHIVE_URL}/{folder}/{path.name}", path)
    return path


def read_csv_rows(zip_path):
    """Yield each row of the single CSV inside a zip, as a dict keyed by the header."""
    with zipfile.ZipFile(zip_path) as archive:
        csv_name = archive.namelist()[0]
        with archive.open(csv_name) as raw:
            yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"))


def kline_row(row):
    """A kline CSV row as a list shaped like the REST API's rows, which candle_differences reads."""
    return [int(row["open_time"]), row["open"], row["high"], row["low"], row["close"], row["volume"],
            int(row["close_time"]), row["quote_volume"]]


def compare(name, ours, theirs):
    """Print one comparison line and return True if it is within tolerance."""
    ok = math.isclose(ours, theirs, rel_tol=REL_TOL)
    rel_diff = abs(ours - theirs) / abs(theirs)
    print(f"  {name:<18} ours {ours:<22.10f} binance {theirs:<22.10f} rel diff {rel_diff:.2e}  {'PASS' if ok else 'FAIL'}")
    return ok


def backtest(symbol, day):
    """Replay one asset's day, print every check, and return True if they all passed."""
    tracker = SessionTracker()
    trade_count = 0
    last_trade = None
    for row in read_csv_rows(fetch_archive(symbol, "aggTrades", day)):
        last_trade = trade_values(row)
        tracker.add_trade(*last_trade)
        trade_count += 1
    print(f"\n{symbol} {day} (UTC): {trade_count:,} aggregated trades, tolerance {REL_TOL:g} relative")

    # The same comparison the live app runs each minute (check_minute): prices equal, volumes to 1e-9,
    # allowing for a trade that Binance's candles split across a minute boundary.
    print("  One-minute candles vs Binance's 1m klines")
    minutes = [kline_row(row) for row in read_csv_rows(fetch_archive(symbol, "1m", day))]
    candles = tracker.candles
    mismatched = splits = 0
    start = start_part(symbol, day, candles, minutes)
    spill = start and start[:2]
    for official in minutes:
        differences, spill = check_minute(official[0] // MINUTE_MS, candles.get(official[0] // MINUTE_MS),
                                          official, spill)
        splits += spill is not None
        if differences:
            mismatched += 1
            if mismatched <= 5:  # enough to see what's wrong without flooding the terminal
                print(f"    {format_time(official[0])[:5]} UTC: " + "; ".join(differences))
    results = [mismatched == 0]
    print(f"  {len(minutes) - mismatched}/{len(minutes)} minutes match "
          f"(open, high, low, close, volume, USDT volume)  {'PASS' if mismatched == 0 else 'FAIL'}")
    print(f"  {splits} trades straddled a minute boundary (Binance's candles count their fills in both minutes)")

    # What the last minute passes on is the part of the day's last trade that Binance counts in the next day.
    end = spill and (*spill, last_trade[3])
    print("  Day totals vs Binance's daily kline")
    totals = [tracker.cumulative_pv, tracker.cumulative_volume, tracker.buy_pv, tracker.buy_volume]
    for part, sign, whose, where in ((start, 1, "the previous day's last trade", "after 00:00"),
                                     (end, -1, "this day's last trade", "in the next day")):
        if part:
            quantity, price, is_buy = part
            print(f"    {'Moved in' if sign > 0 else 'Moved out'}: {quantity:g} {symbol.removesuffix(QUOTE_ASSET)} "
                  f"of {whose} ({'a buy' if is_buy else 'a sell'} at {price}), which Binance counts {where}")
            for i, value in enumerate(trade_totals(price, quantity, is_buy)):
                totals[i] += sign * value
    pv, volume, buy_pv, buy_volume = totals
    kline = next(read_csv_rows(fetch_archive(symbol, "1d", day)))
    official_pv, official_volume = float(kline["quote_volume"]), float(kline["volume"])
    results += [
        compare("cumulative_pv", pv, official_pv),
        compare("cumulative_volume", volume, official_volume),
        compare("vwap", pv / volume, official_pv / official_volume),
        compare("buy_volume", buy_volume, float(kline["taker_buy_volume"])),
        compare("buy_pv", buy_pv, float(kline["taker_buy_quote_volume"])),
    ]
    return all(results)


def trade_values(row):
    """An aggTrades CSV row as (price, quantity, time_ms, is_buy). is_buyer_maker true means the seller was the taker."""
    return (float(row["price"]), float(row["quantity"]), int(row["transact_time"]),
            row["is_buyer_maker"].lower() == "false")


def start_part(symbol, day, candles, minutes):
    """The part of the previous day's last trade that Binance counts in this day, as (quantity, price, is_buy), or None.

    That trade isn't in this day's file. Up to the first boundary no trade can straddle,
    every split carries over to the next minute, so the part is Binance's volume minus
    ours over those minutes. Only if there is one is the previous day's file read, to
    check the trade: it must have started within AGG_WINDOW_MS of midnight, be bigger
    than the part, and have the price Binance's first minute opens with. If it doesn't,
    there's no part, and the minute check reports the difference.
    """
    extra = 0.0
    for official in minutes:
        minute = official[0] // MINUTE_MS
        ours = candles.get(minute)
        extra += float(official[5]) - (ours[7] if ours else 0.0)
        if candles.straddle_free(minute + 1):
            break
    if extra <= REL_TOL * float(minutes[0][5]):
        return None
    price, quantity, time_ms, is_buy = trade_values(
        deque(read_csv_rows(fetch_archive(symbol, "aggTrades", day - timedelta(days=1))), maxlen=1)[0])
    if time_ms < minutes[0][0] - AGG_WINDOW_MS or quantity <= extra or price != float(minutes[0][1]):
        return None
    return extra, price, is_buy


def utc_day(text):
    """argparse type for a YYYY-MM-DD day."""
    return date.fromisoformat(text)


def main():
    parser = argparse.ArgumentParser(description="Check the metrics against Binance's official figures for a full day.")
    parser.add_argument("day", nargs="?", type=utc_day, help="the UTC day as YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--symbol", help="backtest only this symbol (default: " + ", ".join(SYMBOLS) + ")")
    args = parser.parse_args()
    day = args.day or datetime.now(timezone.utc).date() - timedelta(days=1)
    symbols = [args.symbol.upper()] if args.symbol else SYMBOLS

    failed = [symbol for symbol in symbols if not backtest(symbol, day)]
    print("\nAll checks passed." if not failed else f"\nSome checks FAILED: {', '.join(failed)}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
