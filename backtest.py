"""Backtest the engine on a full day of Binance trades, for every asset the app tracks.

Feeds every trade from the day's aggTrades archive through the same SessionTracker
the live app uses, then checks what it built against Binance's official figures:
- the day's totals (volume in USDT and in coins, VWAP, taker buy volume) against
  the daily kline
- every one-minute candle (open, high, low, close, volume) against Binance's 1m
  klines, with the same comparison the live app runs each minute (candle_differences)

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
from datetime import datetime, timedelta, timezone
from pathlib import Path

from metrics import AGG_WINDOW_MS, MINUTE_MS, SYMBOLS, SessionTracker, check_minute, format_time

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
    for row in read_csv_rows(fetch_archive(symbol, "aggTrades", day)):
        # is_buyer_maker true means the seller was the taker
        is_buy = row["is_buyer_maker"].lower() == "false"
        tracker.add_trade(float(row["price"]), float(row["quantity"]), int(row["transact_time"]), is_buy)
        trade_count += 1

    kline = next(read_csv_rows(fetch_archive(symbol, "1d", day)))
    official_pv = float(kline["quote_volume"])
    official_volume = float(kline["volume"])

    print(f"\n{symbol} {day} (UTC): {trade_count:,} aggregated trades, tolerance {REL_TOL:g} relative")
    print("  Day totals vs Binance's daily kline")
    results = [
        compare("cumulative_pv", tracker.cumulative_pv, official_pv),
        compare("cumulative_volume", tracker.cumulative_volume, official_volume),
        compare("vwap", tracker.vwap, official_pv / official_volume),
        compare("buy_volume", tracker.buy_volume, float(kline["taker_buy_volume"])),
        compare("buy_pv", tracker.buy_pv, float(kline["taker_buy_quote_volume"])),
    ]

    # The same comparison the live app runs each minute (check_minute): prices equal, volumes to 1e-9,
    # allowing for a trade that Binance's candles split across a minute boundary.
    print("  One-minute candles vs Binance's 1m klines")
    minutes = [kline_row(row) for row in read_csv_rows(fetch_archive(symbol, "1m", day))]
    candles = tracker.candles
    mismatched = splits = 0
    spill = start_spill(candles, minutes)
    for official in minutes:
        differences, spill = check_minute(official[0] // MINUTE_MS, candles.get(official[0] // MINUTE_MS),
                                          official, spill)
        splits += spill is not None
        if differences:
            mismatched += 1
            if mismatched <= 5:  # enough to see what's wrong without flooding the terminal
                print(f"    {format_time(official[0])[:5]} UTC: " + "; ".join(differences))
    results.append(mismatched == 0)
    print(f"  {len(minutes) - mismatched}/{len(minutes)} minutes match "
          f"(open, high, low, close, volume, USDT volume)  {'PASS' if mismatched == 0 else 'FAIL'}")
    print(f"  {splits} trades straddled a minute boundary (Binance's candles count their fills in both minutes)")
    return all(results)


def start_spill(candles, minutes):
    """The part of the previous day's last trade that Binance counts in this day's first minute, or None.

    That trade isn't in this day's file. Up to the first boundary no trade can straddle,
    every split carries over to the next minute, so the part is Binance's volume minus
    ours over those minutes; it came first, so its price is Binance's first open.
    """
    extra = 0.0
    for official in minutes:
        minute = official[0] // MINUTE_MS
        ours = candles.get(minute)
        extra += float(official[5]) - (ours[7] if ours else 0.0)
        if ours is None or ours[4] < (minute + 1) * MINUTE_MS - AGG_WINDOW_MS:
            break  # a clean boundary after this minute
    return (extra, float(minutes[0][1])) if extra > 1e-9 * float(minutes[0][5]) else None


def utc_day(text):
    """argparse type for a YYYY-MM-DD day."""
    return datetime.strptime(text, "%Y-%m-%d").date()


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
