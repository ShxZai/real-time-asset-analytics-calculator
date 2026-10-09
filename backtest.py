"""Backtest SessionTracker on one day of Binance XAUUSDT trades.

Feeds every trade from the day's aggTrades archive through the same
SessionTracker the live script uses, then compares the totals, VWAP and buy
volume (in USDT and in coins) with Binance's official daily kline for that day.

Usage: python backtest.py [YYYY-MM-DD]   (default: yesterday, UTC)
"""

import csv
import io
import math
import sys
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from metrics import SessionTracker

SYMBOL = "XAUUSDT"
ARCHIVE_URL = "https://data.binance.vision/data/futures/um/daily"
DATA_DIR = Path(__file__).parent / "data"
REL_TOL = 1e-9  # one part in a billion


def download(url, path):
    """Download url to path unless the file is already there."""
    if path.exists():
        return
    DATA_DIR.mkdir(exist_ok=True)
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, path)


def read_csv_rows(zip_path):
    """Yield each row of the single CSV inside a zip, as a dict keyed by the header."""
    with zipfile.ZipFile(zip_path) as archive:
        csv_name = archive.namelist()[0]
        with archive.open(csv_name) as raw:
            yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"))


def compare(name, ours, theirs):
    """Print one comparison line and return True if it is within tolerance."""
    ok = math.isclose(ours, theirs, rel_tol=REL_TOL)
    rel_diff = abs(ours - theirs) / abs(theirs)
    print(f"{name:<18} ours {ours:<22.10f} binance {theirs:<22.10f} rel diff {rel_diff:.2e}  {'PASS' if ok else 'FAIL'}")
    return ok


def main():
    if len(sys.argv) > 1:
        day = sys.argv[1]
    else:
        day = str(datetime.now(timezone.utc).date() - timedelta(days=1))

    trades_zip = DATA_DIR / f"{SYMBOL}-aggTrades-{day}.zip"
    kline_zip = DATA_DIR / f"{SYMBOL}-1d-{day}.zip"
    download(f"{ARCHIVE_URL}/aggTrades/{SYMBOL}/{trades_zip.name}", trades_zip)
    download(f"{ARCHIVE_URL}/klines/{SYMBOL}/1d/{kline_zip.name}", kline_zip)

    tracker = SessionTracker()
    trade_count = 0
    for row in read_csv_rows(trades_zip):
        # is_buyer_maker true means the seller was the taker
        is_buy = row["is_buyer_maker"].lower() == "false"
        tracker.add_trade(float(row["price"]), float(row["quantity"]), int(row["transact_time"]), is_buy)
        trade_count += 1

    kline = next(read_csv_rows(kline_zip))
    official_pv = float(kline["quote_volume"])
    official_volume = float(kline["volume"])
    official_vwap = official_pv / official_volume
    official_buy_volume = float(kline["taker_buy_volume"])
    official_buy_pv = float(kline["taker_buy_quote_volume"])

    print(f"{SYMBOL} {day} (UTC): {trade_count} aggregated trades, tolerance {REL_TOL:g} relative\n")
    results = [
        compare("cumulative_pv", tracker.cumulative_pv, official_pv),
        compare("cumulative_volume", tracker.cumulative_volume, official_volume),
        compare("vwap", tracker.vwap, official_vwap),
        compare("buy_volume", tracker.buy_volume, official_buy_volume),
        compare("buy_pv", tracker.buy_pv, official_buy_pv),
    ]
    print("\nAll checks passed." if all(results) else "\nSome checks FAILED.")


if __name__ == "__main__":
    main()
