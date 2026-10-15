"""Tests for backtest.py. Run with: python -m pytest

The archives are faked: each test hands backtest() the rows of the files it would download.
"""

from datetime import date, datetime, timezone

import backtest
import metrics

DAY = date(2026, 10, 5)
MIDNIGHT_MS = int(datetime(2026, 10, 5, tzinfo=timezone.utc).timestamp() * 1000)
NEXT_MIDNIGHT_MS = MIDNIGHT_MS + metrics.DAY_MS


def trade_row(price, quantity, time_ms, is_buy):
    """An aggTrades CSV row, as read from the archive (every value is text)."""
    return {"price": str(price), "quantity": str(quantity), "transact_time": str(time_ms),
            "is_buyer_maker": "false" if is_buy else "true"}


def minute_row(open_time_ms, open_price, high, low, close, volume, quote_volume):
    """A 1m kline CSV row."""
    return {"open_time": str(open_time_ms), "open": str(open_price), "high": str(high), "low": str(low),
            "close": str(close), "volume": str(volume), "close_time": str(open_time_ms + metrics.MINUTE_MS - 1),
            "quote_volume": str(quote_volume)}


def fake_archives(monkeypatch, previous_day_last_trade):
    """A day that starts with 1 of the previous day's last trade (3 at 100, a buy, 40 ms before 00:00)
    and ends with a buy of 1 at 102, 50 ms before 24:00, of which Binance counts 0.5 in the next day."""
    files = {
        ("aggTrades", DAY.replace(day=4)): [trade_row(*previous_day_last_trade)],
        ("aggTrades", DAY): [trade_row(101, 2, MIDNIGHT_MS + 10_000, False),
                             trade_row(102, 1, NEXT_MIDNIGHT_MS - 50, True)],
        # Binance's minutes: 00:00 opens with the previous day's trade; 23:59 has half of the last trade.
        ("1m", DAY): [minute_row(MIDNIGHT_MS, 100, 101, 100, 101, 3, 100 + 202),
                      minute_row(NEXT_MIDNIGHT_MS - metrics.MINUTE_MS, 102, 102, 102, 102, 0.5, 51)],
        # The day's totals: 1 at 100 (buy) + 2 at 101 (sell) + 0.5 at 102 (buy).
        ("1d", DAY): [{"quote_volume": "353", "volume": "3.5", "taker_buy_quote_volume": "151",
                       "taker_buy_volume": "1.5"}],
    }
    monkeypatch.setattr(backtest, "fetch_archive", lambda symbol, kind, day: (kind, day))
    monkeypatch.setattr(backtest, "read_csv_rows", lambda key: iter(files[key]))


def test_day_totals_move_the_parts_of_trades_that_straddle_midnight(monkeypatch, capsys):
    fake_archives(monkeypatch, (100, 3, MIDNIGHT_MS - 40, True))
    assert backtest.backtest("BTCUSDT", DAY) is True
    out = capsys.readouterr().out
    assert "2/2 minutes match" in out
    assert "Moved in: 1 BTC of the previous day's last trade (a buy at 100.0)" in out
    assert "Moved out: 0.5 BTC of this day's last trade (a buy at 102.0)" in out
    assert "FAIL" not in out


def test_extra_volume_at_the_start_needs_the_previous_days_last_trade_to_explain_it(monkeypatch, capsys):
    # The previous day's last trade started 150 ms before midnight: too early to have fills after it.
    fake_archives(monkeypatch, (100, 3, MIDNIGHT_MS - 150, True))
    assert backtest.backtest("BTCUSDT", DAY) is False
    out = capsys.readouterr().out
    assert "Moved in" not in out and "00:00 UTC: open 101.0 vs Binance 100" in out
