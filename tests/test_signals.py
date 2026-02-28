"""Tests for the signal detection engine."""

import pandas as pd
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from src.config import SignalConfig
from src.signals import detect_signals

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _ts(date_str: str, time_str: str) -> pd.Timestamp:
    """Create a UTC-aware timestamp from ET date and time strings."""
    naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    et = naive.replace(tzinfo=ET)
    return pd.Timestamp(et).tz_convert(UTC)


def _make_bars(rows, symbol="TEST"):
    """Build a bars DataFrame from (date, time, open, close) tuples."""
    records = []
    for date_str, time_str, o, c in rows:
        records.append({
            "symbol": symbol,
            "timestamp": _ts(date_str, time_str),
            "open": o,
            "high": max(o, c),
            "low": min(o, c),
            "close": c,
            "volume": 1000,
            "vwap": (o + c) / 2,
            "trade_count": 10,
        })
    return pd.DataFrame(records)


DEFAULT_CONFIG = SignalConfig(stock_jump_min=0.25, dji_jump_min_bps=19.0)


class TestDetectSignals:
    def test_basic_buy_signal(self):
        """Dip minute + jump minute + DJI up >= 19 bps → buySignal."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 99.80),   # dip: close < open
            ("2025-01-02", "09:31", 99.80,  100.10),   # jump: +0.30 >= 0.25
        ])
        # DJI rises 20 bps during the dip minute
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 400.80),   # +0.20% = 20 bps
            ("2025-01-02", "09:31", 400.80, 401.00),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)

        assert len(result) == 1
        assert bool(result.iloc[0]["is_buy_signal"]) is True
        assert result.iloc[0]["stock_change"] == pytest.approx(0.30, abs=0.01)
        assert result.iloc[0]["dji_change_bps"] == pytest.approx(20.0, abs=0.1)

    def test_signal_without_dji_jump(self):
        """Dip + jump in stock but DJI flat → signal but NOT buySignal."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 50.00, 49.90),
            ("2025-01-02", "09:31", 49.90, 50.20),     # +0.30
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 400.10),   # +2.5 bps < 19
            ("2025-01-02", "09:31", 400.10, 400.12),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)

        assert len(result) == 1
        assert bool(result.iloc[0]["is_buy_signal"]) is False

    def test_no_dip_no_signal(self):
        """Both minutes are up — no signal at all."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 100.50),   # up candle
            ("2025-01-02", "09:31", 100.50, 101.00),
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 401.00),
            ("2025-01-02", "09:31", 401.00, 402.00),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)
        assert len(result) == 0

    def test_dip_but_no_jump(self):
        """Dip followed by small up — below stock_jump_min threshold."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 99.90),    # dip
            ("2025-01-02", "09:31", 99.90,  100.05),   # +0.15 < 0.25
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 401.00),
            ("2025-01-02", "09:31", 401.00, 402.00),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)
        assert len(result) == 0

    def test_flat_candle_counts_as_dip(self):
        """A minute where close == open should count as a dip."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 100.00),   # flat
            ("2025-01-02", "09:31", 100.00, 100.30),   # +0.30
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 400.80),   # 20 bps
            ("2025-01-02", "09:31", 400.80, 401.00),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)

        assert len(result) == 1
        assert bool(result.iloc[0]["is_buy_signal"]) is True

    def test_multiple_signals_same_day(self):
        """Two dip→jump patterns in the same morning."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 99.80),
            ("2025-01-02", "09:31", 99.80,  100.10),   # signal 1
            ("2025-01-02", "09:32", 100.10, 100.00),   # dip
            ("2025-01-02", "09:33", 100.00, 100.30),   # signal 2
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 400.80),
            ("2025-01-02", "09:31", 400.80, 401.00),
            ("2025-01-02", "09:32", 401.00, 401.80),   # 20 bps
            ("2025-01-02", "09:33", 401.80, 402.00),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)
        assert len(result) == 2

    def test_multiple_days(self):
        """Signals across different trading days."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 99.80),
            ("2025-01-02", "09:31", 99.80,  100.10),
            ("2025-01-03", "09:30", 100.00, 99.70),
            ("2025-01-03", "09:31", 99.70,  100.00),
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 400.80),
            ("2025-01-02", "09:31", 400.80, 401.00),
            ("2025-01-03", "09:30", 401.00, 401.80),
            ("2025-01-03", "09:31", 401.80, 402.00),
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)
        assert len(result) == 2
        assert set(result["date"].astype(str)) == {"2025-01-02", "2025-01-03"}

    def test_configurable_thresholds(self):
        """Higher thresholds should reduce signal count."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 99.80),
            ("2025-01-02", "09:31", 99.80,  100.10),   # +0.30
        ])
        dji = _make_bars([
            ("2025-01-02", "09:30", 400.00, 400.80),   # 20 bps
            ("2025-01-02", "09:31", 400.80, 401.00),
        ], symbol="DIA")

        strict = SignalConfig(stock_jump_min=0.50, dji_jump_min_bps=25.0)
        result = detect_signals(stock, dji, strict)
        assert len(result) == 0  # 0.30 < 0.50 threshold

    def test_empty_input(self):
        """Empty DataFrames should return empty result."""
        empty = pd.DataFrame()
        result = detect_signals(empty, empty, DEFAULT_CONFIG)
        assert len(result) == 0

    def test_dji_missing_timestamp(self):
        """If DJI has no bar for the dip minute, signal exists but is not a buySignal."""
        stock = _make_bars([
            ("2025-01-02", "09:30", 100.00, 99.80),
            ("2025-01-02", "09:31", 99.80,  100.10),
        ])
        dji = _make_bars([
            ("2025-01-02", "09:31", 400.80, 401.00),   # only jump minute, not dip
        ], symbol="DIA")

        result = detect_signals(stock, dji, DEFAULT_CONFIG)
        assert len(result) == 1
        assert bool(result.iloc[0]["is_buy_signal"]) is False
        assert result.iloc[0]["dji_change_bps"] == 0.0
