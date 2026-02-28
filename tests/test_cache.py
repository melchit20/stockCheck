"""Tests for the SQLite caching layer."""

import os
import tempfile
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.cache import DataCache


@pytest.fixture
def cache(tmp_path):
    db_path = str(tmp_path / "test.db")
    return DataCache(db_path)


def _sample_bars():
    return pd.DataFrame([
        {
            "symbol": "AAPL",
            "timestamp": pd.Timestamp("2025-01-02 14:30:00+00:00"),
            "open": 150.0,
            "high": 151.0,
            "low": 149.0,
            "close": 150.5,
            "volume": 1000.0,
            "vwap": 150.2,
            "trade_count": 10,
        },
        {
            "symbol": "AAPL",
            "timestamp": pd.Timestamp("2025-01-02 14:31:00+00:00"),
            "open": 150.5,
            "high": 151.5,
            "low": 150.0,
            "close": 151.0,
            "volume": 1200.0,
            "vwap": 150.8,
            "trade_count": 12,
        },
    ])


class TestDataCache:
    def test_store_and_retrieve(self, cache):
        bars = _sample_bars()
        cache.store_bars(bars)

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 3, tzinfo=timezone.utc)
        result = cache.get_bars(["AAPL"], start, end)

        assert len(result) == 2
        assert result.iloc[0]["symbol"] == "AAPL"
        assert result.iloc[0]["close"] == 150.5

    def test_cached_symbols(self, cache):
        bars = _sample_bars()
        cache.store_bars(bars)

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 3, tzinfo=timezone.utc)
        symbols = cache.get_cached_symbols(start, end)

        assert "AAPL" in symbols

    def test_duplicate_insert_ignored(self, cache):
        bars = _sample_bars()
        cache.store_bars(bars)
        cache.store_bars(bars)  # should not raise

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 3, tzinfo=timezone.utc)
        result = cache.get_bars(["AAPL"], start, end)
        assert len(result) == 2

    def test_empty_store(self, cache):
        cache.store_bars(pd.DataFrame())  # should not raise

    def test_clear(self, cache):
        bars = _sample_bars()
        cache.store_bars(bars)
        cache.clear()

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 3, tzinfo=timezone.utc)
        result = cache.get_bars(["AAPL"], start, end)
        assert len(result) == 0
