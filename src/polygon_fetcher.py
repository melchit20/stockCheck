"""Polygon.io API client for pre-market minute bars."""

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from .config import AppConfig

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


class PolygonCache:
    """SQLite cache for Polygon pre-market bars."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS polygon_bars (
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    vwap REAL,
                    trade_count INTEGER,
                    PRIMARY KEY (symbol, timestamp)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_poly_symbol "
                "ON polygon_bars(symbol)"
            )

    def get_cached_symbols(self, start: datetime, end: datetime) -> set:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT DISTINCT symbol FROM polygon_bars "
                "WHERE timestamp >= ? AND timestamp <= ?",
                (start.isoformat(), end.isoformat()),
            )
            return {row[0] for row in cursor.fetchall()}

    def get_bars(
        self, symbols: List[str], start: datetime, end: datetime
    ) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            ph = ",".join("?" * len(symbols))
            df = pd.read_sql_query(
                f"SELECT * FROM polygon_bars "
                f"WHERE symbol IN ({ph}) AND timestamp >= ? AND timestamp <= ?",
                conn,
                params=symbols + [start.isoformat(), end.isoformat()],
            )
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    def store_bars(self, df: pd.DataFrame):
        if df.empty:
            return
        cols = [
            "symbol", "timestamp", "open", "high", "low",
            "close", "volume", "vwap", "trade_count",
        ]
        store = df[cols].copy()
        store["timestamp"] = store["timestamp"].apply(
            lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO polygon_bars "
                "(symbol, timestamp, open, high, low, close, volume, vwap, trade_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                store.values.tolist(),
            )

    def clear(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM polygon_bars")
        logger.info("Polygon cache cleared")


class PolygonFetcher:
    BASE_URL = "https://api.polygon.io/v2/aggs/ticker"

    def __init__(self, config: AppConfig):
        self.config = config
        self.api_key = os.environ.get("POLYGON_API_KEY", "")
        if not self.api_key:
            raise EnvironmentError(
                "POLYGON_API_KEY not set. "
                "Export your Polygon.io API key as POLYGON_API_KEY."
            )
        self.cache = PolygonCache(config.cache.db_path)
        self._request_times: List[float] = []

    def _rate_limit(self, max_per_min: int = 100):
        now = time.time()
        self._request_times = [t for t in self._request_times if now - t < 60]
        if len(self._request_times) >= max_per_min:
            sleep_for = 60 - (now - self._request_times[0]) + 0.5
            if sleep_for > 0:
                logger.info(f"Polygon rate limit: sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)
        self._request_times.append(time.time())

    def _get_date_range(self) -> tuple[str, str]:
        now = datetime.now(ET)
        days_back = int(self.config.scan.lookback_days * 1.6) + 10
        start = now - timedelta(days=days_back)
        return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")

    def _fetch_symbol(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        """Fetch all minute bars for one symbol over the date range."""
        self._rate_limit()
        url = f"{self.BASE_URL}/{symbol}/range/1/minute/{start}/{end}"
        resp = requests.get(
            url,
            params={"apiKey": self.api_key, "adjusted": "true",
                    "sort": "asc", "limit": 50000},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning(f"Polygon {symbol}: HTTP {resp.status_code}")
            return pd.DataFrame()

        data = resp.json()
        results = data.get("results", [])
        if not results:
            return pd.DataFrame()

        rows = []
        for bar in results:
            ts = pd.Timestamp(bar["t"], unit="ms", tz="UTC")
            rows.append({
                "symbol": symbol,
                "timestamp": ts,
                "open": bar["o"],
                "high": bar["h"],
                "low": bar["l"],
                "close": bar["c"],
                "volume": bar.get("v", 0),
                "vwap": bar.get("vw", 0),
                "trade_count": bar.get("n", 0),
            })

        # Handle pagination if needed
        while data.get("next_url"):
            self._rate_limit()
            resp = requests.get(
                data["next_url"],
                params={"apiKey": self.api_key},
                timeout=30,
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            for bar in data.get("results", []):
                ts = pd.Timestamp(bar["t"], unit="ms", tz="UTC")
                rows.append({
                    "symbol": symbol,
                    "timestamp": ts,
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar.get("v", 0),
                    "vwap": bar.get("vw", 0),
                    "trade_count": bar.get("n", 0),
                })

        return pd.DataFrame(rows)

    def _filter_premarket_window(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only bars in the pre-market window + open (9:30)."""
        if df.empty:
            return df
        df = df.copy()
        pm_start = 30 - self.config.signals.premarket_minutes  # e.g. 15 for 15-min window
        et = df["timestamp"].dt.tz_convert(ET)
        mask = (et.dt.hour == 9) & (et.dt.minute >= pm_start) & (et.dt.minute <= 30)
        return df.loc[mask].copy()

    def fetch_premarket_bars(self, symbols: List[str]) -> pd.DataFrame:
        """
        Fetch pre-market + open minute bars for the given symbols.
        Returns bars filtered to the 9:25-9:30 ET window.
        """
        start_str, end_str = self._get_date_range()
        start_dt = pd.Timestamp(start_str, tz="UTC")
        end_dt = pd.Timestamp(end_str, tz="UTC") + pd.Timedelta(days=1)

        cached_syms = self.cache.get_cached_symbols(start_dt, end_dt)
        hits = [s for s in symbols if s in cached_syms]
        misses = [s for s in symbols if s not in cached_syms]

        all_dfs: list[pd.DataFrame] = []

        if hits:
            logger.info(f"Polygon cache hit: {len(hits)} symbols")
            cached_df = self.cache.get_bars(hits, start_dt, end_dt)
            if not cached_df.empty:
                all_dfs.append(cached_df)

        if misses:
            logger.info(f"Fetching {len(misses)} symbols from Polygon")
            for i, sym in enumerate(misses, 1):
                if i % 50 == 0 or i == len(misses):
                    logger.info(f"  Polygon: {i}/{len(misses)}")
                try:
                    df = self._fetch_symbol(sym, start_str, end_str)
                    if not df.empty:
                        filtered = self._filter_premarket_window(df)
                        if not filtered.empty:
                            try:
                                self.cache.store_bars(filtered)
                            except Exception as e:
                                logger.warning(f"Cache write {sym}: {e}")
                            all_dfs.append(filtered)
                except Exception as e:
                    logger.error(f"Polygon fetch {sym}: {e}")

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)
        logger.info(
            f"Pre-market bars: {len(combined)} rows, "
            f"{combined['symbol'].nunique()} symbols"
        )
        return combined
