"""SQLite caching layer for market data."""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataCache:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS minute_bars (
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
                "CREATE INDEX IF NOT EXISTS idx_bars_symbol "
                "ON minute_bars(symbol)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_bars_ts "
                "ON minute_bars(timestamp)"
            )

    def get_bars(
        self, symbols: List[str], start: datetime, end: datetime
    ) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ",".join("?" * len(symbols))
            query = (
                f"SELECT * FROM minute_bars "
                f"WHERE symbol IN ({placeholders}) "
                f"AND timestamp >= ? AND timestamp <= ?"
            )
            params = symbols + [start.isoformat(), end.isoformat()]
            df = pd.read_sql_query(query, conn, params=params)

        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    def get_cached_symbols(self, start: datetime, end: datetime) -> set:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT DISTINCT symbol FROM minute_bars "
                "WHERE timestamp >= ? AND timestamp <= ?",
                (start.isoformat(), end.isoformat()),
            )
            return {row[0] for row in cursor.fetchall()}

    def store_bars(self, df: pd.DataFrame):
        if df.empty:
            return

        cols = [
            "symbol", "timestamp", "open", "high", "low",
            "close", "volume", "vwap", "trade_count",
        ]
        store_df = df[cols].copy()
        store_df["timestamp"] = store_df["timestamp"].apply(
            lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
        )

        records = store_df.values.tolist()
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO minute_bars "
                "(symbol, timestamp, open, high, low, close, volume, vwap, trade_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                records,
            )
        logger.debug(f"Cached {len(records)} bars")

    def clear(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM minute_bars")
        logger.info("Cache cleared")
