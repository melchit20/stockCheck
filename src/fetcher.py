"""Alpaca API client with rate limiting and SQLite caching."""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List
from zoneinfo import ZoneInfo

import pandas as pd
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from .cache import DataCache
from .config import AppConfig

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


class AlpacaFetcher:
    BATCH_SIZE = 20

    def __init__(self, config: AppConfig):
        self.config = config

        api_key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY")
        secret_key = os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_API_SECRET")

        if not api_key or not secret_key:
            raise EnvironmentError(
                "Alpaca API credentials not found. "
                "Set APCA_API_KEY_ID and APCA_API_SECRET_KEY environment variables."
            )

        self.client = StockHistoricalDataClient(
            api_key=api_key, secret_key=secret_key
        )
        self.cache = DataCache(config.cache.db_path)
        self._request_times: List[float] = []

    def _rate_limit(self):
        now = time.time()
        cutoff = now - 60
        self._request_times = [t for t in self._request_times if t > cutoff]

        if len(self._request_times) >= self.config.alpaca.max_requests_per_min:
            sleep_for = 60 - (now - self._request_times[0]) + 0.5
            if sleep_for > 0:
                logger.info(f"Rate limit: sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)

        self._request_times.append(time.time())

    def _get_date_range(self) -> tuple[datetime, datetime]:
        """Calendar date range that covers the required number of trading days."""
        now = datetime.now(ET)
        calendar_days = int(self.config.scan.lookback_days * 1.6) + 10
        start = now - timedelta(days=calendar_days)
        return start, now

    def _filter_morning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only the first N minutes after market open (9:30 ET)."""
        if df.empty:
            return df

        df = df.copy()
        et_times = df["timestamp"].dt.tz_convert(ET)
        market_open = datetime.combine(
            datetime.today(), datetime.strptime("09:30", "%H:%M").time()
        )
        cutoff = (
            market_open + timedelta(minutes=self.config.scan.morning_minutes)
        ).time()

        mask = (et_times.dt.time >= market_open.time()) & (
            et_times.dt.time < cutoff
        )
        return df.loc[mask].copy()

    def _fetch_from_api(self, symbols: List[str], start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch bars from Alpaca, respecting rate limits, in batches."""
        all_dfs: list[pd.DataFrame] = []

        for i in range(0, len(symbols), self.BATCH_SIZE):
            batch = symbols[i : i + self.BATCH_SIZE]
            batch_num = i // self.BATCH_SIZE + 1
            total_batches = (len(symbols) + self.BATCH_SIZE - 1) // self.BATCH_SIZE
            logger.info(
                f"  API batch {batch_num}/{total_batches}: "
                f"{', '.join(batch[:5])}{'...' if len(batch) > 5 else ''}"
            )

            self._rate_limit()
            try:
                feed = DataFeed.IEX if self.config.alpaca.feed == "iex" else DataFeed.SIP
                request = StockBarsRequest(
                    symbol_or_symbols=batch,
                    timeframe=TimeFrame.Minute,
                    start=start,
                    end=end,
                    feed=feed,
                )
                bars = self.client.get_stock_bars(request)
                df = bars.df

                if df is not None and not df.empty:
                    df = df.reset_index()
                    if df["timestamp"].dt.tz is None:
                        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
                    else:
                        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

                    for col in ("vwap", "trade_count"):
                        if col not in df.columns:
                            df[col] = None

                    try:
                        self.cache.store_bars(df)
                    except Exception as e:
                        logger.warning(f"Cache write failed (non-fatal): {e}")

                    all_dfs.append(df)

            except Exception as e:
                logger.error(f"Failed to fetch batch {batch_num}: {e}")

        if not all_dfs:
            return pd.DataFrame()
        return pd.concat(all_dfs, ignore_index=True)

    def fetch_morning_bars(self, symbols: List[str]) -> pd.DataFrame:
        """
        Fetch morning minute bars for the given symbols over the lookback window.
        Uses SQLite cache to avoid redundant API calls.
        """
        start, end = self._get_date_range()

        cached_symbols = self.cache.get_cached_symbols(start, end)
        hits = [s for s in symbols if s in cached_symbols]
        misses = [s for s in symbols if s not in cached_symbols]

        all_dfs: list[pd.DataFrame] = []

        if hits:
            logger.info(f"Cache hit for {len(hits)} symbols")
            cached_df = self.cache.get_bars(hits, start, end)
            if not cached_df.empty:
                all_dfs.append(cached_df)

        if misses:
            logger.info(f"Fetching {len(misses)} symbols from Alpaca API")
            api_df = self._fetch_from_api(misses, start, end)
            if not api_df.empty:
                all_dfs.append(api_df)

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)
        morning = self._filter_morning(combined)
        logger.info(f"Morning bars: {len(morning)} rows for {morning['symbol'].nunique()} symbols")
        return morning
