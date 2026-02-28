"""Fetch live and recent minute bars from Polygon.io REST API."""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class PolygonLiveData:
    BASE = "https://api.polygon.io"

    def __init__(self, api_key: str, max_calls_per_min: int = 5):
        self.api_key = api_key
        self.max_calls = max_calls_per_min
        self._call_times: List[float] = []

    def _rate_limit(self):
        now = time.time()
        self._call_times = [t for t in self._call_times if t > now - 60]
        if len(self._call_times) >= self.max_calls:
            wait = 60 - (now - self._call_times[0]) + 0.5
            if wait > 0:
                logger.debug(f"Rate-limit: sleeping {wait:.1f}s")
                time.sleep(wait)
        self._call_times.append(time.time())

    def _get(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        params = params or {}
        params["apiKey"] = self.api_key
        self._rate_limit()
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                logger.error(f"Polygon HTTP {resp.status_code}: {resp.text[:200]}")
                return None
            return resp.json()
        except Exception as e:
            logger.error(f"Polygon request failed: {e}")
            return None

    def fetch_minute_bars(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Fetch all minute bars for a symbol over a date range (paginated)."""
        url = f"{self.BASE}/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
        all_rows: List[Dict] = []

        while url:
            data = self._get(url, {"adjusted": "true", "sort": "asc", "limit": "50000"})
            if not data:
                break
            for bar in data.get("results", []):
                ts = pd.Timestamp(bar["t"], unit="ms", tz="UTC")
                all_rows.append({
                    "symbol": symbol,
                    "timestamp": ts,
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar.get("v", 0),
                })
            next_url = data.get("next_url")
            if next_url:
                url = f"{next_url}&apiKey={self.api_key}"
                time.sleep(0.25)
            else:
                url = None

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["et"] = df["timestamp"].dt.tz_convert(ET)
            df["date"] = df["et"].dt.date
            df["et_hour"] = df["et"].dt.hour
            df["et_min"] = df["et"].dt.minute
        return df

    def fetch_today_bars(self, symbol: str) -> pd.DataFrame:
        today = datetime.now(ET).strftime("%Y-%m-%d")
        return self.fetch_minute_bars(symbol, today, today)

    def fetch_daily_closes(
        self, symbol: str, lookback_days: int = 30
    ) -> List[float]:
        """Fetch recent daily closing prices for IV calculation."""
        now = datetime.now(ET)
        end = now.strftime("%Y-%m-%d")
        start = (now - timedelta(days=int(lookback_days * 1.6) + 5)).strftime("%Y-%m-%d")

        url = f"{self.BASE}/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}"
        data = self._get(url, {"adjusted": "true", "sort": "asc", "limit": "100"})
        if not data:
            return []
        return [bar["c"] for bar in data.get("results", [])][-lookback_days:]

    def fetch_bars_for_date_range(
        self, symbol: str, days_back: int
    ) -> pd.DataFrame:
        """Fetch minute bars going back N calendar days from today."""
        now = datetime.now(ET)
        end = now.strftime("%Y-%m-%d")
        start = (now - timedelta(days=int(days_back * 1.6) + 10)).strftime("%Y-%m-%d")
        return self.fetch_minute_bars(symbol, start, end)
