"""
Signal detection engine.

This module is intentionally a set of pure functions operating on DataFrames
so it can be reused by a future Monte Carlo tuning harness.
"""

import logging
from typing import List, Dict
from zoneinfo import ZoneInfo

import pandas as pd

from .config import SignalConfig

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def detect_signals(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    config: SignalConfig,
) -> pd.DataFrame:
    """
    Detect buy signals for a single stock.

    Parameters
    ----------
    stock_bars : DataFrame
        Minute bars for one stock with columns
        [symbol, timestamp, open, high, low, close, ...].
        Must be sorted by timestamp and filtered to morning minutes.
    dji_bars : DataFrame
        Minute bars for the DJI proxy with the same schema.
    config : SignalConfig
        Tunable thresholds.

    Returns
    -------
    DataFrame with one row per signal found:
        date, dip_timestamp, jump_timestamp,
        dip_open, dip_close, jump_open, jump_close,
        stock_change, dji_open, dji_close, dji_change_bps,
        is_buy_signal
    """
    if stock_bars.empty:
        return pd.DataFrame()

    stock_bars = stock_bars.sort_values("timestamp").copy()
    stock_bars["date"] = stock_bars["timestamp"].dt.date

    dji_lookup = _build_dji_lookup(dji_bars)

    signals: List[Dict] = []

    for date, day_bars in stock_bars.groupby("date"):
        day_bars = day_bars.sort_values("timestamp").reset_index(drop=True)

        for i in range(len(day_bars) - 1):
            cur = day_bars.iloc[i]
            nxt = day_bars.iloc[i + 1]

            if cur["close"] > cur["open"]:
                continue

            stock_change = nxt["close"] - cur["close"]
            if stock_change < config.stock_jump_min:
                continue

            dji_open, dji_close, dji_bps = _dji_during(
                cur["timestamp"], dji_lookup
            )

            signals.append(
                {
                    "date": date,
                    "dip_timestamp": cur["timestamp"],
                    "jump_timestamp": nxt["timestamp"],
                    "dip_open": cur["open"],
                    "dip_close": cur["close"],
                    "jump_open": nxt["open"],
                    "jump_close": nxt["close"],
                    "stock_change": round(stock_change, 4),
                    "dji_open": dji_open,
                    "dji_close": dji_close,
                    "dji_change_bps": round(dji_bps, 2),
                    "is_buy_signal": dji_bps >= config.dji_jump_min_bps,
                }
            )

    return pd.DataFrame(signals)


def _build_dji_lookup(dji_bars: pd.DataFrame) -> dict:
    """Index DJI bars by timestamp for O(1) lookups."""
    if dji_bars.empty:
        return {}
    lookup = {}
    for _, row in dji_bars.iterrows():
        lookup[row["timestamp"]] = row
    return lookup


def _dji_during(ts, dji_lookup: dict) -> tuple[float | None, float | None, float]:
    """Return (open, close, change_bps) for DJI at the given timestamp."""
    if ts not in dji_lookup:
        return None, None, 0.0
    row = dji_lookup[ts]
    o, c = row["open"], row["close"]
    if o == 0:
        return o, c, 0.0
    bps = ((c - o) / o) * 10_000
    return o, c, bps


# ---------------------------------------------------------------------------
# Pre-market strategy
# ---------------------------------------------------------------------------

def detect_premarket_signals(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    config: SignalConfig,
) -> pd.DataFrame:
    """
    Detect buy signals using the pre-market rubric.

    For each trading day:
      - Dip:  stock net flat/down from 9:25→9:29 ET (pre-market)
      - Jump: stock's 9:30 bar close ≥ pre-market close + stock_jump_min
      - DJI:  DIA net change 9:25→9:29 ≥ dji_jump_min_bps

    Parameters
    ----------
    stock_bars : DataFrame
        Bars for one stock covering 9:25-9:30 ET window.
    dji_bars : DataFrame
        Bars for DJI proxy covering the same window.
    config : SignalConfig
        Tunable thresholds.
    """
    if stock_bars.empty:
        return pd.DataFrame()

    stock_bars = stock_bars.sort_values("timestamp").copy()
    stock_bars["date"] = stock_bars["timestamp"].dt.date
    stock_bars["et_min"] = stock_bars["timestamp"].dt.tz_convert(ET).dt.minute
    stock_bars["et_hour"] = stock_bars["timestamp"].dt.tz_convert(ET).dt.hour

    dji_bars = dji_bars.sort_values("timestamp").copy()
    dji_bars["date"] = dji_bars["timestamp"].dt.date
    dji_bars["et_min"] = dji_bars["timestamp"].dt.tz_convert(ET).dt.minute
    dji_bars["et_hour"] = dji_bars["timestamp"].dt.tz_convert(ET).dt.hour

    dji_by_date = dict(tuple(dji_bars.groupby("date")))

    signals: List[Dict] = []

    pm_start = 30 - config.premarket_minutes  # e.g. 15 for 15-min window

    for date, day in stock_bars.groupby("date"):
        pm = day[(day["et_hour"] == 9) & (day["et_min"] >= pm_start) & (day["et_min"] <= 29)]
        market_open = day[(day["et_hour"] == 9) & (day["et_min"] == 30)]

        if pm.empty or market_open.empty:
            continue

        pm = pm.sort_values("timestamp")
        pm_first_open = pm.iloc[0]["open"]
        pm_last_close = pm.iloc[-1]["close"]

        if pm_last_close > pm_first_open:
            continue

        open_close = market_open.iloc[0]["close"]
        stock_jump = open_close - pm_last_close

        if stock_jump < config.stock_jump_min:
            continue

        dji_day = dji_by_date.get(date, pd.DataFrame())
        dji_pm_open = dji_pm_close = dji_bps = None
        is_buy = False

        if not isinstance(dji_day, pd.DataFrame) or dji_day.empty:
            pass
        else:
            dji_pm = dji_day[
                (dji_day["et_hour"] == 9)
                & (dji_day["et_min"] >= pm_start)
                & (dji_day["et_min"] <= 29)
            ].sort_values("timestamp")
            if not dji_pm.empty:
                dji_pm_open = dji_pm.iloc[0]["open"]
                dji_pm_close = dji_pm.iloc[-1]["close"]
                if dji_pm_open and dji_pm_open > 0:
                    dji_bps = ((dji_pm_close - dji_pm_open) / dji_pm_open) * 10_000
                    is_buy = dji_bps >= config.dji_jump_min_bps

        signals.append({
            "date": date,
            "pm_open": pm_first_open,
            "pm_close": pm_last_close,
            "pm_change": round(pm_last_close - pm_first_open, 4),
            "pm_bars": len(pm),
            "market_open_close": open_close,
            "stock_jump": round(stock_jump, 4),
            "dji_pm_open": dji_pm_open,
            "dji_pm_close": dji_pm_close,
            "dji_change_bps": round(dji_bps, 2) if dji_bps is not None else 0.0,
            "is_buy_signal": is_buy,
        })

    return pd.DataFrame(signals)
