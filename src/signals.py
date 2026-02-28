"""
Signal detection engine.

This module is intentionally a set of pure functions operating on DataFrames
so it can be reused by a future Monte Carlo tuning harness.
"""

import logging
from typing import List, Dict

import pandas as pd

from .config import SignalConfig

logger = logging.getLogger(__name__)


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
