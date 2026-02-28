"""
Monte Carlo parameter sweep engine (optimized).

Pre-computes per-stock per-day pre-market stats for each window size,
then filters by thresholds — turning O(N*bars) into O(N) per combo.
"""

import itertools
import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


@dataclass
class SweepRange:
    stock_jump_values: List[float]
    dji_bps_values: List[float]
    premarket_min_values: List[int]

    @property
    def total_combos(self) -> int:
        return (
            len(self.stock_jump_values)
            * len(self.dji_bps_values)
            * len(self.premarket_min_values)
        )


def load_cached_bars(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM polygon_bars", conn)
    conn.close()
    if df.empty:
        raise ValueError(f"No cached data in {db_path}. Run the scanner first.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    logger.info(
        f"Loaded {len(df)} cached bars for {df['symbol'].nunique()} symbols"
    )
    return df


def _precompute_daily_stats(
    all_bars: pd.DataFrame,
    dji_symbol: str,
    symbols: List[str],
    pm_minutes: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    For a given pre-market window size, compute per-stock per-day stats
    and per-day DJI stats. Returns (stock_stats, dji_stats).
    """
    bars = all_bars.copy()
    bars["et"] = bars["timestamp"].dt.tz_convert(ET)
    bars["date"] = bars["et"].dt.date
    bars["et_hour"] = bars["et"].dt.hour
    bars["et_min"] = bars["et"].dt.minute

    pm_start = 30 - pm_minutes

    # --- DJI daily stats ---
    dji = bars[bars["symbol"] == dji_symbol]
    dji_pm = dji[(dji["et_hour"] == 9) & (dji["et_min"] >= pm_start) & (dji["et_min"] <= 29)]
    dji_stats_rows = []
    for date, grp in dji_pm.groupby("date"):
        grp = grp.sort_values("timestamp")
        o = grp.iloc[0]["open"]
        c = grp.iloc[-1]["close"]
        bps = ((c - o) / o) * 10_000 if o > 0 else 0
        dji_stats_rows.append({"date": date, "dji_bps": bps})
    dji_stats = pd.DataFrame(dji_stats_rows)

    # --- Stock daily stats ---
    stock_rows = []
    for sym in symbols:
        if sym == dji_symbol:
            continue
        s_bars = bars[bars["symbol"] == sym]
        if s_bars.empty:
            continue

        for date, day in s_bars.groupby("date"):
            pm = day[(day["et_hour"] == 9) & (day["et_min"] >= pm_start) & (day["et_min"] <= 29)]
            mkt = day[(day["et_hour"] == 9) & (day["et_min"] == 30)]
            if pm.empty or mkt.empty:
                continue
            pm = pm.sort_values("timestamp")
            pm_open = pm.iloc[0]["open"]
            pm_close = pm.iloc[-1]["close"]
            mkt_close = mkt.iloc[0]["close"]
            stock_rows.append({
                "symbol": sym,
                "date": date,
                "pm_open": pm_open,
                "pm_close": pm_close,
                "is_dip": pm_close <= pm_open,
                "jump": mkt_close - pm_close,
            })

    stock_stats = pd.DataFrame(stock_rows)
    return stock_stats, dji_stats


def run_sweep(
    all_bars: pd.DataFrame,
    dji_symbol: str,
    symbols: List[str],
    sweep: SweepRange,
) -> pd.DataFrame:
    """Run the full parameter sweep using precomputed daily stats."""

    # Precompute stats for each pm_minutes value
    precomputed: Dict[int, Tuple[pd.DataFrame, pd.DataFrame]] = {}
    for pm in sweep.premarket_min_values:
        logger.info(f"Pre-computing daily stats for {pm}-min window...")
        t0 = time.time()
        stock_stats, dji_stats = _precompute_daily_stats(
            all_bars, dji_symbol, symbols, pm
        )
        logger.info(
            f"  {len(stock_stats)} stock-day rows, "
            f"{len(dji_stats)} DJI days ({time.time() - t0:.1f}s)"
        )
        precomputed[pm] = (stock_stats, dji_stats)

    combos = list(itertools.product(
        sweep.stock_jump_values,
        sweep.dji_bps_values,
        sweep.premarket_min_values,
    ))

    results = []
    t0 = time.time()

    for idx, (sj, db, pm) in enumerate(combos, 1):
        stock_stats, dji_stats = precomputed[pm]

        # Filter: dip stocks that jumped enough
        signals = stock_stats[stock_stats["is_dip"] & (stock_stats["jump"] >= sj)]

        if signals.empty:
            results.append({
                "stock_jump": sj, "dji_bps": db, "pm_minutes": pm,
                "total_signals": 0, "buy_signals": 0, "stocks_with_buys": 0,
                "buy_rate_pct": 0, "top_stock": "", "top_stock_buys": 0,
            })
            continue

        # Join with DJI stats to find buy signals
        merged = signals.merge(dji_stats, on="date", how="left")
        merged["dji_bps"] = merged["dji_bps"].fillna(0)
        buys = merged[merged["dji_bps"] >= db]

        total_signals = len(signals)
        buy_signals = len(buys)
        buy_rate = (buy_signals / total_signals * 100) if total_signals > 0 else 0

        stock_buys = buys.groupby("symbol").size()
        stocks_with_buys = len(stock_buys)
        top_stock = stock_buys.idxmax() if not stock_buys.empty else ""
        top_count = int(stock_buys.max()) if not stock_buys.empty else 0

        results.append({
            "stock_jump": sj,
            "dji_bps": db,
            "pm_minutes": pm,
            "total_signals": total_signals,
            "buy_signals": buy_signals,
            "stocks_with_buys": stocks_with_buys,
            "buy_rate_pct": round(buy_rate, 1),
            "top_stock": top_stock,
            "top_stock_buys": top_count,
        })

        if idx % 50 == 0 or idx == len(combos):
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 1
            eta = (len(combos) - idx) / rate
            logger.info(
                f"  [{idx}/{len(combos)}] "
                f"jump=${sj} bps={db} pm={pm}min → "
                f"{buy_signals} buys  (ETA {eta:.0f}s)"
            )

    return pd.DataFrame(results)
