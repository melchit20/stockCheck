"""
Monte Carlo sweep over backtest trade-management parameters.

Fetches BKNG + DIA bars once, then sweeps profit_pct, stop_pct,
and trading_window across all combinations in-memory.
"""

import itertools
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from backtest.trader import fetch_bars_from_polygon

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


@dataclass
class BacktestSweepRange:
    profit_pct_values: List[float]
    stop_pct_values: List[float]
    window_values: List[int]

    @property
    def total_combos(self) -> int:
        return (
            len(self.profit_pct_values)
            * len(self.stop_pct_values)
            * len(self.window_values)
        )


def _precompute_daily_data(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    lookback_days: int,
    stock_jump_min: float,
    dji_jump_min_bps: float,
    premarket_minutes: int,
) -> List[Dict]:
    """
    For each trading day, precompute:
      - whether a buy signal fires
      - entry price (market open)
      - all minute bars during the trading window (9:30-9:59)
    Returns a list of day-records.
    """
    pm_start = 30 - premarket_minutes

    trading_dates = sorted(stock_bars["date"].unique())
    trading_dates = trading_dates[-lookback_days:]

    days = []
    for date in trading_dates:
        s_day = stock_bars[stock_bars["date"] == date]
        d_day = dji_bars[dji_bars["date"] == date]

        s_pm = s_day[
            (s_day["et_hour"] == 9)
            & (s_day["et_min"] >= pm_start)
            & (s_day["et_min"] <= 29)
        ].sort_values("timestamp")

        s_open_bar = s_day[
            (s_day["et_hour"] == 9) & (s_day["et_min"] == 30)
        ]

        if s_pm.empty or s_open_bar.empty:
            continue

        pm_first_open = s_pm.iloc[0]["open"]
        pm_last_close = s_pm.iloc[-1]["close"]
        is_dip = pm_last_close <= pm_first_open

        mkt_open_close = s_open_bar.iloc[0]["close"]
        stock_jump = mkt_open_close - pm_last_close
        is_stock_jump = stock_jump >= stock_jump_min

        dji_bps = 0.0
        d_pm = d_day[
            (d_day["et_hour"] == 9)
            & (d_day["et_min"] >= pm_start)
            & (d_day["et_min"] <= 29)
        ].sort_values("timestamp")
        if not d_pm.empty:
            do = d_pm.iloc[0]["open"]
            dc = d_pm.iloc[-1]["close"]
            if do > 0:
                dji_bps = ((dc - do) / do) * 10_000

        signal = is_dip and is_stock_jump and (dji_bps >= dji_jump_min_bps)
        entry_price = s_open_bar.iloc[0]["open"]

        # Grab all trading bars from 9:30 onward (up to 9:59 to cover any window)
        trading = s_day[
            (s_day["et_hour"] == 9) & (s_day["et_min"] >= 30)
        ].sort_values("timestamp")

        bar_list = [
            (int(row["et_min"]), row["high"], row["low"], row["close"])
            for _, row in trading.iterrows()
        ]

        days.append({
            "date": date,
            "signal": signal,
            "entry_price": entry_price,
            "bars": bar_list,
        })

    return days


def _simulate_trade(
    entry_price: float,
    bars: List[Tuple[int, float, float, float]],
    profit_pct: float,
    stop_pct: float,
    window_minutes: int,
    shares: int,
) -> Dict:
    """Simulate a single trade given parameters."""
    profit_price = entry_price * (1 + profit_pct / 100)
    stop_price = entry_price * (1 - stop_pct / 100)
    cutoff_min = 30 + window_minutes

    exit_price = None
    exit_reason = None
    exit_minute = None

    for minute, high, low, close in bars:
        if minute >= cutoff_min:
            break

        if low <= stop_price and high >= profit_price:
            exit_price = stop_price
            exit_reason = "stop_loss"
            exit_minute = minute
            break
        elif high >= profit_price:
            exit_price = profit_price
            exit_reason = "profit_target"
            exit_minute = minute
            break
        elif low <= stop_price:
            exit_price = stop_price
            exit_reason = "stop_loss"
            exit_minute = minute
            break

    if exit_price is None:
        last_in_window = [
            (m, h, l, c) for m, h, l, c in bars if m < cutoff_min
        ]
        if last_in_window:
            exit_price = last_in_window[-1][3]  # close of last bar
            exit_minute = last_in_window[-1][0]
        else:
            exit_price = entry_price
            exit_minute = 30
        exit_reason = "window_expired"

    pnl = (exit_price - entry_price) * shares
    pnl_pct = ((exit_price - entry_price) / entry_price) * 100

    return {
        "entry_price": entry_price,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "exit_minute": exit_minute,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
    }


def run_backtest_sweep(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    sweep: BacktestSweepRange,
    lookback_days: int = 100,
    shares: int = 10,
    stock_jump_min: float = 2.0,
    dji_jump_min_bps: float = 3.0,
    premarket_minutes: int = 10,
) -> pd.DataFrame:
    """Run full parameter sweep over trade-management params."""

    logger.info("Pre-computing daily signal data...")
    t0 = time.time()
    days = _precompute_daily_data(
        stock_bars, dji_bars, lookback_days,
        stock_jump_min, dji_jump_min_bps, premarket_minutes,
    )
    signal_days = [d for d in days if d["signal"]]
    logger.info(
        f"  {len(days)} trading days, {len(signal_days)} signal days "
        f"({time.time() - t0:.1f}s)"
    )

    combos = list(itertools.product(
        sweep.profit_pct_values,
        sweep.stop_pct_values,
        sweep.window_values,
    ))

    results = []
    t0 = time.time()

    for idx, (pp, sp, wm) in enumerate(combos, 1):
        trades = []
        for day in signal_days:
            trade = _simulate_trade(
                day["entry_price"], day["bars"],
                pp, sp, wm, shares,
            )
            trade["date"] = day["date"]
            trades.append(trade)

        if not trades:
            results.append({
                "profit_pct": pp, "stop_pct": sp, "window_min": wm,
                "trades": 0, "wins": 0, "losses": 0,
                "win_rate": 0, "total_pnl": 0, "avg_pnl": 0,
                "avg_pnl_pct": 0, "best_pnl": 0, "worst_pnl": 0,
                "profit_hits": 0, "stop_hits": 0, "window_exits": 0,
                "sharpe_approx": 0,
            })
            continue

        tdf = pd.DataFrame(trades)
        wins = (tdf["pnl"] > 0).sum()
        losses = (tdf["pnl"] <= 0).sum()
        total_pnl = tdf["pnl"].sum()
        avg_pnl = tdf["pnl"].mean()
        std_pnl = tdf["pnl"].std() if len(tdf) > 1 else 1
        sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0

        results.append({
            "profit_pct": pp,
            "stop_pct": sp,
            "window_min": wm,
            "trades": len(tdf),
            "wins": int(wins),
            "losses": int(losses),
            "win_rate": round(wins / len(tdf) * 100, 1),
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(avg_pnl, 2),
            "avg_pnl_pct": round(tdf["pnl_pct"].mean(), 3),
            "best_pnl": round(tdf["pnl"].max(), 2),
            "worst_pnl": round(tdf["pnl"].min(), 2),
            "profit_hits": int((tdf["exit_reason"] == "profit_target").sum()),
            "stop_hits": int((tdf["exit_reason"] == "stop_loss").sum()),
            "window_exits": int((tdf["exit_reason"] == "window_expired").sum()),
            "sharpe_approx": round(sharpe, 3),
        })

        if idx % 100 == 0 or idx == len(combos):
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 1
            eta = (len(combos) - idx) / rate
            logger.info(f"  [{idx}/{len(combos)}] ETA {eta:.0f}s")

    return pd.DataFrame(results)
