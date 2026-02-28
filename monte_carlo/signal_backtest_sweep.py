#!/usr/bin/env python3
"""
Joint MC sweep: signal params × backtest, targeting signal frequency + win rate.

Sweeps stock_jump_min, dji_jump_min_bps, premarket_minutes while holding
Strategy A trade params fixed (0.25% profit, 0.50% stop). For each combo,
runs a full backtest and reports signal count, win rate, and P&L.
"""

import argparse
import itertools
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from backtest.trader import fetch_bars_from_polygon

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def _run_single_backtest(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    trading_dates: list,
    stock_jump_min: float,
    dji_jump_min_bps: float,
    premarket_minutes: int,
    profit_pct: float,
    stop_pct: float,
    window_minutes: int,
    shares: int,
) -> Dict:
    """Run one backtest config and return summary stats."""
    pm_start = 30 - premarket_minutes

    signal_count = 0
    trades_executed = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    pnls = []
    exit_reasons = {"profit_target": 0, "stop_loss": 0, "window_expired": 0}

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

        pm_open = s_pm.iloc[0]["open"]
        pm_close = s_pm.iloc[-1]["close"]
        if pm_close > pm_open:
            continue

        mkt_close = s_open_bar.iloc[0]["close"]
        if (mkt_close - pm_close) < stock_jump_min:
            continue

        d_pm = d_day[
            (d_day["et_hour"] == 9)
            & (d_day["et_min"] >= pm_start)
            & (d_day["et_min"] <= 29)
        ].sort_values("timestamp")
        if d_pm.empty:
            continue
        do = d_pm.iloc[0]["open"]
        dc = d_pm.iloc[-1]["close"]
        dji_bps = ((dc - do) / do) * 10_000 if do > 0 else 0
        if dji_bps < dji_jump_min_bps:
            continue

        signal_count += 1

        entry = s_open_bar.iloc[0]["open"]
        profit_price = entry * (1 + profit_pct / 100)
        stop_price = entry * (1 - stop_pct / 100)
        cutoff = 30 + window_minutes

        bars = s_day[
            (s_day["et_hour"] == 9) & (s_day["et_min"] >= 30) & (s_day["et_min"] < cutoff)
        ].sort_values("timestamp")

        exit_price = None
        reason = None

        for _, bar in bars.iterrows():
            if bar["low"] <= stop_price and bar["high"] >= profit_price:
                exit_price = stop_price
                reason = "stop_loss"
                break
            elif bar["high"] >= profit_price:
                exit_price = profit_price
                reason = "profit_target"
                break
            elif bar["low"] <= stop_price:
                exit_price = stop_price
                reason = "stop_loss"
                break

        if exit_price is None:
            if not bars.empty:
                exit_price = bars.iloc[-1]["close"]
            else:
                exit_price = entry
            reason = "window_expired"

        pnl = (exit_price - entry) * shares
        total_pnl += pnl
        pnls.append(pnl)
        trades_executed += 1
        exit_reasons[reason] = exit_reasons.get(reason, 0) + 1
        if pnl > 0:
            wins += 1
        else:
            losses += 1

    win_rate = (wins / trades_executed * 100) if trades_executed > 0 else 0
    avg_pnl = (total_pnl / trades_executed) if trades_executed > 0 else 0
    signal_rate = (signal_count / len(trading_dates) * 100) if trading_dates else 0

    return {
        "stock_jump": stock_jump_min,
        "dji_bps": dji_jump_min_bps,
        "pm_minutes": premarket_minutes,
        "trading_days": len(trading_dates),
        "signal_days": signal_count,
        "signal_rate_pct": round(signal_rate, 1),
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 1),
        "total_pnl": round(total_pnl, 2),
        "avg_pnl": round(avg_pnl, 2),
        "profit_hits": exit_reasons.get("profit_target", 0),
        "stop_hits": exit_reasons.get("stop_loss", 0),
        "window_exits": exit_reasons.get("window_expired", 0),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Joint MC sweep: signal params × backtest",
    )
    parser.add_argument("--symbol", default="BKNG")
    parser.add_argument("--dji-proxy", default="DIA")
    parser.add_argument("--days", type=int, default=200)
    parser.add_argument("--shares", type=int, default=10)
    parser.add_argument("--profit-pct", type=float, default=0.25)
    parser.add_argument("--stop-pct", type=float, default=0.50)
    parser.add_argument("--window", type=int, default=20)
    parser.add_argument("-o", "--output", default="output/mc_signal_sweep.csv")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        logging.error("Set POLYGON_API_KEY")
        return

    now = datetime.now(ET)
    cal_days = int(args.days * 1.6) + 10
    start = (now - timedelta(days=cal_days)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")

    logging.info(f"Fetching {args.symbol}...")
    stock_bars = fetch_bars_from_polygon(args.symbol, start, end, api_key)
    logging.info(f"  {len(stock_bars)} bars")

    logging.info(f"Fetching {args.dji_proxy}...")
    dji_bars = fetch_bars_from_polygon(args.dji_proxy, start, end, api_key)
    logging.info(f"  {len(dji_bars)} bars")

    trading_dates = sorted(stock_bars["date"].unique())[-args.days:]

    stock_jump_values = [0.10, 0.25, 0.50, 0.75, 1.00, 1.50, 2.00]
    dji_bps_values = [0, 1, 2, 3, 5, 8]
    pm_values = [5, 10, 15, 20]

    combos = list(itertools.product(stock_jump_values, dji_bps_values, pm_values))
    logging.info(f"Sweeping {len(combos)} signal param combos × full backtest")

    results = []
    t0 = time.time()

    for idx, (sj, db, pm) in enumerate(combos, 1):
        res = _run_single_backtest(
            stock_bars, dji_bars, trading_dates,
            sj, db, pm,
            args.profit_pct, args.stop_pct, args.window, args.shares,
        )
        results.append(res)

        if idx % 50 == 0 or idx == len(combos):
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 1
            eta = (len(combos) - idx) / rate
            logging.info(f"  [{idx}/{len(combos)}] ETA {eta:.0f}s")

    df = pd.DataFrame(results)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logging.info(f"Results saved to {out}")

    elapsed = time.time() - t0

    print("\n" + "=" * 95)
    print(f"JOINT SIGNAL × BACKTEST SWEEP — {args.symbol}")
    print("=" * 95)
    print(f"Days: {len(trading_dates)}  |  Trade: {args.profit_pct}% profit / {args.stop_pct}% stop / {args.window}min window")
    print(f"Combos: {len(combos)}  |  Time: {elapsed:.1f}s")
    print()

    print("100% WIN RATE COMBOS (sorted by signal frequency):")
    perfect = df[df["win_rate"] == 100.0].sort_values("signal_rate_pct", ascending=False)
    if perfect.empty:
        print("  (none)")
    else:
        print(perfect.head(25).to_string(index=False))

    print(f"\n\n≥90% WIN RATE COMBOS (sorted by signal frequency):")
    high_wr = df[df["win_rate"] >= 90.0].sort_values("signal_rate_pct", ascending=False)
    print(high_wr.head(25).to_string(index=False))

    print(f"\n\nSIGNAL RATE ≥30% (sorted by win rate, then P&L):")
    freq = df[df["signal_rate_pct"] >= 30].sort_values(
        ["win_rate", "total_pnl"], ascending=[False, False]
    )
    if freq.empty:
        print("  (none — max signal rate is {:.1f}%)".format(df["signal_rate_pct"].max()))
        print(f"\n  Closest to 30%:")
        close = df.sort_values("signal_rate_pct", ascending=False).head(15)
        print(close.to_string(index=False))
    else:
        print(freq.head(25).to_string(index=False))

    print()


if __name__ == "__main__":
    main()
