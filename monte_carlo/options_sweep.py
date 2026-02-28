#!/usr/bin/env python3
"""
Optimized MC sweep over options backtest parameters.

Precomputes signal days and option premium time-series for each
(strike_otm, expiry) pair, then sweeps profit/stop/window as
instant threshold filters.
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
from backtest.options_pricer import black_scholes_call, call_delta, historical_volatility

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def _find_signal_days(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    stock_jump_min: float,
    dji_jump_min_bps: float,
    premarket_minutes: int,
    lookback_days: int,
) -> List[Dict]:
    """Find all signal days and precompute entry data + trading bars."""
    pm_start = 30 - premarket_minutes
    trading_dates = sorted(stock_bars["date"].unique())[-lookback_days:]

    signals = []
    for date in trading_dates:
        s_day = stock_bars[stock_bars["date"] == date]
        d_day = dji_bars[dji_bars["date"] == date]

        s_pm = s_day[(s_day["et_hour"] == 9) & (s_day["et_min"] >= pm_start) & (s_day["et_min"] <= 29)].sort_values("timestamp")
        s_open = s_day[(s_day["et_hour"] == 9) & (s_day["et_min"] == 30)]
        if s_pm.empty or s_open.empty:
            continue

        pm_o = s_pm.iloc[0]["open"]
        pm_c = s_pm.iloc[-1]["close"]
        if pm_c > pm_o:
            continue

        mkt_c = s_open.iloc[0]["close"]
        if (mkt_c - pm_c) < stock_jump_min:
            continue

        d_pm = d_day[(d_day["et_hour"] == 9) & (d_day["et_min"] >= pm_start) & (d_day["et_min"] <= 29)].sort_values("timestamp")
        dji_bps = 0.0
        if not d_pm.empty:
            do, dc = d_pm.iloc[0]["open"], d_pm.iloc[-1]["close"]
            if do > 0:
                dji_bps = ((dc - do) / do) * 10_000
        if dji_bps < dji_jump_min_bps:
            continue

        spot_entry = s_open.iloc[0]["open"]
        bars_9 = s_day[(s_day["et_hour"] == 9) & (s_day["et_min"] >= 30)].sort_values("timestamp")
        bar_list = [(int(r["et_min"]), r["high"], r["low"], r["close"]) for _, r in bars_9.iterrows()]

        # Compute IV for this date
        daily_closes = (
            stock_bars[stock_bars["date"] < date]
            .groupby("date")["close"].last().sort_index().tail(30)
        )
        sigma = historical_volatility(daily_closes.tolist())

        signals.append({
            "date": date,
            "spot_entry": spot_entry,
            "sigma": sigma,
            "bars": bar_list,
        })

    return signals


def _precompute_premiums(
    signals: List[Dict],
    strike_otm: float,
    expiry_days: int,
    risk_free: float,
    max_window: int,
) -> List[Dict]:
    """For each signal day, compute option premium at each minute."""
    T_entry = expiry_days / 365.0
    cutoff = 30 + max_window
    result = []

    for sig in signals:
        spot = sig["spot_entry"]
        strike = spot * (1.0 + strike_otm / 100.0)
        sigma = sig["sigma"]

        entry_prem = black_scholes_call(spot, strike, T_entry, risk_free, sigma)
        if entry_prem < 0.01:
            entry_prem = 0.01

        delta = call_delta(spot, strike, T_entry, risk_free, sigma)

        minute_premiums = []
        for minute, high, low, close in sig["bars"]:
            if minute >= cutoff:
                break
            elapsed = (minute - 30)
            T_now = T_entry - (elapsed / (390 * 252))
            if T_now <= 0:
                T_now = 1e-6

            p_high = black_scholes_call(high, strike, T_now, risk_free, sigma)
            p_low = black_scholes_call(low, strike, T_now, risk_free, sigma)
            p_close = black_scholes_call(close, strike, T_now, risk_free, sigma)
            minute_premiums.append((minute, p_high, p_low, p_close))

        result.append({
            "date": sig["date"],
            "spot_entry": spot,
            "strike": strike,
            "sigma": sigma,
            "delta": delta,
            "entry_prem": entry_prem,
            "premiums": minute_premiums,
        })

    return result


def _simulate_trade(
    precomputed: Dict,
    profit_pct: float,
    stop_pct: float,
    window_min: int,
    contracts: int,
) -> Dict:
    """Simulate one trade from precomputed premiums."""
    entry = precomputed["entry_prem"]
    profit_price = entry * (1 + profit_pct / 100)
    stop_price = entry * (1 - stop_pct / 100)
    cutoff = 30 + window_min

    exit_prem = None
    exit_reason = None

    for minute, p_high, p_low, p_close in precomputed["premiums"]:
        if minute >= cutoff:
            break
        if p_low <= stop_price and p_high >= profit_price:
            exit_prem = stop_price
            exit_reason = "stop_loss"
            break
        elif p_high >= profit_price:
            exit_prem = profit_price
            exit_reason = "profit_target"
            break
        elif p_low <= stop_price:
            exit_prem = stop_price
            exit_reason = "stop_loss"
            break

    if exit_prem is None:
        in_window = [(m, h, l, c) for m, h, l, c in precomputed["premiums"] if m < cutoff]
        exit_prem = in_window[-1][3] if in_window else entry
        exit_reason = "window_expired"

    pnl = (exit_prem - entry) * 100 * contracts
    cost = entry * 100 * contracts
    pnl_pct = (exit_prem - entry) / entry * 100 if entry > 0 else 0

    return {
        "pnl": pnl,
        "cost": cost,
        "pnl_pct": pnl_pct,
        "exit_reason": exit_reason,
        "win": pnl > 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Optimized MC options sweep")
    parser.add_argument("--symbols", nargs="+", default=["TSLA", "APP", "AMD"])
    parser.add_argument("--dji-proxy", default="DIA")
    parser.add_argument("--days", type=int, default=200)
    parser.add_argument("--contracts", type=int, default=1)
    parser.add_argument("--stock-jump", type=float, default=0.75)
    parser.add_argument("--dji-bps", type=float, default=0.0)
    parser.add_argument("--pm-minutes", type=int, default=20)
    parser.add_argument("-o", "--output", default="output/mc_options_sweep.csv")
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

    logging.info("Fetching data...")
    dji_bars = fetch_bars_from_polygon(args.dji_proxy, start, end, api_key)
    stock_data = {}
    for sym in args.symbols:
        logging.info(f"  {sym}...")
        stock_data[sym] = fetch_bars_from_polygon(sym, start, end, api_key)

    strike_otm_values = [5, 10, 15, 20, 25]
    expiry_values = [3, 5, 7, 14]
    profit_pct_values = [20, 30, 50, 75, 100]
    stop_pct_values = [25, 50, 75, 100]
    window_values = [10, 20, 30]

    max_window = max(window_values)

    # Phase 1: find signals and precompute premiums per (sym, otm, exp)
    logging.info("Phase 1: Precomputing signal days and option premiums...")
    t0 = time.time()

    precomputed_cache: Dict[Tuple, List[Dict]] = {}
    for sym in args.symbols:
        signals = _find_signal_days(
            stock_data[sym], dji_bars,
            args.stock_jump, args.dji_bps, args.pm_minutes, args.days,
        )
        logging.info(f"  {sym}: {len(signals)} signal days")

        for otm in strike_otm_values:
            for exp in expiry_values:
                key = (sym, otm, exp)
                precomputed_cache[key] = _precompute_premiums(
                    signals, otm, exp, 0.05, max_window
                )

    phase1_time = time.time() - t0
    logging.info(f"  Precompute done: {len(precomputed_cache)} (sym,otm,exp) combos in {phase1_time:.1f}s")

    # Phase 2: sweep profit/stop/window
    logging.info("Phase 2: Sweeping trade management params...")
    t1 = time.time()

    combos = list(itertools.product(
        strike_otm_values, expiry_values,
        profit_pct_values, stop_pct_values, window_values,
    ))
    logging.info(f"  {len(combos)} total combos")

    results = []
    for idx, (otm, exp, pp, sp, wm) in enumerate(combos, 1):
        row = {
            "strike_otm": otm, "expiry": exp, "profit_pct": pp,
            "stop_pct": sp, "window_min": wm,
        }

        total_pnl = 0.0
        total_cost = 0.0
        total_trades = 0
        total_wins = 0

        for sym in args.symbols:
            precomputed = precomputed_cache[(sym, otm, exp)]
            sym_pnl = 0.0
            sym_cost = 0.0
            sym_trades = 0
            sym_wins = 0

            for pc in precomputed:
                trade = _simulate_trade(pc, pp, sp, wm, args.contracts)
                sym_pnl += trade["pnl"]
                sym_cost += trade["cost"]
                sym_trades += 1
                if trade["win"]:
                    sym_wins += 1

            sym_wr = sym_wins / sym_trades * 100 if sym_trades > 0 else 0
            sym_roi = sym_pnl / sym_cost * 100 if sym_cost > 0 else 0

            row[f"{sym}_trades"] = sym_trades
            row[f"{sym}_wr"] = round(sym_wr, 1)
            row[f"{sym}_roi"] = round(sym_roi, 1)
            row[f"{sym}_pnl"] = round(sym_pnl, 2)

            total_pnl += sym_pnl
            total_cost += sym_cost
            total_trades += sym_trades
            total_wins += sym_wins

        row["total_trades"] = total_trades
        row["total_wins"] = total_wins
        row["total_win_rate"] = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0
        row["total_pnl"] = round(total_pnl, 2)
        row["total_cost"] = round(total_cost, 2)
        row["total_roi"] = round(total_pnl / total_cost * 100, 1) if total_cost > 0 else 0

        results.append(row)

        if idx % 200 == 0 or idx == len(combos):
            elapsed = time.time() - t1
            rate = idx / elapsed if elapsed > 0 else 1
            eta = (len(combos) - idx) / rate
            logging.info(f"  [{idx}/{len(combos)}] ETA {eta:.0f}s")

    df = pd.DataFrame(results)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    total_time = time.time() - t0
    logging.info(f"Total time: {total_time:.1f}s")

    # --- Analysis ---
    print("\n" + "=" * 100)
    print("OPTIONS MONTE CARLO SWEEP RESULTS")
    print("=" * 100)
    print(f"Stocks: {', '.join(args.symbols)}  |  Combos: {len(combos)}  |  Time: {total_time:.1f}s")
    print()

    cols = ["strike_otm", "expiry", "profit_pct", "stop_pct", "window_min",
            "total_trades", "total_win_rate", "total_pnl", "total_roi"]
    sym_cols = []
    for sym in args.symbols:
        sym_cols.extend([f"{sym}_wr", f"{sym}_roi"])

    print("TOP 20 BY PORTFOLIO ROI:")
    print(df.sort_values("total_roi", ascending=False).head(20)[cols + sym_cols].to_string(index=False))

    print("\n\nTOP 20 BY WIN RATE (profitable only):")
    prof = df[df["total_roi"] > 0]
    print(prof.sort_values("total_win_rate", ascending=False).head(20)[cols + sym_cols].to_string(index=False))

    print("\n\nSWEET SPOT (WR ≥ 55%, ROI ≥ 10%, sorted by ROI):")
    sweet = df[(df["total_win_rate"] >= 55) & (df["total_roi"] >= 10)]
    print(sweet.sort_values("total_roi", ascending=False).head(20)[cols + sym_cols].to_string(index=False))

    print("\n\n" + "=" * 100)
    print("PER-STOCK ANALYSIS")
    print("=" * 100)
    for sym in args.symbols:
        avg_roi = df[f"{sym}_roi"].mean()
        avg_wr = df[f"{sym}_wr"].mean()
        best_roi = df[f"{sym}_roi"].max()
        pct_prof = (df[f"{sym}_roi"] > 0).sum() / len(df) * 100
        print(f"  {sym:>4}: avg ROI={avg_roi:>+6.1f}%  avg WR={avg_wr:>5.1f}%  best ROI={best_roi:>+6.1f}%  profitable in {pct_prof:.0f}% of combos")
    print()


if __name__ == "__main__":
    main()
