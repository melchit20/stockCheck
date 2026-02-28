#!/usr/bin/env python3
"""
Monte Carlo sweep over backtest trade-management parameters.

Fetches data once from Polygon, then sweeps profit_pct × stop_pct × window
entirely in-memory.

Usage:
    python3 -m monte_carlo.backtest_main
    python3 -m monte_carlo.backtest_main --symbol BKNG --days 100
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from backtest.trader import fetch_bars_from_polygon
from monte_carlo.backtest_tuner import BacktestSweepRange, run_backtest_sweep

ET = ZoneInfo("America/New_York")


def main():
    parser = argparse.ArgumentParser(
        description="Monte Carlo sweep over backtest trade-management params",
    )
    parser.add_argument("--symbol", default="BKNG", help="Stock (default: BKNG)")
    parser.add_argument("--dji-proxy", default="DIA", help="DJI proxy (default: DIA)")
    parser.add_argument("--days", type=int, default=100, help="Trading days (default: 100)")
    parser.add_argument("--shares", type=int, default=10, help="Shares per trade")
    parser.add_argument("--stock-jump", type=float, default=2.0, help="Signal: min stock jump $")
    parser.add_argument("--dji-bps", type=float, default=3.0, help="Signal: min DJI bps")
    parser.add_argument("--pm-minutes", type=int, default=10, help="Signal: pre-market window")
    parser.add_argument("-o", "--output", default="output/mc_backtest_sweep.csv")
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

    logging.info(f"Fetching {args.symbol} bars...")
    stock_bars = fetch_bars_from_polygon(args.symbol, start, end, api_key)
    logging.info(f"  {len(stock_bars)} bars")

    logging.info(f"Fetching {args.dji_proxy} bars...")
    dji_bars = fetch_bars_from_polygon(args.dji_proxy, start, end, api_key)
    logging.info(f"  {len(dji_bars)} bars")

    sweep = BacktestSweepRange(
        profit_pct_values=[0.25, 0.50, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0],
        stop_pct_values=[0.25, 0.50, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0],
        window_values=[5, 10, 15, 20, 30, 45, 60],
    )

    logging.info(
        f"Sweeping {sweep.total_combos} combos "
        f"(profit × stop × window)"
    )
    t0 = time.time()

    results = run_backtest_sweep(
        stock_bars, dji_bars, sweep,
        lookback_days=args.days,
        shares=args.shares,
        stock_jump_min=args.stock_jump,
        dji_jump_min_bps=args.dji_bps,
        premarket_minutes=args.pm_minutes,
    )

    elapsed = time.time() - t0
    logging.info(f"Sweep completed in {elapsed:.1f}s")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(out, index=False)
    logging.info(f"Results saved to {out}")

    # --- Print analysis ---
    print("\n" + "=" * 90)
    print("BACKTEST MONTE CARLO SWEEP — TRADE MANAGEMENT TUNING")
    print("=" * 90)
    print(f"Symbol: {args.symbol}  |  Days: {args.days}  |  Shares: {args.shares}")
    print(f"Signal params: jump=${args.stock_jump}, bps={args.dji_bps}, pm={args.pm_minutes}min")
    print(f"Combinations: {len(results)}  |  Time: {elapsed:.1f}s")
    print()

    print("TOP 15 BY TOTAL P&L:")
    top_pnl = results.sort_values("total_pnl", ascending=False).head(15)
    print(top_pnl[[
        "profit_pct", "stop_pct", "window_min",
        "trades", "win_rate", "total_pnl", "avg_pnl",
        "profit_hits", "stop_hits", "window_exits", "sharpe_approx",
    ]].to_string(index=False))

    print("\n\nTOP 15 BY WIN RATE (min 5 trades):")
    qualified = results[results["trades"] >= 5]
    top_wr = qualified.sort_values("win_rate", ascending=False).head(15)
    print(top_wr[[
        "profit_pct", "stop_pct", "window_min",
        "trades", "win_rate", "total_pnl", "avg_pnl",
        "profit_hits", "stop_hits", "window_exits", "sharpe_approx",
    ]].to_string(index=False))

    print("\n\nTOP 15 BY SHARPE RATIO:")
    top_sharpe = results.sort_values("sharpe_approx", ascending=False).head(15)
    print(top_sharpe[[
        "profit_pct", "stop_pct", "window_min",
        "trades", "win_rate", "total_pnl", "avg_pnl",
        "avg_pnl_pct", "sharpe_approx",
    ]].to_string(index=False))

    print("\n\nSWEET SPOT (win rate ≥ 70%, total P&L > $500, sorted by Sharpe):")
    sweet = results[
        (results["win_rate"] >= 70) & (results["total_pnl"] > 500)
    ].sort_values("sharpe_approx", ascending=False)
    print(sweet.head(20)[[
        "profit_pct", "stop_pct", "window_min",
        "trades", "win_rate", "total_pnl", "avg_pnl",
        "avg_pnl_pct", "profit_hits", "stop_hits", "window_exits",
        "sharpe_approx",
    ]].to_string(index=False))
    print()


if __name__ == "__main__":
    main()
