#!/usr/bin/env python3
"""
Options backtest runner.

Usage:
    python3 -m backtest.options_main
    python3 -m backtest.options_main --symbols TSLA APP AMD
    python3 -m backtest.options_main --strike-otm 5 --expiry 3 --profit-pct 20 --stop-pct 50
"""

import argparse
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backtest.trader import fetch_bars_from_polygon
from backtest.options_trader import run_options_backtest, print_options_summary

ET = ZoneInfo("America/New_York")


def main():
    parser = argparse.ArgumentParser(
        description="Backtest call option scalping on pre-market buy signals",
    )
    parser.add_argument("--symbols", nargs="+", default=["TSLA", "APP", "AMD"])
    parser.add_argument("--dji-proxy", default="DIA")
    parser.add_argument("--days", type=int, default=200)
    parser.add_argument("--contracts", type=int, default=1)
    parser.add_argument("--strike-otm", type=float, default=5.0, help="Strike OTM %% (default: 5)")
    parser.add_argument("--expiry", type=int, default=3, help="Days to expiry (default: 3)")
    parser.add_argument("--risk-free", type=float, default=0.05, help="Risk-free rate (default: 0.05)")
    parser.add_argument("--iv-lookback", type=int, default=30, help="IV lookback days (default: 30)")
    parser.add_argument("--stock-jump", type=float, default=0.75)
    parser.add_argument("--dji-bps", type=float, default=0.0)
    parser.add_argument("--pm-minutes", type=int, default=20)
    parser.add_argument("--profit-pct", type=float, default=20.0, help="Option profit target %% (default: 20)")
    parser.add_argument("--stop-pct", type=float, default=50.0, help="Option stop loss %% (default: 50)")
    parser.add_argument("--window", type=int, default=20, help="Trading window minutes")
    parser.add_argument("-o", "--output", default="output/options_backtest.csv")
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

    logging.info(f"Fetching {args.dji_proxy}...")
    dji_bars = fetch_bars_from_polygon(args.dji_proxy, start, end, api_key)
    logging.info(f"  {len(dji_bars)} bars")

    all_trades = []
    for sym in args.symbols:
        logging.info(f"Fetching {sym}...")
        stock_bars = fetch_bars_from_polygon(sym, start, end, api_key)
        logging.info(f"  {len(stock_bars)} bars")

        trades = run_options_backtest(
            stock_bars=stock_bars,
            dji_bars=dji_bars,
            symbol=sym,
            lookback_days=args.days,
            contracts=args.contracts,
            strike_otm_pct=args.strike_otm,
            days_to_expiry=args.expiry,
            risk_free_rate=args.risk_free,
            iv_lookback=args.iv_lookback,
            stock_jump_min=args.stock_jump,
            dji_jump_min_bps=args.dji_bps,
            premarket_minutes=args.pm_minutes,
            profit_target_pct=args.profit_pct,
            stop_loss_pct=args.stop_pct,
            trading_window_minutes=args.window,
        )
        if not trades.empty:
            all_trades.append(trades)

    if not all_trades:
        logging.error("No trades generated")
        return

    combined = pd.concat(all_trades, ignore_index=True)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out, index=False)
    logging.info(f"Trade log saved to {out}")

    print_options_summary(combined, args.symbols)


if __name__ == "__main__":
    main()
