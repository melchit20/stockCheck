#!/usr/bin/env python3
"""
Backtest runner for pre-market buy signal strategy.

Usage:
    python3 -m backtest.main
    python3 -m backtest.main --symbol BKNG --days 100 --shares 10
    python3 -m backtest.main --profit-pct 5 --stop-pct 5 --window 20
"""

import argparse
import logging
from pathlib import Path

from .config import BacktestConfig
from .trader import run_backtest, print_summary


def main():
    parser = argparse.ArgumentParser(
        description="Backtest pre-market buy signal strategy",
    )
    parser.add_argument("--symbol", default=None, help="Stock to trade (default: BKNG)")
    parser.add_argument("--dji-proxy", default=None, help="DJI proxy (default: DIA)")
    parser.add_argument("--shares", type=int, default=None, help="Shares per trade")
    parser.add_argument("--days", type=int, default=None, help="Trading days to backtest")
    parser.add_argument("--stock-jump", type=float, default=None, help="Min stock jump $")
    parser.add_argument("--dji-bps", type=float, default=None, help="Min DJI jump bps")
    parser.add_argument("--pm-minutes", type=int, default=None, help="Pre-market window")
    parser.add_argument("--profit-pct", type=float, default=None, help="Profit target %")
    parser.add_argument("--stop-pct", type=float, default=None, help="Stop loss %")
    parser.add_argument("--window", type=int, default=None, help="Trading window minutes")
    parser.add_argument("-o", "--output", default=None, help="Output CSV path")
    parser.add_argument("-c", "--config", default=None, help="YAML config file")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = BacktestConfig()
    if args.config:
        config = BacktestConfig.from_yaml(args.config)

    if args.symbol:
        config.symbol = args.symbol
    if args.dji_proxy:
        config.dji_proxy = args.dji_proxy
    if args.shares is not None:
        config.shares = args.shares
    if args.days is not None:
        config.lookback_days = args.days
    if args.stock_jump is not None:
        config.stock_jump_min = args.stock_jump
    if args.dji_bps is not None:
        config.dji_jump_min_bps = args.dji_bps
    if args.pm_minutes is not None:
        config.premarket_minutes = args.pm_minutes
    if args.profit_pct is not None:
        config.profit_target_pct = args.profit_pct
    if args.stop_pct is not None:
        config.stop_loss_pct = args.stop_pct
    if args.window is not None:
        config.trading_window_minutes = args.window
    if args.output:
        config.output_csv = args.output

    trades = run_backtest(config)

    if trades.empty:
        logging.error("No trades generated")
        return

    out = Path(config.output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out, index=False)
    logging.info(f"Trade log saved to {out}")

    print_summary(trades, config)


if __name__ == "__main__":
    main()
