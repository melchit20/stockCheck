#!/usr/bin/env python3
"""
Monte Carlo parameter sweep for pre-market buy signal thresholds.

Reads cached Polygon bars from SQLite — zero API calls.

Usage:
    python3 -m monte_carlo.main
    python3 -m monte_carlo.main --top 100
    python3 -m monte_carlo.main -o output/sweep_results.csv
"""

import argparse
import logging
from pathlib import Path

from monte_carlo.tuner import SweepRange, load_cached_bars, run_sweep
from src.universe import get_stock_universe


def main():
    parser = argparse.ArgumentParser(
        description="Monte Carlo sweep over pre-market signal thresholds",
    )
    parser.add_argument(
        "--db", default="cache/market_data.db",
        help="Path to SQLite cache (default: cache/market_data.db)",
    )
    parser.add_argument(
        "--dji-proxy", default="DIA",
        help="DJI proxy symbol (default: DIA)",
    )
    parser.add_argument(
        "--top", type=int, default=500,
        help="Number of stocks from universe to include (default: 500)",
    )
    parser.add_argument(
        "-o", "--output", default="output/mc_sweep.csv",
        help="Output CSV path (default: output/mc_sweep.csv)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Debug-level logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    all_bars = load_cached_bars(args.db)
    symbols = get_stock_universe(args.top)
    cached_syms = set(all_bars["symbol"].unique())
    symbols = [s for s in symbols if s in cached_syms]
    logging.info(f"Symbols in cache: {len(symbols)}")

    sweep = SweepRange(
        stock_jump_values=[0.10, 0.25, 0.50, 0.75, 1.00, 1.50, 2.00, 3.00],
        dji_bps_values=[2, 3, 5, 8, 10, 12, 15, 19],
        premarket_min_values=[5, 10, 15, 20, 30],
    )

    logging.info(
        f"Sweeping {sweep.total_combos} parameter combinations "
        f"across {len(symbols)} stocks"
    )

    results = run_sweep(all_bars, args.dji_proxy, symbols, sweep)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(out, index=False)
    logging.info(f"Results saved to {out}")

    results_sorted = results.sort_values("buy_signals", ascending=False)

    print("\n" + "=" * 80)
    print("MONTE CARLO SWEEP RESULTS")
    print("=" * 80)
    print(f"Parameter combinations tested: {len(results)}")
    print(f"Stocks analyzed: {len(symbols)}")
    print()

    print("Top 20 combos by buy signal count:")
    print(
        results_sorted.head(20).to_string(index=False)
    )

    print("\n\nTop 20 combos by buy rate (min 20 buy signals):")
    qualified = results[results["buy_signals"] >= 20].sort_values(
        "buy_rate_pct", ascending=False
    )
    print(qualified.head(20).to_string(index=False))

    print("\n\nSweet spot (50-200 buy signals, sorted by buy rate):")
    sweet = results[
        (results["buy_signals"] >= 50) & (results["buy_signals"] <= 200)
    ].sort_values("buy_rate_pct", ascending=False)
    print(sweet.head(20).to_string(index=False))
    print()


if __name__ == "__main__":
    main()
