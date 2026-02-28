#!/usr/bin/env python3
"""
Stock Scanner — find buy signals correlated with DJI movements.

Scans the top NASDAQ stocks by market cap, identifies dip→jump patterns
in the first minutes of each trading day, and cross-references them with
DJI (via DIA proxy) to classify buy signals.
"""

import argparse
import logging
import sys
from pathlib import Path

from src.config import AppConfig
from src.scanner import Scanner


def main():
    parser = argparse.ArgumentParser(
        description="Scan NASDAQ stocks for DJI-correlated buy signals",
    )
    parser.add_argument(
        "-c", "--config",
        default="config/default.yaml",
        help="Path to YAML config (default: config/default.yaml)",
    )
    parser.add_argument(
        "-n", "--num-stocks",
        type=int,
        default=None,
        help="Override: number of stocks to scan",
    )
    parser.add_argument(
        "-d", "--days",
        type=int,
        default=None,
        help="Override: trading days to look back",
    )
    parser.add_argument(
        "--stock-jump",
        type=float,
        default=None,
        help="Override: min stock jump in dollars",
    )
    parser.add_argument(
        "--dji-jump-bps",
        type=float,
        default=None,
        help="Override: min DJI jump in basis points",
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the SQLite cache before running",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Debug-level logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    cfg_path = Path(args.config)
    if cfg_path.exists():
        config = AppConfig.from_yaml(str(cfg_path))
    else:
        logging.warning(f"Config not found at {cfg_path}, using defaults")
        config = AppConfig()

    if args.num_stocks is not None:
        config.scan.num_stocks = args.num_stocks
    if args.days is not None:
        config.scan.lookback_days = args.days
    if args.stock_jump is not None:
        config.signals.stock_jump_min = args.stock_jump
    if args.dji_jump_bps is not None:
        config.signals.dji_jump_min_bps = args.dji_jump_bps

    if args.clear_cache:
        from src.cache import DataCache
        DataCache(config.cache.db_path).clear()

    try:
        scanner = Scanner(config)
        results = scanner.run()
    except EnvironmentError as e:
        logging.error(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()
