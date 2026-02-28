#!/usr/bin/env python3
"""
One-time helper: build a top-N NASDAQ universe sorted by daily dollar volume.

Filters out ETFs/ETNs/funds and keeps only common stocks.
"""

import os
import sys
import time
import logging
import re

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

BATCH = 1000
TARGET = 500

ETF_KEYWORDS = [
    "etf", "fund", "trust", "index", "proshares", "direxion",
    "ishares", "vanguard", "spdr", "wisdomtree", "invesco",
    "first trust", "schwab", "fidelity", "grayscale",
    "amplify", "global x", "vaneck", "ark ", "roundhill",
]


def is_likely_etf(name: str) -> bool:
    name_lower = name.lower()
    return any(kw in name_lower for kw in ETF_KEYWORDS)


def main():
    api_key = os.environ.get("APCA_API_KEY_ID")
    secret = os.environ.get("APCA_API_SECRET_KEY")

    trading = TradingClient(api_key=api_key, secret_key=secret, paper=True)
    data_client = StockHistoricalDataClient(api_key=api_key, secret_key=secret)

    logger.info("Fetching NASDAQ assets...")
    assets = trading.get_all_assets()
    stock_names = {}
    nasdaq = []
    for a in assets:
        if (
            getattr(a, "exchange", "") == "NASDAQ"
            and a.tradable
            and a.status == "active"
            and getattr(a, "asset_class", "") == "us_equity"
            and not any(c in a.symbol for c in "./-")
            and len(a.symbol) <= 5
        ):
            name = getattr(a, "name", "")
            if not is_likely_etf(name):
                nasdaq.append(a.symbol)
                stock_names[a.symbol] = name

    logger.info(f"Found {len(nasdaq)} NASDAQ stocks (after filtering ETFs)")

    scored = {}
    for i in range(0, len(nasdaq), BATCH):
        batch = nasdaq[i : i + BATCH]
        batch_num = i // BATCH + 1
        total = (len(nasdaq) + BATCH - 1) // BATCH
        logger.info(f"Snapshot batch {batch_num}/{total} ({len(batch)} symbols)")

        try:
            snapshots = data_client.get_stock_snapshot(
                StockSnapshotRequest(symbol_or_symbols=batch, feed="iex")
            )
            for sym, snap in snapshots.items():
                if snap.daily_bar and snap.daily_bar.close and snap.daily_bar.volume:
                    dollar_vol = snap.daily_bar.close * snap.daily_bar.volume
                    scored[sym] = dollar_vol
        except Exception as e:
            logger.warning(f"Batch {batch_num} failed: {e}")

        time.sleep(0.3)

    ranked = sorted(scored.items(), key=lambda x: x[1], reverse=True)
    top = [sym for sym, _ in ranked[:TARGET]]

    logger.info(f"\nTop {TARGET} NASDAQ stocks by daily dollar volume:")
    # Print as Python list literal
    print("NASDAQ_TOP_500 = [")
    for i in range(0, len(top), 10):
        chunk = top[i : i + 10]
        line = ", ".join(f'"{s}"' for s in chunk)
        print(f"    {line},")
    print("]")

    print(f"\n# Total: {len(top)} symbols")
    top10 = [f"{s} ({stock_names.get(s, '?')})" for s in top[:10]]
    bot10 = [f"{s} ({stock_names.get(s, '?')})" for s in top[-10:]]
    print(f"\n# Top 10: {top10}")
    print(f"# Bottom 10: {bot10}")


if __name__ == "__main__":
    main()
