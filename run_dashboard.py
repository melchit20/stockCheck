#!/usr/bin/env python3
"""
Start the paper-trading dashboard and bot scheduler.

Usage:
    python3 run_dashboard.py
    python3 run_dashboard.py --no-scheduler   # dashboard only, no live trading
    python3 run_dashboard.py --port 8080
"""

import argparse
import logging
import os

from dashboard.app import create_app, load_config, scheduler_ref
import dashboard.app as dashboard_module
from paper_trader.db import PaperTradingDB
from paper_trader.live_data import PolygonLiveData
from paper_trader.bot import TradingBot
from paper_trader.scheduler import BotScheduler


def main():
    parser = argparse.ArgumentParser(description="Paper Trading Dashboard")
    parser.add_argument("--config", default="config/paper_trading.yaml")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--no-scheduler", action="store_true", help="Run dashboard only")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_config(args.config)
    app = create_app(config)

    host = args.host or config["dashboard"].get("host", "0.0.0.0")
    port = args.port or config["dashboard"].get("port", 5000)

    if not args.no_scheduler:
        api_key = os.environ.get("POLYGON_API_KEY", "")
        if not api_key:
            logging.warning("POLYGON_API_KEY not set — scheduler will not start. "
                            "Dashboard-only mode.")
        else:
            db = PaperTradingDB(config.get("database", {}).get("path", "data/paper_trading.db"))
            data = PolygonLiveData(api_key)
            bot = TradingBot(db, data, config["trading"])
            sched = BotScheduler(bot)
            sched.start()
            dashboard_module.scheduler_ref = sched
            logging.info("Bot scheduler started.")

    logging.info(f"Dashboard at http://{host}:{port}")
    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
