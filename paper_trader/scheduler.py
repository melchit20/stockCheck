"""APScheduler integration — ticks the trading bot once per minute on weekdays."""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from paper_trader.bot import TradingBot

logger = logging.getLogger(__name__)


class BotScheduler:
    def __init__(self, bot: TradingBot):
        self.bot = bot
        self.scheduler = BackgroundScheduler(timezone="America/New_York")

    def start(self):
        self.scheduler.add_job(
            self.bot.tick,
            CronTrigger(
                day_of_week="mon-fri",
                hour="9",
                minute="0-50",
                timezone="America/New_York",
            ),
            id="trading_tick",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=30,
        )
        self.scheduler.add_job(
            self.bot.tick,
            CronTrigger(
                day_of_week="mon-fri",
                hour="16",
                minute="0",
                timezone="America/New_York",
            ),
            id="eod_reconcile",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
        )
        self.scheduler.start()
        logger.info("Scheduler started — trading tick every minute 9:00-9:50 ET, Mon-Fri")

    def stop(self):
        self.scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

    @property
    def running(self) -> bool:
        return self.scheduler.running
