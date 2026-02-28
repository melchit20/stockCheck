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
        self._paused = True  # start paused; dashboard or CLI can resume

    def _guarded_tick(self):
        if self._paused:
            return
        self.bot.tick()

    def start(self, paused: bool = True):
        """Start the APScheduler background thread. Jobs only fire when not paused."""
        self._paused = paused
        self.scheduler.add_job(
            self._guarded_tick,
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
            self._guarded_tick,
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
        state = "paused" if paused else "running"
        logger.info(f"Scheduler started ({state}) — trading tick every minute 9:00-9:50 ET, Mon-Fri")

    def stop(self):
        self.scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down")

    def resume(self):
        self._paused = False
        logger.info("Scheduler resumed — bot will trade on next tick")

    def pause(self):
        self._paused = True
        logger.info("Scheduler paused — bot will not trade until resumed")

    @property
    def running(self) -> bool:
        return self.scheduler.running and not self._paused

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def ready(self) -> bool:
        """True if APScheduler is alive (even if paused)."""
        return self.scheduler.running
