"""
Paper-trading bot engine.

Called once per minute during market hours by the scheduler.  Walks through
pre-market scanning, signal detection, position entry, monitoring, and exit.
"""

import logging
from datetime import datetime, date as date_type
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from backtest.options_pricer import black_scholes_call, call_delta, historical_volatility
from paper_trader.db import PaperTradingDB
from paper_trader.live_data import PolygonLiveData

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class TradingBot:
    """Manages the daily lifecycle for all configured symbols."""

    def __init__(self, db: PaperTradingDB, data: PolygonLiveData, config: Dict):
        self.db = db
        self.data = data
        self.cfg = config
        self.symbols: List[str] = config["symbols"]
        self.dji_proxy: str = config["dji_proxy"]

        self._bars_cache: Dict[str, pd.DataFrame] = {}
        self._daily_closes_cache: Dict[str, List[float]] = {}
        self._cache_date: Optional[str] = None

    # ── public entry point ────────────────────────────────────────────

    def tick(self):
        """Called every minute. Decides what to do based on current ET time."""
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return

        today = now.strftime("%Y-%m-%d")
        minute_of_day = now.hour * 60 + now.minute

        if self._cache_date != today:
            self._bars_cache.clear()
            self._daily_closes_cache.clear()
            self._cache_date = today

        if minute_of_day == 540:                     # 9:00
            self._initialize(today)
        elif 550 <= minute_of_day <= 569:            # 9:10‒9:29
            self._scan_premarket(today, now)
        elif minute_of_day == 570:                   # 9:30
            self._check_signals_and_enter(today, now)
        elif 571 <= minute_of_day <= 589:            # 9:31‒9:49
            self._monitor_positions(today, now)
        elif minute_of_day == 590:                   # 9:50
            self._close_remaining(today, now)
        elif minute_of_day == 960:                   # 16:00  end-of-day reconciliation
            self._reconcile(today)

    # ── phases ────────────────────────────────────────────────────────

    def _initialize(self, today: str):
        logger.info(f"Initializing bots for {today}")
        for sym in self.symbols:
            bot_id = self.db.create_bot(today, sym)
            self.db.update_bot(bot_id, status="watching", stage="initialized")

    def _scan_premarket(self, today: str, now: datetime):
        bots = self.db.get_bots_for_date(today)
        if not bots:
            self._initialize(today)
            bots = self.db.get_bots_for_date(today)

        for bot in bots:
            if bot["status"] not in ("initialized", "watching"):
                continue
            if bot["cancel_requested"]:
                self.db.update_bot(bot["id"], status="cancelled", stage="cancelled")
                continue

            self.db.update_bot(bot["id"], status="watching", stage="pre_market_scan")

        self._refresh_bars(today, [self.dji_proxy] + self.symbols)

    def _check_signals_and_enter(self, today: str, now: datetime):
        self._refresh_bars(today, [self.dji_proxy] + self.symbols)

        bots = self.db.get_bots_for_date(today)
        if not bots:
            self._initialize(today)
            bots = self.db.get_bots_for_date(today)

        for bot in bots:
            if bot["status"] not in ("initialized", "watching"):
                continue
            if bot["cancel_requested"]:
                self.db.update_bot(bot["id"], status="cancelled", stage="cancelled")
                continue

            sym = bot["symbol"]
            signal = self._detect_signal(sym)

            if signal is None:
                self.db.update_bot(
                    bot["id"], status="no_signal", stage="completed",
                    notes="Insufficient data for signal check",
                )
                continue

            self.db.update_bot(
                bot["id"],
                pm_open=signal["pm_open"],
                pm_close=signal["pm_close"],
                mkt_open=signal["mkt_open"],
                stock_jump=signal["stock_jump"],
                dji_bps=signal["dji_bps"],
                signal_detected=int(signal["is_signal"]),
            )

            if not signal["is_signal"]:
                self.db.update_bot(
                    bot["id"], status="no_signal", stage="completed",
                    notes=signal.get("reason", ""),
                )
                continue

            entry = self._enter_position(bot["id"], today, sym, signal)
            if entry:
                self.db.update_bot(
                    bot["id"], status="position_open", stage="monitoring_position",
                )
                logger.info(
                    f"SIGNAL {sym}: spot={signal['mkt_open']:.2f} "
                    f"jump={signal['stock_jump']:.2f} → opened position"
                )
            else:
                self.db.update_bot(
                    bot["id"], status="no_signal", stage="completed",
                    notes="Could not price option",
                )

    def _monitor_positions(self, today: str, now: datetime):
        self._refresh_bars(today, self.symbols)

        for bot in self.db.get_bots_for_date(today):
            if bot["status"] != "position_open":
                continue

            trade = self.db.get_trade_by_bot(bot["id"])
            if not trade or trade["status"] != "open":
                continue

            if bot["cancel_requested"]:
                self._force_close(bot, trade, now, reason="cancelled")
                continue

            self._check_exit(bot, trade, now)

    def _close_remaining(self, today: str, now: datetime):
        for bot in self.db.get_bots_for_date(today):
            if bot["status"] != "position_open":
                continue
            trade = self.db.get_trade_by_bot(bot["id"])
            if not trade or trade["status"] != "open":
                continue
            self._force_close(bot, trade, now, reason="window_expired")

        for bot in self.db.get_bots_for_date(today):
            if bot["status"] in ("initialized", "watching"):
                self.db.update_bot(bot["id"], status="no_signal", stage="completed")

    def _reconcile(self, today: str):
        """End-of-day reconciliation using complete daily data."""
        bots = self.db.get_bots_for_date(today)
        if not bots:
            return

        has_activity = any(
            b["status"] in ("position_open", "no_signal", "completed") and b["signal_detected"]
            for b in bots
        )
        if has_activity:
            logger.info(f"End-of-day reconciliation for {today}")
            for bot in bots:
                if bot["status"] == "position_open":
                    trade = self.db.get_trade_by_bot(bot["id"])
                    if trade and trade["status"] == "open":
                        self._force_close(
                            bot, trade, datetime.now(ET), reason="window_expired"
                        )
                self.db.update_bot(bot["id"], stage="completed")

    # ── signal detection ──────────────────────────────────────────────

    def _detect_signal(self, symbol: str) -> Optional[Dict]:
        s_bars = self._bars_cache.get(symbol)
        d_bars = self._bars_cache.get(self.dji_proxy)
        if s_bars is None or s_bars.empty or d_bars is None or d_bars.empty:
            return None

        pm_minutes = self.cfg["premarket_minutes"]
        pm_start = 30 - pm_minutes

        today = s_bars["date"].max()

        s_today = s_bars[s_bars["date"] == today]
        d_today = d_bars[d_bars["date"] == today]

        s_pm = s_today[
            (s_today["et_hour"] == 9) &
            (s_today["et_min"] >= pm_start) &
            (s_today["et_min"] <= 29)
        ].sort_values("timestamp")

        s_open = s_today[
            (s_today["et_hour"] == 9) & (s_today["et_min"] == 30)
        ]

        if s_pm.empty or s_open.empty:
            return None

        pm_open = float(s_pm.iloc[0]["open"])
        pm_close = float(s_pm.iloc[-1]["close"])
        mkt_open = float(s_open.iloc[0]["close"])
        stock_jump = mkt_open - pm_close

        d_pm = d_today[
            (d_today["et_hour"] == 9) &
            (d_today["et_min"] >= pm_start) &
            (d_today["et_min"] <= 29)
        ].sort_values("timestamp")
        dji_bps = 0.0
        if not d_pm.empty:
            do = float(d_pm.iloc[0]["open"])
            dc = float(d_pm.iloc[-1]["close"])
            if do > 0:
                dji_bps = ((dc - do) / do) * 10_000

        is_dip = pm_close <= pm_open
        is_jump = stock_jump >= self.cfg["stock_jump_min"]
        is_dji = dji_bps >= self.cfg["dji_jump_min_bps"]
        is_signal = is_dip and is_jump and is_dji

        reason_parts = []
        if not is_dip:
            reason_parts.append("no pre-market dip")
        if not is_jump:
            reason_parts.append(f"stock jump {stock_jump:.2f} < {self.cfg['stock_jump_min']}")
        if not is_dji:
            reason_parts.append(f"DJI bps {dji_bps:.1f} < {self.cfg['dji_jump_min_bps']}")

        return {
            "pm_open": pm_open,
            "pm_close": pm_close,
            "mkt_open": mkt_open,
            "stock_jump": stock_jump,
            "dji_bps": dji_bps,
            "is_signal": is_signal,
            "reason": "; ".join(reason_parts) if reason_parts else "Signal fired",
        }

    # ── position management ───────────────────────────────────────────

    def _enter_position(
        self, bot_id: int, today: str, symbol: str, signal: Dict
    ) -> Optional[int]:
        spot = signal["mkt_open"]
        strike = spot * (1.0 + self.cfg["strike_otm_pct"] / 100.0)
        T = self.cfg["days_to_expiry"] / 365.0
        r = self.cfg["risk_free_rate"]

        closes = self._get_daily_closes(symbol)
        sigma = historical_volatility(closes) if len(closes) >= 2 else 0.40

        premium = black_scholes_call(spot, strike, T, r, sigma)
        if premium < 0.01:
            premium = 0.01

        delta = call_delta(spot, strike, T, r, sigma)

        trade_id = self.db.create_trade(
            bot_id=bot_id,
            date=today,
            symbol=symbol,
            spot_entry=round(spot, 2),
            strike=round(strike, 2),
            sigma=round(sigma, 4),
            delta_entry=round(delta, 4),
            entry_premium=round(premium, 4),
            contracts=self.cfg["contracts"],
        )
        return trade_id

    def _check_exit(self, bot: Dict, trade: Dict, now: datetime):
        sym = trade["symbol"]
        bars = self._bars_cache.get(sym)
        if bars is None or bars.empty:
            return

        today = bars["date"].max()
        today_bars = bars[bars["date"] == today]

        current_minute = now.hour * 60 + now.minute
        et_min_target = current_minute - 540   # minutes since 9:00
        recent = today_bars[
            (today_bars["et_hour"] == 9) & (today_bars["et_min"] >= 30)
        ].sort_values("timestamp")

        if recent.empty:
            return

        last_bar = recent.iloc[-1]
        spot_now = float(last_bar["close"])
        spot_high = float(last_bar["high"])
        spot_low = float(last_bar["low"])

        strike = trade["strike"]
        sigma = trade["sigma"]
        r = self.cfg["risk_free_rate"]
        T_entry = self.cfg["days_to_expiry"] / 365.0
        minutes_elapsed = int(last_bar["et_min"]) - 30
        T_now = T_entry - (minutes_elapsed / (390 * 252))
        if T_now <= 0:
            T_now = 1e-6

        prem_high = black_scholes_call(spot_high, strike, T_now, r, sigma)
        prem_low = black_scholes_call(spot_low, strike, T_now, r, sigma)
        prem_close = black_scholes_call(spot_now, strike, T_now, r, sigma)

        entry_prem = trade["entry_premium"]
        profit_price = entry_prem * (1 + self.cfg["profit_target_pct"] / 100)
        stop_price = entry_prem * (1 - self.cfg["stop_loss_pct"] / 100)

        if prem_low <= stop_price and prem_high >= profit_price:
            self._close_position(bot, trade, spot_low, stop_price, "stop_loss", now)
        elif prem_high >= profit_price:
            self._close_position(bot, trade, spot_high, profit_price, "profit_target", now)
        elif prem_low <= stop_price:
            self._close_position(bot, trade, spot_low, stop_price, "stop_loss", now)

    def _force_close(self, bot: Dict, trade: Dict, now: datetime, reason: str):
        sym = trade["symbol"]
        bars = self._bars_cache.get(sym)
        spot_now = trade["spot_entry"]

        if bars is not None and not bars.empty:
            today = bars["date"].max()
            recent = bars[
                (bars["date"] == today) &
                (bars["et_hour"] == 9) & (bars["et_min"] >= 30)
            ].sort_values("timestamp")
            if not recent.empty:
                spot_now = float(recent.iloc[-1]["close"])

        strike = trade["strike"]
        sigma = trade["sigma"]
        r = self.cfg["risk_free_rate"]
        T_entry = self.cfg["days_to_expiry"] / 365.0
        minutes_elapsed = max((now.hour * 60 + now.minute) - 570, 0)
        T_now = T_entry - (minutes_elapsed / (390 * 252))
        if T_now <= 0:
            T_now = 1e-6

        exit_prem = black_scholes_call(spot_now, strike, T_now, r, sigma)
        self._close_position(bot, trade, spot_now, exit_prem, reason, now)

    def _close_position(
        self, bot: Dict, trade: Dict, spot_exit: float,
        exit_premium: float, reason: str, now: datetime,
    ):
        self.db.close_trade(
            trade["id"],
            spot_exit=round(spot_exit, 2),
            exit_premium=round(exit_premium, 4),
            exit_reason=reason,
            exit_time=now.isoformat(),
        )
        final_status = "cancelled" if reason == "cancelled" else "completed"
        self.db.update_bot(bot["id"], status=final_status, stage="completed")
        logger.info(
            f"CLOSED {trade['symbol']}: reason={reason} "
            f"prem={trade['entry_premium']:.4f}→{exit_premium:.4f}"
        )

    # ── helpers ───────────────────────────────────────────────────────

    def _refresh_bars(self, today: str, symbols: List[str]):
        for sym in symbols:
            try:
                df = self.data.fetch_today_bars(sym)
                if not df.empty:
                    self._bars_cache[sym] = df
            except Exception as e:
                logger.error(f"Failed to fetch bars for {sym}: {e}")

    def _get_daily_closes(self, symbol: str) -> List[float]:
        if symbol not in self._daily_closes_cache:
            self._daily_closes_cache[symbol] = self.data.fetch_daily_closes(
                symbol, self.cfg["iv_lookback_days"]
            )
        return self._daily_closes_cache[symbol]
