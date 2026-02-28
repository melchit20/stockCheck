"""
Paper trading simulator for pre-market buy signal backtesting.

For each trading day:
  1. Check pre-market for a buy signal (stock dip + DJI jump)
  2. If signal fires: buy N shares at market open price
  3. Hold and monitor minute-by-minute during the trading window
  4. Sell when: profit target hit, stop loss hit, or window expires
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from .config import BacktestConfig

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def fetch_bars_from_polygon(
    symbol: str, start_date: str, end_date: str, api_key: str
) -> pd.DataFrame:
    """Fetch all minute bars for a symbol from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
    all_rows = []

    while url:
        resp = requests.get(
            url,
            params={"apiKey": api_key, "adjusted": "true", "sort": "asc", "limit": 50000},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.error(f"Polygon {symbol}: HTTP {resp.status_code}")
            break

        data = resp.json()
        for bar in data.get("results", []):
            ts = pd.Timestamp(bar["t"], unit="ms", tz="UTC")
            all_rows.append({
                "symbol": symbol,
                "timestamp": ts,
                "open": bar["o"],
                "high": bar["h"],
                "low": bar["l"],
                "close": bar["c"],
                "volume": bar.get("v", 0),
            })

        url = data.get("next_url")
        if url:
            url = f"{url}&apiKey={api_key}"
            time.sleep(0.2)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["et"] = df["timestamp"].dt.tz_convert(ET)
        df["date"] = df["et"].dt.date
        df["et_hour"] = df["et"].dt.hour
        df["et_min"] = df["et"].dt.minute
    return df


def run_backtest(config: BacktestConfig) -> pd.DataFrame:
    """
    Run the full paper-trading backtest.

    Returns a DataFrame of trades with columns:
      date, signal, entry_price, exit_price, exit_reason,
      exit_minute, shares, pnl, pnl_pct, cumulative_pnl
    """
    api_key = config.polygon_api_key or os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        raise EnvironmentError("POLYGON_API_KEY not set.")

    now = datetime.now(ET)
    cal_days = int(config.lookback_days * 1.6) + 10
    start = (now - timedelta(days=cal_days)).strftime("%Y-%m-%d")
    end = now.strftime("%Y-%m-%d")

    logger.info(f"Fetching {config.symbol} bars from Polygon ({start} → {end})...")
    stock_bars = fetch_bars_from_polygon(config.symbol, start, end, api_key)
    logger.info(f"  {len(stock_bars)} bars")

    logger.info(f"Fetching {config.dji_proxy} bars from Polygon ({start} → {end})...")
    dji_bars = fetch_bars_from_polygon(config.dji_proxy, start, end, api_key)
    logger.info(f"  {len(dji_bars)} bars")

    if stock_bars.empty or dji_bars.empty:
        logger.error("No data available")
        return pd.DataFrame()

    trading_dates = sorted(stock_bars["date"].unique())
    trading_dates = trading_dates[-config.lookback_days:]
    logger.info(f"Trading days: {len(trading_dates)}")

    pm_start_min = 30 - config.premarket_minutes

    trades = []
    cumulative_pnl = 0.0

    for date in trading_dates:
        s_day = stock_bars[stock_bars["date"] == date]
        d_day = dji_bars[dji_bars["date"] == date]

        # --- Pre-market signal check ---
        s_pm = s_day[
            (s_day["et_hour"] == 9)
            & (s_day["et_min"] >= pm_start_min)
            & (s_day["et_min"] <= 29)
        ].sort_values("timestamp")

        d_pm = d_day[
            (d_day["et_hour"] == 9)
            & (d_day["et_min"] >= pm_start_min)
            & (d_day["et_min"] <= 29)
        ].sort_values("timestamp")

        s_open_bar = s_day[
            (s_day["et_hour"] == 9) & (s_day["et_min"] == 30)
        ]

        if s_pm.empty or s_open_bar.empty:
            continue

        pm_first_open = s_pm.iloc[0]["open"]
        pm_last_close = s_pm.iloc[-1]["close"]
        is_dip = pm_last_close <= pm_first_open

        mkt_open_close = s_open_bar.iloc[0]["close"]
        stock_jump = mkt_open_close - pm_last_close
        is_stock_jump = stock_jump >= config.stock_jump_min

        dji_bps = 0.0
        if not d_pm.empty:
            do = d_pm.iloc[0]["open"]
            dc = d_pm.iloc[-1]["close"]
            if do > 0:
                dji_bps = ((dc - do) / do) * 10_000

        is_dji_jump = dji_bps >= config.dji_jump_min_bps
        signal = is_dip and is_stock_jump and is_dji_jump

        if not signal:
            trades.append({
                "date": date,
                "signal": False,
                "entry_price": None,
                "exit_price": None,
                "exit_reason": "no_signal",
                "exit_minute": None,
                "shares": 0,
                "pnl": 0.0,
                "pnl_pct": 0.0,
                "cumulative_pnl": cumulative_pnl,
                "pm_change": round(pm_last_close - pm_first_open, 2),
                "stock_jump": round(stock_jump, 2),
                "dji_bps": round(dji_bps, 2),
            })
            continue

        # --- Execute trade ---
        entry_price = s_open_bar.iloc[0]["open"]
        profit_price = entry_price * (1 + config.profit_target_pct / 100)
        stop_price = entry_price * (1 - config.stop_loss_pct / 100)

        cutoff_min = 30 + config.trading_window_minutes
        trading_bars = s_day[
            (s_day["et_hour"] == 9)
            & (s_day["et_min"] >= 30)
            & (s_day["et_min"] < cutoff_min)
        ].sort_values("timestamp")

        exit_price = None
        exit_reason = None
        exit_minute = None

        for _, bar in trading_bars.iterrows():
            if bar["low"] <= stop_price and bar["high"] >= profit_price:
                exit_price = stop_price
                exit_reason = "stop_loss"
                exit_minute = bar["et_min"]
                break
            elif bar["high"] >= profit_price:
                exit_price = profit_price
                exit_reason = "profit_target"
                exit_minute = bar["et_min"]
                break
            elif bar["low"] <= stop_price:
                exit_price = stop_price
                exit_reason = "stop_loss"
                exit_minute = bar["et_min"]
                break

        if exit_price is None:
            if not trading_bars.empty:
                exit_price = trading_bars.iloc[-1]["close"]
                exit_reason = "window_expired"
                exit_minute = int(trading_bars.iloc[-1]["et_min"])
            else:
                exit_price = entry_price
                exit_reason = "no_bars"
                exit_minute = 30

        pnl_per_share = exit_price - entry_price
        pnl = pnl_per_share * config.shares
        pnl_pct = (pnl_per_share / entry_price) * 100
        cumulative_pnl += pnl

        trades.append({
            "date": date,
            "signal": True,
            "entry_price": round(entry_price, 2),
            "exit_price": round(exit_price, 2),
            "exit_reason": exit_reason,
            "exit_minute": exit_minute,
            "shares": config.shares,
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "cumulative_pnl": round(cumulative_pnl, 2),
            "pm_change": round(pm_last_close - pm_first_open, 2),
            "stock_jump": round(stock_jump, 2),
            "dji_bps": round(dji_bps, 2),
        })

    return pd.DataFrame(trades)


def print_summary(trades: pd.DataFrame, config: BacktestConfig):
    """Print a summary of backtest results."""
    executed = trades[trades["signal"] == True]
    no_signal = trades[trades["signal"] == False]

    print("\n" + "=" * 70)
    print(f"BACKTEST RESULTS — {config.symbol} vs {config.dji_proxy}")
    print("=" * 70)
    print(f"Period:           {trades['date'].min()} → {trades['date'].max()}")
    print(f"Trading days:     {len(trades)}")
    print(f"Signal days:      {len(executed)}")
    print(f"No-signal days:   {len(no_signal)}")
    print(f"Signal rate:      {len(executed)/len(trades)*100:.1f}%")
    print()

    if executed.empty:
        print("No trades executed.")
        return

    print(f"Shares per trade: {config.shares}")
    entry_cost = executed["entry_price"].iloc[0] * config.shares
    print(f"Capital required: ~${entry_cost:,.0f}")
    print()

    print("TRADE OUTCOMES:")
    for reason in ["profit_target", "stop_loss", "window_expired"]:
        subset = executed[executed["exit_reason"] == reason]
        if not subset.empty:
            avg_pnl = subset["pnl"].mean()
            print(
                f"  {reason:20s}  {len(subset):3d} trades  "
                f"avg P&L: ${avg_pnl:>+10,.2f}  "
                f"total: ${subset['pnl'].sum():>+12,.2f}"
            )

    print()
    total_pnl = executed["pnl"].sum()
    avg_pnl = executed["pnl"].mean()
    win_rate = (executed["pnl"] > 0).sum() / len(executed) * 100

    print(f"Total P&L:        ${total_pnl:>+12,.2f}")
    print(f"Avg P&L/trade:    ${avg_pnl:>+12,.2f}")
    print(f"Best trade:       ${executed['pnl'].max():>+12,.2f}")
    print(f"Worst trade:      ${executed['pnl'].min():>+12,.2f}")
    print(f"Win rate:         {win_rate:.1f}%")
    print(f"Avg P&L %:        {executed['pnl_pct'].mean():>+.2f}%")
    print()

    print("P&L BY DAY (signal days only):")
    for _, t in executed.iterrows():
        marker = "✓" if t["pnl"] > 0 else "✗"
        print(
            f"  {t['date']}  {marker}  "
            f"entry=${t['entry_price']:>8,.2f}  "
            f"exit=${t['exit_price']:>8,.2f}  "
            f"P&L=${t['pnl']:>+10,.2f} ({t['pnl_pct']:>+.2f}%)  "
            f"[{t['exit_reason']}@9:{t['exit_minute']:02.0f}]"
        )

    print()
    print(f"Final cumulative P&L: ${total_pnl:>+12,.2f}")
    print("=" * 70)
