"""
Options paper trading simulator.

On a buy signal: buys 1 call option contract per stock.
Prices options using Black-Scholes with historical volatility.
Tracks position minute-by-minute, sells on profit/stop/window expiry.
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict
from zoneinfo import ZoneInfo

import pandas as pd

from .options_pricer import black_scholes_call, call_delta, historical_volatility
from backtest.trader import fetch_bars_from_polygon

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

TRADING_DAYS_PER_YEAR = 252
MINUTES_PER_TRADING_DAY = 390


def _compute_iv(stock_bars: pd.DataFrame, date, lookback: int = 30) -> float:
    """Compute annualized historical vol from daily closes preceding `date`."""
    daily = (
        stock_bars[stock_bars["date"] < date]
        .groupby("date")["close"]
        .last()
        .sort_index()
        .tail(lookback)
    )
    return historical_volatility(daily.tolist())


def run_options_backtest(
    stock_bars: pd.DataFrame,
    dji_bars: pd.DataFrame,
    symbol: str,
    lookback_days: int = 200,
    contracts: int = 1,
    strike_otm_pct: float = 15.0,
    days_to_expiry: int = 7,
    risk_free_rate: float = 0.05,
    iv_lookback: int = 30,
    stock_jump_min: float = 0.75,
    dji_jump_min_bps: float = 0.0,
    premarket_minutes: int = 20,
    profit_target_pct: float = 50.0,
    stop_loss_pct: float = 50.0,
    trading_window_minutes: int = 20,
) -> pd.DataFrame:
    """
    Run options backtest for a single stock.

    Returns DataFrame with one row per trading day.
    """
    pm_start = 30 - premarket_minutes
    trading_dates = sorted(stock_bars["date"].unique())[-lookback_days:]

    trades: List[Dict] = []
    cumulative_pnl = 0.0

    for date in trading_dates:
        s_day = stock_bars[stock_bars["date"] == date]
        d_day = dji_bars[dji_bars["date"] == date]

        # --- Signal check ---
        s_pm = s_day[
            (s_day["et_hour"] == 9)
            & (s_day["et_min"] >= pm_start)
            & (s_day["et_min"] <= 29)
        ].sort_values("timestamp")

        s_open_bar = s_day[
            (s_day["et_hour"] == 9) & (s_day["et_min"] == 30)
        ]

        if s_pm.empty or s_open_bar.empty:
            continue

        pm_open = s_pm.iloc[0]["open"]
        pm_close = s_pm.iloc[-1]["close"]
        if pm_close > pm_open:
            continue

        mkt_close = s_open_bar.iloc[0]["close"]
        stock_jump = mkt_close - pm_close
        if stock_jump < stock_jump_min:
            continue

        d_pm = d_day[
            (d_day["et_hour"] == 9)
            & (d_day["et_min"] >= pm_start)
            & (d_day["et_min"] <= 29)
        ].sort_values("timestamp")
        dji_bps = 0.0
        if not d_pm.empty:
            do = d_pm.iloc[0]["open"]
            dc = d_pm.iloc[-1]["close"]
            if do > 0:
                dji_bps = ((dc - do) / do) * 10_000
        if dji_bps < dji_jump_min_bps:
            continue

        # --- Price the option at entry ---
        spot_entry = s_open_bar.iloc[0]["open"]
        strike = spot_entry * (1.0 + strike_otm_pct / 100.0)
        T_entry = days_to_expiry / 365.0
        sigma = _compute_iv(stock_bars, date, iv_lookback)

        entry_premium = black_scholes_call(
            spot_entry, strike, T_entry, risk_free_rate, sigma
        )

        if entry_premium < 0.01:
            entry_premium = 0.01

        entry_cost = entry_premium * 100 * contracts
        delta_entry = call_delta(
            spot_entry, strike, T_entry, risk_free_rate, sigma
        )

        profit_price = entry_premium * (1 + profit_target_pct / 100)
        stop_price = entry_premium * (1 - stop_loss_pct / 100)

        # --- Monitor minute by minute ---
        cutoff = 30 + trading_window_minutes
        bars = s_day[
            (s_day["et_hour"] == 9)
            & (s_day["et_min"] >= 30)
            & (s_day["et_min"] < cutoff)
        ].sort_values("timestamp")

        exit_premium = None
        exit_reason = None
        exit_minute = None
        exit_spot = None
        exit_delta = None

        for _, bar in bars.iterrows():
            minutes_elapsed = (bar["et_min"] - 30)
            T_now = T_entry - (minutes_elapsed / MINUTES_PER_TRADING_DAY / TRADING_DAYS_PER_YEAR)
            if T_now <= 0:
                T_now = 1e-6

            high_premium = black_scholes_call(
                bar["high"], strike, T_now, risk_free_rate, sigma
            )
            low_premium = black_scholes_call(
                bar["low"], strike, T_now, risk_free_rate, sigma
            )
            close_premium = black_scholes_call(
                bar["close"], strike, T_now, risk_free_rate, sigma
            )

            if low_premium <= stop_price and high_premium >= profit_price:
                exit_premium = stop_price
                exit_reason = "stop_loss"
                exit_minute = int(bar["et_min"])
                exit_spot = bar["close"]
                break
            elif high_premium >= profit_price:
                exit_premium = profit_price
                exit_reason = "profit_target"
                exit_minute = int(bar["et_min"])
                exit_spot = bar["high"]
                break
            elif low_premium <= stop_price:
                exit_premium = stop_price
                exit_reason = "stop_loss"
                exit_minute = int(bar["et_min"])
                exit_spot = bar["low"]
                break

        if exit_premium is None:
            if not bars.empty:
                last = bars.iloc[-1]
                minutes_elapsed = last["et_min"] - 30
                T_now = T_entry - (minutes_elapsed / MINUTES_PER_TRADING_DAY / TRADING_DAYS_PER_YEAR)
                if T_now <= 0:
                    T_now = 1e-6
                exit_premium = black_scholes_call(
                    last["close"], strike, T_now, risk_free_rate, sigma
                )
                exit_spot = last["close"]
                exit_minute = int(last["et_min"])
            else:
                exit_premium = entry_premium
                exit_spot = spot_entry
                exit_minute = 30
            exit_reason = "window_expired"

        pnl_per_share = exit_premium - entry_premium
        pnl = pnl_per_share * 100 * contracts
        pnl_pct = (pnl_per_share / entry_premium * 100) if entry_premium > 0 else 0
        cumulative_pnl += pnl

        trades.append({
            "date": date,
            "symbol": symbol,
            "signal": True,
            "spot_entry": round(spot_entry, 2),
            "spot_exit": round(exit_spot, 2) if exit_spot else None,
            "spot_change_pct": round((exit_spot - spot_entry) / spot_entry * 100, 3) if exit_spot else 0,
            "strike": round(strike, 2),
            "sigma": round(sigma, 3),
            "delta_entry": round(delta_entry, 4),
            "entry_premium": round(entry_premium, 4),
            "exit_premium": round(exit_premium, 4) if exit_premium else 0,
            "entry_cost": round(entry_cost, 2),
            "exit_reason": exit_reason,
            "exit_minute": exit_minute,
            "contracts": contracts,
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 1),
            "cumulative_pnl": round(cumulative_pnl, 2),
        })

    return pd.DataFrame(trades)


def print_options_summary(
    trades: pd.DataFrame,
    symbols: list[str],
):
    """Print summary of options backtest across all symbols."""
    if trades.empty:
        print("No trades executed.")
        return

    print("\n" + "=" * 85)
    print("OPTIONS BACKTEST RESULTS")
    print("=" * 85)
    print(f"Period:  {trades['date'].min()} → {trades['date'].max()}")
    print(f"Stocks:  {', '.join(symbols)}")
    print()

    total_cost = trades["entry_cost"].sum()
    total_pnl = trades["pnl"].sum()
    wins = (trades["pnl"] > 0).sum()
    losses = (trades["pnl"] <= 0).sum()
    wr = wins / len(trades) * 100 if len(trades) > 0 else 0

    print("AGGREGATE:")
    print(f"  Trades:          {len(trades)}")
    print(f"  Wins / Losses:   {wins} / {losses}  ({wr:.1f}% win rate)")
    print(f"  Total invested:  ${total_cost:>10,.2f}")
    print(f"  Total P&L:       ${total_pnl:>+10,.2f}")
    print(f"  ROI:             {total_pnl/total_cost*100:>+.1f}%")
    print(f"  Avg P&L/trade:   ${trades['pnl'].mean():>+10,.2f}")
    print(f"  Avg P&L %:       {trades['pnl_pct'].mean():>+.1f}%")
    print()

    print("PER-STOCK:")
    header = f"{'Stock':>6}  {'Trades':>6}  {'Win%':>5}  {'Invested':>10}  {'P&L':>10}  {'ROI':>7}  {'AvgΔ':>6}  {'AvgPrem':>8}"
    print(header)
    print("-" * len(header))

    for sym in symbols:
        s = trades[trades["symbol"] == sym]
        if s.empty:
            continue
        w = (s["pnl"] > 0).sum()
        invested = s["entry_cost"].sum()
        pnl = s["pnl"].sum()
        roi = pnl / invested * 100 if invested > 0 else 0
        print(
            f"{sym:>6}  {len(s):>6}  {w/len(s)*100:>4.0f}%  "
            f"${invested:>9,.2f}  ${pnl:>+9,.2f}  {roi:>+5.1f}%  "
            f"{s['delta_entry'].mean():>.4f}  "
            f"${s['entry_premium'].mean():>7.4f}"
        )

    print()
    print("EXIT REASONS:")
    for reason in ["profit_target", "stop_loss", "window_expired"]:
        subset = trades[trades["exit_reason"] == reason]
        if not subset.empty:
            print(
                f"  {reason:20s}  {len(subset):>3} trades  "
                f"P&L: ${subset['pnl'].sum():>+10,.2f}  "
                f"avg: {subset['pnl_pct'].mean():>+.1f}%"
            )

    print()
    print("TRADE LOG:")
    for _, t in trades.sort_values("date").iterrows():
        marker = "✓" if t["pnl"] > 0 else "✗"
        print(
            f"  {t['date']}  {t['symbol']:>4}  {marker}  "
            f"spot=${t['spot_entry']:>7,.2f}→${t['spot_exit']:>7,.2f} ({t['spot_change_pct']:>+.2f}%)  "
            f"K=${t['strike']:>7,.2f}  "
            f"prem=${t['entry_premium']:.4f}→{t['exit_premium']:.4f}  "
            f"P&L=${t['pnl']:>+8,.2f} ({t['pnl_pct']:>+.0f}%)  "
            f"[{t['exit_reason']}@9:{t['exit_minute']:02.0f}]"
        )

    print()
    print(f"Total P&L: ${total_pnl:>+10,.2f}  on ${total_cost:,.2f} invested  ({total_pnl/total_cost*100:>+.1f}% ROI)")
    print("=" * 85)
