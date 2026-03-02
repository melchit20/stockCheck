# StockCheck — Pre-Market Signal Detection, Options Backtesting & Paper Trading

A Python toolkit for detecting DJI-correlated pre-market buy signals on NASDAQ stocks, backtesting options strategies with Black-Scholes pricing, tuning parameters via Monte Carlo sweeps, and paper-trading live against the market with a web dashboard.

## What's in the box

| Module | Purpose |
|--------|---------|
| **Stock Scanner** (`main.py`) | Scans top NASDAQ stocks for pre-market dip→jump buy signals correlated with DJI |
| **Backtester** (`backtest/`) | Simulates stock and options trades on historical minute-bar data |
| **Monte Carlo Tuner** (`monte_carlo/`) | Sweeps signal thresholds and trade-management parameters to find optimal combos |
| **Paper Trading Bot** (`paper_trader/`) | Automated bot that checks for live signals and simulates options trades each trading day |
| **Web Dashboard** (`dashboard/`) | Real-time monitoring UI with Google OAuth, P&L charts, trade history, and bot controls |

---

## Prerequisites

- **Python 3.12+**
- [Alpaca](https://alpaca.markets/) account (free tier, for the stock scanner)
- [Polygon.io](https://polygon.io/) API key (for backtesting, MC sweeps, and paper trading)
- Google OAuth credentials (optional, for dashboard authentication)

## Setup

```bash
pip3 install -r requirements.txt
```

```bash
export APCA_API_KEY_ID="..."
export APCA_API_SECRET_KEY="..."
export POLYGON_API_KEY="..."
```

## Tests

```bash
python3 -m pytest tests/ -v
```

No API keys needed for unit tests.

---

## 1. Stock Scanner

Scans the top NASDAQ stocks for **buy signals** — a pre-market dip followed by a jump at market open, correlated with DJI movement.

```bash
python3 main.py                          # full scan (500 stocks, 30 days)
python3 main.py -n 10 -d 5 -v           # quick scan, verbose
python3 main.py --stock-jump 0.50 --dji-jump-bps 25
python3 main.py --clear-cache            # reset SQLite cache
python3 main.py -c config/qqq.yaml       # use QQQ as proxy instead of DIA
```

**Output:** `output/buy_signals.csv`, `output/signals_detail.csv`

**Config:** `config/default.yaml` — signal thresholds, scan window, Alpaca feed settings, cache path.

---

## 2. Backtester

### Stock backtester

Simulates buying shares at market open when a signal fires, then managing the position with configurable profit target, stop loss, and time window.

```bash
python3 -m backtest.main                               # uses config/backtest.yaml
python3 -m backtest.main --symbol TSLA --days 200 -v
```

**Config:** `config/backtest.yaml` — symbol, shares, signal thresholds, trade management (Strategy A: 0.25% profit target, 0.50% stop, 20-min window).

### Options backtester

Simulates buying call options on signal days. Prices options with Black-Scholes using historical volatility, then monitors premium minute-by-minute.

```bash
python3 -m backtest.options_main                       # TSLA, APP, AMD — 200 days
python3 -m backtest.options_main --symbols TSLA APP AMD --days 200 -v
python3 -m backtest.options_main --strike-otm 5 --expiry 3 --profit-pct 20 --stop-pct 50
```

**Current tuned defaults:**

| Parameter | Value |
|-----------|-------|
| Strike OTM | 5% |
| Expiry | 3 DTE |
| Profit target | 20% |
| Stop loss | 50% |
| Trading window | 20 min |

**Output:** `output/options_backtest.csv` — full trade log with spot prices, premiums, Greeks, P&L, exit reasons.

---

## 3. Monte Carlo Tuner

Four sweep engines for finding optimal parameter combinations.

### Signal sweep

Sweeps signal thresholds (stock jump, DJI bps, pre-market window) across the stock universe.

```bash
python3 -m monte_carlo.main -v
```

### Backtest trade-management sweep

Sweeps profit target, stop loss, and window duration for a single stock.

```bash
python3 -m monte_carlo.backtest_main --symbol BKNG --days 200 -v
```

### Joint signal + backtest sweep

Sweeps signal thresholds with fixed trade-management params to find combos that maximize win rate.

```bash
python3 -m monte_carlo.signal_backtest_sweep --symbol BKNG --days 200 -v
```

### Options sweep

Sweeps strike OTM, expiry, profit target, stop loss, and window across multiple tickers. Two-phase optimization: precomputes option premiums, then sweeps trade params instantly.

```bash
python3 -m monte_carlo.options_sweep --symbols TSLA APP AMD --days 200 -v
```

**Output:** CSVs in `output/` with per-combo ROI, win rate, Sharpe ratio, and per-stock breakdowns.

---

## 4. Paper Trading Bot

Automated bot that runs Monday–Friday during market hours. On each trading day it:

1. **9:00 ET** — Initializes bot instances for TSLA, APP, AMD
2. **9:10–9:29 ET** — Scans pre-market minute bars for dip patterns
3. **9:30 ET** — Checks for the buy signal (stock jump at open); enters a simulated call option position if signal fires
4. **9:31–9:49 ET** — Monitors the position minute-by-minute, exits on 20% profit target or 50% stop loss
5. **9:50 ET** — Closes any remaining positions (window expired)
6. **16:00 ET** — End-of-day reconciliation

All trades use Black-Scholes pricing with the same parameters as the backtester.

```bash
python3 run_dashboard.py                  # start bot + dashboard
python3 run_dashboard.py --no-scheduler   # dashboard only (no live trading)
python3 run_dashboard.py --port 8080 -v   # custom port, verbose
```

**Config:** `config/paper_trading.yaml` — trading symbols, options params, signal thresholds, dashboard host/port, Google OAuth credentials, database path.

**Data store:** `data/paper_trading.db` (SQLite) — bots, trades, P&L history.

---

## 5. Web Dashboard

A Flask web app for monitoring the paper trading bot in real-time.

**Views:**
- **Bot Status** — Live cards for each ticker showing current stage, signal status, open position details, and a Cancel button (force-sells open positions)
- **Today's Summary** — Trades, P&L, win rate, open positions, ROI for the current day
- **Historical P&L** — Cumulative P&L chart (Chart.js) with total and per-ticker lines, day over day
- **Trade History** — Sortable table of all trades with entry/exit premiums, P&L, exit reason, status
- **Overall Performance** — Aggregate stats across the entire paper trading period

**Authentication:** Google OAuth with a configurable email allowlist. When OAuth credentials are not configured, the dashboard runs without auth (for local development).

**Auto-refresh:** Bot status and summary update every 15 seconds; trades every 30 seconds; chart every 60 seconds.

### Google OAuth setup

1. Create a project at [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the OAuth consent screen
3. Create OAuth 2.0 credentials (Web application)
4. Set authorized redirect URI to `http://your-host:5000/auth/callback`
5. Update `config/paper_trading.yaml`:

```yaml
auth:
  google_client_id: "your-client-id.apps.googleusercontent.com"
  google_client_secret: "your-secret"
  allowed_emails:
    - "you@gmail.com"
```

---

## Architecture

```
main.py                     ← Stock scanner CLI
run_dashboard.py            ← Paper trading bot + dashboard entry point

src/
  config.py                 ← YAML config loader (dataclasses)
  universe.py               ← Top 500 NASDAQ stocks by dollar volume
  cache.py                  ← SQLite caching layer for Alpaca data
  fetcher.py                ← Alpaca API client (rate limiting, batching, caching)
  polygon_fetcher.py        ← Polygon.io API client for pre-market data
  signals.py                ← Pure-function signal detection (reusable by MC)
  scanner.py                ← Orchestrator: fetch → detect → rank

backtest/
  main.py                   ← Stock backtest CLI
  options_main.py           ← Options backtest CLI
  trader.py                 ← Stock paper-trading simulator + Polygon data fetcher
  options_trader.py         ← Options paper-trading simulator
  options_pricer.py         ← Black-Scholes pricing and Greeks (stdlib only)
  config.py                 ← Backtest config dataclass

monte_carlo/
  main.py                   ← Signal threshold sweep CLI
  backtest_main.py          ← Trade-management sweep CLI
  signal_backtest_sweep.py  ← Joint signal + backtest sweep CLI
  options_sweep.py          ← Options parameter sweep CLI
  tuner.py                  ← Signal sweep engine
  backtest_tuner.py         ← Trade-management sweep engine

paper_trader/
  bot.py                    ← Trading bot engine (daily lifecycle)
  scheduler.py              ← APScheduler cron integration
  live_data.py              ← Polygon.io live data fetcher
  db.py                     ← SQLite persistence (bots, trades, P&L)

dashboard/
  app.py                    ← Flask app (routes, OAuth, REST API)
  templates/
    base.html               ← Layout with Tailwind CSS + Chart.js
    login.html              ← Google sign-in page
    dashboard.html          ← Full dashboard (status, chart, table)

config/
  default.yaml              ← Scanner config
  backtest.yaml             ← Stock backtest config (Strategy A)
  paper_trading.yaml        ← Paper trading bot + dashboard config
  qqq.yaml                  ← Alternate config using QQQ as proxy

scripts/
  build_universe.py         ← One-time utility to rebuild NASDAQ universe

tests/
  test_signals.py           ← Signal detection tests
  test_config.py            ← Config loading tests
  test_cache.py             ← SQLite cache tests
```

## Caching

- **Alpaca data:** `cache/market_data.db` (SQLite). Subsequent scanner runs skip API calls for cached symbols/dates. Use `--clear-cache` to reset.
- **Paper trading state:** `data/paper_trading.db` (SQLite). Persists bot status, trades, and P&L across restarts.
- Both `cache/` and `data/` are git-ignored. `output/` is also git-ignored.
