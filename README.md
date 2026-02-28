# Stock Scanner — DJI-Correlated Buy Signals

Scans the top NASDAQ stocks by market cap for **buy signals** correlated with
Dow Jones Industrial Average (DJI) movements during the first minutes of each
trading day.

## How It Works

1. **Fetch** minute-by-minute price bars from the Alpaca API for the top 100
   NASDAQ stocks and the DJI proxy (DIA ETF) over the last 30 trading days.
2. **Detect signals** — for each stock on each day, scan the first 20 minutes
   of trading for a *dip→jump* pattern:
   - **Dip**: a minute where `close ≤ open` (flat or negative candle).
   - **Jump**: the very next minute where the stock's close is at least
     `$0.25` above the dip minute's close.
3. **Classify buy signals** — if, during the dip minute, the DJI proxy rose by
   at least 19 basis points (open→close), the signal is promoted to a
   **buySignal**.
4. **Rank** stocks by total buySignal count (descending) and output to CSV.

All thresholds are **configurable** via YAML to support future Monte Carlo
parameter tuning.

## Prerequisites

- Python 3.11+
- An [Alpaca](https://alpaca.markets/) account (free tier works)

## Setup

```bash
pip install -r requirements.txt
```

Set your Alpaca API credentials:

```bash
export APCA_API_KEY_ID="your-key"
export APCA_API_SECRET_KEY="your-secret"
```

## Usage

```bash
# Full scan with default config
python main.py

# Scan only 10 stocks, last 5 days, verbose
python main.py -n 10 -d 5 -v

# Override signal thresholds
python main.py --stock-jump 0.50 --dji-jump-bps 25

# Clear cached data before running
python main.py --clear-cache

# Use a custom config file
python main.py -c my_config.yaml
```

## Configuration

Edit `config/default.yaml`:

```yaml
scan:
  num_stocks: 100        # top N NASDAQ stocks
  lookback_days: 30      # trading days to analyze
  morning_minutes: 20    # minutes from market open

signals:
  stock_jump_min: 0.25   # min $ jump (configurable for tuning)
  dji_jump_min_bps: 19   # min DJI basis-point jump (configurable)

dji_proxy:
  symbol: "DIA"          # ETF proxy for DJI
```

Or supply a custom list of symbols:

```yaml
stock_symbols:
  - AAPL
  - MSFT
  - NVDA
```

## Output

| File | Description |
|------|-------------|
| `output/buy_signals.csv` | Ranked list: symbol, buy_signal_count, total_signals |
| `output/signals_detail.csv` | Every signal with timestamps, prices, DJI data |

## Architecture

```
config/default.yaml    ← tunable thresholds
src/
  config.py            ← loads YAML into dataclasses
  universe.py          ← top 100 NASDAQ stocks (static, overridable)
  cache.py             ← SQLite caching layer
  fetcher.py           ← Alpaca API client, rate limiting, caching
  signals.py           ← pure-function signal detection (reusable for Monte Carlo)
  scanner.py           ← orchestrator: fetch → detect → rank
main.py                ← CLI entry point with argparse overrides
```

`signals.py` is deliberately a pure function (DataFrames in, DataFrames out)
so a future Monte Carlo tuning app can import and sweep over parameter
combinations without touching the API layer.

## Caching

Fetched bars are stored in `cache/market_data.db` (SQLite). Subsequent runs
skip API calls for already-cached symbols/dates. Use `--clear-cache` to reset.
