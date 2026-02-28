## Cursor Cloud specific instructions

This is a Python stock scanning app that queries the Alpaca API for minute-bar data and detects DJI-correlated buy signals across top NASDAQ stocks.

### Running the app

```bash
# Requires Alpaca credentials
export APCA_API_KEY_ID="..."
export APCA_API_SECRET_KEY="..."

# Full scan (default config)
python3 main.py

# Quick test scan: 3 stocks, 5 days, verbose
python3 main.py -n 3 -d 5 -v
```

### Key notes

- **Python 3.12** is used; invoke via `python3` (no `python` symlink).
- **Dependencies**: `pip3 install -r requirements.txt` (alpaca-py, pandas, pyyaml, pytz).
- **Tests**: `python3 -m pytest tests/ -v` — no API keys needed for unit tests.
- **Alpaca free tier** uses IEX feed and has a 200 req/min rate limit. The fetcher handles this automatically.
- **SQLite cache** at `cache/market_data.db` avoids redundant API fetches. Use `--clear-cache` to reset.
- All signal thresholds are configurable in `config/default.yaml` or via CLI flags (see `--help`).
- Output CSVs are written to `output/`.
