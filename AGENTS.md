## Cursor Cloud specific instructions

This is a Python stock scanning app that queries the Alpaca API for minute-bar data and detects DJI-correlated buy signals across top NASDAQ stocks.

### Running the app

```bash
# Requires Alpaca credentials as env vars
export APCA_API_KEY_ID="..."
export APCA_API_SECRET_KEY="..."

# Full scan (default: 100 stocks, 30 days)
python3 main.py

# Quick test scan: 3 stocks, 5 days, verbose
python3 main.py -n 3 -d 5 -v
```

See `python3 main.py --help` and `README.md` for all CLI flags and config options.

### Key notes

- **Python 3.12** is used; invoke via `python3` (no `python` symlink).
- **Dependencies**: `pip3 install -r requirements.txt` (alpaca-py, pandas, pyyaml, pytz).
- **Tests**: `python3 -m pytest tests/ -v` — no API keys needed for unit tests.
- **Alpaca free tier** requires IEX feed (`feed: "iex"` in config). The default SIP feed returns a 403 error on free-tier keys. This is already configured in `config/default.yaml`.
- **SQLite cache** at `cache/market_data.db` avoids redundant API fetches between runs. Use `--clear-cache` to reset.
- All signal thresholds are configurable in `config/default.yaml` or via CLI flags.
- Output CSVs are written to `output/`.
- `signals.py` is a pure function module (DataFrames in/out) — designed for reuse by a future Monte Carlo tuning app.
