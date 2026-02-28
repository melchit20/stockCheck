"""Backtest configuration."""

from dataclasses import dataclass

import yaml


@dataclass
class BacktestConfig:
    # What to trade
    symbol: str = "BKNG"
    dji_proxy: str = "DIA"
    shares: int = 10
    lookback_days: int = 100

    # Signal thresholds (sweet spot defaults from MC sweep)
    stock_jump_min: float = 2.0
    dji_jump_min_bps: float = 3.0
    premarket_minutes: int = 10

    # Trade management
    profit_target_pct: float = 10.0
    stop_loss_pct: float = 10.0
    trading_window_minutes: int = 20

    # Data
    polygon_api_key: str = ""
    cache_db: str = "cache/market_data.db"

    # Output
    output_csv: str = "output/backtest_trades.csv"
    summary_csv: str = "output/backtest_summary.csv"

    @classmethod
    def from_yaml(cls, path: str) -> "BacktestConfig":
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        config = cls()
        for k, v in data.items():
            if hasattr(config, k):
                setattr(config, k, v)
        return config
