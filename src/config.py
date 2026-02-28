"""Configuration management for the stock scanner."""

from dataclasses import dataclass, field
from typing import List, Optional

import yaml


@dataclass
class ScanConfig:
    num_stocks: int = 100
    lookback_days: int = 30
    morning_minutes: int = 20


@dataclass
class SignalConfig:
    stock_jump_min: float = 0.25
    dji_jump_min_bps: float = 19.0


@dataclass
class AlpacaConfig:
    feed: str = "iex"
    max_requests_per_min: int = 200


@dataclass
class OutputConfig:
    results_csv: str = "output/buy_signals.csv"
    signals_csv: str = "output/signals_detail.csv"


@dataclass
class CacheConfig:
    db_path: str = "cache/market_data.db"


@dataclass
class AppConfig:
    scan: ScanConfig = field(default_factory=ScanConfig)
    signals: SignalConfig = field(default_factory=SignalConfig)
    alpaca: AlpacaConfig = field(default_factory=AlpacaConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    dji_proxy_symbol: str = "DIA"
    stock_symbols: Optional[List[str]] = None

    @classmethod
    def from_yaml(cls, path: str) -> "AppConfig":
        with open(path) as f:
            data = yaml.safe_load(f) or {}

        config = cls()

        section_map = {
            "scan": config.scan,
            "signals": config.signals,
            "alpaca": config.alpaca,
            "output": config.output,
            "cache": config.cache,
        }
        for section_name, section_obj in section_map.items():
            if section_name in data:
                for k, v in data[section_name].items():
                    if hasattr(section_obj, k):
                        setattr(section_obj, k, v)

        if "dji_proxy" in data:
            config.dji_proxy_symbol = data["dji_proxy"].get("symbol", "DIA")

        if "stock_symbols" in data:
            config.stock_symbols = data["stock_symbols"]

        return config
