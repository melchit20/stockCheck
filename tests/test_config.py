"""Tests for configuration management."""

import tempfile
from pathlib import Path

from src.config import AppConfig


class TestAppConfig:
    def test_defaults(self):
        config = AppConfig()
        assert config.scan.num_stocks == 100
        assert config.scan.lookback_days == 30
        assert config.scan.morning_minutes == 20
        assert config.signals.stock_jump_min == 1.0
        assert config.signals.dji_jump_min_bps == 19.0
        assert config.signals.premarket_minutes == 15
        assert config.dji_proxy_symbol == "DIA"

    def test_from_yaml(self):
        config = AppConfig.from_yaml("config/default.yaml")
        assert config.scan.num_stocks == 500
        assert config.signals.stock_jump_min == 2.0

    def test_custom_yaml(self, tmp_path):
        yaml_content = """
scan:
  num_stocks: 10
  lookback_days: 5
signals:
  stock_jump_min: 0.50
  dji_jump_min_bps: 25.0
stock_symbols:
  - AAPL
  - MSFT
"""
        p = tmp_path / "custom.yaml"
        p.write_text(yaml_content)

        config = AppConfig.from_yaml(str(p))
        assert config.scan.num_stocks == 10
        assert config.scan.lookback_days == 5
        assert config.signals.stock_jump_min == 0.50
        assert config.signals.dji_jump_min_bps == 25.0
        assert config.stock_symbols == ["AAPL", "MSFT"]
