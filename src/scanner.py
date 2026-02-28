"""Orchestrates the full scan: fetch → detect → rank."""

import logging
from pathlib import Path

import pandas as pd

from .config import AppConfig
from .fetcher import AlpacaFetcher
from .signals import detect_signals
from .universe import get_stock_universe

logger = logging.getLogger(__name__)


class Scanner:
    def __init__(self, config: AppConfig):
        self.config = config
        self.fetcher = AlpacaFetcher(config)

    def run(self) -> pd.DataFrame:
        symbols = self.config.stock_symbols or get_stock_universe(
            self.config.scan.num_stocks
        )
        logger.info(
            f"Scanning {len(symbols)} stocks over "
            f"{self.config.scan.lookback_days} trading days"
        )

        dji_sym = self.config.dji_proxy_symbol
        logger.info(f"Fetching DJI proxy ({dji_sym}) data...")
        dji_bars = self.fetcher.fetch_morning_bars([dji_sym])
        if dji_bars.empty:
            logger.error("No DJI proxy data returned — cannot continue")
            return pd.DataFrame()

        logger.info(f"Fetching data for {len(symbols)} stocks...")
        all_bars = self.fetcher.fetch_morning_bars(symbols)
        if all_bars.empty:
            logger.error("No stock data returned")
            return pd.DataFrame()

        dji_only = dji_bars[dji_bars["symbol"] == dji_sym]
        all_signals: list[pd.DataFrame] = []

        for idx, symbol in enumerate(symbols, 1):
            stock_bars = all_bars[all_bars["symbol"] == symbol]
            if stock_bars.empty:
                continue

            sigs = detect_signals(stock_bars, dji_only, self.config.signals)
            if not sigs.empty:
                sigs.insert(0, "symbol", symbol)
                all_signals.append(sigs)

            if idx % 20 == 0 or idx == len(symbols):
                logger.info(f"  Processed {idx}/{len(symbols)} stocks")

        if not all_signals:
            logger.info("No signals detected across any stock")
            return self._empty_results(symbols)

        signals_df = pd.concat(all_signals, ignore_index=True)
        self._save_signals(signals_df)

        results = self._rank(signals_df, symbols)
        self._save_results(results)

        self._print_summary(signals_df, results)
        return results

    def _rank(self, signals_df: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
        buy = signals_df[signals_df["is_buy_signal"]]
        buy_counts = (
            buy.groupby("symbol")
            .size()
            .reset_index(name="buy_signal_count")
        )

        total_counts = (
            signals_df.groupby("symbol")
            .size()
            .reset_index(name="total_signals")
        )

        results = pd.DataFrame({"symbol": symbols})
        results = results.merge(buy_counts, on="symbol", how="left")
        results = results.merge(total_counts, on="symbol", how="left")
        results["buy_signal_count"] = results["buy_signal_count"].fillna(0).astype(int)
        results["total_signals"] = results["total_signals"].fillna(0).astype(int)
        results = results.sort_values(
            "buy_signal_count", ascending=False
        ).reset_index(drop=True)
        return results

    def _empty_results(self, symbols: list[str]) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": symbols,
                "buy_signal_count": 0,
                "total_signals": 0,
            }
        )

    def _save_signals(self, df: pd.DataFrame):
        path = Path(self.config.output.signals_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info(f"Detailed signals → {path}")

    def _save_results(self, df: pd.DataFrame):
        path = Path(self.config.output.results_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info(f"Rankings → {path}")

    @staticmethod
    def _print_summary(signals_df: pd.DataFrame, results: pd.DataFrame):
        total_signals = len(signals_df)
        buy_signals = signals_df["is_buy_signal"].sum()
        stocks_with_buys = (results["buy_signal_count"] > 0).sum()

        print("\n" + "=" * 60)
        print("SCAN SUMMARY")
        print("=" * 60)
        print(f"Total signals detected:  {total_signals}")
        print(f"Buy signals:             {buy_signals}")
        print(f"Stocks with buy signals: {stocks_with_buys}")
        print("=" * 60)

        top = results[results["buy_signal_count"] > 0].head(20)
        if not top.empty:
            print("\nTop stocks by buy signal count:")
            print(top.to_string(index=False))
        else:
            print("\nNo buy signals found with current thresholds.")
        print()
