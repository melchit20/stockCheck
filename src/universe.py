"""
NASDAQ top stocks by market cap.

This list is a static approximation and should be updated periodically.
Override via the stock_symbols key in config YAML for a custom universe.
"""

NASDAQ_TOP_100 = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "AVGO", "TSLA",
    "COST", "NFLX", "ASML", "TMUS", "AMD", "AZN", "PEP", "ADBE",
    "CSCO", "TXN", "QCOM", "ISRG", "INTU", "AMGN", "BKNG", "AMAT",
    "PDD", "CMCSA", "VRTX", "ADI", "REGN", "PANW", "KLAC", "SBUX",
    "LRCX", "MU", "MDLZ", "GILD", "MELI", "INTC", "SNPS", "CDNS",
    "CTAS", "CRWD", "PYPL", "NXPI", "MAR", "MRVL", "DASH", "ORLY",
    "WDAY", "ROP", "FTNT", "MNST", "ABNB", "PCAR", "ROST", "DXCM",
    "TTD", "ODFL", "IDXX", "CHTR", "CPRT", "FAST", "MCHP", "PAYX",
    "KHC", "KDP", "EA", "VRSK", "CTSH", "LULU", "BKR", "ON",
    "FANG", "DDOG", "ANSS", "CCEP", "CDW", "BIIB", "ZS", "ILMN",
    "WBD", "GFS", "TEAM", "MRNA", "DLTR", "WBA", "ALGN", "ENPH",
    "RIVN", "SMCI", "ARM", "COIN", "HOOD", "MSTR", "APP", "PLTR",
    "TTWO", "CPNG", "ROKU", "OKTA",
]


def get_stock_universe(n: int = 100) -> list[str]:
    """Return the top N NASDAQ stocks by market cap."""
    return NASDAQ_TOP_100[:n]
