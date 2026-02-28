"""
NASDAQ top stocks by daily dollar volume (proxy for market cap).

Generated via scripts/build_universe.py using Alpaca snapshot data.
Override via the stock_symbols key in config YAML for a custom universe.
"""

NASDAQ_TOP_500 = [
    "NVDA", "AAPL", "MSFT", "AMZN", "TSLA", "GOOGL", "INTU", "META", "AVGO", "MU",
    "BKNG", "SNDK", "LITE", "PLTR", "CRWV", "WBD", "WMT", "CSCO", "AMD", "INTC",
    "GOOG", "MELI", "COST", "ADI", "CMCSA", "CEG", "TER", "ASML", "SNPS", "SHOP",
    "DUOL", "KLAC", "ZS", "LRCX", "SOFI", "AMAT", "MNST", "WDAY", "NBIS", "LIN",
    "WDC", "AMGN", "CSX", "QCOM", "EQIX", "CME", "FITB", "APP", "PYPL", "FTAI",
    "CRWD", "TXN", "PSKY", "ADSK", "CDNS", "GILD", "ILMN", "STX", "KTOS", "CRDO",
    "EXPE", "AAL", "SBAC", "MPWR", "AAOI", "ODFL", "EXC", "PAYX", "COIN", "VRTX",
    "FSLR", "SATS", "ADBE", "RUN", "IREN", "UAL", "TLN", "NTRA", "RKLB", "LPLA",
    "PANW", "MKSI", "RNAM", "HBAN", "SMCI", "MRVL", "ASTS", "HOOD", "MDB", "PEP",
    "AXON", "ISRG", "ORLY", "ABNB", "DKNG", "ADP", "TMUS", "MCHP", "TTD", "REGN",
    "KHC", "FCNCA", "MAR", "DASH", "MSTR", "WTW", "MDLZ", "CPRT", "ALAB", "CELH",
    "ROST", "EXE", "CHRW", "TEAM", "EOSE", "ALNY", "ON", "DPZ", "AFRM", "ROKU",
    "ZM", "MARA", "FWONK", "CASY", "SNY", "ULTA", "KDP", "EBAY", "SBUX", "CSGP",
    "FAST", "HON", "APA", "NXPI", "AKAM", "PDD", "ROP", "DDOG", "XEL", "NDAQ",
    "WULF", "ARM", "EXAS", "INSM", "FTNT", "ARCC", "CZR", "TRI", "NTAP", "MEDP",
    "MASI", "FISV", "LNT", "TPG", "PODD", "TSCO", "OKTA", "SAIA", "RGLD", "Z",
    "CFLT", "BKR", "CPB", "FIVE", "TTWO", "CG", "JBHT", "CORZ", "RGTI", "PFG",
    "CTSH", "MDGL", "RIVN", "LFUS", "IBKR", "LYFT", "VRSK", "ARGX", "STRL", "ICLR",
    "PCAR", "IDXX", "UTHR", "ACLX", "RVMD", "MRNA", "MTSI", "AEP", "NXT", "BTSG",
    "ENTG", "VRSN", "TXRH", "NTNX", "VNOM", "CHKP", "LULU", "BPOP", "ZION", "XRAY",
    "PENN", "CHTR", "FIGR", "DLTR", "POOL", "WING", "TROW", "APLD", "CIFR", "IONS",
    "TW", "MAT", "FANG", "ONDS", "ZBRA", "VTRS", "LASR", "GLPI", "LSTR", "HUT",
    "ONB", "HST", "DXCM", "AMBA", "CDW", "ENPH", "AGNC", "JD", "RGEN", "RIOT",
    "RYTM", "MKTX", "ROIV", "TSEM", "BIIB", "WWD", "LSCC", "CTAS", "SOUN", "JKHY",
    "SEDG", "EVRG", "UCTT", "SIRI", "FFIV", "ONC", "VISN", "AEIS", "EWBC", "VSNT",
    "SSNC", "MANH", "VLY", "BBIO", "MTCH", "GH", "FROG", "ASND", "NDSN", "IRTC",
    "NXST", "XP", "GDS", "MNDY", "GEHC", "TCOM", "GEN", "WIX", "HLNE", "STLD",
    "REAL", "SWKS", "PGNY", "SOLS", "SLM", "SITM", "WYNN", "CGNX", "TMDX", "CENX",
    "PLUG", "SMTC", "DOCU", "DAVE", "PCTY", "FOXA", "CHDN", "IDCC", "HTHT", "MIDD",
    "AAON", "OLLI", "BSY", "IBRX", "QRVO", "HOLX", "OPEN", "ALGN", "NBIX", "RPRX",
    "URBN", "NTES", "TEM", "CWST", "CLSK", "VSEC", "LAMR", "PI", "BIDU", "VOD",
    "CHYM", "CCEP", "NWSA", "SFM", "OS", "COLB", "CVLT", "VIAV", "SLAB", "INCY",
    "DBX", "GFS", "CINF", "EA", "VICR", "AUR", "CHRD", "ARRY", "TXG", "ERIC",
    "GRAB", "PTC", "BCRX", "TIGO", "BMRN", "TTEK", "COCO", "HAS", "SANM", "CART",
    "CROX", "PCT", "WFRD", "CBSH", "MDLN", "FRSH", "TECH", "REG", "FRPT", "FLEX",
    "RYAAY", "PRCT", "AVAV", "NVTS", "GTLB", "FOX", "VNET", "AMKR", "CVCO", "COO",
    "LUNR", "CACC", "MORN", "RMBS", "UPST", "TTMI", "COKE", "VITL", "GLXY", "ARWR",
    "KYMR", "FSLY", "OPCH", "GBDC", "DOX", "QUBT", "JAZZ", "SEZL", "RNA", "PRAX",
    "COGT", "MWH", "ODD", "LNTH", "GLNG", "ACMR", "BRKR", "LAUR", "IESC", "CIGI",
    "BILI", "LIVN", "POWL", "NVAX", "ACHC", "XENE", "AUGO", "ALM", "HSIC", "TTAN",
    "VC", "AXTI", "STEP", "FULT", "FORM", "PVLA", "NVMI", "HYMC", "OTEX", "SEIC",
    "RDNT", "PAGP", "FCFS", "VCTR", "SLDE", "PZZA", "NWS", "ACLS", "PAA", "LOGI",
    "CAI", "TERN", "AXSM", "ACGL", "PECO", "TRMB", "ENSG", "QLYS", "FER", "ABVX",
    "SHLS", "INDV", "MYRG", "EXEL", "CAKE", "PTEN", "APPF", "LECO", "USAR", "BHF",
    "SSRM", "SRRK", "RLAY", "IOVA", "ALKT", "LLYVK", "UMBF", "EBC", "GTM", "SYNA",
    "MIRM", "PTGX", "PPTA", "CCC", "CRUS", "SGML", "FTDR", "INTR", "SIMO", "LYTS",
    "ASO", "ZG", "OLED", "ALHC", "BNTX", "HALO", "WGS", "ADMA", "KRYS", "CRSP",
    "LOPE", "SBRA", "QURE", "PEGA", "WLDN", "NUVL", "GCT", "ALKS", "BOKF", "CYTK",
    "WAY", "EXLS", "BWIN", "SAIC", "RELY", "OZK", "CAR", "BRZE", "FIVN", "SBLK",
    "IPGP", "WSC", "ITRI", "ESLT", "FRMI", "NOVT", "UPWK", "RRR", "BITF", "INOD",
]


def get_stock_universe(n: int = 100) -> list[str]:
    """Return the top N NASDAQ stocks by daily dollar volume."""
    return NASDAQ_TOP_500[:n]
