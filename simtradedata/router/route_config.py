"""Route table and fetcher registry for SmartRouter."""

# Dotted import paths for lazy instantiation.
FETCHER_REGISTRY = {
    "public": "simtradedata.fetchers.public_source_fetcher.PublicSourceFetcher",
    "mootdx": "simtradedata.fetchers.mootdx_unified_fetcher.MootdxUnifiedFetcher",
    "eastmoney": "simtradedata.fetchers.eastmoney_fetcher.EastMoneyFetcher",
    "baostock": "simtradedata.fetchers.unified_fetcher.UnifiedDataFetcher",
    "yfinance": "simtradedata.fetchers.yfinance_fetcher.YFinanceFetcher",
}

# data_type -> market -> [source priority list]
DEFAULT_ROUTE_TABLE = {
    "daily_bars": {
        "cn": ["public", "mootdx"],
        "us": ["yfinance"],
    },
    "xdxr": {
        "cn": ["mootdx"],
    },
    "fundamentals": {
        "cn": ["public", "mootdx"],
        "us": ["yfinance"],
    },
    "valuation": {
        "cn": ["baostock"],
        "us": ["yfinance"],
    },
    "money_flow": {
        "cn": ["eastmoney"],
    },
    "lhb": {
        "cn": ["eastmoney"],
    },
    "margin": {
        "cn": ["eastmoney"],
    },
    "stock_list": {
        "cn": ["mootdx"],
        "us": ["yfinance"],
    },
    "trade_calendar": {
        "cn": ["mootdx", "baostock"],
    },
    "index_data": {
        "cn": ["mootdx", "baostock"],
    },
    "realtime_quotes": {
        "cn": ["mootdx"],
    },
    "minute_bars": {
        "cn": ["public", "mootdx"],
    },
    "fund_bars": {
        "cn": ["public"],
    },
}
