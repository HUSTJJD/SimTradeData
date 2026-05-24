"""Route table and fetcher registry for SmartRouter."""

# Dotted import paths for lazy instantiation.
FETCHER_REGISTRY = {
    "etf_pcf": "simtradedata.fetchers.etf_pcf_fetcher.EtfPcfFetcher",
    "public": "simtradedata.fetchers.public_source_fetcher.PublicSourceFetcher",
    "mootdx": "simtradedata.fetchers.mootdx_unified_fetcher.MootdxUnifiedFetcher",
    "eastmoney": "simtradedata.fetchers.eastmoney_fetcher.EastMoneyFetcher",
    "baostock": "simtradedata.fetchers.unified_fetcher.UnifiedDataFetcher",
    "yfinance": "simtradedata.fetchers.yfinance_fetcher.YFinanceFetcher",
    "akshare": "simtradedata.fetchers.oss_fetchers.AKShareFetcher",
    "adata": "simtradedata.fetchers.oss_fetchers.ADataFetcher",
    "efinance": "simtradedata.fetchers.oss_fetchers.EfinanceFetcher",
}

# data_type -> market -> [source priority list]
DEFAULT_ROUTE_TABLE = {
    "etf_pcf": {"cn": ["etf_pcf"]},
    "daily_bars": {
        # public 为不复权主源（腾讯日线 + 新浪收盘价对账）；
        # eastmoney / baostock 是第一层兜底；AKShare / ADATA / efinance
        # 是可选开源库兜底，未安装时由 SmartRouter 自动跳过。
        # 上游的 mootdx/TDX 本地日线源必须保留为最后一层兜底：它是
        # 唯一不依赖第三方 HTTP 接口的日线来源。所有来源只写同一个
        # DuckDB，不产生第二份数据集。
        "cn": [
            "public", "eastmoney", "baostock",
            "akshare", "adata", "efinance", "mootdx",
        ],
        "us": ["yfinance"],
    },
    "xdxr": {
        "cn": ["mootdx"],
    },
    "fundamentals": {
        # 上游季度批量财务由 mootdx/TDX 提供；public 只作为补充源，
        # 放在后面，避免每次季度批量调用先撞上未实现的接口。
        "cn": ["mootdx", "public"],
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
        "cn": ["baostock"],
    },
    "index_data": {
        "cn": ["baostock"],
    },
    "realtime_quotes": {
        "cn": ["mootdx"],
    },
    "minute_bars": {
        # 保留上游只读的 mootdx 分钟线为首选；public 的 1m 累积落盘
        # 只在 mootdx 不可用时作为补充触发。
        "cn": ["mootdx", "public"],
    },
    "fund_bars": {
        "cn": ["public"],
    },
    # Upstream TDX extras all route to the existing unified Mootdx fetcher.
    "blocks": {"cn": ["mootdx"]},
    "f10": {"cn": ["mootdx"]},
    "history_transactions": {"cn": ["mootdx"]},
    "history_minutes": {"cn": ["mootdx"]},
}
