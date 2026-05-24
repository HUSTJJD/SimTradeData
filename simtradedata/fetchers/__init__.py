"""Fetchers package. 按名字导入，避免没装某个数据源时整个包起不来。"""

__all__ = [
    "BaoStockFetcher",
    "PublicSourceFetcher",
    "EastMoneyFetcher",
    "MootdxFetcher",
    "MootdxAffairFetcher",
    "MootdxUnifiedFetcher",
    "YFinanceFetcher",
]

_LAZY = {
    "BaoStockFetcher": "simtradedata.fetchers.baostock_fetcher",
    "PublicSourceFetcher": "simtradedata.fetchers.public_source_fetcher",
    "EastMoneyFetcher": "simtradedata.fetchers.eastmoney_fetcher",
    "MootdxFetcher": "simtradedata.fetchers.mootdx_fetcher",
    "MootdxAffairFetcher": "simtradedata.fetchers.mootdx_affair_fetcher",
    "MootdxUnifiedFetcher": "simtradedata.fetchers.mootdx_unified_fetcher",
    "YFinanceFetcher": "simtradedata.fetchers.yfinance_fetcher",
}


def __getattr__(name: str):
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError("module 'simtradedata.fetchers' has no attribute %r" % name)
    import importlib
    module = importlib.import_module(module_name)
    return getattr(module, name)
