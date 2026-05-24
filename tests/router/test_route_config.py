from simtradedata.router.route_config import (
    DEFAULT_ROUTE_TABLE,
    FETCHER_REGISTRY,
)


class TestRouteConfig:
    def test_all_sources_in_route_table_are_registered(self):
        """Every source name referenced in the route table must exist in FETCHER_REGISTRY."""
        for data_type, markets in DEFAULT_ROUTE_TABLE.items():
            for market, sources in markets.items():
                for source in sources:
                    assert source in FETCHER_REGISTRY, (
                        f"Source '{source}' in {data_type}/{market} "
                        f"not found in FETCHER_REGISTRY"
                    )

    def test_daily_bars_cn_has_fallback(self):
        # Exact list: adding or dropping a source must be an explicit decision.
        assert DEFAULT_ROUTE_TABLE["daily_bars"]["cn"] == [
            "public", "eastmoney", "baostock",
            "akshare", "adata", "efinance", "mootdx",
        ]

    def test_upstream_mootdx_sources_are_preserved(self):
        """上游 TDX/mootdx 不得被新增源挤出：既是日线最后兜底，
        也是非日线接口的首源。"""
        assert "mootdx" in FETCHER_REGISTRY
        assert DEFAULT_ROUTE_TABLE["daily_bars"]["cn"][-1] == "mootdx"
        for data_type in ["xdxr", "stock_list", "realtime_quotes"]:
            assert DEFAULT_ROUTE_TABLE[data_type]["cn"] == ["mootdx"]
        for data_type in ["fundamentals", "minute_bars"]:
            assert DEFAULT_ROUTE_TABLE[data_type]["cn"] == ["mootdx", "public"]

    def test_public_is_supplement_for_reconciled_interfaces(self):
        for data_type in ["fundamentals", "minute_bars"]:
            assert DEFAULT_ROUTE_TABLE[data_type]["cn"][0] == "mootdx"
            assert "public" in DEFAULT_ROUTE_TABLE[data_type]["cn"]
        assert DEFAULT_ROUTE_TABLE["fund_bars"]["cn"] == ["public"]

    def test_calendar_and_daily_index_do_not_depend_on_mootdx(self):
        assert DEFAULT_ROUTE_TABLE["trade_calendar"]["cn"] == ["baostock"]
        assert DEFAULT_ROUTE_TABLE["index_data"]["cn"] == ["baostock"]

    def test_daily_bars_us_uses_yfinance(self):
        sources = DEFAULT_ROUTE_TABLE["daily_bars"]["us"]
        assert sources == ["yfinance"]

    def test_money_flow_only_eastmoney(self):
        sources = DEFAULT_ROUTE_TABLE["money_flow"]["cn"]
        assert sources == ["eastmoney"]

    def test_fetcher_registry_has_import_paths(self):
        for name, path in FETCHER_REGISTRY.items():
            assert "." in path, f"Registry entry '{name}' must be a dotted import path"
            parts = path.rsplit(".", 1)
            assert len(parts) == 2, f"Registry entry '{name}' must be module.ClassName"
