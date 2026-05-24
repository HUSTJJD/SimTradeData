"""
Centralized field mapping configurations

This module contains all field mapping definitions from data sources to PTrade format.
Supports: BaoStock, mootdx
"""

import os
from datetime import date

# BaoStock -> PTrade field mappings

MARKET_FIELD_MAP = {
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "preclose": "preclose",
    "volume": "volume",
    "amount": "money",  # BaoStock 'amount' -> PTrade 'money'
}

# Mootdx market data field mapping (mootdx -> PTrade)
MOOTDX_MARKET_FIELD_MAP = {
    "datetime": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "vol": "volume",
    "amount": "money",
}

VALUATION_FIELD_MAP = {
    "peTTM": "pe_ttm",
    "pbMRQ": "pb",
    "psTTM": "ps_ttm",
    "pcfNcfTTM": "pcf",
    "turn": "turnover_rate",
}

FUNDAMENTAL_FIELD_MAP = {
    # From profit data
    "roeAvg": "roe",
    "roa": "roa",
    "npMargin": "net_profit_ratio",
    "gpMargin": "gross_income_ratio",
    # From balance data
    "currentRatio": "current_ratio",
    "quickRatio": "quick_ratio",
    "liabilityToAsset": "_liability_to_asset",  # Derived into debt/equity by the fetcher
    # From operation data
    "ARTurnRatio": "accounts_receivables_turnover_rate",
    "INVTurnRatio": "inventory_turnover_rate",
    "TATurnRatio": "total_asset_turnover_rate",
    "CATurnRatio": "current_assets_turnover_rate",
    # From growth data
    "YOYORev": "operating_revenue_grow_rate",
    "YOYNI": "net_profit_grow_rate",
    "YOYAsset": "total_asset_grow_rate",
    "YOYEPSBasic": "basic_eps_yoy",
    "YOYPNI": "np_parent_company_yoy",
    # From cash flow data
    "ebitToInterest": "interest_cover",
}

# Mootdx XDXR field mapping (for ex-rights/ex-dividend data).
# PTrade exrights 契约（均为每股口径）：
#   allotted_ps = 送股/转增，rationed_ps = 配股，rationed_px = 配股价，
#   bonus_ps = 税前现金分红，dividend = 与 bonus_ps 同值（兼容字段）。
MOOTDX_XDXR_FIELD_MAP = {
    "datetime": "date",
    "songzhuangu": "allotted_ps",    # 送股/转增
    "peigu": "rationed_ps",          # 配股
    "peigujia": "rationed_px",       # 配股价
    "fenhong": "bonus_ps",           # 税前现金分红
}

# Data routing configuration for DataSplitter
DATA_ROUTING = {
    'market': {
        'target_table': 'stocks',
        'fields': list(MARKET_FIELD_MAP),
        'rename': {k: v for k, v in MARKET_FIELD_MAP.items() if k != v},
    },
    'valuation': {
        'target_table': 'valuation',
        'fields': ['date', *VALUATION_FIELD_MAP],
        'rename': VALUATION_FIELD_MAP.copy(),
    },
    'status': {
        'target_table': None,  # Stored in memory
        'fields': ['date', 'isST', 'tradestatus'],
        'rename': {}
    }
}

# Benchmark configuration
BENCHMARK_CONFIG = {
    'default_index': '000300.SS',  # CSI300 (沪深300)
    'alternatives': {
        '000016.SS': 'SSE50',      # 上证50
        '000905.SS': 'CSI500',     # 中证500
        '000001.SS': 'SSE',        # 上证指数
        '399001.SZ': 'SZSE',       # 深证成指
        '399006.SZ': 'CYBZ',       # 创业板指
    }
}

# Earliest date each configured benchmark index can physically supply, measured
# from the TDX full-history index bars. A CN history floor earlier than the
# default index's inception is unachievable: no source can produce index bars
# from before the index existed. Gates clamp the required floor to this value
# instead of failing an impossible requirement.
BENCHMARK_INDEX_INCEPTION = {
    '000300.SS': '2005-01-04',  # CSI300 (default)
    '000016.SS': '2004-01-02',  # SSE50
    '000905.SS': '2005-01-04',  # CSI500
    '000001.SS': '1990-12-19',  # SSE Composite
    '399001.SZ': '1991-04-03',  # SZSE Component
    '399006.SZ': '2010-06-01',  # ChiNext
}

# Default benchmark history floor for CN exports. The exported benchmark must
# cover dates back to at least this value for the canonical 2015-era package.
# A deployment-wide floor set through SIMTRADE_CN_HISTORY_START replaces this
# default through effective_benchmark_history_floor(); use that helper rather
# than importing the constant directly in gates/exporters.
DEFAULT_BENCHMARK_HISTORY_FLOOR = '2015-01-05'
BENCHMARK_HISTORY_FLOOR = DEFAULT_BENCHMARK_HISTORY_FLOOR

# Optional per-deployment CN history floor / trim. When set to a canonical
# YYYY-MM-DD value it controls both the full-history rebuild start and the
# strict release floor. Unset keeps the legacy 2015 floor for checks and lets
# TDX import retain its full source history.
CN_HISTORY_START = os.environ.get("SIMTRADE_CN_HISTORY_START") or None

# Minimum share of CN stock_metadata rows that must carry industry blocks,
# enforced by the pre-release integrity gate and the repair job's target.
BLOCKS_COVERAGE_FLOOR = 0.95


def effective_benchmark_history_floor(history_start: str | None = None) -> str:
    """Return the benchmark floor required by the active CN history start.

    A full-history rebuild may set a floor far earlier than the benchmark index
    existed (e.g. 1990-12-19 with CSI300, which starts 2005-01-04). Clamping to
    the index inception keeps the gate strict about truncated exports without
    demanding history that no data source can provide. Deployments using a
    long-lived index such as 000001.SS keep the configured floor unchanged.
    """
    candidate = CN_HISTORY_START if history_start is None else history_start
    required = DEFAULT_BENCHMARK_HISTORY_FLOOR
    if candidate:
        try:
            parsed = date.fromisoformat(candidate)
            if parsed.isoformat() == candidate:
                required = candidate
        except (TypeError, ValueError):
            pass
    inception = BENCHMARK_INDEX_INCEPTION.get(
        BENCHMARK_CONFIG["default_index"], DEFAULT_BENCHMARK_HISTORY_FLOOR
    )
    return max(required, inception)


def benchmark_history_ok(start_text: str | None, floor: str | None = None) -> bool:
    """True when the earliest benchmark date covers the effective floor."""
    required = floor or effective_benchmark_history_floor()
    return bool(start_text) and str(start_text)[:10] <= required
