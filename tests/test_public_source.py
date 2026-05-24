# -*- coding: utf-8 -*-
"""公开行情的翻页、对账和分钟累积。不访问网络。"""

import pandas as pd

from simtradedata.fetchers.public_source_fetcher import (
    collect_paged_bars,
    statement_on_date,
)
from simtradedata.validators.crosscheck import keep_agreed_bars
from simtradedata.writers.minute_store import append_minute_bars, data_symbol


def _bar(day, close):
    return [day, "1", str(close), "1", "1", "10"]


def test_paging_walks_backward_until_start():
    pages = {
        "2026-03-01": [_bar("2026-02-01", 2), _bar("2026-03-01", 3)],
        "2026-01-31": [_bar("2026-01-01", 1), _bar("2026-01-15", 1.5)],
    }

    def fetch_page(cursor):
        return pages.get(cursor, [])

    bars = collect_paged_bars(fetch_page, "2026-01-01", "2026-03-01")
    assert [bar[0] for bar in bars] == ["2026-01-01", "2026-01-15", "2026-02-01", "2026-03-01"]


def test_paging_stops_when_the_page_does_not_move():
    stuck = [_bar("2026-03-01", 3)]
    calls = {"n": 0}

    def fetch_page(cursor):
        calls["n"] += 1
        return stuck

    bars = collect_paged_bars(fetch_page, "2020-01-01", "2026-03-01")
    assert len(bars) == 1
    assert calls["n"] == 2


def test_conflict_rows_are_dropped():
    left = pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        "close": [10.0, 10.0],
    })
    right = pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        "close": [10.0, 12.0],
    })
    kept = keep_agreed_bars(left, right, tolerance=0.005)
    assert list(pd.to_datetime(kept["date"]).dt.strftime("%Y-%m-%d")) == ["2026-01-01"]


def test_statement_overlay_skips_missing_period():
    statement = pd.DataFrame({
        "end_date": [pd.Timestamp("2024-12-31")],
        "dividend_interest_payment": [100.0],
        "np_parent_company_cut_yoy": [None],
    })
    assert statement_on_date(statement, "2024-12-31") == {
        "dividend_interest_payment": 100.0,
    }
    assert statement_on_date(statement, "2024-09-30") == {}


def test_minute_bars_accumulate(tmp_path):
    first = pd.DataFrame({
        "datetime": ["2026-11-02 09:31:00"],
        "open": [1.0],
        "close": [1.1],
        "high": [1.2],
        "low": [1.0],
        "volume": [100.0],
    })
    second = pd.DataFrame({
        "datetime": ["2026-11-02 09:31:00", "2026-11-02 09:32:00"],
        "open": [1.0, 1.1],
        "close": [1.3, 1.2],
        "high": [1.3, 1.2],
        "low": [1.0, 1.1],
        "volume": [120.0, 80.0],
    })
    append_minute_bars("600519.XSHG", first, base=tmp_path)
    merged = append_minute_bars("600519.SS", second, base=tmp_path)
    path = tmp_path / "stocks_1m" / "600519.SS.parquet"
    assert path.exists()
    assert data_symbol("600519.SH") == "600519.SS"
    assert len(merged) == 2
    assert merged.iloc[0]["close"] == 1.3
