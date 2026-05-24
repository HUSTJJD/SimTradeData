"""XDXR conversion keeps full corporate-action history for adjusted prices.

A configured daily-history floor trims OHLCV imports, but exrights events
before that floor must remain because post-floor adjusted prices depend on
historical company actions.
"""

import importlib

import pandas as pd
import pytest


def _make_xdxr_df():
    return pd.DataFrame(
        {
            "category": [1, 1, 1],
            "year": [1993, 2004, 2020],
            "month": [11, 1, 6],
            "day": [1, 2, 3],
            "songzhuangu": [10, 10, 10],
            "peigu": [0, 0, 0],
            "peigujia": [0, 0, 0],
            "fenhong": [1, 1, 1],
        }
    )


def _convert(xdxr_df):
    from scripts import download_mootdx

    downloader = download_mootdx.MootdxDownloader.__new__(
        download_mootdx.MootdxDownloader
    )
    return downloader._convert_xdxr_to_exrights(xdxr_df)


def test_xdxr_conversion_keeps_pre_floor_rows(monkeypatch):
    import simtradedata.config.field_mappings as fm
    from scripts import download_mootdx

    monkeypatch.setenv("SIMTRADE_CN_HISTORY_START", "2005-05-09")
    importlib.reload(fm)
    importlib.reload(download_mootdx)

    result = _convert(_make_xdxr_df())

    assert len(result) == 3
    assert list(result["date"]) == [
        pd.Timestamp("1993-11-01"),
        pd.Timestamp("2004-01-02"),
        pd.Timestamp("2020-06-03"),
    ]


def test_xdxr_conversion_keeps_full_history_without_floor(monkeypatch):
    import simtradedata.config.field_mappings as fm
    from scripts import download_mootdx

    monkeypatch.delenv("SIMTRADE_CN_HISTORY_START", raising=False)
    importlib.reload(fm)
    importlib.reload(download_mootdx)

    result = _convert(_make_xdxr_df())

    assert len(result) == 3


def test_xdxr_conversion_uses_ptrade_event_contract(monkeypatch):
    import simtradedata.config.field_mappings as fm
    from scripts import download_mootdx

    monkeypatch.delenv("SIMTRADE_CN_HISTORY_START", raising=False)
    importlib.reload(fm)
    importlib.reload(download_mootdx)

    row = _convert(_make_xdxr_df()).iloc[0]
    # songzhuangu=10 -> allotted_ps=1.0; fenhong=1 -> bonus_ps=dividend=0.1
    assert row["allotted_ps"] == pytest.approx(1.0)
    assert row["rationed_ps"] == pytest.approx(0.0)
    assert row["rationed_px"] == pytest.approx(0.0)
    assert row["bonus_ps"] == pytest.approx(0.1)
    assert row["dividend"] == pytest.approx(0.1)
