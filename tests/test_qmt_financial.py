# -*- coding: utf-8 -*-
"""QMT 财务列和除权列的对照。不读安装目录。"""

import pandas as pd

from simtradedata.importers.qmt_exrights import to_exrights
from simtradedata.importers.qmt_financial import build_fundamentals, qmt_time


def test_millisecond_announce_time_is_shanghai_date():
    # 2024-04-26 00:00 +08
    stamp = 1714060800000
    assert qmt_time(stamp) == pd.Timestamp("2024-04-26")


def test_cashflow_and_cut_profit_keep_their_own_columns():
    cash = pd.DataFrame({
        "code": ["600519.SH"] * 4,
        "m_timetag": ["20230331", "20230630", "20230930", "20231231"],
        "m_anntime": ["20230426", "20230812", "20231028", "20240426"],
        "cash_pay_dist_dpcp_int_exp": [1.0, 1.0, 1.0, 2.0],
        "net_cash_flows_oper_act": [10.0, 10.0, 10.0, 30.0],
    })
    income = pd.DataFrame({
        "code": ["600519.SH"],
        "m_timetag": ["20231231"],
        "net_profit_incl_min_int_inc": [99.0],
        "net_profit_incl_min_int_inc_after": [80.0],
    })
    pershare = pd.DataFrame({
        "code": ["600519.SH"],
        "m_timetag": ["20231231"],
        "adjusted_net_profit_rate": [12.5],
        "total_shares_ignored": [1],
    })
    capital = pd.DataFrame({
        "code": ["600519.SH"],
        "m_timetag": ["20231231"],
        "total_capital": [100.0],
    })
    out = build_fundamentals({
        "ASHARECASHFLOW": cash,
        "ASHAREINCOME": income,
        "PERSHAREINDEX": pershare,
        "CAPITALSTRUCTURE": capital,
    })
    row = out[out["end_date"] == pd.Timestamp("2023-12-31")].iloc[0]
    assert row["symbol"] == "600519.SS"
    assert row["dividend_interest_payment"] == 2.0
    assert row["np_parent_company_cut"] == 80.0
    assert row["np_parent_company_cut_yoy"] == 12.5
    assert row["net_profit"] == 99.0
    assert row["publ_date"] == "20240426"
    assert row["net_operate_cash_flow_ttm"] == 60.0
    assert row["net_operate_cash_flow_ps_ttm"] == 0.6


def test_missing_cut_profit_stays_empty():
    cash = pd.DataFrame({
        "code": ["000001.SZ"],
        "m_timetag": ["20231231"],
        "net_cash_flows_oper_act": [5.0],
        "net_profit_incl_min_int_inc": [7.0],
    })
    out = build_fundamentals({"ASHARECASHFLOW": cash})
    assert "np_parent_company_cut" not in out.columns or pd.isna(out.iloc[0].get("np_parent_company_cut", pd.NA))


def test_exrights_adds_bonus_and_transfer():
    frame = pd.DataFrame({
        "code": ["600000.SH"],
        "time": [20240615],
        "interest": [0.3],
        "stockBonus": [0.1],
        "stockGift": [0.2],
        "allotNum": [0.0],
        "allotPrice": [0.0],
    })
    out = to_exrights(frame)
    assert out.loc[0, "symbol"] == "600000.SS"
    assert round(out.loc[0, "allotted_ps"], 6) == 0.3
    assert out.loc[0, "bonus_ps"] == 0.3
    assert out.loc[0, "dividend"] == 0.3
