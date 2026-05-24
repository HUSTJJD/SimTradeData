# -*- coding: utf-8 -*-
"""把 QMT 财务报表列收成 PTrade 财务列。

字段名来自君弘君智 config 里的现金流量表、利润表、主要指标、股本表。
公告日用 m_anntime，报告期用 m_timetag。扣非只在利润表或主要指标真有这一列时写入。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# QMT 列名 -> PTrade 列名。同一张表里只取这些列。
CASHFLOW_FIELDS = {
    "cash_pay_dist_dpcp_int_exp": "dividend_interest_payment",
    "m_cashPayDistDpcpIntExp": "dividend_interest_payment",
    "net_cash_flows_oper_act": "net_operate_cash_flow",
    "net_cash_flows_inv_act": "net_invest_cash_flow",
    "net_cash_flows_fnc_act": "net_finance_cash_flow",
}
INCOME_FIELDS = {
    "revenue_inc": "operating_revenue",
    "revenue": "operating_revenue",
    "total_expense": "operating_cost",
    "financial_expense": "financial_expense",
    "oper_profit": "operating_profit",
    "tot_profit": "total_profit",
    "net_profit_incl_min_int_inc": "net_profit",
    "net_profit_excl_min_int_inc": "np_parent_company_owners",
    "net_profit_incl_min_int_inc_after": "np_parent_company_cut",
    "s_fa_eps_basic": "basic_eps",
}
PERSHARE_FIELDS = {
    "s_fa_ocfps": "net_operate_cash_flow_ps",
    "m_sFaOcfps": "net_operate_cash_flow_ps",
    "s_fa_bps": "naps",
    "s_fa_eps_basic": "basic_eps",
    "s_fa_undistributedps": "undivided_profit",
    "s_fa_surpluscapitalps": "capital_surplus_fund_ps",
    "adjusted_net_profit_rate": "np_parent_company_cut_yoy",
    "m_adjustedNetProfitRate": "np_parent_company_cut_yoy",
    "adjusted_net_profit": "np_parent_company_cut",
    "m_adjustedNetProfit": "np_parent_company_cut",
    "inc_revenue_rate": "operating_revenue_grow_rate",
    "du_profit_rate": "net_profit_grow_rate",
    "inc_net_profit_rate": "np_parent_company_yoy",
    "inventory_turnover": "inventory_turnover_rate",
    "m_inventoryTurnover": "inventory_turnover_rate",
    "du_return_on_equity": "roe",
    "equity_roe": "roe_weighted",
}
CAPITAL_FIELDS = {
    "total_capital": "total_shares",
    "totalCapital": "total_shares",
    "circulating_capital": "a_floats",
    "floatCapitalA": "a_floats",
}
_ANNOUNCE = ("m_anntime", "m_annTime", "declareDate")
_PERIOD_END = ("m_timetag", "m_endtime", "changeDate")
_CODE = ("code", "stock_code", "secu_code", "S_INFO_WINDCODE")
_TABLES = {
    "ASHARECASHFLOW": CASHFLOW_FIELDS,
    "ASHAREINCOME": INCOME_FIELDS,
    "ASHAREBALANCE": {},
    "PERSHAREINDEX": PERSHARE_FIELDS,
    "CAPITALSTRUCTURE": CAPITAL_FIELDS,
}
_READABLE = {".csv", ".txt", ".parquet", ".xlsx"}


def qmt_time(value):
    """QMT 时间可能是毫秒、秒或 YYYYMMDD。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NaT
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return pd.NaT
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        number = int(text)
        if number > 10**12:
            return pd.to_datetime(number, unit="ms", utc=True).tz_convert("Asia/Shanghai").tz_localize(None).normalize()
        if number > 10**9:
            return pd.to_datetime(number, unit="s", utc=True).tz_convert("Asia/Shanghai").tz_localize(None).normalize()
        if number > 10**7:
            return pd.to_datetime(str(number), format="%Y%m%d")
    return pd.to_datetime(text, errors="coerce").normalize()


def project_table(frame: pd.DataFrame, fields: dict[str, str]) -> pd.DataFrame:
    """只复制能对上的列，并带上报告期、公告日、代码。"""
    if frame is None or frame.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=frame.index)
    end = _first_column(frame, _PERIOD_END)
    announce = _first_column(frame, _ANNOUNCE)
    code = _first_column(frame, _CODE)
    if end is not None:
        out["end_date"] = end.map(qmt_time)
    if announce is not None:
        out["publ_date"] = announce.map(qmt_time)
    if code is not None:
        out["symbol"] = code.map(_symbol)
    for source, target in fields.items():
        if source in frame.columns and target not in out.columns:
            out[target] = pd.to_numeric(frame[source], errors="coerce")
    return out.dropna(how="all")


def build_fundamentals(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """按股票和报告期合并各表，并补经营现金流同比和四个季度之和。"""
    pieces = []
    for name, fields in _TABLES.items():
        if not fields:
            continue
        frame = tables.get(name)
        if frame is None:
            continue
        projected = project_table(frame, fields)
        if projected.empty or "end_date" not in projected.columns:
            continue
        pieces.append(projected)
    if not pieces:
        return pd.DataFrame()
    merged = pieces[0]
    for piece in pieces[1:]:
        keys = [key for key in ("symbol", "end_date") if key in merged.columns and key in piece.columns]
        if not keys:
            continue
        overlap = [col for col in piece.columns if col in merged.columns and col not in keys]
        piece = piece.drop(columns=overlap, errors="ignore")
        merged = merged.merge(piece, on=keys, how="outer")
    if "symbol" not in merged.columns:
        return pd.DataFrame()
    frames = []
    for symbol, group in merged.groupby("symbol", sort=False):
        frames.append(_derive(group.sort_values("end_date"), symbol))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def list_statement_files(root: Path) -> tuple[list[Path], list[Path]]:
    """返回 (可直接读取的表文件, 看到了表名但还不能读的文件)。"""
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    readable = []
    unknown = []
    if not datadir.is_dir():
        return readable, unknown
    for path in datadir.rglob("*"):
        if not path.is_file():
            continue
        token = path.stem.upper()
        if not any(name in token or name in path.parent.name.upper() for name in _TABLES):
            continue
        if path.suffix.lower() in _READABLE:
            readable.append(path)
        elif path.suffix.lower() not in {".ini"}:
            unknown.append(path)
    return readable, unknown


def read_statement_file(path: Path) -> tuple[str | None, pd.DataFrame]:
    """按文件名判断是哪一张表。读不出就返回空表。"""
    table = _table_name(path)
    if table is None:
        return None, pd.DataFrame()
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        frame = pd.read_parquet(path)
    elif suffix == ".xlsx":
        frame = pd.read_excel(path)
    else:
        frame = pd.read_csv(path)
    if "symbol" not in frame.columns and "code" not in frame.columns:
        frame = frame.copy()
        frame["code"] = path.stem
    return table, frame


def _derive(group: pd.DataFrame, symbol: str) -> pd.DataFrame:
    work = group.copy()
    work["symbol"] = symbol
    if "net_operate_cash_flow" in work.columns:
        previous = work["net_operate_cash_flow"].shift(4)
        derived = (work["net_operate_cash_flow"] / previous - 1.0) * 100.0
        derived = derived.where(previous.notna() & (previous != 0))
        if "oper_cash_ps_grow_rate" in work.columns:
            work["oper_cash_ps_grow_rate"] = work["oper_cash_ps_grow_rate"].where(
                work["oper_cash_ps_grow_rate"].notna(), derived
            )
        else:
            work["oper_cash_ps_grow_rate"] = derived
        work["net_operate_cash_flow_ttm"] = work["net_operate_cash_flow"].rolling(4).sum()
        if "total_shares" in work.columns:
            shares = work["total_shares"].where(work["total_shares"] > 0)
            work["net_operate_cash_flow_ps_ttm"] = work["net_operate_cash_flow_ttm"] / shares
    work["date"] = work["end_date"]
    if "publ_date" in work.columns:
        work["publ_date"] = (
            work["publ_date"].dt.strftime("%Y%m%d").where(work["publ_date"].notna(), None)
        )
    return work


def _first_column(frame: pd.DataFrame, names: tuple[str, ...]):
    for name in names:
        if name in frame.columns:
            return frame[name]
    return None


def _symbol(value) -> str:
    text = str(value).strip().upper()
    if text.endswith(".SH") or text.endswith(".XSHG"):
        return text.split(".")[0][-6:] + ".SS"
    if text.endswith(".SZ") or text.endswith(".XSHE"):
        return text.split(".")[0][-6:] + ".SZ"
    if text.endswith(".SS") or text.endswith(".BJ"):
        return text.split(".")[0][-6:] + "." + text.rsplit(".", 1)[-1]
    code = text[-6:]
    if code.startswith(("5", "6", "9")):
        return code + ".SS"
    if code.startswith(("4", "8")):
        return code + ".BJ"
    return code + ".SZ"


def _table_name(path: Path) -> str | None:
    blob = (path.stem + " " + path.parent.name).upper()
    for name in _TABLES:
        if name in blob:
            return name
    return None
