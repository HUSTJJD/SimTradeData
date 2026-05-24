# -*- coding: utf-8 -*-
"""QMT 除权因子收成 PTrade 的每股事件。

迅投 get_divid_factors 的列是 interest、stockBonus、stockGift、allotNum、allotPrice。
datadir/DividData 里的 LevelDB 用的是内部代码，不是这张表，不能当成每股分红。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from simtradedata.importers.qmt_financial import qmt_time, _symbol

_FIELDS = {
    "interest": "bonus_ps",
    "stockBonus": "stock_bonus",
    "stockGift": "stock_gift",
    "allotNum": "rationed_ps",
    "allotPrice": "rationed_px",
}
_TIME = ("time", "date", "m_timetag", "m_anntime")
_CODE = ("code", "stock_code", "secu_code")


def to_exrights(frame: pd.DataFrame) -> pd.DataFrame:
    """送股和转增相加，得到每股送转。现金分红用每股派息。"""
    if frame is None or frame.empty:
        return pd.DataFrame()
    if not any(name in frame.columns for name in _FIELDS):
        return pd.DataFrame()
    out = pd.DataFrame(index=frame.index)
    when = None
    for name in _TIME:
        if name in frame.columns:
            when = frame[name].map(qmt_time)
            break
    if when is None:
        return pd.DataFrame()
    out["date"] = when
    for source, target in _FIELDS.items():
        if source in frame.columns:
            out[target] = pd.to_numeric(frame[source], errors="coerce").fillna(0.0)
        else:
            out[target] = 0.0
    out["allotted_ps"] = out["stock_bonus"] + out["stock_gift"]
    out["bonus_ps"] = out["bonus_ps"]
    out["dividend"] = out["bonus_ps"]
    code = None
    for name in _CODE:
        if name in frame.columns:
            code = frame[name]
            break
    if code is not None:
        out["symbol"] = code.map(_symbol)
    return out.drop(columns=["stock_bonus", "stock_gift"])


def list_exrights_files(root: Path) -> list[Path]:
    """只认带除权因子列名的 csv / parquet。LevelDB 不在这里读。"""
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    found = []
    if not datadir.is_dir():
        return found
    for path in datadir.rglob("*"):
        if not path.is_file():
            continue
        name = path.stem.lower()
        if path.suffix.lower() not in {".csv", ".parquet", ".txt"}:
            continue
        if "divid" in name or "exright" in name or "除权" in path.stem:
            found.append(path)
    return found
