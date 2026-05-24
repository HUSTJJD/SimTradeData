# -*- coding: utf-8 -*-
"""两源对账。对得上的收盘价写入 close，对不上的留空并标 conflict。"""

from __future__ import annotations

import pandas as pd


def reconcile_closes(left: pd.DataFrame, right: pd.DataFrame, tolerance: float = 0.005) -> pd.DataFrame:
    """按日期对齐两份不复权收盘价。

    left/right 都要有 date、close。
    相对差不超过 tolerance 时采用左源。只有左源时也采用左源，并标明 single。
    两边都有且超出容差时 close 为空。
    """
    a = left.copy()
    b = right.copy()
    a["date"] = pd.to_datetime(a["date"])
    b["date"] = pd.to_datetime(b["date"])
    merged = a[["date", "close"]].merge(
        b[["date", "close"]], on="date", how="outer", suffixes=("_left", "_right")
    )
    rows = []
    for rec in merged.itertuples(index=False):
        lv, rv = rec.close_left, rec.close_right
        if pd.notna(lv) and pd.notna(rv) and rv != 0:
            gap = abs(lv - rv) / abs(rv)
            if gap <= tolerance:
                rows.append((rec.date, lv, rv, lv, "agree"))
            else:
                rows.append((rec.date, lv, rv, None, "conflict"))
        elif pd.notna(lv):
            rows.append((rec.date, lv, None, lv, "single"))
        else:
            rows.append((rec.date, None, rv, None, "right_only"))
    return pd.DataFrame(rows, columns=["date", "close_left", "close_right", "close", "status"])


def keep_agreed_bars(left: pd.DataFrame, right: pd.DataFrame, tolerance: float = 0.005) -> pd.DataFrame:
    """留下对得上的、以及只有左源的 K 线。两边都有且超出容差的日期整行丢掉。"""
    if left is None or left.empty:
        return left
    if right is None or right.empty:
        return left.reset_index(drop=True)
    checked = reconcile_closes(left, right, tolerance)
    bad = set(pd.to_datetime(checked.loc[checked["status"] == "conflict", "date"]))
    if not bad:
        return left.reset_index(drop=True)
    dates = pd.to_datetime(left["date"])
    return left.loc[~dates.isin(bad)].reset_index(drop=True)
