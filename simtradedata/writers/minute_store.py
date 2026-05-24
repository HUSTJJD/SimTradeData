# -*- coding: utf-8 -*-
"""1 分钟线按日累积。腾讯只给最近一个窗口，本地文件按 datetime 合并后留给引擎。"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from simtradedata.utils.paths import get_data_path


def data_symbol(symbol: str) -> str:
    """统一成引擎文件名：600000.SS / 000001.SZ。"""
    raw = str(symbol).split(".")[0][-6:]
    suffix = str(symbol).rsplit(".", 1)[-1].upper() if "." in str(symbol) else ""
    if suffix == "BJ" or raw.startswith(("4", "8")):
        return raw + ".BJ"
    if suffix in {"SS", "XSHG", "SH"} or raw.startswith(("5", "6", "9")):
        return raw + ".SS"
    return raw + ".SZ"


def minute_store_dir(base: Path | None = None) -> Path:
    """引擎从 data/cn/stocks_1m 或 data/stocks_1m 读。cn 目录已经存在才往里面写。"""
    if base is None:
        env = os.environ.get("SIMTRADELAB_DATA_PATH")
        base = Path(env) if env else get_data_path()
    root = base / "cn" if (base / "cn").is_dir() else base
    path = root / "stocks_1m"
    path.mkdir(parents=True, exist_ok=True)
    return path


def append_minute_bars(symbol: str, df: pd.DataFrame, base: Path | None = None) -> pd.DataFrame:
    """合并进 stocks_1m/<代码>.parquet，同一时刻保留后到的一根。"""
    if df is None or df.empty or "datetime" not in df.columns:
        return df
    incoming = df.copy()
    incoming["datetime"] = pd.to_datetime(incoming["datetime"])
    path = minute_store_dir(base) / (data_symbol(symbol) + ".parquet")
    if path.exists():
        old = pd.read_parquet(path)
        old["datetime"] = pd.to_datetime(old["datetime"])
        merged = pd.concat([old, incoming], ignore_index=True)
    else:
        merged = incoming
    merged = (
        merged.drop_duplicates("datetime", keep="last")
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    merged.to_parquet(path, index=False)
    return merged
