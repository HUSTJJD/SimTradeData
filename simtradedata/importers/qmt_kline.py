# -*- coding: utf-8 -*-
"""读取君弘君智 / QMT datadir 里的不复权 K 线。

一根记录 64 字节，文件头 8 字节。价格是毫元，时间是这根 K 线的结束时刻。
成交量先按股；若成交额约等于价格乘成交量再乘 100，则原值是手，写成股。
前复权价不在这个文件里，价格保持文件中的原始价。
"""

from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

_TZ = timezone(timedelta(hours=8))
_HEADER = bytes.fromhex("feffffffffffff7f")
_RECORD = 64
_PRICE_SCALE = 1000.0
_MIN_TIME = 946684800  # 2000-01-01 UTC
_PERIOD_DIRS = {
    "60": "1m",
    "60000": "1m",
    "300": "5m",
    "300000": "5m",
    "900": "15m",
    "900000": "15m",
    "1800": "30m",
    "1800000": "30m",
    "3600": "60m",
    "3600000": "60m",
    "86400": "1d",
    "86400000": "1d",
}
_MARKETS = {"SH": "SS", "SZ": "SZ", "BJ": "BJ"}


def list_kline_files(root: Path, markets: tuple[str, ...] = ("SH", "SZ", "BJ")) -> list[tuple[str, str, Path]]:
    """返回 (周期名, 引擎代码, 文件)。root 是安装目录或 datadir。"""
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    found = []
    for market, suffix in _MARKETS.items():
        if market not in markets:
            continue
        market_dir = datadir / market
        if not market_dir.is_dir():
            continue
        for period, name in _PERIOD_DIRS.items():
            folder = market_dir / period
            if not folder.is_dir():
                continue
            for path in sorted(folder.glob("*.DAT")):
                code = path.stem[-6:]
                found.append((name, f"{code}.{suffix}", path))
    return found


def decode_kline_dat(path: Path) -> pd.DataFrame:
    """解开一只标的的 K 线。空文件或写到一半的文件返回空表。"""
    data = path.read_bytes()
    body = _body(data)
    if body is None:
        return pd.DataFrame()
    rows = []
    for offset in range(0, len(body) - _RECORD + 1, _RECORD):
        time, raw_open, raw_high, raw_low, raw_close, _last, volume, _flag, amount = struct.unpack_from(
            "<9I", body, offset
        )
        if time < _MIN_TIME or raw_close == 0:
            continue
        rows.append((
            datetime.fromtimestamp(time, _TZ).replace(tzinfo=None),
            raw_open / _PRICE_SCALE,
            raw_high / _PRICE_SCALE,
            raw_low / _PRICE_SCALE,
            raw_close / _PRICE_SCALE,
            int(volume),
            float(amount),
        ))
    if not rows:
        return pd.DataFrame()
    rows = _unwrap(rows)
    frame = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume", "money"])
    frame = frame.drop_duplicates("datetime", keep="last").sort_values("datetime")
    scale = _share_scale(frame)
    if scale != 1:
        frame["volume"] = frame["volume"] * scale
    return frame.reset_index(drop=True)


def to_daily_frame(bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """收成引擎日线列。涨跌停按板块幅度，用前一日收盘价。基金、债券不填涨跌停。"""
    if bars.empty:
        return bars
    work = bars.copy()
    work["date"] = pd.to_datetime(work["datetime"]).dt.normalize()
    daily = (
        work.groupby("date", sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            money=("money", "sum"),
        )
        .reset_index()
    )
    daily["preclose"] = daily["close"].shift(1)
    limit = _limit_ratio(symbol)
    if limit is None:
        daily["high_limit"] = None
        daily["low_limit"] = None
    else:
        daily["high_limit"] = (daily["preclose"] * (1 + limit)).round(2)
        daily["low_limit"] = (daily["preclose"] * (1 - limit)).round(2)
    return daily[["date", "open", "close", "high", "low", "high_limit", "low_limit", "preclose", "volume", "money"]]


def _body(data: bytes) -> bytes | None:
    if len(data) < _RECORD:
        return None
    if data[:8] == _HEADER or len(data) % _RECORD == 8:
        body = data[8:]
    elif len(data) % _RECORD == 0:
        body = data
    else:
        return None
    if len(body) < _RECORD:
        return None
    return body[: len(body) // _RECORD * _RECORD]


def _unwrap(rows: list[tuple]) -> list[tuple]:
    """环形缓冲写满后，最旧的一根可能不在文件开头。"""
    if len(rows) < 2:
        return rows
    split = 0
    for index in range(1, len(rows)):
        if rows[index][0] < rows[index - 1][0]:
            split = index
            break
    if split == 0:
        return rows
    return rows[split:] + rows[:split]


def _share_scale(frame: pd.DataFrame) -> int:
    usable = frame[(frame["volume"] > 0) & (frame["close"] > 0) & (frame["money"] > 0)]
    if usable.empty:
        return 1
    ratio = usable["money"] / (usable["close"] * usable["volume"])
    median = float(ratio.median())
    if 50 <= median <= 150:
        return 100
    return 1


def _limit_ratio(symbol: str) -> float | None:
    code = symbol.split(".")[0]
    suffix = symbol.rsplit(".", 1)[-1]
    if suffix == "BJ":
        return 0.30
    if code.startswith(("688", "689")) and suffix == "SS":
        return 0.20
    if code.startswith("30") and suffix == "SZ":
        return 0.20
    if code.startswith(("60", "00")):
        return 0.10
    return None
