# -*- coding: utf-8 -*-
"""QMT K 线解码。用内存里的记录，不读安装目录。"""

import struct
from datetime import datetime, timedelta, timezone

import pandas as pd

from simtradedata.importers.qmt_kline import decode_kline_dat, to_daily_frame

_TZ = timezone(timedelta(hours=8))


def _record(when: datetime, price_milli: int, volume: int, amount: int) -> bytes:
    raw = struct.pack(
        "<9I",
        int(when.replace(tzinfo=_TZ).timestamp()),
        price_milli,
        price_milli,
        price_milli,
        price_milli,
        price_milli,
        volume,
        540,
        amount,
    )
    return raw.ljust(64, b"\x00")


def _write(path, records: list[bytes]) -> None:
    path.write_bytes(bytes.fromhex("feffffffffffff7f") + b"".join(records))


def test_price_is_milli_yuan_and_time_is_kept(tmp_path):
    path = tmp_path / "600519.DAT"
    _write(path, [
        _record(datetime(2026, 11, 2, 15, 0), 1_392_050, 100, 139205),
    ])
    frame = decode_kline_dat(path)
    assert frame.loc[0, "close"] == 1392.05
    assert frame.loc[0, "volume"] == 100
    assert frame.loc[0, "datetime"] == datetime(2026, 11, 2, 15, 0)


def test_hand_volume_becomes_shares(tmp_path):
    path = tmp_path / "600000.DAT"
    # 成交额 = 价格 × 手数 × 100
    _write(path, [
        _record(datetime(2026, 11, 2, 15, 0), 10_000, 20, 20_000),
    ])
    frame = decode_kline_dat(path)
    assert frame.loc[0, "volume"] == 2000


def test_ring_buffer_starts_at_the_oldest_bar(tmp_path):
    path = tmp_path / "000001.DAT"
    _write(path, [
        _record(datetime(2026, 11, 2, 15, 0), 10_000, 1, 10),
        _record(datetime(2026, 11, 2, 9, 31), 10_000, 1, 10),
        _record(datetime(2026, 11, 2, 9, 32), 10_000, 1, 10),
    ])
    frame = decode_kline_dat(path)
    assert list(frame["datetime"].dt.strftime("%H:%M")) == ["09:31", "09:32", "15:00"]


def test_daily_limit_uses_previous_close():
    bars = pd.DataFrame({
        "datetime": [datetime(2026, 11, 2, 15, 0), datetime(2026, 11, 3, 15, 0)],
        "open": [10.0, 10.0],
        "high": [10.0, 11.0],
        "low": [10.0, 10.0],
        "close": [10.0, 11.0],
        "volume": [100, 100],
        "money": [1000.0, 1100.0],
    })
    daily = to_daily_frame(bars, "600000.SS")
    assert daily.loc[1, "preclose"] == 10.0
    assert daily.loc[1, "high_limit"] == 11.0
    assert daily.loc[1, "low_limit"] == 9.0


def test_chinext_limit_is_twenty_percent():
    bars = pd.DataFrame({
        "datetime": [datetime(2026, 11, 2, 15, 0), datetime(2026, 11, 3, 15, 0)],
        "open": [10.0, 10.0],
        "high": [10.0, 10.0],
        "low": [10.0, 10.0],
        "close": [10.0, 10.0],
        "volume": [1, 1],
        "money": [10.0, 10.0],
    })
    daily = to_daily_frame(bars, "300001.SZ")
    assert daily.loc[1, "high_limit"] == 12.0
