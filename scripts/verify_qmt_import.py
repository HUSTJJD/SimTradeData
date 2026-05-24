# -*- coding: utf-8 -*-
"""用 PTrade 原生回测接口验证 QMT 日线导入结果。

只 import SimTradeLab（PTrade 回测接口的本地实现），不 import 任何 QMT SDK / xtdata，
也不改动引擎代码：数据仍由引擎自己的 storage / get_history 读取。

用法：
    python scripts/verify_qmt_import.py
"""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
sys.path.insert(0, str(ROOT / "SimTradeLab" / "src"))
DATA_DIR = ROOT / "SimTradeData" / "data" / "cn"
os.environ["SIMTRADELAB_DATA_PATH"] = str(DATA_DIR)

from simtradelab.ptrade import storage  # noqa: E402
from simtradelab.ptrade.api import PtradeAPI  # noqa: E402
from simtradelab.ptrade.data_context import DataContext  # noqa: E402

SYMBOLS = ["600000.SS", "000001.SZ", "300750.SZ", "600519.SS", "000300.SS"]
EXPECTED_LAST = pd.Timestamp("2026-11-04")


def main() -> int:
    print(f"引擎数据目录：{DATA_DIR}\n")
    print("== 1) 引擎数据层 storage.load_stock ==")
    frames = {}
    ok = True
    for symbol in SYMBOLS:
        df = storage.load_stock(DATA_DIR, symbol)
        frames[symbol] = df
        if df.empty:
            print(f"  {symbol}: 读取为空")
            ok = False
            continue
        last = df.index.max()
        flag = "OK" if last >= EXPECTED_LAST else "落后"
        print(f"  {symbol}: {len(df)} 行  {df.index.min().date()} ~ {last.date()}  "
              f"末日收盘 {df['close'].iloc[-1]}  [{flag}]")
        if last < EXPECTED_LAST:
            ok = False

    # 构造最小上下文，调用 PTrade 原生 get_history
    class _G:
        pass

    ctx = types.SimpleNamespace(
        current_dt=pd.Timestamp("2026-11-04 15:00:00"),
        frequency="1d",
        g=_G(),
        broker_profile="auto",
    )
    dc = DataContext(
        stock_data_dict=frames,
        valuation_dict={},
        fundamentals_dict={},
        exrights_dict={},
        benchmark_data={},
        stock_metadata=pd.DataFrame(),
        index_constituents={},
        stock_status_history={},
        adj_pre_cache=None,
        adj_post_cache=None,
        trade_days=None,
    )
    api = PtradeAPI(dc, ctx, None)
    api.active_universe = set(SYMBOLS)

    print("\n== 2) PTrade 原生接口 get_history(count=5, frequency='1d') ==")
    try:
        result = api.get_history(
            5, frequency="1d",
            field=["open", "high", "low", "close", "volume"],
            security_list=SYMBOLS, fq=None,
        )
        print(result)
        print("\n返回类型：", type(result).__name__)
    except Exception as exc:
        print(f"  get_history 调用失败：{type(exc).__name__}: {exc}")
        ok = False

    # include=False 的语义是不含当前日，把回测日推进一天即可取到 11-04
    print("\n== 3) include=True 确认 11-04 行情可取 ==")
    ctx.current_dt = pd.Timestamp("2026-11-04 15:00:00")
    api2 = PtradeAPI(dc, ctx, None)
    api2.active_universe = set(SYMBOLS)
    try:
        res2 = api2.get_history(3, frequency="1d", field=["close"], security_list=SYMBOLS,
                                fq=None, include=True)
        close_panel = res2["close"] if isinstance(res2, dict) else res2
        print(close_panel)
        if hasattr(close_panel, "index") and len(close_panel.index):
            latest = pd.Timestamp(close_panel.index.max()).date()
            print("  get_history 可见最新日期：", latest)
            if str(latest) < "2026-11-04":
                ok = False
    except Exception as exc:
        print(f"  调用失败：{type(exc).__name__}: {exc}")
        ok = False

    print("\n结论：", "导入后的数据可被 PTrade 原生回测接口正常读取" if ok else "存在未达标项，请检查")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
