# -*- coding: utf-8 -*-
"""用 PTrade 原生回测接口验证 QMT 5m -> stocks_1m 的落地结果。

只 import SimTradeLab（PTrade 回测接口的本地实现），不 import QMT SDK / xtdata，
不改引擎代码：数据仍由引擎自己的 storage / get_history 读取。

预期（已由 audit_three_questions.py 实测）：
    5m / 15m / 30m / 60m  -> 完整无 NaN，数值与直接聚合一致
    1m                    -> 按标准 1m 时间轴 reindex 后出现空洞（QMT 1m 未下载）

用法：
    python scripts/verify_qmt_5m_import.py
"""

from __future__ import annotations

import logging
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

SYMBOLS = ["600000.SS", "000001.SZ", "300750.SZ", "600519.SS", "601318.SS"]


def main() -> int:
    ok = True
    one_m_dir = DATA_DIR / "stocks_1m"
    files = list(one_m_dir.glob("*.parquet")) if one_m_dir.is_dir() else []
    size_gb = sum(p.stat().st_size for p in files) / 1024 ** 3 if files else 0
    print(f"引擎分钟目录：{one_m_dir}")
    print(f"  文件数 {len(files):,}，占用 {size_gb:.2f} GB\n")

    print("== 1) 引擎数据层 storage.load_stock_1m ==")
    frames = {}
    for symbol in SYMBOLS:
        df = storage.load_stock_1m(DATA_DIR, symbol)
        frames[symbol] = df
        if df.empty:
            print(f"  {symbol}: 读取为空  [FAIL]")
            ok = False
            continue
        last = df.index.max()
        gap = (df.index[-1] - df.index[-2]).total_seconds() / 60 if len(df) > 1 else None
        print(f"  {symbol}: {len(df):,} 行  {df.index.min()} ~ {last}  末两根间隔 {gap} 分钟")
        if gap != 5:
            print("     ^ 非 5 分钟间隔，请检查 DAT 解码")
            ok = False

    print("\n== 2) PTrade 原生接口 get_history（分钟频率） ==")
    ref = frames.get("600000.SS")
    if ref is None or ref.empty:
        print("  缺少 600000.SS，无法继续")
        return 1
    last_bar = ref.index.max()

    class _G:
        pass

    ctx = types.SimpleNamespace(
        current_dt=last_bar, frequency="1m", g=_G(), broker_profile="auto",
    )
    dc = DataContext(
        stock_data_dict={s: f.iloc[0:0].copy() for s, f in frames.items()},
        valuation_dict={}, fundamentals_dict={}, exrights_dict={},
        benchmark_data={}, stock_metadata=pd.DataFrame(),
        index_constituents={}, stock_status_history={},
        adj_pre_cache=None, adj_post_cache=None, trade_days=None,
        stock_data_dict_1m=frames,
    )
    api = PtradeAPI(dc, ctx, logging.getLogger("verify_5m"))
    api.active_universe = set(SYMBOLS)

    results = {}
    for freq in ("1m", "5m", "15m", "30m", "60m"):
        try:
            res = api.get_history(20, frequency=freq, field=["close"],
                                  security_list=SYMBOLS, fq=None, include=True)
            panel = res["close"] if isinstance(res, dict) else res
            if not isinstance(panel, pd.DataFrame):
                raise TypeError(f"返回类型异常 {type(panel).__name__}")
            vals = panel["600000.SS"] if "600000.SS" in panel.columns else panel.iloc[:, 0]
            non_nan = int(vals.notna().sum())
            last_valid = float(vals.dropna().iloc[-1]) if non_nan else None
            results[freq] = (len(vals), non_nan, last_valid)
            print(f"  {freq:>3}: 返回 {len(vals):>2} 根，非 NaN {non_nan:>2} 根，"
                  f"末值 {last_valid}")
            if freq != "1m" and non_nan != len(vals):
                print("     ^ 应为完整无 NaN  [FAIL]")
                ok = False
            if freq == "1m" and non_nan == len(vals):
                print("     ^ 预期 1m 有空洞（5m bar 落在 5 分钟刻度），实际却完整，请复核")
        except Exception as exc:  # noqa: BLE001
            print(f"  {freq:>3}: 调用失败 {type(exc).__name__}: {exc}")
            ok = False

    closes = {v[2] for k, v in results.items() if k != "1m" and v[2] is not None}
    print(f"\n  5m/15m/30m/60m 末值集合：{closes}")
    if len(closes) > 1:
        print("  ^ 各周期末值应一致  [FAIL]")
        ok = False

    print("\n结论：", "QMT 5m 已可被 PTrade 原生回测接口正常读取（1m 空洞属已知取舍）"
          if ok else "存在未达标项，请检查")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
