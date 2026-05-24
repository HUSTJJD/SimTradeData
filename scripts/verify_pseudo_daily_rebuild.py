# -*- coding: utf-8 -*-
"""验证非 A 股伪日线重建结果：只用 PTrade 原生接口 + 文件系统只读检查。

不 import QMT SDK / xtdata，不改引擎代码。检查：
  1) 备份目录存在且文件数与变更清单一致；
  2) stocks/ 文件总数不变，被重建的标的日期范围明显变长；
  3) storage.load_stock 能读、列 dtype 全是数值、无重复/未来日期、OHLC 自洽；
  4) PTrade 原生 get_history(1d) 能取到最新交易日。

用法：
    python scripts/verify_pseudo_daily_rebuild.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import types
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
sys.path.insert(0, str(ROOT / "SimTradeLab" / "src"))
DATA_DIR = ROOT / "SimTradeData" / "data" / "cn"
SCRIPTS = ROOT / "SimTradeData" / "scripts"
os.environ["SIMTRADELAB_DATA_PATH"] = str(DATA_DIR)

from simtradelab.ptrade import storage  # noqa: E402
from simtradelab.ptrade.api import PtradeAPI  # noqa: E402
from simtradelab.ptrade.data_context import DataContext  # noqa: E402

NUMERIC_COLS = ["open", "close", "high", "low", "volume", "money",
                "preclose", "high_limit", "low_limit"]


def main() -> int:
    ok = True
    apply_report = SCRIPTS / "qmt_pseudo_rebuild_apply.json"
    if not apply_report.is_file():
        print(f"未找到 {apply_report.name}，请先执行 --apply")
        return 1
    report = json.loads(apply_report.read_text(encoding="utf-8"))
    rebuilt = {c["symbol"]: c for c in report["changes"] if c["status"] == "rebuilt"}
    print(f"== 0) 备份与变更清单 ==")
    print(f"  mode={report['mode']} universe={report['universe']} "
          f"rebuilt={len(rebuilt):,} 补日期总数={report['total_dates_added']:,}")
    backup = Path(report["backup_dir"]) if report.get("backup_dir") else None
    n_backup = len(list(backup.glob("*.parquet"))) if backup and backup.is_dir() else 0
    print(f"  备份目录：{backup}")
    print(f"  备份文件数：{n_backup:,}")
    if n_backup != len(rebuilt):
        print("  ^ 备份文件数与重建数不一致  [FAIL]")
        ok = False

    stocks_dir = DATA_DIR / "stocks"
    print("\n== 1) stocks/ 文件总数 ==")
    n_files = sum(1 for _ in stocks_dir.glob("*.parquet"))
    print(f"  当前 {n_files:,}（重建只覆盖已有文件，不应新建/删除标的）")
    if n_files != 23698:
        print("  ^ 与重建前 23698 不一致  [FAIL]")
        ok = False

    manifest = json.loads((SCRIPTS / "qmt_pseudo_daily_manifest.json").read_text(encoding="utf-8"))
    by_type: dict[str, str] = {}
    for row in manifest["symbols"]:
        by_type.setdefault(str(row["security_type"]), row["symbol"])
    samples = [s for s in by_type.values() if s in rebuilt]
    samples += ["000002.SS", "000009.SS"]
    samples = list(dict.fromkeys(samples))[:8]

    print("\n== 2) storage.load_stock 抽样 ==")
    frames = {}
    for symbol in samples:
        df = storage.load_stock(DATA_DIR, symbol)
        frames[symbol] = df
        if df.empty:
            print(f"  {symbol}: 空  [FAIL]")
            ok = False
            continue
        before = rebuilt[symbol].get("local_rows")
        after = len(df)
        dup = int(df.index.duplicated().sum())
        future = int((df.index > pd.Timestamp("2026-11-05")).sum())
        bad_ohlc = int(((df["high"] < df["low"]) |
                        (df["close"] > df["high"] + 1e-6) |
                        (df["close"] < df["low"] - 1e-6)).sum())
        non_numeric = [c for c in NUMERIC_COLS
                       if c in df.columns and not pd.api.types.is_numeric_dtype(df[c])]
        print(f"  {symbol}: {before} -> {after:,} 行  "
              f"{df.index.min().date()} ~ {df.index.max().date()}  "
              f"重复{dup} 未来{future} OHLC异常{bad_ohlc} 非数值列{non_numeric}")
        if dup or future or bad_ohlc or non_numeric:
            ok = False
        if after <= (before or 0):
            print("     ^ 行数未增加，重建可能未生效  [FAIL]")
            ok = False

    print("\n== 3) PTrade 原生 get_history(1d, include=True) ==")

    class _G:
        pass

    ctx = types.SimpleNamespace(
        current_dt=pd.Timestamp("2026-11-04 15:00:00"),
        frequency="1d", g=_G(), broker_profile="auto",
    )
    dc = DataContext(
        stock_data_dict=frames, valuation_dict={}, fundamentals_dict={},
        exrights_dict={}, benchmark_data={}, stock_metadata=pd.DataFrame(),
        index_constituents={}, stock_status_history={},
        adj_pre_cache=None, adj_post_cache=None, trade_days=None,
    )
    api = PtradeAPI(dc, ctx, logging.getLogger("verify_pseudo"))
    api.active_universe = set(frames)
    try:
        res = api.get_history(3, frequency="1d", field=["close"],
                              security_list=list(frames), fq=None, include=True)
        panel = res["close"] if isinstance(res, dict) else res
        latest = pd.Timestamp(panel.index.max()).date() if hasattr(panel, "index") and len(panel.index) else None
        print(f"  返回形状 {getattr(panel, 'shape', None)}，最新日期 {latest}，"
              f"非空列 {int(panel.notna().any().sum()) if hasattr(panel, 'notna') else '?'}")
        if str(latest) < "2026-11-04":
            print("  ^ 未取到 2026-11-04  [FAIL]")
            ok = False
    except Exception as exc:  # noqa: BLE001
        print(f"  调用失败 {type(exc).__name__}: {exc}")
        ok = False

    print("\n结论：", "非 A 股伪日线重建结果可被 PTrade 原生回测接口正常读取" if ok
          else "存在未达标项，请检查")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
