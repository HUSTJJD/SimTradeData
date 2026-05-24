# -*- coding: utf-8 -*-
"""验证两处收尾修正：回测 data_path 配置 + stock_metadata 品种/上市日。

只读验证，不改任何文件：
  1) 4 个策略 yaml 的 data_path 能否解析到真实数据目录（用引擎 market_profile
     的 data_dir_name 常量复现 DataServer.__init__ 的解析分支）；
  2) 与备份逐行对比 stock_metadata.parquet，确认只有 security_type / listed_date
     两列发生预期的变化，其余列与 symbol 顺序完全不变；
  3) 引擎 storage.load_metadata 能读回，DataContext 的 listed_date_ts 可用；
  4) 默认基准 000300.SS 在历史回测日不再被 listed_date 过滤掉。

用法：
    python scripts/verify_config_and_metadata.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
sys.path.insert(0, str(ROOT / "SimTradeLab" / "src"))
DATA_CN = ROOT / "SimTradeData" / "data" / "cn"
os.environ["SIMTRADELAB_DATA_PATH"] = str(DATA_CN)
os.chdir(ROOT)  # run_backtest.py 用相对路径，cwd 假设是 PTrade/

from simtradelab.ptrade import storage  # noqa: E402
from simtradelab.ptrade.data_context import DataContext  # noqa: E402
from simtradelab.ptrade.market_profile import get_market_profile  # noqa: E402

CONFIGS = [
    "backtest_config.cn.yaml", "backtest_config.growth_div.yaml",
    "backtest_config.rl.yaml", "backtest_config.trend.yaml",
]
COLS = ["symbol", "stock_name", "listed_date", "de_listed_date", "blocks", "security_type"]


def main() -> int:
    ok = True
    print("== 1) 策略配置 data_path 解析 ==")
    for name in CONFIGS:
        path = ROOT / "strategies" / name
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        raw = cfg["data_path"]
        base = Path(raw).expanduser().resolve()
        profile = get_market_profile(cfg.get("market", "CN"))
        if base.name == profile.data_dir_name:
            final = base
        else:
            candidate = base / profile.data_dir_name
            final = candidate if candidate.exists() else base
        need = [final / d for d in ("stocks", "stocks_1m", "metadata")]
        missing = [str(p.name) for p in need if not p.is_dir()]
        same = final == DATA_CN.resolve()
        print(f"  {name}: {raw} -> {final.name}  子目录缺失={missing} 指向正确={same}")
        if missing or not same:
            ok = False

    print("\n== 2) metadata 与备份逐行对比 ==")
    report = json.loads((Path(__file__).resolve().parent /
                         "qmt_metadata_fix_report.json").read_text(encoding="utf-8"))
    backup = Path(report["backup_dir"]) / "stock_metadata.parquet"
    cur_path = DATA_CN / "metadata" / "stock_metadata.parquet"
    if not backup.is_file():
        print(f"  备份不存在：{backup}  [FAIL]")
        return 1
    old = pd.read_parquet(backup)
    new = pd.read_parquet(cur_path)
    print(f"  备份 {old.shape} / 当前 {new.shape} / 列顺序一致={list(new.columns) == COLS}")
    if old.shape != new.shape or list(new.columns) != COLS:
        ok = False
    if not new["symbol"].equals(old["symbol"]):
        print("  symbol 列顺序发生变化  [FAIL]")
        ok = False

    for col in COLS:
        if col in ("security_type", "listed_date"):
            continue
        n = int((old[col].astype(str) != new[col].astype(str)).sum())
        print(f"  {col}: 变化 {n} 行")
        if n:
            ok = False

    # 用 astype(str) 比较：原表里 18 行 listed_date 是 None，
    # pandas 的 None != None 会被算成差异（NaN 语义），属于假差异。
    sec_diff = old["security_type"].astype(str) != new["security_type"].astype(str)
    date_diff = old["listed_date"].astype(str) != new["listed_date"].astype(str)
    nat_old = int(pd.to_datetime(old["listed_date"], errors="coerce").isna().sum())
    nat_new = int(pd.to_datetime(new["listed_date"], errors="coerce").isna().sum())
    print(f"  原有不可解析 listed_date={nat_old}，修正后={nat_new}（应相同，未新增空值）")
    if nat_old != nat_new:
        ok = False
    print(f"  security_type: 变化 {int(sec_diff.sum())} 行（预期 {report['relabel_count']}）")
    print(f"  listed_date  : 变化 {int(date_diff.sum())} 行（预期 {report['listed_date_changed']}）")
    if int(sec_diff.sum()) != report["relabel_count"]:
        ok = False
    if int(date_diff.sum()) != report["listed_date_changed"]:
        ok = False
    bad_sec = set(new.loc[sec_diff & (new["security_type"] != "index"), "symbol"])
    if bad_sec:
        print(f"  存在未改成 index 的品种变化：{list(bad_sec)[:5]}  [FAIL]")
        ok = False

    print("\n== 3) 关键标的抽样 ==")
    idx = new.set_index("symbol")
    for sym in ("000300.SS", "000852.SS", "000922.SS", "000009.SS", "600000.SS", "000001.SZ"):
        if sym not in idx.index:
            print(f"  {sym}: 缺失  [FAIL]")
            ok = False
            continue
        row = idx.loc[sym]
        print(f"  {sym}: {row['stock_name']}  type={row['security_type']}  listed={row['listed_date']}")

    print("\n== 4) 引擎读取与历史回测可见性 ==")
    loaded = storage.load_metadata(DATA_CN, "stock_metadata")
    # 按 storage._load_metadata_parquet 的约定，stock_metadata 返回 {'data': records}
    n_loaded = len(loaded["data"]) if isinstance(loaded, dict) and "data" in loaded else -1
    print(f"  storage.load_metadata 返回 {type(loaded).__name__}，records 数 {n_loaded}")
    if n_loaded != len(new):
        ok = False

    ctx = DataContext(
        stock_data_dict={}, valuation_dict={}, fundamentals_dict={}, exrights_dict={},
        benchmark_data={}, stock_metadata=new, index_constituents={},
        stock_status_history={}, adj_pre_cache=None, adj_post_cache=None, trade_days=None,
    )
    ts = ctx.listed_date_ts
    print(f"  DataContext.listed_date_ts 可用={ts is not None}，NaT 数={int(ts.isna().sum())}")
    if ts is None:
        ok = False
    target = pd.Timestamp("2020-01-02")
    visible = new.loc[(ts <= target) & (new["de_listed_date"].astype(str) == "2900-01-01"), "symbol"]
    for sym in ("000300.SS", "000009.SS"):
        hit = sym in set(visible)
        print(f"  {sym} 在 {target.date()} 可见={hit}")
        if not hit:
            ok = False

    print("\n结论：", "配置与 metadata 修正均通过验证" if ok else "存在未达标项，请检查")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
