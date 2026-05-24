# -*- coding: utf-8 -*-
"""修正 stock_metadata.parquet 的两处污染（只读引擎数据目录，不改引擎代码）。

问题 1：149 只沪市指数被误标成 security_type='stock'
    典型：000300.SS 沪深300（还是回测默认基准）、000852.SS 中证1000、
          000009.SS 上证380、000922.SS 中证红利。
    沪市 000xxx.SS 是指数代码段，A 股代码段是 600/601/603/605/688/689。
    修正：security_type -> 'index'。

问题 2：listed_date 被填成「数据起点」而非上市日
    伪日线群里 14320 只的 listed_date 是 2025-10-31，其余也是各自的数据首日。
    引擎 api.py 用 listed_date <= 回测日 过滤 universe；刚用 QMT 官方日线把
    历史补到 2000 年，但 listed_date 仍是 2025-10-31 会把这些标的挡在历史回测外，
    连默认基准 000300.SS 都会漏掉。
    修正：对伪日线清单中的非 A 股，改为该标的 stocks/<symbol>.parquet 的
          实际首个交易日（语义从「上市日」变为「本地数据起始日」，最诚实可用值）。
    不动：A 股（含 2025-10 后上市的正常新股，它们的 listed_date 是真实的）、
          伪日线清单之外的标的。

安全：
  * 先把原 parquet 复制到独立备份目录，确认成功后才写。
  * 临时文件 + 原子替换；行数 / 列顺序 / 各列 dtype 保持不变。
  * 默认 dry-run 只出清单，--apply 才落盘；脚本可重入。

用法：
    python scripts/fix_stock_metadata.py                  # dry-run 清单
    python scripts/fix_stock_metadata.py --apply          # 备份 + 写盘
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data\cn")
SCRIPTS = Path(__file__).resolve().parent
MANIFEST = SCRIPTS / "qmt_pseudo_daily_manifest.json"
COLS = ["symbol", "stock_name", "listed_date", "de_listed_date", "blocks", "security_type"]
INDEX_CODE = r"^000\d{3}\.SS$"


def is_a_share(symbol: str) -> bool:
    code, market = symbol.split(".")[0], symbol.rsplit(".", 1)[-1]
    if market == "SS":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    return False


def first_dates(stocks_dir: Path, symbols: set[str]) -> dict[str, str]:
    """用 parquet footer statistics 批量取每只标的的首个交易日，不读数据行。"""
    out = {}
    for symbol in sorted(symbols):
        path = stocks_dir / f"{symbol}.parquet"
        if not path.is_file():
            continue
        try:
            meta = pq.ParquetFile(path).metadata
            lo = None
            for rg in range(meta.num_row_groups):
                st = meta.row_group(rg).column(0).statistics
                if st is None or not st.has_min_max:
                    continue
                lo = st.min if lo is None else min(lo, st.min)
            if lo is not None:
                out[symbol] = pd.Timestamp(lo).strftime("%Y-%m-%d 00:00:00")
        except Exception:
            continue
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="修正 stock_metadata 品种误标与 listed_date 污染")
    ap.add_argument("--data-dir", type=Path, default=DATA)
    ap.add_argument("--manifest", type=Path, default=MANIFEST)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--report", type=Path, default=SCRIPTS / "qmt_metadata_fix_report.json")
    args = ap.parse_args()

    meta_path = args.data_dir / "metadata" / "stock_metadata.parquet"
    df = pd.read_parquet(meta_path)
    if list(df.columns) != COLS:
        print(f"列不符合预期：{list(df.columns)}")
        return 1

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    targets = {r["symbol"] for r in manifest["symbols"] if not is_a_share(r["symbol"])}
    print(f"原表 {len(df)} 行；伪日线清单中非 A 股 {len(targets)} 只")

    # 1) 品种误标
    relabel_mask = (df["security_type"] == "stock") & df["symbol"].str.match(INDEX_CODE, na=False)
    relabeled = df.loc[relabel_mask, "symbol"].tolist()
    extra_in_targets = [s for s in relabeled if s not in targets]
    print(f"误标为 stock 的沪市指数 {len(relabeled)} 只（其中不在伪日线清单 {len(extra_in_targets)} 只）")

    # 2) listed_date：改为真实行情首日
    first = first_dates(args.data_dir / "stocks", targets)
    print(f"取到行情首日 {len(first)} 只")

    work = df.copy()
    date_changes = []
    for idx, symbol in work["symbol"].items():
        if symbol not in targets or symbol not in first:
            continue
        old = str(work.at[idx, "listed_date"])
        new = first[symbol]
        if old == new:
            continue
        # 只改明显伪造的数据起点，真实上市日一律不动
        if old and old < "2025-10-01":
            continue
        work.at[idx, "listed_date"] = new
        date_changes.append({"symbol": symbol, "old": old, "new": new})

    work.loc[relabel_mask, "security_type"] = "index"

    report = {
        "mode": "apply" if args.apply else "dry-run",
        "generated_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "target": str(meta_path),
        "rows": len(work),
        "relabeled_to_index": relabeled,
        "relabel_count": len(relabeled),
        "listed_date_changed": len(date_changes),
        "listed_date_samples": date_changes[:10],
        "listed_date_changes": date_changes,
        "untouched": {
            "a_share": "listed_date 视为真实上市日，未改动",
            "outside_manifest": "伪日线清单之外的标的未改动",
            "bond_index": "000022.SS / 000832.SS 等债券指数保持 bond",
        },
        "semantics_note": ("listed_date 对这批标的的语义由『上市日』变为『本地数据起始日』，"
                           "以匹配 api.py 的 listed_date <= 回测日 过滤逻辑"),
        "backup_dir": None,
    }

    if args.apply:
        backup = args.data_dir / f"_backup_metadata_{report['generated_at']}"
        backup.mkdir(parents=True, exist_ok=True)
        dst = backup / meta_path.name
        shutil.copy2(meta_path, dst)
        if not dst.is_file() or dst.stat().st_size != meta_path.stat().st_size:
            print("备份失败，终止写盘")
            return 1
        report["backup_dir"] = str(backup)
        tmp = meta_path.with_suffix(".parquet.tmp")
        work.to_parquet(tmp, index=False)
        tmp.replace(meta_path)
        print(f"已写盘，备份：{backup}")
    else:
        print("dry-run，未写盘。确认清单后加 --apply 执行（自动备份）。")

    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                           encoding="utf-8")
    print(f"\n统计：改 security_type {len(relabeled)} 只，改 listed_date {len(date_changes)} 只")
    print(f"清单：{args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
