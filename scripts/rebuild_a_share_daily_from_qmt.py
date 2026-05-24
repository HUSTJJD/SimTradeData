# -*- coding: utf-8 -*-
"""用 QMT 官方 86400 日线修复日线中的 5m 合成伪日线段 / 历史缺口。

严格边界：
  * SimTradeLab 只是 PTrade 原生回测接口的本地实现。本脚本只改引擎原生读取的
    ``data/cn/stocks/<symbol>.parquet``（1d），不改引擎代码、不加接口、不接 QMT SDK。
  * --universe 控制范围：
      a-share  按代码前缀识别的 A 股（默认，历史核查用）
      pseudo   以 qmt_pseudo_daily_manifest.json 为准，重建「有 QMT 官方日线、
               且非 A 股」的伪日线标的（含被误标为 stock 的上证指数）
      all      QMT 86400 中存在且 PTrade 已有 parquet 的全部标的

为什么不是「全量覆盖」：
  QMT 86400 对部分老股的历史反而比 PTrade 短（例：600000.SS 本地自 1999-11-10，
  QMT 自 2000-01-04）。直接覆盖会丢历史。因此采用 **并集合并**：
      - 重叠日期：OHLCV 以 QMT 官方日线为准；preclose / high_limit / low_limit
        保留本地原值（本地复权/涨跌停口径更贴近引擎现有数据）
      - QMT 独有日期：补齐（这几列按本地既有规则推算，属于近似值，清单里会标注）
      - 本地独有日期：完整保留，一行不删
  只有确实能补齐日期、或重叠段收盘价口径不一致的标的才会重建。

安全：
  * 默认 dry-run，只出变更清单，不写盘。
  * ``--apply`` 时先把所有待改文件复制到独立备份目录，确认备份成功后才写。
  * 写入用临时文件 + 原子替换；脚本可重入。

用法：
    # 非 A 股伪日线重建：先出清单，再写盘（自动备份）
    python scripts/rebuild_a_share_daily_from_qmt.py --universe pseudo --limit 50
    python scripts/rebuild_a_share_daily_from_qmt.py --universe pseudo --workers 8 \
        --changes scripts/qmt_pseudo_rebuild_dryrun.json
    python scripts/rebuild_a_share_daily_from_qmt.py --universe pseudo --apply --workers 8 \
        --changes scripts/qmt_pseudo_rebuild_apply.json

    # A 股全量交叉核验（历史用法，默认 universe）
    python scripts/rebuild_a_share_daily_from_qmt.py --workers 8
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from simtradedata.importers.qmt_kline import decode_kline_dat, to_daily_frame  # noqa: E402

DEFAULT_ROOT = Path(r"D:\君弘君智交易系统")
DEFAULT_DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data\cn")
OUT_DIR = Path(__file__).resolve().parent
STOCK_COLS = ["date", "open", "close", "high", "low", "high_limit", "low_limit",
              "preclose", "volume", "money"]
CLOSE_TOL = 0.005          # 单日收盘价相对差超过此值算一根「差异」
MIN_OVERLAP = 20           # 重叠天数太少不做统计判定
MAX_CONFLICT_RATE = 0.001  # 孤立差异容忍：最多 2 根或 0.1%


def is_a_share(symbol: str) -> bool:
    code, market = symbol.split(".")[0], symbol.rsplit(".", 1)[-1]
    if market == "SS":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    return False


def build_tasks(root: Path, data_dir: Path, universe: str,
                manifest: Path | None = None) -> list[tuple[str, Path, Path]]:
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    targets: set[str] | None = None
    if universe == "pseudo":
        if manifest is None or not manifest.is_file():
            raise FileNotFoundError(f"pseudo 模式需要伪日线清单：{manifest}")
        rows = json.loads(manifest.read_text(encoding="utf-8"))["symbols"]
        targets = {r["symbol"] for r in rows
                   if r.get("has_qmt_official_daily") and not is_a_share(r["symbol"])}
    out = []
    for market, suffix in (("SH", "SS"), ("SZ", "SZ")):
        folder = datadir / market / "86400"
        if not folder.is_dir():
            continue
        for dat in sorted(folder.glob("*.DAT")):
            symbol = f"{dat.stem[-6:]}.{suffix}"
            if universe == "pseudo":
                if symbol not in targets:
                    continue
            elif universe == "a-share" and not is_a_share(symbol):
                continue
            local = data_dir / "stocks" / f"{symbol}.parquet"
            if not local.exists():
                continue
            out.append((symbol, dat, local))
    return out


def process_one(task: tuple[str, Path, Path], apply: bool, backup_dir: Path | None) -> dict:
    symbol, dat, local = task
    info: dict = {"symbol": symbol}
    bars = decode_kline_dat(dat)
    if bars.empty:
        info["status"] = "empty_dat"
        return info
    qmt = to_daily_frame(bars, symbol)
    qmt["date"] = pd.to_datetime(qmt["date"])
    qmt = qmt.drop_duplicates("date", keep="last").set_index("date").sort_index()

    try:
        old = pd.read_parquet(local)
    except Exception as exc:  # noqa: BLE001
        info["status"] = f"read_error:{type(exc).__name__}"
        return info
    if "date" not in old.columns:
        info["status"] = "bad_schema"
        return info
    old["date"] = pd.to_datetime(old["date"])
    old = old.drop_duplicates("date", keep="last").set_index("date").sort_index()

    added = qmt.index.difference(old.index)
    kept_local = old.index.difference(qmt.index)
    overlap = qmt.index.intersection(old.index)
    close_diff = None
    bad_days = 0
    if len(overlap):
        a = pd.to_numeric(qmt.loc[overlap, "close"], errors="coerce")
        b = pd.to_numeric(old.loc[overlap, "close"], errors="coerce")
        rel = ((a - b).abs() / b.abs().replace(0, pd.NA)).dropna()
        if len(rel):
            close_diff = round(float(rel.max()), 6)
            bad_days = int((rel > CLOSE_TOL).sum())

    info.update({
        "qmt_rows": len(qmt),
        "local_rows": len(old),
        "qmt_first": str(qmt.index.min().date()),
        "qmt_last": str(qmt.index.max().date()),
        "local_first": str(old.index.min().date()),
        "local_last": str(old.index.max().date()),
        "dates_added_from_qmt": len(added),
        "dates_kept_local_only": len(kept_local),
        "overlap_days": len(overlap),
        "overlap_max_close_rel_diff": close_diff,
        "overlap_bad_days": bad_days,
    })
    info["overlap_agree_rate"] = (round(1 - bad_days / len(overlap), 6) if len(overlap) else None)

    allowed_bad = max(2, int(len(overlap) * MAX_CONFLICT_RATE))
    conflict = len(overlap) >= MIN_OVERLAP and bad_days > allowed_bad
    info["overlap_conflict"] = conflict
    if conflict:
        # 口径不一致时整只跳过，交人工判断，不擅自覆盖
        info["status"] = "overlap_conflict_skipped"
        return info
    if not len(added):
        info["status"] = "no_new_date"
        return info

    # 并集合并：QMT 骨架 + 本地独有日期；重叠日期保留本地的复权/涨跌停列
    merged = pd.concat([qmt, old.loc[kept_local]], axis=0).sort_index()
    for col in ("preclose", "high_limit", "low_limit"):
        if col not in old.columns:
            continue
        overlay = old[col].reindex(overlap)
        target = merged.loc[overlap, col] if col in merged.columns else pd.Series(index=overlap, dtype=float)
        merged.loc[overlap, col] = overlay.combine_first(target)
    # index 名就是 date，必须先 reset 再按标准列重排；
    # 否则 reindex(columns=STOCK_COLS) 会造出一个全 NaN 的 date 列，reset_index 再撞名。
    merged = merged.reset_index()
    if "date" not in merged.columns:
        merged = merged.rename(columns={"index": "date"})
    merged = merged.reindex(columns=STOCK_COLS)
    # 非 A 股的 high_limit/low_limit 在 to_daily_frame 里是 None（无涨跌停），
    # 不强制转数值会让 parquet 写出 object 列，引擎做数值运算时容易出错。
    for col in ("open", "close", "high", "low", "high_limit", "low_limit",
                "preclose", "volume", "money"):
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    info["final_rows"] = len(merged)
    info["final_first"] = str(pd.Timestamp(merged["date"].min()).date())
    info["final_last"] = str(pd.Timestamp(merged["date"].max()).date())

    if apply:
        if backup_dir is None:
            info["status"] = "no_backup_dir"
            return info
        dst = backup_dir / local.name
        if not dst.exists():
            shutil.copy2(local, dst)
        if not dst.exists():
            info["status"] = "backup_failed"
            return info
        tmp = local.with_suffix(".parquet.tmp")
        merged.to_parquet(tmp, index=False)
        tmp.replace(local)
        info["status"] = "rebuilt"
    else:
        info["status"] = "to_rebuild"
    return info


def main() -> int:
    ap = argparse.ArgumentParser(description="QMT 官方日线修复伪日线 / 历史缺口（默认 dry-run）")
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--universe", choices=("a-share", "pseudo", "all"), default="a-share")
    ap.add_argument("--manifest", type=Path, default=OUT_DIR / "qmt_pseudo_daily_manifest.json")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--changes", type=Path, default=OUT_DIR / "qmt_daily_rebuild_changes.json")
    args = ap.parse_args()

    tasks = build_tasks(args.root, args.data_dir, args.universe, args.manifest)
    if args.limit:
        tasks = tasks[:args.limit]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = (args.data_dir / f"_backup_daily_rebuild_{args.universe}_{stamp}"
                  if args.apply else None)
    if backup_dir:
        backup_dir.mkdir(parents=True, exist_ok=True)
        print(f"备份目录：{backup_dir}")

    print(f"模式={'APPLY' if args.apply else 'DRY-RUN'} universe={args.universe} "
          f"待核 {len(tasks)} 只，workers={args.workers}")
    if not tasks:
        print("没有匹配的日线 DAT / 本地 parquet")
        return 1

    stats: Counter = Counter()
    changes = []
    all_info = []
    payload = [(t, args.apply, backup_dir) for t in tasks]
    if args.workers > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(process_one, *p) for p in payload]
            for i, fut in enumerate(as_completed(futures), 1):
                info = fut.result()
                stats[info["status"]] += 1
                all_info.append(info)
                if info["status"] in ("to_rebuild", "rebuilt", "overlap_conflict_skipped"):
                    changes.append(info)
                if i % 500 == 0:
                    print(f"  已核 {i}/{len(tasks)}，需重建 {sum(1 for c in changes if c['status'] in ('to_rebuild','rebuilt'))}", flush=True)
    else:
        for p in payload:
            info = process_one(*p)
            stats[info["status"]] += 1
            all_info.append(info)
            if info["status"] in ("to_rebuild", "rebuilt", "overlap_conflict_skipped"):
                changes.append(info)

    verified = [i for i in all_info if i.get("overlap_days", 0) > 0]
    total_overlap = sum(i["overlap_days"] for i in verified)
    total_bad = sum(i.get("overlap_bad_days", 0) for i in verified)
    audit = {
        "purpose": (f"universe={args.universe}：PTrade 本地日线 vs QMT 官方 86400 "
                    "日线逐日收盘价交叉核验"),
        "symbols_verified": len(verified),
        "total_overlap_days": total_overlap,
        "total_bad_days_over_0_5pct": total_bad,
        "overall_agree_rate": round(1 - total_bad / total_overlap, 8) if total_overlap else None,
        "symbols_with_any_bad_day": sum(1 for i in verified if i.get("overlap_bad_days", 0) > 0),
        "symbols_systematic_conflict": sum(1 for i in verified if i.get("overlap_conflict")),
        "symbols_local_superset_of_qmt": sum(1 for i in verified if i.get("dates_added_from_qmt", 0) == 0),
        "symbols_qmt_can_extend": sum(1 for i in verified if i.get("dates_added_from_qmt", 0) > 0),
        "outliers": [i for i in verified if i.get("overlap_bad_days", 0) > 0],
    }

    report = {
        "mode": "apply" if args.apply else "dry-run",
        "generated_at": stamp,
        "universe": args.universe,
        "selection": ("qmt_pseudo_daily_manifest.json 中 has_qmt_official_daily=true "
                      "且按代码前缀非 A 股的伪日线标的" if args.universe == "pseudo"
                      else "按代码前缀识别的 A 股" if args.universe == "a-share"
                      else "QMT 86400 与本地 parquet 的交集"),
        "manifest": str(args.manifest) if args.universe == "pseudo" else None,
        "source": f"{args.root}/datadir/{{SH,SZ}}/86400 (QMT 官方日线)",
        "target": str(args.data_dir / "stocks"),
        "backup_dir": str(backup_dir) if backup_dir else None,
        "strategy": "并集合并：QMT 补缺失日期、本地独有历史一行不删、重叠日期保留本地 preclose/涨跌停口径",
        "covered_symbols": len(tasks),
        "stats": dict(stats),
        "total_dates_added": sum(c.get("dates_added_from_qmt", 0) for c in changes
                                 if c["status"] in ("to_rebuild", "rebuilt")),
        "a_share_cross_check": audit,
        "changes": changes,
    }
    args.changes.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                            encoding="utf-8")
    print("\n统计：", dict(stats))
    print(f"可补齐日期总数：{report['total_dates_added']:,}")
    print(f"变更清单：{args.changes}")
    if not args.apply:
        print("这是 dry-run，未写盘。确认清单后加 --apply 执行（会自动先备份）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
