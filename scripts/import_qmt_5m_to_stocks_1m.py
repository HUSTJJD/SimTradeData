# -*- coding: utf-8 -*-
"""把本机 QMT 客户端的 5m K 线写成 PTrade 回测引擎原生读取的 stocks_1m。

严格边界（务必遵守）：
  * SimTradeLab 只是 PTrade **原生回测接口**的本地实现。本脚本只产出引擎原生会读的
    ``data/cn/stocks_1m/<symbol>.parquet``，不新增/修改任何引擎接口，
    不 import QMT SDK / xtdata，不改引擎代码。
  * 引擎唯一的分钟数据入口就是 stocks_1m；5m/15m/30m/60m/120m 都由
    ``PtradeAPI._aggregate_intraday_kline`` 从 stocks_1m 里的 bar 聚合。

重要语义（已实测，见 qmt_three_questions_report.json 的 q2）：
  * 写进去的是 **5 分钟间隔** 的 bar。原生 get_history 取 5m/15m/30m/60m
    结果完整且数值正确；但 frequency='1m' 会按标准 1m 时间轴 reindex，
    10 根里只有 2 根有值 —— 1m 语义失真。
  * 本机 QMT 的 1m 数据几乎未下载（SH/SZ 各 1 只样本），1m 本来就没有可用数据。
    若以后在 QMT 客户端补下载 1m，用 ``--period 1m`` 重跑本脚本即可恢复全频率正确性。

范围控制：
  * 默认只写 A 股（约 5223 只 / 3.6 GB 原始 DAT），避免把 17073 只债券、回购、
    以及 Q1 查出的 5m 合成伪日线标的也灌进分钟目录。
  * ``--universe all`` 写 PTrade 已有标的的全白名单（约 23357 只 / 15.1 GB 原始 DAT）。

行为：
  * 默认 dry-run 只统计，不写盘；显式 ``--apply`` 才落盘。
  * 增量合并：目标已存在时按 datetime 去重，只补新 bar，不覆盖历史。
  * 每次输出变更清单 JSON，可审阅、可回滚。脚本可重入。

用法：
    python scripts/import_qmt_5m_to_stocks_1m.py --limit 20          # 小范围口径验证
    python scripts/import_qmt_5m_to_stocks_1m.py                      # 全量 dry-run
    python scripts/import_qmt_5m_to_stocks_1m.py --apply --workers 8   # 实际写入
    python scripts/import_qmt_5m_to_stocks_1m.py --apply --universe all --workers 8
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from simtradedata.importers.qmt_kline import decode_kline_dat  # noqa: E402

DEFAULT_ROOT = Path(r"D:\君弘君智交易系统")
DEFAULT_DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data\cn")
OUT_DIR = Path(__file__).resolve().parent
MINUTE_COLS = ["datetime", "open", "high", "low", "close", "volume", "money"]
PERIOD_DIR = {"5m": "300", "1m": "60"}
_TZ8 = timezone(timedelta(hours=8))


def is_a_share(symbol: str) -> bool:
    code, market = symbol.split(".")[0], symbol.rsplit(".", 1)[-1]
    if market == "SS":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    return False


def dat_last_stamp(path: Path) -> datetime | None:
    """只读 DAT 最后 64 字节拿最新 bar 时刻。"""
    try:
        if path.stat().st_size < 72:
            return None
        with path.open("rb") as fh:
            fh.seek(-64, 2)
            tail = fh.read(64)
        stamp = struct.unpack_from("<9I", tail, 0)[0]
        if stamp < 946684800:
            return None
        return datetime.fromtimestamp(stamp, _TZ8).replace(tzinfo=None)
    except Exception:
        return None


def build_tasks(root: Path, data_dir: Path, universe: str, period: str) -> list[tuple[str, Path, Path]]:
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    white = {p.stem for p in (data_dir / "stocks").glob("*.parquet")}
    out_dir = data_dir / "stocks_1m"
    tasks = []
    for market, suffix in (("SH", "SS"), ("SZ", "SZ")):
        folder = datadir / market / PERIOD_DIR[period]
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("*.DAT")):
            symbol = f"{path.stem[-6:]}.{suffix}"
            if symbol not in white:
                continue
            if universe == "a-share" and not is_a_share(symbol):
                continue
            tasks.append((symbol, path, out_dir / f"{symbol}.parquet"))
    return tasks


def process_one(task: tuple[str, Path, Path], apply: bool) -> dict:
    symbol, dat, out = task
    info: dict = {"symbol": symbol}
    frame = decode_kline_dat(dat)
    if frame.empty:
        info["status"] = "empty_dat"
        return info
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = (frame.drop_duplicates("datetime", keep="last")
                  .sort_values("datetime").reset_index(drop=True))
    frame = frame[MINUTE_COLS]

    if out.exists():
        try:
            old = pd.read_parquet(out)
            if "datetime" not in old.columns:
                info["status"] = "bad_existing_schema"
                return info
            old["datetime"] = pd.to_datetime(old["datetime"])
            merged = pd.concat([old, frame], ignore_index=True)
            merged = (merged.drop_duplicates("datetime", keep="last")
                            .sort_values("datetime").reset_index(drop=True))
            added = len(merged) - len(old)
            if added <= 0:
                info["status"] = "no_new_bar"
                info["bars"] = len(old)
                return info
            info["existing_bars"] = len(old)
            info["added_bars"] = added
        except Exception as exc:  # noqa: BLE001
            info["status"] = f"read_error:{type(exc).__name__}"
            return info
    else:
        merged = frame
        info["added_bars"] = len(frame)

    info["bars"] = len(merged)
    info["first"] = str(merged["datetime"].iloc[0])
    info["last"] = str(merged["datetime"].iloc[-1])
    info["status"] = "written" if apply else "to_write"
    if apply:
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".parquet.tmp")
        merged.to_parquet(tmp, index=False)
        tmp.replace(out)
    return info


def main() -> int:
    ap = argparse.ArgumentParser(description="QMT 5m -> PTrade stocks_1m（默认 dry-run）")
    ap.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DATA)
    ap.add_argument("--universe", choices=("a-share", "all"), default="a-share")
    ap.add_argument("--period", choices=tuple(PERIOD_DIR), default="5m",
                    help="5m 用 datadir/*/300；补下载 1m 后用 --period 1m 重建")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--changes", type=Path, default=OUT_DIR / "qmt_5m_changes.json")
    args = ap.parse_args()

    tasks = build_tasks(args.root, args.data_dir, args.universe, args.period)
    if args.limit:
        tasks = tasks[:args.limit]
    print(f"周期={args.period} universe={args.universe} 模式={'APPLY' if args.apply else 'DRY-RUN'} "
          f"待处理 {len(tasks)} 只，workers={args.workers}")
    if not tasks:
        print("没有匹配的 DAT，请检查 QMT 目录 / universe 参数")
        return 1

    stats: Counter = Counter()
    changes = []
    if args.workers > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process_one, t, args.apply): t[0] for t in tasks}
            for i, fut in enumerate(as_completed(futures), 1):
                info = fut.result()
                stats[info["status"]] += 1
                if info["status"] in ("to_write", "written"):
                    changes.append(info)
                if i % 500 == 0:
                    print(f"  已扫描 {i}/{len(tasks)}，待写/已写 {len(changes)}", flush=True)
    else:
        for i, t in enumerate(tasks, 1):
            info = process_one(t, args.apply)
            stats[info["status"]] += 1
            if info["status"] in ("to_write", "written"):
                changes.append(info)
            if i % 500 == 0:
                print(f"  已扫描 {i}/{len(tasks)}，待写/已写 {len(changes)}", flush=True)

    report = {
        "mode": "apply" if args.apply else "dry-run",
        "period": args.period,
        "universe": args.universe,
        "source": f"{args.root}/datadir/{{SH,SZ}}/{PERIOD_DIR[args.period]}",
        "target": str(args.data_dir / "stocks_1m"),
        "semantics": "5m bar 存入 stocks_1m：5m/15m/30m/60m 正确，1m 有空洞（QMT 1m 未下载）",
        "tasks": len(tasks),
        "stats": dict(stats),
        "total_bars_to_write": sum(c.get("bars", 0) for c in changes),
        "changes": changes,
    }
    args.changes.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                            encoding="utf-8")
    print("\n统计：", dict(stats))
    print(f"总 bar 数（待写/已写文件）：{report['total_bars_to_write']:,}")
    print(f"变更清单：{args.changes}")
    if not args.apply:
        print("这是 dry-run，未写盘。确认无误后加 --apply 执行。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
