# -*- coding: utf-8 -*-
"""把本机 QMT 客户端的日线 DAT 增量导入 PTrade 本地回测引擎的数据目录。

严格边界（务必遵守）：
  * SimTradeLab 只是 PTrade **原生回测接口**的本地实现。本脚本只产出引擎原生会读的
    ``data/cn/stocks/<symbol>.parquet``（1d 日线），不新增/修改任何引擎接口，
    不写 stocks_5m/、stocks_1m/，不 import 任何 QMT SDK / xtdata。
  * 只处理 QMT 已下载的日线（86400）。5m / 1m / 财务 / 除权 / 板块 的完备度与
    可用性见 audit_qmt_local.py 的报告，不在本脚本范围：
      - 5m DAT 不能充当引擎的 1m bar（引擎 5m/15m/30m/60m 是从 stocks_1m 聚合的）；
      - 1m 在本地几乎没下载；
      - Finance 是 QMT 私有二进制，DividData 是 LevelDB，现有 importer 不认，不猜字段。
  * 只做「追加新日期」，不覆盖任何已有历史行，避免破坏 PTrade 现有复权/字段口径。
    追加前用重叠日期的收盘价做交叉校验，冲突超 1% 的标的整只跳过。
  * 默认 dry-run；只有显式 --apply 才落盘。每次都写变更清单 JSON 便于审阅/回滚。
  * 脚本可重入：已更新过的标的会被快速跳过，重跑不会产生重复行。

用法：
    python scripts/import_qmt_daily_to_ptrade.py                       # 体检，不写盘
    python scripts/import_qmt_daily_to_ptrade.py --workers 8 --apply    # 并行实际导入
    python scripts/import_qmt_daily_to_ptrade.py --limit 100            # 小范围验证口径
"""

from __future__ import annotations

import argparse
import json
import re
import struct
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from simtradedata.importers.qmt_kline import decode_kline_dat, to_daily_frame  # noqa: E402

DEFAULT_ROOT = Path(r"D:\君弘君智交易系统")
DEFAULT_DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data")
STOCK_COLS = ["date", "open", "close", "high", "low", "high_limit", "low_limit", "preclose", "volume", "money"]
AGREE_TOL = 0.005
MIN_OVERLAP = 20
MAX_CONFLICT_RATE = 0.01
_TZ8 = timezone(timedelta(hours=8))


def is_a_share(market: str, code: str) -> bool:
    if market == "SH":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    return False


def symbol_of(market: str, code: str) -> str:
    return code + (".SS" if market == "SH" else ".SZ")


def limit_ratio(symbol: str) -> float | None:
    code, suffix = symbol.split(".")[0], symbol.rsplit(".", 1)[-1]
    if suffix == "BJ":
        return 0.30
    if code.startswith(("688", "689")) and suffix == "SS":
        return 0.20
    if code.startswith("30") and suffix == "SZ":
        return 0.20
    if code.startswith(("60", "00")):
        return 0.10
    return None


def load_names(root: Path) -> dict:
    """从 QMT increase/SH|SZ 里抽 (市场, 代码) -> 名称，仅用于补 metadata。"""
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    found: dict[tuple[str, str], str] = {}
    for market in ("SH", "SZ"):
        path = datadir / "increase" / market
        if not path.is_file():
            continue
        data = path.read_bytes()
        for match in re.finditer(rb"\x020\x00[\x01-\x10]\x00\x00\x00([0-9A-Z]{4,8})\x00\x021\x00", data):
            code = match.group(1).decode("ascii", "ignore")
            chunk = data[match.end(): match.end() + 80]
            end = chunk.find(b"\x00\x01")
            if end <= 4:
                continue
            name = chunk[4:end].decode("utf-8", "ignore").strip("\x00")
            if name:
                found[(market, code)] = name
    return found


def qmt_last_date(path: Path) -> pd.Timestamp | None:
    """只读 DAT 最后 64 字节拿最新 K 线日期，避免全量解码。"""
    try:
        size = path.stat().st_size
        if size < 72:
            return None
        with path.open("rb") as fh:
            fh.seek(-64, 2)
            tail = fh.read(64)
        if len(tail) < 64:
            return None
        stamp = struct.unpack_from("<9I", tail, 0)[0]
        if stamp < 946684800:
            return None
        return pd.Timestamp(datetime.fromtimestamp(stamp, _TZ8).replace(tzinfo=None)).normalize()
    except Exception:
        return None


def build_local_index(
    stocks_dir: Path, cache: Path | None = None, refresh: bool = False
) -> dict[str, tuple[Path, pd.Timestamp]]:
    """用 parquet footer 的 column statistics 批量取每只标的最后日期（不读整表）。

    结果缓存成 JSON，避免每次运行都扫 2 万多个文件；索引偏旧只会导致多干活，
    不会漏数据。执行 --apply 写盘后缓存会自动删除。
    """
    if cache is not None and cache.is_file() and not refresh:
        try:
            raw = json.loads(cache.read_text(encoding="utf-8"))
            return {k: (stocks_dir / f"{k}.parquet", pd.Timestamp(v)) for k, v in raw.items()}
        except Exception:
            pass
    index: dict[str, tuple[Path, pd.Timestamp]] = {}
    for path in stocks_dir.glob("*.parquet"):
        last = None
        try:
            pf = pq.ParquetFile(path)
            col = pf.schema_arrow.get_field_index("date")
            if col >= 0:
                maxima = []
                for rg in range(pf.metadata.num_row_groups):
                    stat = pf.metadata.row_group(rg).column(col).statistics
                    if stat is not None and stat.has_min_max and stat.max is not None:
                        maxima.append(stat.max)
                if maxima:
                    last = pd.to_datetime(max(maxima)).normalize()
        except Exception:
            last = None
        if last is None:
            try:
                last = pd.to_datetime(pd.read_parquet(path, columns=["date"])["date"]).max().normalize()
            except Exception:
                last = pd.Timestamp.min
        index[path.stem] = (path, last)
    if cache is not None:
        cache.write_text(
            json.dumps({k: str(v[1].date()) for k, v in index.items()}, ensure_ascii=False),
            encoding="utf-8",
        )
    return index


def qmt_daily(path: Path, symbol: str) -> pd.DataFrame:
    bars = decode_kline_dat(path)
    if bars.empty:
        return pd.DataFrame()
    return to_daily_frame(bars, symbol)


def qmt_tail_frame(path: Path, symbol: str, n: int = 80) -> pd.DataFrame:
    """只解 DAT 尾部 n 根记录（QMT 是追加写，新 bar 在文件尾）。

    增量导入只需要最近若干根，读尾部可把单只文件的读取量从数百 KB 降到几 KB，
    同时保留足够多的重叠日期用于收盘价交叉校验。
    """
    try:
        size = path.stat().st_size
        if size < 72:
            return pd.DataFrame()
        n = min(n, (size - 8) // 64)
        if n <= 0:
            return pd.DataFrame()
        with path.open("rb") as fh:
            fh.seek(-n * 64, 2)
            body = fh.read(n * 64)
    except Exception:
        return qmt_daily(path, symbol)

    rows = []
    for offset in range(0, len(body) - 63, 64):
        stamp, raw_open, raw_high, raw_low, raw_close, _last, volume, _flag, amount = struct.unpack_from(
            "<9I", body, offset
        )
        if stamp < 946684800 or raw_close == 0:
            continue
        rows.append((
            datetime.fromtimestamp(stamp, _TZ8).replace(tzinfo=None),
            raw_open / 1000.0, raw_high / 1000.0, raw_low / 1000.0,
            raw_close / 1000.0, int(volume), float(amount),
        ))
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume", "money"])
    frame = frame.drop_duplicates("datetime", keep="last").sort_values("datetime")
    usable = frame[(frame["volume"] > 0) & (frame["close"] > 0) & (frame["money"] > 0)]
    if not usable.empty:
        ratio = usable["money"] / (usable["close"] * usable["volume"])
        if 50 <= float(ratio.median()) <= 150:
            frame["volume"] = frame["volume"] * 100
    # 日线 DAT 一根即一天，直接投影成引擎日线列，省去 groupby
    frame = frame.reset_index(drop=True)
    frame["date"] = pd.to_datetime(frame["datetime"]).dt.normalize()
    frame = frame.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
    daily = frame[["date", "open", "close", "high", "low", "volume", "money"]].copy()
    daily["preclose"] = daily["close"].shift(1)
    ratio = limit_ratio(symbol)
    if ratio is None:
        daily["high_limit"] = None
        daily["low_limit"] = None
    else:
        daily["high_limit"] = (daily["preclose"] * (1 + ratio)).round(2)
        daily["low_limit"] = (daily["preclose"] * (1 - ratio)).round(2)
    return daily[STOCK_COLS]


def cross_check(qmt: pd.DataFrame, local: pd.DataFrame) -> tuple[int, int, int]:
    """返回 (重叠根数, 一致根数, 冲突根数)。"""
    q = qmt[["date", "close"]].copy()
    q["date"] = pd.to_datetime(q["date"]).dt.normalize()
    l = local[["date", "close"]].copy()
    l["date"] = pd.to_datetime(l["date"]).dt.normalize()
    merged = q.merge(l, on="date", suffixes=("_q", "_l")).dropna(subset=["close_q", "close_l"])
    if merged.empty:
        return 0, 0, 0
    denom = merged["close_l"].abs().replace(0, pd.NA)
    rel = (merged["close_q"] - merged["close_l"]).abs() / denom
    agree = int((rel <= AGREE_TOL).sum())
    return len(merged), agree, len(merged) - agree


def append_new_dates(local: pd.DataFrame, qmt: pd.DataFrame, symbol: str) -> tuple[pd.DataFrame | None, int]:
    """只追加 QMT 里晚于本地最后日期的行，并重算 preclose / 涨跌停以衔接本地。"""
    work = local.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    last = work["date"].max()
    q = qmt.copy()
    q["date"] = pd.to_datetime(q["date"]).dt.normalize()
    add = q[q["date"] > last].copy().sort_values("date")
    if add.empty:
        return None, 0

    prev = float(work.sort_values("date")["close"].iloc[-1])
    pres = []
    for close in add["close"]:
        pres.append(prev)
        prev = float(close)
    add["preclose"] = pres
    ratio = limit_ratio(symbol)
    if ratio is None:
        add["high_limit"] = None
        add["low_limit"] = None
    else:
        add["high_limit"] = (add["preclose"] * (1 + ratio)).round(2)
        add["low_limit"] = (add["preclose"] * (1 - ratio)).round(2)

    add = add[STOCK_COLS]
    out = pd.concat([work[STOCK_COLS], add], ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    out = out.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
    return out, len(add)


def new_frame(qmt: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """本地没有该标的时，用整段 QMT 日线建新文件（仅 A 股新股）。"""
    q = qmt.copy()
    q["date"] = pd.to_datetime(q["date"]).dt.normalize()
    q["preclose"] = q["preclose"].fillna(q["open"])
    ratio = limit_ratio(symbol)
    if ratio is None:
        q["high_limit"] = None
        q["low_limit"] = None
    else:
        q["high_limit"] = (q["preclose"] * (1 + ratio)).round(2)
        q["low_limit"] = (q["preclose"] * (1 - ratio)).round(2)
    return q[STOCK_COLS].sort_values("date").reset_index(drop=True)


def process_one(task: tuple) -> tuple[dict, dict | None, dict | None, dict | None]:
    """处理单只标的，供多进程 worker 调用。返回 (统计, 变更, 冲突, 新元数据行)。"""
    (market, code, symbol, dat_path, exists, apply, target_path, name) = task
    del market, code  # 仅用于构造 symbol / 名称，worker 内不再需要
    path, target = Path(dat_path), Path(target_path)
    st: Counter = Counter()
    try:
        qmt = qmt_tail_frame(path, symbol) if exists else qmt_daily(path, symbol)
        if qmt.empty:
            st["empty"] += 1
            return dict(st), None, None, None
        if exists:
            local = pd.read_parquet(target, columns=None if apply else ["date", "close"])
            overlap, agree, bad = cross_check(qmt, local)
            # 允许最多 2 根孤立差异（指数调样/数据修正等噪声），大面积不一致才整只跳过
            allowed_bad = max(2, int(overlap * MAX_CONFLICT_RATE))
            if overlap >= MIN_OVERLAP and bad > allowed_bad:
                st["conflict"] += 1
                return dict(st), None, {"symbol": symbol, "overlap": overlap, "agree": agree, "conflict": bad}, None
            last_local = pd.to_datetime(local["date"]).max().normalize()
            qdates = pd.to_datetime(qmt["date"]).dt.normalize()
            added = int((qdates > last_local).sum())
            if added == 0:
                st["unchanged"] += 1
                return dict(st), None, None, None
            st["to_update"] += 1
            change = {
                "symbol": symbol, "action": "append", "added_rows": added,
                "old_last": str(last_local.date()), "new_last": str(qdates.max().date()),
                "overlap": overlap, "agree": agree, "conflict": bad,
            }
            if apply:
                merged, _ = append_new_dates(local, qmt, symbol)
                if merged is not None:
                    merged.to_parquet(target, index=False)
            return dict(st), change, None, None
        frame = new_frame(qmt, symbol)
        st["to_create"] += 1
        change = {
            "symbol": symbol, "action": "create", "rows": len(frame),
            "first": str(frame["date"].min().date()), "last": str(frame["date"].max().date()),
        }
        if apply:
            frame.to_parquet(target, index=False)
        meta = {
            "symbol": symbol, "stock_name": name,
            "listed_date": f"{frame['date'].min().date()} 00:00:00",
            "de_listed_date": "2900-01-01", "blocks": None, "security_type": "stock",
        }
        return dict(st), change, None, meta
    except Exception as exc:
        st["error"] += 1
        return dict(st), None, {"symbol": symbol, "reason": f"{type(exc).__name__}: {exc}"}, None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QMT 日线 -> PTrade 本地回测数据目录（仅 1d，增量）")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="QMT 客户端安装目录")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA, help="PTrade 引擎 data 目录")
    parser.add_argument("--apply", action="store_true", help="实际写盘（默认只体检）")
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少只（0 = 不限），用于快速验证")
    parser.add_argument("--workers", type=int, default=1, help="并行进程数（建议 8，加速大量小文件读写）")
    parser.add_argument("--changes", type=Path, default=None, help="变更清单 JSON 输出路径")
    parser.add_argument("--refresh-index", action="store_true", help="强制重建本地 parquet 日期索引缓存")
    args = parser.parse_args(argv)

    datadir = args.root / "datadir" if (args.root / "datadir").is_dir() else args.root
    cn = args.data / "cn" if (args.data / "cn").is_dir() else args.data
    stocks_dir = cn / "stocks"
    if not datadir.is_dir() or not stocks_dir.is_dir():
        print(f"目录不存在：QMT={datadir}  PTrade={stocks_dir}")
        return 1

    names = load_names(args.root)
    stats: Counter = Counter()
    changes: list[dict] = []
    conflicts: list[dict] = []
    meta_rows: list[dict] = []

    cache_path = Path(__file__).with_name("qmt_local_index_cache.json")
    print("构建/加载本地 parquet 日期索引（只读 footer）...", flush=True)
    local_index = build_local_index(stocks_dir, cache=cache_path, refresh=args.refresh_index)
    print(f"本地已有标的 {len(local_index)} 只", flush=True)

    # 先在主进程收集任务：只做文件名与 DAT 尾部的轻量判断
    tasks: list[tuple] = []
    for market in ("SH", "SZ"):
        folder = datadir / market / "86400"
        if not folder.is_dir():
            print(f"  {market}/86400 不存在，跳过（未下载）")
            continue
        for path in sorted(folder.glob("*.DAT")):
            code = path.stem[-6:]
            if len(code) != 6 or not code.isdigit():
                stats["bad_name"] += 1
                continue
            symbol = symbol_of(market, code)
            entry = local_index.get(symbol)
            if entry is None and not is_a_share(market, code):
                stats["skip_outside_ptrade_universe"] += 1
                continue
            stats["seen"] += 1
            if args.limit and stats["seen"] > args.limit:
                break
            if entry is not None:
                q_last = qmt_last_date(path)
                if q_last is not None and q_last <= entry[1]:
                    stats["no_new_date"] += 1
                    continue
            target = entry[0] if entry is not None else stocks_dir / f"{symbol}.parquet"
            tasks.append((market, code, symbol, str(path), entry is not None,
                          args.apply, str(target), names.get((market, code), "")))
        if args.limit and stats["seen"] > args.limit:
            break

    total = len(tasks)
    print(f"待处理 {total} 只（已快速跳过 {stats['no_new_date']} 只无新日期），workers={args.workers}", flush=True)

    if args.workers > 1 and total > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(process_one, t) for t in tasks]
            for i, fut in enumerate(as_completed(futures), 1):
                st, change, conflict, meta = fut.result()
                stats.update(st)
                if change:
                    changes.append(change)
                if conflict:
                    conflicts.append(conflict)
                if meta:
                    meta_rows.append(meta)
                if i % 2000 == 0:
                    print(f"  已完成 {i}/{total} ...", flush=True)
    else:
        for i, task in enumerate(tasks, 1):
            st, change, conflict, meta = process_one(task)
            stats.update(st)
            if change:
                changes.append(change)
            if conflict:
                conflicts.append(conflict)
            if meta:
                meta_rows.append(meta)
            if i % 2000 == 0:
                print(f"  已完成 {i}/{total} ...", flush=True)

    # metadata：仅为新建的 A 股补一行，不改动已有记录
    if args.apply and meta_rows:
        meta_path = cn / "metadata" / "stock_metadata.parquet"
        old = pd.read_parquet(meta_path) if meta_path.is_file() else pd.DataFrame()
        have = set(old["symbol"]) if "symbol" in old.columns else set()
        fresh = pd.DataFrame([r for r in meta_rows if r["symbol"] not in have])
        if not fresh.empty:
            merged_meta = pd.concat([old, fresh], ignore_index=True).drop_duplicates("symbol", keep="first")
            meta_path.parent.mkdir(parents=True, exist_ok=True)
            merged_meta.to_parquet(meta_path, index=False)
            stats["metadata_added"] = len(fresh)

    if args.apply:
        cache_path.unlink(missing_ok=True)  # 数据已变，本地日期索引缓存作废

    print(f"模式：{'APPLY（已写盘）' if args.apply else 'DRY-RUN（未写盘）'}")
    print(f"QMT 日线目录：{datadir}    目标：{stocks_dir}")
    print("统计：", json.dumps(dict(stats), ensure_ascii=False))
    print(f"冲突/异常跳过 {len(conflicts)} 只，示例：{conflicts[:5]}")
    print(f"变更 {len(changes)} 只，示例：{changes[:5]}")

    changes_path = args.changes or Path(__file__).with_name("qmt_daily_import_changes.json")
    changes_path.write_text(
        json.dumps({"mode": "apply" if args.apply else "dry-run", "stats": dict(stats),
                    "changes": changes, "conflicts": conflicts}, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"变更清单：{changes_path}")
    if not args.apply:
        print("确认无误后加 --apply 才会写入 PTrade 数据目录。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
