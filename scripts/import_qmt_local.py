# -*- coding: utf-8 -*-
"""把君弘君智本地 K 线导入引擎读取的 parquet。

下载还在进行时可以重复执行。文件没写完会跳过，已导入且没有更新的文件不重写。
默认只取 SH / SZ / BJ。港股不写进 A 股目录。

用法:
    python scripts/import_qmt_local.py
    python scripts/import_qmt_local.py --root D:\\君弘君智交易系统 --output D:\\data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from simtradedata.importers.qmt_exrights import list_exrights_files, to_exrights
from simtradedata.importers.qmt_financial import (
    build_fundamentals,
    list_statement_files,
    read_statement_file,
)
from simtradedata.importers.qmt_kline import decode_kline_dat, list_kline_files, to_daily_frame
from simtradedata.validators.crosscheck import reconcile_closes
from simtradedata.writers.minute_store import data_symbol, minute_store_dir

_DEFAULT_ROOT = Path(r"D:\君弘君智交易系统")
_PERIOD_FOLDER = {
    "1m": "stocks_1m",
    "5m": "stocks_5m",
    "15m": "stocks_15m",
    "30m": "stocks_30m",
    "60m": "stocks_60m",
    "1d": "stocks",
}


def _daily_close(bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.copy()
    work["date"] = pd.to_datetime(work["datetime"]).dt.normalize()
    work = work.sort_values("datetime")
    return work.groupby("date", as_index=False).tail(1)[["date", "close"]]


def _existing_close(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path, columns=["date", "close"])
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return frame


def _cross_status(qmt_daily: pd.DataFrame, local_daily: pd.DataFrame) -> tuple[str, dict]:
    """重叠日期对得上才允许写入。没有重叠，或冲突超过 1%，整只跳过。"""
    if local_daily.empty:
        return "no_local", {"overlap": 0, "agree": 0, "conflict": 0}
    checked = reconcile_closes(qmt_daily, local_daily, tolerance=0.005)
    overlap = checked[checked["status"].isin(["agree", "conflict"])]
    agree = int((overlap["status"] == "agree").sum())
    conflict = int((overlap["status"] == "conflict").sum())
    stats = {"overlap": int(len(overlap)), "agree": agree, "conflict": conflict}
    if len(overlap) < 20:
        return "thin", stats
    if conflict / len(overlap) > 0.01:
        return "conflict", stats
    return "ok", stats


_KIND_BOND = ("债", "转债", "国债", "国开", "特国")


def _load_names(root: Path) -> dict:
    """从 increase 目录抽出 (市场, 代码) 到名称。同号在沪深不是同一只。"""
    import re
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    found = {}
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


def _security_type(code: str, name: str) -> str:
    text = name or ""
    if code.startswith(("60", "68", "00", "30")) and "指数" not in text and "债" not in text:
        return "stock"
    if "指数" in text:
        return "index"
    if any(k in text for k in ("ETF", "LOF", "基金")):
        return "fund"
    if any(k in text for k in _KIND_BOND) or code.startswith((
        "01", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "23", "24", "27",
    )):
        return "bond"
    if code.startswith(("50", "51", "56", "58", "15", "16")):
        return "fund"
    return "other"


_NEW_META = []


def _remember_new(symbol: str, bars: pd.DataFrame, names: dict) -> None:
    code = symbol.split(".")[0]
    market = "SH" if symbol.endswith(".SS") else "SZ"
    name = names.get((market, code), "")
    first = pd.to_datetime(bars["datetime"]).min()
    _NEW_META.append({
        "symbol": symbol,
        "stock_name": name,
        "listed_date": pd.Timestamp(first).strftime("%Y-%m-%d 00:00:00"),
        "de_listed_date": "2900-01-01",
        "blocks": "",
        "security_type": _security_type(code, name),
    })


def _flush_metadata(base: Path) -> int:
    if not _NEW_META:
        return 0
    path = base / "metadata" / "stock_metadata.parquet"
    old = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    if "security_type" not in old.columns:
        old["security_type"] = "stock"
    have = set(old["symbol"]) if "symbol" in old.columns else set()
    fresh = pd.DataFrame([row for row in _NEW_META if row["symbol"] not in have])
    if fresh.empty and "security_type" in old.columns:
        return 0
    merged = pd.concat([old, fresh], ignore_index=True).drop_duplicates("symbol", keep="first")
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(path, index=False)
    return len(fresh)


def import_klines(root: Path, output: Path | None = None) -> dict:
    """扫描并写入。已有日线的先对收盘价，没有的按新品种写入。"""
    files = list_kline_files(root)
    base = _output_root(output)
    written = {name: 0 for name in _PERIOD_FOLDER}
    counts = {"ok": 0, "conflict": 0, "thin": 0, "new": 0, "empty": 0, "skipped": 0, "bad_name": 0}
    names = _load_names(root)
    samples = []
    for period, symbol, path in files:
        symbol = data_symbol(symbol)
        code = symbol.split(".")[0]
        if len(code) != 6 or not code.isdigit():
            counts["bad_name"] += 1
            continue
        local_path = base / "stocks" / f"{symbol}.parquet"
        if not local_path.exists():
            counts["new"] += 1
            dest_dir = _folder(period, output)
            dest = dest_dir / f"{symbol}.parquet"
            bars = decode_kline_dat(path)
            if bars.empty:
                counts["empty"] += 1
                continue
            _merge_parquet(dest, bars, "datetime")
            if period != "1d":
                daily_path = base / "stocks" / f"{symbol}.parquet"
                if not daily_path.exists():
                    _merge_parquet(daily_path, to_daily_frame(bars, symbol), "date")
            _remember_new(symbol, bars, names)
            written[period] += 1
            done = sum(v for k, v in written.items() if k in _PERIOD_FOLDER)
            if done % 500 == 0:
                print("已写入", done, symbol, flush=True)
            continue
        dest_dir = _folder(period, output)
        dest = dest_dir / f"{symbol}.parquet"
        if dest.exists() and path.stat().st_mtime <= dest.stat().st_mtime:
            counts["skipped"] += 1
            continue
        bars = decode_kline_dat(path)
        if bars.empty:
            counts["empty"] += 1
            continue
        qmt_daily = _daily_close(bars)
        status, stats = _cross_status(qmt_daily, _existing_close(local_path))
        counts[status] = counts.get(status, 0) + 1
        if len(samples) < 8:
            samples.append((symbol, period, status, stats))
        if status != "ok":
            continue
        if period == "1d":
            frame = to_daily_frame(bars, symbol)
            _merge_parquet(dest, frame, "date")
        else:
            _merge_parquet(dest, bars, "datetime")
        written[period] += 1
        done = sum(written.values())
        if done % 100 == 0:
            print("已写入", done, symbol, flush=True)
    written["metadata"] = _flush_metadata(base)
    written["seen"] = len(files)
    written["counts"] = counts
    written["samples"] = samples
    return written


def import_statements(root: Path, output: Path | None = None) -> dict:
    """读已经落成表格的财务报表。二进制表还不能认时只计数，不猜字段。"""
    readable, unknown = list_statement_files(root)
    grouped: dict[str, list[pd.DataFrame]] = {}
    for path in readable:
        name, frame = read_statement_file(path)
        if name and not frame.empty:
            grouped.setdefault(name, []).append(frame)
    tables = {name: pd.concat(frames, ignore_index=True) for name, frames in grouped.items()}
    built = build_fundamentals(tables) if tables else pd.DataFrame()
    written = 0
    if not built.empty:
        folder = _output_root(output) / "fundamentals"
        folder.mkdir(parents=True, exist_ok=True)
        for symbol, group in built.groupby("symbol"):
            dest = folder / f"{symbol}.parquet"
            _merge_parquet(dest, group.drop(columns=["symbol"]), "date")
            written += 1
    return {"statements": written, "statement_files": len(readable), "statement_unknown": len(unknown)}


def import_exrights(root: Path, output: Path | None = None) -> dict:
    """读带 interest / stockBonus 列的除权表。"""
    written = 0
    for path in list_exrights_files(root):
        frame = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)
        events = to_exrights(frame)
        if events.empty or "symbol" not in events.columns:
            continue
        folder = _output_root(output) / "exrights"
        folder.mkdir(parents=True, exist_ok=True)
        for symbol, group in events.groupby("symbol"):
            dest = folder / f"{symbol}.parquet"
            _merge_parquet(dest, group.drop(columns=["symbol"]), "date")
            written += 1
    return {"exrights": written}


def _output_root(output: Path | None) -> Path:
    if output is None:
        return minute_store_dir().parent
    return output / "cn" if (output / "cn").is_dir() else output


def _folder(period: str, output: Path | None) -> Path:
    path = _output_root(output) / _PERIOD_FOLDER[period]
    path.mkdir(parents=True, exist_ok=True)
    return path


def _merge_parquet(dest: Path, frame: pd.DataFrame, key: str) -> None:
    incoming = frame.copy()
    incoming[key] = pd.to_datetime(incoming[key])
    if dest.exists():
        old = pd.read_parquet(dest)
        old[key] = pd.to_datetime(old[key])
        incoming = pd.concat([old, incoming], ignore_index=True)
    incoming = incoming.drop_duplicates(key, keep="last").sort_values(key)
    incoming.to_parquet(dest, index=False)


def _report(root: Path) -> None:
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    print(f"数据目录 {datadir}")
    for market in ("SH", "SZ", "BJ", "HK"):
        market_dir = datadir / market
        if not market_dir.is_dir():
            print(f"  {market} 目录不存在")
            continue
        periods = [item.name for item in market_dir.iterdir() if item.is_dir()] or ["(无周期)"]
        for period in periods:
            folder = market_dir / period
            count = len(list(folder.glob("*.DAT"))) if folder.is_dir() else 0
            note = "" if market in {"SH", "SZ", "BJ"} else "，不导入"
            print(f"  {market}/{period}  {count} 个文件{note}")
    divid = datadir / "DividData"
    if divid.is_dir() and any(divid.glob("*.ldb")):
        print("  除权是 LevelDB，不是逐只股票的分红表，这次不解析")
    ep = datadir / "EP"
    if ep.is_dir():
        print("  EP 因子目录只有配置，没有财务数值，这次不解析")
    sample = datadir / "HK" / "300" / "00700.DAT"
    if sample.is_file():
        bars = decode_kline_dat(sample)
        if not bars.empty:
            print(
                "  抽样 HK/300/00700.DAT  {0} 根，{1} 至 {2}，最后收盘 {3}".format(
                    len(bars),
                    bars["datetime"].iloc[0],
                    bars["datetime"].iloc[-1],
                    bars["close"].iloc[-1],
                )
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="从君弘君智本地 K 线导入 PTrade 数据目录")
    parser.add_argument("--root", type=Path, default=_DEFAULT_ROOT)
    parser.add_argument("--output", type=Path, default=None, help="引擎 data 目录，默认 SimTradeData/data")
    args = parser.parse_args(argv)
    if not args.root.exists():
        print(f"找不到 {args.root}")
        return 1
    _report(args.root)
    result = import_klines(args.root, args.output)
    statements = import_statements(args.root, args.output)
    rights = import_exrights(args.root, args.output)
    counts = result.get("counts") or {}
    print(
        "看到 {seen} 个文件。对上 {ok}，冲突 {conflict}，重叠不足 {thin}，"
        "本地新增 {new}，空文件 {empty}，已导入未更新 {skipped}，名称无效 {bad_name}，元数据 {meta}".format(
            seen=result["seen"], meta=result.get("metadata", 0), **counts
        )
    )
    print("写入 日线 {1d}，1分钟 {1m}，5分钟 {5m}，15分钟 {15m}".format(**result))
    for symbol, period, status, stats in result.get("samples") or []:
        print(
            "  抽样 %s %s %s 重叠%s 一致%s 冲突%s"
            % (symbol, period, status, stats["overlap"], stats["agree"], stats["conflict"])
        )
    print(
        "财务报表写入 {statements} 只，可读文件 {statement_files}，"
        "认不出的表文件 {statement_unknown}；除权写入 {exrights} 只".format(
            **statements, **rights
        )
    )
    if result["seen"] == 0 and statements["statement_files"] == 0:
        print("沪深京 K 线和财务报表都还没落到 datadir。下载完成后把本命令再跑一次。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
