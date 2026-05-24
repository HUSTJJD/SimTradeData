# -*- coding: utf-8 -*-
"""从腾讯和新浪补 2014 年以来的日线和财务报表。

已有日线只补缺口，不覆盖原有交易日。财务只写入新浪能对上的列。
分钟线公开接口到不了 2014，这里不下。

用法:
    python scripts/download_public_history.py
    python scripts/download_public_history.py --limit 20
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd

from simtradedata.fetchers.public_source_fetcher import PublicSourceFetcher
from simtradedata.importers.qmt_kline import _limit_ratio

_DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data\cn")
_START = "2014-01-01"
_FILL_COLUMNS = (
    "dividend_interest_payment",
    "net_operate_cash_flow",
    "oper_cash_ps_grow_rate",
    "np_parent_company_cut",
    "np_parent_company_cut_yoy",
    "net_operate_cash_flow_ttm",
    "net_operate_cash_flow_ps_ttm",
)


def symbols_on_disk(root: Path) -> list[str]:
    folder = root / "stocks"
    return sorted(path.stem for path in folder.glob("*.parquet"))


def append_daily(path: Path, bars: pd.DataFrame, symbol: str) -> int:
    """只追加本地没有的交易日，并给新行填前收和涨跌停。"""
    if bars is None or bars.empty:
        return 0
    incoming = bars.copy()
    incoming["date"] = pd.to_datetime(incoming["date"]).dt.normalize()
    incoming = incoming.drop_duplicates("date").sort_values("date")
    if path.exists():
        old = pd.read_parquet(path)
        old["date"] = pd.to_datetime(old["date"]).dt.normalize()
        have = set(old["date"])
        incoming = incoming[~incoming["date"].isin(have)]
        if incoming.empty:
            return 0
        frame = pd.concat([old, incoming], ignore_index=True)
    else:
        frame = incoming
    frame = frame.sort_values("date").reset_index(drop=True)
    previous = frame["close"].shift(1)
    new_rows = frame["preclose"].isna() if "preclose" in frame.columns else pd.Series(True, index=frame.index)
    if "preclose" not in frame.columns:
        frame["preclose"] = previous
    else:
        frame.loc[new_rows, "preclose"] = previous[new_rows]
    limit = _limit_ratio(symbol)
    for name in ("high_limit", "low_limit", "money"):
        if name not in frame.columns:
            frame[name] = None
    if limit is not None:
        base = frame["preclose"]
        frame.loc[new_rows, "high_limit"] = (base[new_rows] * (1 + limit)).round(2)
        frame.loc[new_rows, "low_limit"] = (base[new_rows] * (1 - limit)).round(2)
    ordered = [col for col in (
        "date", "open", "close", "high", "low", "high_limit", "low_limit", "preclose", "volume", "money",
    ) if col in frame.columns]
    frame[ordered].to_parquet(path, index=False)
    return len(incoming)


def merge_statements(path: Path, statement: pd.DataFrame, start: str) -> int:
    """按报告期补财务列。空值不覆盖已有数。"""
    if statement is None or statement.empty:
        return 0
    work = statement.copy()
    work["date"] = pd.to_datetime(work["end_date"]).dt.normalize()
    work = work[work["date"] >= pd.Timestamp(start)]
    if work.empty:
        return 0
    columns = [name for name in _FILL_COLUMNS if name in work.columns]
    if path.exists():
        old = pd.read_parquet(path)
        old["date"] = pd.to_datetime(old["date"]).dt.normalize()
    else:
        old = pd.DataFrame({"date": work["date"]})
    for name in columns:
        if name not in old.columns:
            old[name] = None
    indexed = work.drop_duplicates("date").set_index("date")
    filled = 0
    for day, row in indexed.iterrows():
        mask = old["date"] == day
        if not mask.any():
            extra = {name: row[name] if name in columns else None for name in old.columns if name != "date"}
            extra["date"] = day
            old = pd.concat([old, pd.DataFrame([extra])], ignore_index=True)
            filled += 1
            continue
        for name in columns:
            value = row[name]
            if value is None or pd.isna(value):
                continue
            old.loc[mask, name] = value
            filled += 1
    old = old.sort_values("date")
    path.parent.mkdir(parents=True, exist_ok=True)
    old.to_parquet(path, index=False)
    return filled


def sync_symbol(fetcher: PublicSourceFetcher, root: Path, symbol: str, start: str, end: str) -> str:
    stock_path = root / "stocks" / f"{symbol}.parquet"
    start_fetch = start
    if stock_path.exists():
        dates = pd.to_datetime(pd.read_parquet(stock_path, columns=["date"])["date"])
        if dates.max() >= pd.Timestamp(end):
            start_fetch = None
        else:
            start_fetch = (dates.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        if dates.min() > pd.Timestamp(start):
            early = fetcher.fetch_reconciled_daily(
                symbol, start, (dates.min() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            )
            append_daily(stock_path, early, symbol)
    if start_fetch is not None:
        bars = fetcher.fetch_reconciled_daily(symbol, start_fetch, end)
        append_daily(stock_path, bars, symbol)
    statement = fetcher.fetch_fundamentals(symbol, periods=80)
    merge_statements(root / "fundamentals" / f"{symbol}.parquet", statement, start)
    return "ok"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="用腾讯和新浪补 2014 年以来的日线和财务")
    parser.add_argument("--data", type=Path, default=_DATA)
    parser.add_argument("--start", default=_START)
    parser.add_argument("--end", default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args(argv)
    names = symbols_on_disk(args.data)
    progress_path = args.data / "_public_history_progress.json"
    done = set()
    if progress_path.exists():
        done = set(json.loads(progress_path.read_text(encoding="utf-8")))
    pending = [name for name in names if name not in done]
    if args.limit:
        pending = pending[: args.limit]
    print(f"目录 {args.data}  共 {len(names)} 只，待补 {len(pending)}，{args.start} 至 {args.end}")
    fetcher = PublicSourceFetcher()
    failed = 0
    for index, symbol in enumerate(pending, 1):
        try:
            sync_symbol(fetcher, args.data, symbol, args.start, args.end)
            done.add(symbol)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"失败 {symbol}: {exc}")
        if index % 20 == 0 or index == len(pending):
            progress_path.write_text(json.dumps(sorted(done)), encoding="utf-8")
            print(f"进度 {index}/{len(pending)}  已完成 {len(done)}  失败 {failed}")
        time.sleep(0.12)
    print("日线和财务补完。公开分钟线没有 2014 年以来的历史，这次没有下分钟线。")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
