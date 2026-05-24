# -*- coding: utf-8 -*-
"""Import real Dragon-Tiger-Board data from the local QMT SQLite into `lhb`.

The canonical CN database has an empty ``lhb`` table. The QMT client ships a
read-only local SQLite with per-seat buy/sell detail, so this job aggregates
it offline instead of contacting EastMoney. It never invents values: every row
is the sum of real seat amounts for one (symbol, date, reason).

Provenance is explicit and conservative: these are **seat-detail sums**, not a
verified reproduction of PTrade/东财 ``BILLBOARD_*`` semantics. The report
records ``ptrade_semantics_verified: false``; consumers must not treat this as
full PTrade equivalence.

Usage:
    python scripts/import_qmt_lhb.py
    python scripts/import_qmt_lhb.py --apply
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SIMDATA = HERE.parent
sys.path.insert(0, str(SIMDATA))

from simtradedata.utils.code_utils import convert_to_ptrade_code  # noqa: E402
from simtradedata.utils.paths import DUCKDB_PATH  # noqa: E402
from simtradedata.writers.duckdb_writer import DuckDBWriter  # noqa: E402

DEFAULT_SQLITE = Path(r"D:\君弘君智交易系统\data\longhubang.db3")
TZ8 = timezone(timedelta(hours=8))

AGG_SQL = """
SELECT STOCK_CODE, TIMETAG, REASON_ID,
       SUM(CASE WHEN DIRECTION = 'B' THEN BUYIN_AMOUNT ELSE 0 END) AS buy_amount,
       SUM(CASE WHEN DIRECTION = 'S' THEN SELLOUT_AMOUNT ELSE 0 END) AS sell_amount
FROM DEPARTMENT_INFO
GROUP BY STOCK_CODE, TIMETAG, REASON_ID
"""


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


def collect(sqlite_path: Path, min_date: date) -> tuple[pd.DataFrame, dict]:
    con = sqlite3.connect(f"file:{sqlite_path.as_posix()}?mode=ro", uri=True)
    con.text_factory = bytes
    try:
        reasons = {int(rid): _text(reason)
                   for rid, reason in con.execute("SELECT REASON_ID, REASON FROM REASON_TABLE")}
        rows = []
        skipped_codes = 0
        skipped_dates = 0
        for code, timetag, reason_id, buy_amount, sell_amount in con.execute(AGG_SQL):
            day = datetime.fromtimestamp(timetag / 1000, TZ8).date()
            if day < min_date:
                skipped_dates += 1
                continue
            symbol = convert_to_ptrade_code(_text(code), "qstock")
            if "." not in symbol:
                skipped_codes += 1
                continue
            buy = float(buy_amount or 0)
            sell = float(sell_amount or 0)
            rows.append({
                "symbol": symbol,
                "date": day,
                "reason": reasons.get(int(reason_id), ""),
                "net_buy": buy - sell,
                "buy_amount": buy,
                "sell_amount": sell,
            })
        frame = pd.DataFrame(rows, columns=["symbol", "date", "reason", "net_buy",
                                            "buy_amount", "sell_amount"])
        stats = {
            "source_rows": len(rows) + skipped_codes + skipped_dates,
            "imported_rows": len(rows),
            "skipped_unmapped_codes": skipped_codes,
            "skipped_before_min_date": skipped_dates,
            "min_date": min_date.isoformat(),
        }
        return frame, stats
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", type=Path, default=Path(os.environ.get("SIMTRADE_QMT_LHB_DB", str(DEFAULT_SQLITE))))
    parser.add_argument("--db-path", type=Path, default=Path(DUCKDB_PATH))
    parser.add_argument("--min-date", type=date.fromisoformat, default=date(1990, 12, 19))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json-output", type=Path, default=None)
    args = parser.parse_args()
    if not args.sqlite.is_file():
        parser.error(f"missing QMT longhubang SQLite: {args.sqlite}")

    frame, stats = collect(args.sqlite, args.min_date)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "apply" if args.apply else "dry-run",
        "source": str(args.sqlite),
        "derived_from": "QMT DEPARTMENT_INFO seat buy/sell detail aggregated by (code, session, reason)",
        "unit": "source amount units (seat sums, not normalised)",
        "ptrade_semantics_verified": False,
        "caveat": "Seat-detail sums are not verified against PTrade/EastMoney BILLBOARD_NET_AMT/BUY_AMT/SELL_AMT; do not claim full PTrade equivalence.",
        **stats,
    }
    if not frame.empty:
        report["date_range"] = [str(frame["date"].min()), str(frame["date"].max())]
        report["distinct_symbols"] = int(frame["symbol"].nunique())
    if args.apply:
        writer = DuckDBWriter(str(args.db_path))
        try:
            report["written_rows"] = writer.write_lhb(frame)
        finally:
            writer.close()
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    print(payload)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(payload + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
