# -*- coding: utf-8 -*-
"""Backfill historical NULL fundamentals cells from cached offline quarter packs.

The 173 newly mapped FINVALUE positions exist as columns in ``fundamentals``
but are NULL for historical quarters that were downloaded before the mapping
was added. The cached ``data/mootdx_financial/gpcw*.zip`` packs (downloaded
once, no network needed) contain the same source positions, so this job parses
them with the production ``MootdxAffairFetcher`` and fills **NULL cells only**.
Non-NULL values are never overwritten, zero source values are kept as zeros
(the same semantics as the normal bulk writer), and no field is invented.

Placeholder packs (<1 KB, e.g. empty 1990 quarters) are skipped: they contain
no real financial data and cannot extend 1990s coverage.

Usage:
    python scripts/refresh_finvalue_from_cache.py --limit 3
    python scripts/refresh_finvalue_from_cache.py --apply
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
SIMDATA = HERE.parent
sys.path.insert(0, str(SIMDATA))

from simtradedata.fetchers.mootdx_affair_fetcher import MootdxAffairFetcher  # noqa: E402
from simtradedata.utils.code_utils import convert_to_ptrade_code  # noqa: E402
from simtradedata.utils.paths import DUCKDB_PATH  # noqa: E402

CACHE_DIR = SIMDATA / "data" / "mootdx_financial"
PROBE_COLUMNS = ("trading_assets", "good_will", "capital_reserve_fund",
                 "operating_expense", "income_tax_cost", "end_period_cash_equivalent")


def _target_columns(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame) -> list[str]:
    db_cols = {r[0] for r in conn.execute("DESCRIBE fundamentals").fetchall()}
    skip = {"symbol", "date", "publ_date", "roa_derived", "code", "end_date"}
    return [c for c in frame.columns
            if c in db_cols and c not in skip and not c.startswith("_")]


def _quarter_frame(fetcher: MootdxAffairFetcher, package: Path) -> pd.DataFrame:
    raw = fetcher.parse_local(package.name)
    frame = fetcher._convert_to_ptrade_format(raw)
    if frame.empty:
        return frame
    frame = frame.copy()
    frame["symbol"] = frame["code"].map(
        lambda value: convert_to_ptrade_code(str(value), "qstock")
    )
    frame["date"] = pd.to_datetime(frame["end_date"]).dt.date
    return frame


def _null_count(conn: duckdb.DuckDBPyConnection, columns: list[str]) -> int:
    expr = " + ".join(f"CASE WHEN {c} IS NULL THEN 1 ELSE 0 END" for c in columns)
    return int(conn.execute(f"SELECT COALESCE(SUM({expr}), 0) FROM fundamentals").fetchone()[0])


def run(db_path: Path, apply: bool, limit: int | None) -> dict[str, Any]:
    packages = [p for p in sorted(CACHE_DIR.glob("gpcw*.zip")) if p.stat().st_size > 1024]
    if limit:
        packages = packages[-limit:]
    fetcher = MootdxAffairFetcher(str(CACHE_DIR))
    conn = duckdb.connect(str(db_path), read_only=not apply)
    report: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "apply" if apply else "dry-run",
        "packages": len(packages),
        "first_package": packages[0].name if packages else None,
        "last_package": packages[-1].name if packages else None,
        "quarters": [],
    }
    try:
        first_frame = _quarter_frame(fetcher, packages[-1]) if packages else pd.DataFrame()
        columns = _target_columns(conn, first_frame) if not first_frame.empty else []
        report["target_columns"] = len(columns)
        report["null_cells_before"] = _null_count(conn, columns) if columns else 0
        for package in packages:
            try:
                frame = _quarter_frame(fetcher, package)
            except Exception as exc:  # noqa: BLE001
                # Older FINVALUE packs have fewer source columns than the
                # current 659-column layout; mootdx cannot parse them. Skip
                # and record instead of aborting the whole idempotent backfill.
                report["quarters"].append({
                    "package": package.name,
                    "parse_error": f"{type(exc).__name__}: {str(exc)[:200]}",
                })
                continue
            if frame.empty or not columns:
                continue
            cols = [c for c in columns if c in frame.columns]
            bf = frame[["symbol", "date"] + cols].drop_duplicates(["symbol", "date"])
            conn.register("finvalue_bf", bf)
            matched = int(conn.execute(
                "SELECT COUNT(*) FROM fundamentals f JOIN finvalue_bf b "
                "ON f.symbol=b.symbol AND f.date=b.date"
            ).fetchone()[0])
            if apply and matched:
                assignments = ", ".join(
                    f"{c} = COALESCE(f.{c}, b.{c})" for c in cols
                )
                any_null = " OR ".join(f"f.{c} IS NULL" for c in cols)
                conn.execute(
                    f"UPDATE fundamentals f SET {assignments} FROM finvalue_bf b "
                    f"WHERE f.symbol=b.symbol AND f.date=b.date AND ({any_null})"
                )
            conn.unregister("finvalue_bf")
            report["quarters"].append({
                "package": package.name,
                "parsed_rows": len(frame),
                "matched_db_rows": matched,
                "columns": len(cols),
            })
        report["null_cells_after"] = _null_count(conn, columns) if columns else 0
        available_probes = [c for c in PROBE_COLUMNS if c in columns]
        report["probe_columns"] = {
            c: conn.execute(f"SELECT COUNT(*) FILTER (WHERE {c} IS NULL) FROM fundamentals").fetchone()[0]
            for c in available_probes
        }
    finally:
        conn.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=Path(DUCKDB_PATH))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--json-output", type=Path, default=None)
    args = parser.parse_args()
    report = run(args.db_path, args.apply, args.limit)
    # Keep stdout compact: the per-quarter list is only in the JSON artifact.
    summary = {k: v for k, v in report.items() if k != "quarters"}
    summary["quarters_processed"] = len(report["quarters"])
    payload = json.dumps(summary, ensure_ascii=False, indent=2, default=str)
    print(payload)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
                                    encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
