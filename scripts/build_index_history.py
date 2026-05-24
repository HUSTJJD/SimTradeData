# -*- coding: utf-8 -*-
"""Publish 1990s index history as an additive CN serving-view asset.

The ``benchmark`` table carries only the default CSI300 index, whose inception
is 2005-01-04. Long-lived index bars (SSE Composite since 1990-12-19, SZSE
Component since 1991-04-03) already exist in ``stocks`` but are deliberately
excluded from the active-stock export because index pseudo-symbols are not
stocks. This job materialises them as a separate, additive asset:

* ``index_history`` table in ``data/cn.duckdb`` (one row per symbol/date);
* ``data/cn/metadata/index_history.parquet`` in the serving view.

It never changes ``BENCHMARK_CONFIG['default_index']``, never invents
pre-inception bars, and never touches per-symbol stock Parquet files. The
default mode is a read-only dry run; ``--apply`` writes both assets idempotently.

Usage:
    python scripts/build_index_history.py
    python scripts/build_index_history.py --apply
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb

HERE = Path(__file__).resolve().parent
SIMDATA = HERE.parent
sys.path.insert(0, str(SIMDATA))

from simtradedata.config.field_mappings import (  # noqa: E402
    BENCHMARK_CONFIG,
    BENCHMARK_INDEX_INCEPTION,
)
from simtradedata.utils.paths import DUCKDB_PATH  # noqa: E402

DEFAULT_EXPORT = SIMDATA / "data" / "cn" / "metadata" / "index_history.parquet"


def index_symbols() -> list[str]:
    symbols = {BENCHMARK_CONFIG["default_index"], *BENCHMARK_CONFIG["alternatives"]}
    return sorted(symbols)


def _placeholders(symbols: list[str]) -> str:
    return ", ".join("?" for _ in symbols)


def _source_query(symbols: list[str]) -> str:
    return (
        "SELECT date, symbol, open, high, low, close, volume, money "
        f"FROM stocks WHERE symbol IN ({_placeholders(symbols)})"
    )


def _coverage(conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT symbol, MIN(date), MAX(date), COUNT(*) FROM stocks "
        f"WHERE symbol IN ({_placeholders(symbols)}) GROUP BY symbol ORDER BY symbol",
        symbols,
    ).fetchall()
    return [
        {
            "symbol": row[0],
            "min_date": str(row[1]),
            "max_date": str(row[2]),
            "rows": int(row[3]),
            "declared_inception": BENCHMARK_INDEX_INCEPTION.get(row[0]),
        }
        for row in rows
    ]


def build(db_path: Path, export_path: Path, apply: bool) -> dict[str, Any]:
    symbols = index_symbols()
    conn = duckdb.connect(str(db_path), read_only=not apply)
    try:
        coverage = _coverage(conn, symbols)
        missing = [s for s in symbols if not any(c["symbol"] == s for c in coverage)]
        report: dict[str, Any] = {
            "mode": "apply" if apply else "dry-run",
            "db_path": str(db_path),
            "export_path": str(export_path),
            "symbols": symbols,
            "coverage": coverage,
            "missing_symbols": missing,
            "earliest_bar": min((c["min_date"] for c in coverage), default=None),
            "rows": sum(c["rows"] for c in coverage),
        }
        if not apply:
            return report

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS index_history (
                date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                volume DOUBLE, money DOUBLE,
                PRIMARY KEY (symbol, date)
            )
            """
        )
        source = _source_query(symbols)
        conn.execute("BEGIN TRANSACTION")
        conn.execute("DELETE FROM index_history")
        conn.execute(f"INSERT INTO index_history {source}", symbols)
        conn.execute("COMMIT")
        written = conn.execute("SELECT COUNT(*) FROM index_history").fetchone()[0]
        report["written_rows"] = int(written)

        export_path.parent.mkdir(parents=True, exist_ok=True)
        target = str(export_path).replace("'", "''")
        conn.execute(
            "COPY (SELECT * FROM index_history ORDER BY symbol, date) "
            f"TO '{target}' (FORMAT PARQUET, CODEC 'ZSTD')"
        )
        report["parquet_written"] = export_path.exists()
        return report
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=Path(DUCKDB_PATH))
    parser.add_argument("--export-path", type=Path, default=DEFAULT_EXPORT)
    parser.add_argument("--apply", action="store_true", help="write DB table and Parquet asset")
    parser.add_argument("--json-output", type=Path, default=None)
    args = parser.parse_args()
    report = build(args.db_path, args.export_path, args.apply)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    print(payload)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(payload + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
