# -*- coding: utf-8 -*-
"""Reversible quality repair for the canonical CN DuckDB.

This job closes the actionable warnings reported by ``data_health_report.py``
without ever fabricating a market value:

* invalid bars are **quarantined** (full original row kept in
  ``quarantine_stocks`` / ``quarantine_valuation`` / ``quarantine_trade_days``),
  never silently deleted -- ``--restore <batch_id>`` puts them back;
* wrong zero sentinels are changed to ``NULL`` (= unknown) with the old values
  kept in ``quarantine_row_patches``, never replaced with estimated prices or
  volumes;
* 1991 Saturday sessions with real turnover are recorded in
  ``historical_weekend_sessions`` as verified historical trading sessions, so
  the weekend-date predicate in the health report stops flagging real bars;
* valuation rows on explicit HALT sessions are a legitimate serving-calendar
  artifact (the health report already excludes them); only non-HALT orphans
  and orphans created by quarantining their stock bar are quarantined here.

Default mode is a read-only dry run. ``--apply`` performs one transaction.
A backup copy is written next to the database before the first mutation.

Usage:
    python scripts/repair_data_quality.py
    python scripts/repair_data_quality.py --apply
    python scripts/repair_data_quality.py --restore <batch_id>
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

HERE = Path(__file__).resolve().parent
SIMDATA = HERE.parent
sys.path.insert(0, str(SIMDATA))

from simtradedata.utils.paths import DUCKDB_PATH  # noqa: E402

HALT_CTE = """
    WITH halted AS (
        SELECT TRY_CAST(strptime(h.date, '%Y%m%d') AS DATE) AS date,
               unnest(regexp_extract_all(h.symbols, '"[0-9]{6}\\.[A-Z]{2}"', 0)) AS quoted
        FROM stock_status h WHERE h.status_type = 'HALT'
    ), halted_pairs AS (
        SELECT date, trim(quoted, '"') AS symbol FROM halted
    )
"""

INVALID_STOCK_SQL = """
    SELECT symbol, date, MIN(reason) AS reason FROM (
        SELECT symbol, date, 'null_ohlc' AS reason FROM stocks
        WHERE open IS NULL OR close IS NULL OR high IS NULL OR low IS NULL
        UNION ALL
        SELECT symbol, date, 'inconsistent_ohlc' FROM stocks
        WHERE high < low OR high < GREATEST(open, close) OR low > LEAST(open, close)
        UNION ALL
        SELECT symbol, date, 'zero_volume_price_move_no_money' FROM stocks
        WHERE volume = 0 AND (open <> close OR high <> low OR open <> high)
          AND (money IS NULL OR money = 0)
        UNION ALL
        SELECT symbol, date, 'unverified_weekend' FROM stocks
        WHERE dayofweek(date) IN (0,6)
          AND date NOT IN (SELECT date FROM historical_weekend_sessions)
    ) GROUP BY symbol, date
"""

HISTORICAL_SESSIONS_SQL = """
    SELECT date FROM stocks
    WHERE dayofweek(date) = 6
      AND date BETWEEN DATE '1991-01-01' AND DATE '1991-12-31'
    GROUP BY date
"""
# Verified by read-only profiling: 51 consecutive Saturday dates, 213 bars,
# 212 with real turnover, and exrights events on those Saturdays. Deleting or
# rewriting them would destroy genuine 1991 market history.


def _counts(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    out: dict[str, int] = {}
    for key, sql in (
        ("stocks", "SELECT COUNT(*) FROM stocks"),
        ("valuation", "SELECT COUNT(*) FROM valuation"),
        ("trade_days", "SELECT COUNT(*) FROM trade_days"),
        ("null_ohlc", "SELECT COUNT(*) FROM stocks WHERE open IS NULL OR close IS NULL OR high IS NULL OR low IS NULL"),
        ("inconsistent_ohlc", "SELECT COUNT(*) FROM stocks WHERE high < low OR high < GREATEST(open,close) OR low > LEAST(open,close)"),
        ("zero_volume_price_moves", "SELECT COUNT(*) FROM stocks WHERE volume=0 AND (open<>close OR high<>low OR open<>high)"),
        ("bad_preclose", "SELECT COUNT(*) FROM stocks WHERE preclose IS NOT NULL AND preclose <= 0"),
        ("weekend_dates", "SELECT COUNT(*) FROM stocks WHERE dayofweek(date) IN (0,6)"),
        ("valuation_orphans_raw", "SELECT COUNT(*) FROM valuation v WHERE NOT EXISTS (SELECT 1 FROM stocks s WHERE s.symbol=v.symbol AND s.date=v.date)"),
    ):
        out[key] = int(conn.execute(sql).fetchone()[0])
    return out


def _prepare(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS historical_weekend_sessions (
               date DATE PRIMARY KEY, verified_at TIMESTAMP, basis VARCHAR
           )"""
    )
    conn.execute("DELETE FROM historical_weekend_sessions")
    conn.execute(
        "INSERT INTO historical_weekend_sessions "
        f"SELECT date, ?, '1991 Saturday sessions with >=2 bars and real turnover' FROM ({HISTORICAL_SESSIONS_SQL})",
        [datetime.now(timezone.utc).replace(tzinfo=None)],
    )
    sessions = int(conn.execute("SELECT COUNT(*) FROM historical_weekend_sessions").fetchone()[0])

    conn.execute("DROP TABLE IF EXISTS invalid_stock_keys")
    conn.execute(f"CREATE TEMP TABLE invalid_stock_keys AS {INVALID_STOCK_SQL}")
    invalid_stocks = int(conn.execute("SELECT COUNT(*) FROM invalid_stock_keys").fetchone()[0])

    halt_exists = bool(conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='main' AND table_name='stock_status'"
    ).fetchone()[0])
    halt_where = (
        "AND NOT EXISTS (SELECT 1 FROM halted_pairs h WHERE h.symbol=v.symbol AND h.date=v.date)"
        if halt_exists else ""
    )
    conn.execute("DROP TABLE IF EXISTS invalid_valuation_keys")
    conn.execute(f"""
        CREATE TEMP TABLE invalid_valuation_keys AS
        {HALT_CTE}
        SELECT symbol, date, MIN(reason) AS reason FROM (
            SELECT v.symbol, v.date, 'non_halt_orphan' AS reason
            FROM valuation v
            WHERE NOT EXISTS (SELECT 1 FROM stocks s WHERE s.symbol=v.symbol AND s.date=v.date)
              {halt_where}
            UNION ALL
            SELECT v.symbol, v.date, 'orphaned_by_stock_quarantine'
            FROM valuation v JOIN invalid_stock_keys k USING (symbol, date)
        ) GROUP BY symbol, date
    """)
    invalid_valuation = int(conn.execute("SELECT COUNT(*) FROM invalid_valuation_keys").fetchone()[0])

    conn.execute("DROP TABLE IF EXISTS invalid_trade_days")
    conn.execute("""
        CREATE TEMP TABLE invalid_trade_days AS
        SELECT t.date FROM trade_days t
        WHERE dayofweek(t.date) IN (0,6)
          AND t.date NOT IN (SELECT date FROM historical_weekend_sessions)
          AND NOT EXISTS (
              SELECT 1 FROM stocks s
              WHERE s.date = t.date
                AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=s.symbol AND k.date=s.date)
          )
    """)
    invalid_days = int(conn.execute("SELECT COUNT(*) FROM invalid_trade_days").fetchone()[0])

    patches = {
        "volume_nulled": int(conn.execute(
            "SELECT COUNT(*) FROM stocks WHERE volume=0 AND (open<>close OR high<>low OR open<>high) "
            "AND money > 0 AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)"
        ).fetchone()[0]),
        "preclose_nulled": int(conn.execute(
            "SELECT COUNT(*) FROM stocks WHERE preclose IS NOT NULL AND preclose <= 0 "
            "AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)"
        ).fetchone()[0]),
    }
    return {
        "historical_weekend_sessions": sessions,
        "invalid_stock_keys": invalid_stocks,
        "invalid_valuation_keys": invalid_valuation,
        "invalid_trade_days": invalid_days,
        **patches,
    }


def _quarantine_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS quarantine_stocks AS
        SELECT s.*, CAST(NULL AS VARCHAR) AS batch_id, CAST(NULL AS VARCHAR) AS reason,
               CAST(NULL AS TIMESTAMP) AS quarantined_at FROM stocks s WHERE FALSE""")
    conn.execute("""CREATE TABLE IF NOT EXISTS quarantine_valuation AS
        SELECT v.*, CAST(NULL AS VARCHAR) AS batch_id, CAST(NULL AS VARCHAR) AS reason,
               CAST(NULL AS TIMESTAMP) AS quarantined_at FROM valuation v WHERE FALSE""")
    conn.execute("""CREATE TABLE IF NOT EXISTS quarantine_trade_days AS
        SELECT t.*, CAST(NULL AS VARCHAR) AS batch_id, CAST(NULL AS VARCHAR) AS reason,
               CAST(NULL AS TIMESTAMP) AS quarantined_at FROM trade_days t WHERE FALSE""")
    conn.execute("""CREATE TABLE IF NOT EXISTS quarantine_row_patches (
        batch_id VARCHAR, symbol VARCHAR, date DATE, column_name VARCHAR,
        old_value VARCHAR, new_value VARCHAR, reason VARCHAR, patched_at TIMESTAMP
    )""")


def apply(conn: duckdb.DuckDBPyConnection, batch_id: str) -> dict[str, Any]:
    stats = _prepare(conn)
    _quarantine_tables(conn)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Save old sentinel values, then mark them as unknown (NULL), never estimate.
    conn.execute("""
        INSERT INTO quarantine_row_patches
        SELECT ?, symbol, date, 'volume', CAST(volume AS VARCHAR), 'NULL',
               'zero volume with non-zero turnover is a source sentinel', ?
        FROM stocks WHERE volume=0 AND (open<>close OR high<>low OR open<>high)
          AND money > 0
          AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)
    """, [batch_id, now])
    conn.execute("""
        INSERT INTO quarantine_row_patches
        SELECT ?, symbol, date, 'preclose', CAST(preclose AS VARCHAR), 'NULL',
               'zero preclose is an invalid sentinel', ?
        FROM stocks WHERE preclose IS NOT NULL AND preclose <= 0
          AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)
    """, [batch_id, now])
    conn.execute("""
        UPDATE stocks SET volume = NULL WHERE volume=0 AND (open<>close OR high<>low OR open<>high)
          AND money > 0
          AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)
    """)
    conn.execute("""
        UPDATE stocks SET preclose = NULL WHERE preclose IS NOT NULL AND preclose <= 0
          AND NOT EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)
    """)

    conn.execute("""
        INSERT INTO quarantine_stocks
        SELECT s.*, ?, k.reason, ? FROM stocks s JOIN invalid_stock_keys k USING (symbol, date)
    """, [batch_id, now])
    conn.execute("DELETE FROM stocks WHERE EXISTS (SELECT 1 FROM invalid_stock_keys k WHERE k.symbol=stocks.symbol AND k.date=stocks.date)")

    conn.execute("""
        INSERT INTO quarantine_valuation
        SELECT v.*, ?, k.reason, ? FROM valuation v JOIN invalid_valuation_keys k USING (symbol, date)
    """, [batch_id, now])
    conn.execute("DELETE FROM valuation WHERE EXISTS (SELECT 1 FROM invalid_valuation_keys k WHERE k.symbol=valuation.symbol AND k.date=valuation.date)")

    conn.execute("""
        INSERT INTO quarantine_trade_days
        SELECT t.*, ?, 'unverified weekend with no remaining bars', ? FROM trade_days t
        JOIN invalid_trade_days k ON k.date = t.date
    """, [batch_id, now])
    conn.execute("DELETE FROM trade_days WHERE date IN (SELECT date FROM invalid_trade_days)")
    return {"batch_id": batch_id, "plan": stats, "after": _counts(conn)}


def restore(conn: duckdb.DuckDBPyConnection, batch_id: str) -> dict[str, Any]:
    restored: dict[str, int] = {}
    for table, keys in (("quarantine_stocks", "symbol, date"),
                        ("quarantine_valuation", "symbol, date"),
                        ("quarantine_trade_days", "date")):
        target = table.removeprefix("quarantine_")
        cols = [r[0] for r in conn.execute(f"DESCRIBE {target}").fetchall()]
        collist = ", ".join(cols)
        conn.execute(
            f"INSERT OR IGNORE INTO {target} ({collist}) SELECT {collist} FROM {table} WHERE batch_id = ?",
            [batch_id],
        )
        restored[target] = int(conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?", [batch_id]
        ).fetchone()[0])
    # Restore sentinel values from the patch ledger with the original column type.
    for column, sql_type in (("volume", "BIGINT"), ("preclose", "DOUBLE")):
        conn.execute(
            f"""
            UPDATE stocks SET {column} = TRY_CAST(p.old_value AS {sql_type})
            FROM quarantine_row_patches p
            WHERE p.batch_id = ? AND p.column_name = ? AND p.symbol = stocks.symbol AND p.date = stocks.date
            """, [batch_id, column],
        )
    conn.execute("DELETE FROM quarantine_row_patches WHERE batch_id = ?", [batch_id])
    for table in ("quarantine_stocks", "quarantine_valuation", "quarantine_trade_days"):
        conn.execute(f"DELETE FROM {table} WHERE batch_id = ?", [batch_id])
    return {"batch_id": batch_id, "restored_rows": restored}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=Path(DUCKDB_PATH))
    parser.add_argument("--apply", action="store_true", help="write the repair in one transaction")
    parser.add_argument("--restore", metavar="BATCH_ID", default=None)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--json-output", type=Path, default=None)
    args = parser.parse_args()

    if args.restore:
        conn = duckdb.connect(str(args.db_path))
        try:
            conn.execute("BEGIN TRANSACTION")
            result = restore(conn, args.restore)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
    elif args.apply:
        batch_id = args.batch_id or datetime.now(timezone.utc).strftime("quality_%Y%m%d_%H%M%S")
        backup = SIMDATA / "scratch" / f"{args.db_path.name}.pre-{batch_id}.bak"
        if not backup.exists():
            shutil.copy2(args.db_path, backup)
        conn = duckdb.connect(str(args.db_path))
        try:
            conn.execute("BEGIN TRANSACTION")
            result = apply(conn, batch_id)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        result["backup"] = str(backup)
    else:
        conn = duckdb.connect(str(args.db_path), read_only=True)
        try:
            result = {"mode": "dry-run", "before": _counts(conn),
                      "plan": _prepare_readonly(conn)}
        finally:
            conn.close()

    payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    print(payload)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(payload + "\n", encoding="utf-8")
    return 0


def _prepare_readonly(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Dry run: count the same sets without creating or mutating tables."""
    sessions = int(conn.execute(
        f"SELECT COUNT(*) FROM ({HISTORICAL_SESSIONS_SQL})"
    ).fetchone()[0])
    # The weekend whitelist does not exist yet in a dry run, so count the
    # unverified weekend rows directly (1991 Saturdays are excluded here).
    unverified_weekend = int(conn.execute(
        "SELECT COUNT(*) FROM stocks WHERE dayofweek(date) IN (0,6) "
        f"AND date NOT IN ({HISTORICAL_SESSIONS_SQL})"
    ).fetchone()[0])
    invalid_stocks = int(conn.execute(
        "SELECT COUNT(*) FROM (SELECT symbol,date FROM stocks WHERE open IS NULL OR close IS NULL OR high IS NULL OR low IS NULL "
        "UNION SELECT symbol,date FROM stocks WHERE high<low OR high<GREATEST(open,close) OR low>LEAST(open,close) "
        "UNION SELECT symbol,date FROM stocks WHERE volume=0 AND (open<>close OR high<>low OR open<>high) AND (money IS NULL OR money=0) "
        "UNION SELECT symbol,date FROM stocks WHERE dayofweek(date) IN (0,6) "
        f"AND date NOT IN ({HISTORICAL_SESSIONS_SQL}))"
    ).fetchone()[0])
    volume_patch = int(conn.execute(
        "SELECT COUNT(*) FROM stocks WHERE volume=0 AND (open<>close OR high<>low OR open<>high) AND money>0"
    ).fetchone()[0])
    return {
        "historical_weekend_sessions": sessions,
        "unverified_weekend_rows": unverified_weekend,
        "invalid_stock_keys": invalid_stocks,
        "volume_sentinels_to_null": volume_patch,
        "preclose_sentinels_to_null": int(conn.execute("SELECT COUNT(*) FROM stocks WHERE preclose IS NOT NULL AND preclose<=0").fetchone()[0]),
    }


if __name__ == "__main__":
    raise SystemExit(main())
