# -*- coding: utf-8 -*-
"""Generate a comprehensive, read-only health report for the canonical CN DuckDB.

The report focuses on PTrade/SimTradeLab serving coverage:
* instrument universe (stocks / ETF-like / index-like / fund-like metadata)
* daily market bars, valuation, fundamentals, exrights, calendar and status
* time range, active-universe coverage ratios, and stale/missing symbols
* detectable data errors (schema, duplicates, nulls, OHLC inconsistencies,
  impossible prices, negative volume/money, date gaps, metadata hygiene)
* auto-repairability classification for each detected issue

By default the script does not mutate the database. ``--json-output`` writes a
machine-readable report and ``--markdown-output`` writes a human-readable
summary. ``--strict`` exits non-zero when any error-severity anomaly is found.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

HERE = Path(__file__).resolve().parent
SIMDATA = HERE.parent
sys.path.insert(0, str(SIMDATA))

from simtradedata.utils.paths import DUCKDB_PATH  # noqa: E402

DEFAULT_DB = Path(DUCKDB_PATH)
DEFAULT_EXPORT = SIMDATA / "data" / "cn"

STOCK_COLS = [
    "date", "open", "close", "high", "low", "high_limit", "low_limit",
    "preclose", "volume", "money",
]
CN_PREFIXES = {
    "000", "001", "002", "003", "300", "301", "302",
    "600", "601", "603", "605", "688", "689",
}
STOCK_SUFFIX_PREFIXES = {
    "SS": ("600", "601", "603", "605", "688", "689"),
    "SZ": ("000", "001", "002", "003", "300", "301", "302"),
}
ETF_PREFIXES_S = ("510", "511", "512", "513", "515", "516", "518", "588", "560", "561", "562", "563")
ETF_PREFIXES_Z = ("159", "160", "161", "162", "163", "164", "165", "166", "167", "168")
INDEX_PREFIXES_S = ("000",)
INDEX_PREFIXES_Z = ("399",)

# PTrade-like data domains we can assess from the local package. "most complete"
# is not claimed: this is an inventory, not a comparison with a live PTrade
# platform entitlement.
DOMAINS = {
    "daily_market_bars": "stocks",
    "valuation": "valuation",
    "fundamentals": "fundamentals",
    "exrights": "exrights",
    "stock_status": "stock_status",
    "index_constituents": "index_constituents",
    "money_flow": "money_flow",
    "margin_trading": "margin_trading",
    "lhb": "lhb",
    "benchmark": "benchmark",
    "trade_days": "trade_days",
    "stock_metadata": "stock_metadata",
}

# Only this many missing/stale symbols per domain are written to the JSON;
# exact counts are always reported alongside the samples.
COVERAGE_EXAMPLE_LIMIT = 20


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)[:10]


def _canonical_floor_text(inventory: dict[str, Any]) -> str:
    env = os.environ.get("SIMTRADE_CN_HISTORY_START")
    if env:
        try:
            parsed = date.fromisoformat(env)
            if parsed.isoformat() == env:
                return env
        except ValueError:
            pass
    return _date_text(inventory.get("stocks", {}).get("date_min")) or "2015-01-05"


def _pct(n: int | float, d: int | float) -> float:
    return round(float(n) / float(d), 6) if d else 0.0


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
        [table],
    ).fetchone()[0])


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=?",
        [table],
    ).fetchall()}


def _table_inventory(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """One aggregate query per table for rows and every date-column range.

    COUNT(*)/COUNT(DISTINCT date)/MIN/MAX used to be recomputed by
    ``_date_coverage`` for each domain. Computing them here lets the domain
    inventory reuse the values instead of rescanning the big tables.
    """
    tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()]
    out: dict[str, Any] = {}
    for table in tables:
        cols = [r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
            [table],
        ).fetchall()]
        expressions = ["COUNT(*)"]
        labels = ["rows"]
        for date_col in ("date", "listed_date", "last_seen_date"):
            cast = f"TRY_CAST({date_col} AS DATE)" if date_col in cols else "NULL::DATE"
            expressions.extend([f"MIN({cast})", f"MAX({cast})"])
            labels.extend([f"{date_col}_min", f"{date_col}_max"])
            if date_col == "date":
                expressions.append(f"COUNT(DISTINCT {cast})")
                labels.append("date_distinct")
        values = dict(zip(labels, conn.execute(
            f"SELECT {', '.join(expressions)} FROM {table}"
        ).fetchone()))
        item: dict[str, Any] = {"rows": int(values["rows"]), "columns": cols}
        if "date" in cols:
            item["date_min"] = _date_text(values["date_min"])
            item["date_max"] = _date_text(values["date_max"])
            item["date_distinct"] = int(values["date_distinct"] or 0)
        if "listed_date" in cols:
            item["listed_date_min"] = _date_text(values["listed_date_min"])
            item["listed_date_max"] = _date_text(values["listed_date_max"])
        if "last_seen_date" in cols:
            item["last_seen_date_min"] = _date_text(values["last_seen_date_min"])
            item["last_seen_date_max"] = _date_text(values["last_seen_date_max"])
        out[table] = item
    return out


def _classify_symbol(symbol: str, security_type: str | None = None) -> str:
    if not isinstance(symbol, str) or "." not in symbol:
        return "other"
    code, suffix = symbol.split(".", 1)
    if len(code) != 6 or not code.isdigit():
        return "other"
    if suffix == "SS":
        if code.startswith(STOCK_SUFFIX_PREFIXES["SS"]):
            return "stock"
        if code.startswith(ETF_PREFIXES_S):
            return "etf"
        if code.startswith(INDEX_PREFIXES_S):
            return "index"
    elif suffix == "SZ":
        if code.startswith(STOCK_SUFFIX_PREFIXES["SZ"]):
            return "stock"
        if code.startswith(ETF_PREFIXES_Z):
            return "etf"
        if code.startswith(INDEX_PREFIXES_Z):
            return "index"
    st = (security_type or "").strip()
    if st in {"2", "3", "4", "5"}:
        return "fund_or_other"
    return "other"


def _metadata_universe(conn: duckdb.DuckDBPyConnection, target_date: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT symbol, stock_name, listed_date, de_listed_date, blocks, security_type, listing_status
        FROM stock_metadata
        """
    ).fetchall()
    counts: dict[str, int] = {}
    active: dict[str, list[str]] = {}
    dirty_names: list[str] = []
    blocks_missing: list[str] = []
    for symbol, name, listed, delisted, blocks, sec_type, listing_status in rows:
        kind = _classify_symbol(symbol, sec_type)
        counts[kind] = counts.get(kind, 0) + 1
        is_active = (not _date_text(delisted) or _date_text(delisted) > target_date) and (
            not _date_text(listed) or _date_text(listed) <= target_date
        ) and not str(name or "").strip("\x00").startswith("退市")
        if is_active:
            active.setdefault(kind, []).append(symbol)
        if name is None or "\x00" in str(name) or not str(name).strip("\x00 ").strip():
            dirty_names.append(symbol)
        if not blocks or "ZJHHY" not in str(blocks):
            blocks_missing.append(symbol)
    return {
        "metadata_rows": len(rows),
        "by_kind": counts,
        "active_by_kind": {k: len(v) for k, v in active.items()},
        "active_symbols": active,
        "dirty_names": dirty_names,
        "blocks_missing": blocks_missing,
        "security_type_values": dict(conn.execute(
            "SELECT COALESCE(security_type, '<NULL>'), COUNT(*) FROM stock_metadata GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()),
        "listing_status_values": dict(conn.execute(
            "SELECT COALESCE(listing_status, '<NULL>'), COUNT(*) FROM stock_metadata GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()),
    }


def _active_symbols(conn: duckdb.DuckDBPyConnection, target_date: str) -> list[str]:
    # Mirror check_integrity's active universe: security_type=1 when available,
    # otherwise standard CN prefixes, with stock_pool as a current-pool filter.
    current_pool = None
    if _table_exists(conn, "stock_pool"):
        latest_pool = _date_text(conn.execute("SELECT MAX(last_seen_date) FROM stock_pool").fetchone()[0])
        if latest_pool and latest_pool >= target_date:
            current_pool = {r[0] for r in conn.execute(
                "SELECT symbol FROM stock_pool WHERE last_seen_date >= ?", [target_date]
            ).fetchall()}
    rows = conn.execute(
        "SELECT symbol, listed_date, de_listed_date, security_type, stock_name FROM stock_metadata"
    ).fetchall()
    has_sec = any(r[3] not in (None, "") for r in rows)
    symbols = []
    for symbol, listed, delisted, sec_type, name in rows:
        if _date_text(listed) and _date_text(listed) > target_date:
            continue
        if _date_text(delisted) and _date_text(delisted) <= target_date:
            continue
        if str(name or "").strip("\x00").startswith("退市"):
            continue
        if current_pool is not None and symbol not in current_pool:
            continue
        if has_sec:
            if sec_type != "1" or not isinstance(symbol, str) or not symbol.endswith((".SS", ".SZ")):
                continue
        else:
            code, _, suffix = symbol.partition(".")
            # Match check_integrity's exchange-specific prefixes: 000xxx.SS is
            # an SSE index, not a Shenzhen main-board stock. Using the flat
            # CN_PREFIXES here produced a different (larger) active universe
            # and made blocks coverage disagree with the release gate.
            if (len(code) != 6 or suffix not in STOCK_SUFFIX_PREFIXES
                    or code[:3] not in STOCK_SUFFIX_PREFIXES[suffix]):
                continue
        symbols.append(symbol)
    return sorted(set(symbols))


def _symbol_table_coverage(conn: duckdb.DuckDBPyConnection, table: str, symbols: list[str], target_date: str) -> dict[str, Any]:
    if not _table_exists(conn, table) or not symbols:
        return {"expected": len(symbols), "covered": 0, "coverage": 0.0, "latest": 0,
                "latest_coverage": 0.0, "missing_count": len(symbols),
                "missing": symbols[:COVERAGE_EXAMPLE_LIMIT], "stale_count": 0, "stale": []}
    placeholders = ",".join(["?"] * len(symbols))
    latest = {s: _date_text(d) for s, d in conn.execute(
        f"SELECT symbol, MAX(date) FROM {table} WHERE symbol IN ({placeholders}) GROUP BY symbol",
        symbols,
    ).fetchall()}
    missing = [s for s in symbols if s not in latest]
    stale = [{"symbol": s, "max_date": latest[s]} for s in symbols if s in latest and latest[s] != target_date]
    covered = len(symbols) - len(missing)
    latest_count = sum(1 for s in symbols if latest.get(s) == target_date)
    return {
        "expected": len(symbols),
        "covered": covered,
        "coverage": _pct(covered, len(symbols)),
        "latest": latest_count,
        "latest_coverage": _pct(latest_count, len(symbols)),
        # Keep exact totals plus a handful of examples: the full stale/missing
        # symbol lists used to put tens of thousands of rows into the JSON.
        "missing_count": len(missing),
        "missing": missing[:COVERAGE_EXAMPLE_LIMIT],
        "stale_count": len(stale),
        "stale": stale[:COVERAGE_EXAMPLE_LIMIT],
    }


def _date_coverage(inventory: dict[str, Any], table: str) -> dict[str, Any]:
    """Reuse _table_inventory's COUNT/MIN/MAX/DISTINCT; no second table scan."""
    item = inventory.get(table)
    if not item or "date" not in item["columns"]:
        return {"present": False}
    return {
        "present": True,
        "rows": int(item["rows"]),
        "distinct_dates": int(item.get("date_distinct", 0)),
        "min": item.get("date_min"),
        "max": item.get("date_max"),
    }


def _valuation_orphan_stats(conn: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    """Return (raw orphans, effective orphans, HALT-excluded) counts.

    A HALT session has no trade and therefore no same-day stock bar, but the
    serving layer legitimately carries a valuation snapshot on that date. Only
    non-HALT orphans are actionable; raw/excluded counts stay in the report.
    """
    raw = int(conn.execute(
        "SELECT COUNT(*) FROM valuation v "
        "WHERE NOT EXISTS (SELECT 1 FROM stocks s WHERE s.symbol=v.symbol AND s.date=v.date)"
    ).fetchone()[0])
    if not _table_exists(conn, "stock_status"):
        return raw, raw, 0
    excluded = int(conn.execute(
        """
        WITH halted AS (
            SELECT TRY_CAST(strptime(h.date, '%Y%m%d') AS DATE) AS date,
                   unnest(regexp_extract_all(h.symbols, '"[0-9]{6}\\.[A-Z]{2}"', 0)) AS quoted
            FROM stock_status h WHERE h.status_type = 'HALT'
        ), halted_pairs AS (
            SELECT date, trim(quoted, '"') AS symbol FROM halted
        )
        SELECT COUNT(*) FROM valuation v
        WHERE NOT EXISTS (SELECT 1 FROM stocks s WHERE s.symbol=v.symbol AND s.date=v.date)
          AND EXISTS (SELECT 1 FROM halted_pairs h WHERE h.symbol=v.symbol AND h.date=v.date)
        """
    ).fetchone()[0])
    return raw, raw - excluded, excluded


def _quality_checks(conn: duckdb.DuckDBPyConnection, target_date: str, symbols: list[str], history_floor: str) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def add(name: str, status: str, severity: str, repair: str, **details: Any) -> None:
        checks.append({"name": name, "status": status, "severity": severity,
                       "auto_repair": repair, **details})

    # Required schema/contract checks.
    add("ptrade_daily_schema", "pass" if set(STOCK_COLS).issubset(_columns(conn, "stocks")) else "fail",
        "error", "rebuild_export",
        expected=STOCK_COLS, actual=sorted(_columns(conn, "stocks")))

    # One pass over stocks for duplicate keys + OHLC/price/volume + date sanity.
    # Grouping by (symbol, date) keeps the original duplicate metric (number of
    # keys with >1 rows); every other metric is a conditional count. The same
    # query returns distinct stock dates so the calendar-gap check below does
    # not need another NOT EXISTS scan of the 22M-row table.
    row = conn.execute(
        """
        WITH per_key AS (
            SELECT
                symbol, date,
                COUNT(*) AS key_rows,
                COUNT(*) FILTER (WHERE open IS NULL OR close IS NULL OR high IS NULL OR low IS NULL) AS null_ohlc,
                COUNT(*) FILTER (WHERE open <= 0 OR close <= 0 OR high <= 0 OR low <= 0) AS nonpositive_price,
                COUNT(*) FILTER (WHERE high < low OR high < GREATEST(open, close) OR low > LEAST(open, close)) AS inconsistent_ohlc,
                COUNT(*) FILTER (WHERE volume < 0 OR money < 0) AS negative_volume_money,
                COUNT(*) FILTER (WHERE volume = 0 AND (open <> close OR high <> low OR open <> high)) AS zero_volume_price_moves,
                COUNT(*) FILTER (WHERE preclose IS NOT NULL AND preclose <= 0) AS bad_preclose,
                COUNT(*) FILTER (WHERE high_limit IS NOT NULL AND low_limit IS NOT NULL AND high_limit < low_limit) AS inverted_limits,
                COUNT(*) FILTER (WHERE dayofweek(date) IN (0,6)) AS weekend,
                COUNT(*) FILTER (WHERE date > ?::DATE) AS future,
                COUNT(*) FILTER (WHERE date < ?::DATE) AS before_floor
            FROM stocks
            GROUP BY symbol, date
        )
        SELECT
            COUNT(*) FILTER (WHERE key_rows > 1),
            COALESCE(SUM(null_ohlc), 0),
            COALESCE(SUM(nonpositive_price), 0),
            COALESCE(SUM(inconsistent_ohlc), 0),
            COALESCE(SUM(negative_volume_money), 0),
            COALESCE(SUM(zero_volume_price_moves), 0),
            COALESCE(SUM(bad_preclose), 0),
            COALESCE(SUM(inverted_limits), 0),
            COALESCE(SUM(weekend), 0),
            COALESCE(SUM(future), 0),
            COALESCE(SUM(before_floor), 0),
            LIST(DISTINCT CAST(date AS DATE))
        FROM per_key
        """, [target_date, history_floor]
    ).fetchone()
    metric_labels = ["duplicate_symbol_date", "null_ohlc", "nonpositive_price",
                     "inconsistent_ohlc", "negative_volume_money",
                     "zero_volume_price_moves", "bad_preclose", "inverted_limits",
                     "weekend", "future", "before_floor"]
    counts = {label: int(value) for label, value in zip(metric_labels, row[:-1])}
    stock_dates = [value for value in (row[-1] or []) if value is not None]

    # 1991-era Saturday sessions were real trading sessions in the source
    # (continuous weekly bars with turnover and exrights). They are recorded in
    # historical_weekend_sessions by the quality-repair job; keep the raw count
    # visible in the report so excluding them is auditable, not a hidden relax.
    weekend_raw = counts["weekend"]
    weekend_excluded = 0
    if weekend_raw and _table_exists(conn, "historical_weekend_sessions"):
        weekend_excluded = int(conn.execute(
            "SELECT COUNT(*) FROM stocks WHERE dayofweek(date) IN (0,6) "
            "AND date IN (SELECT date FROM historical_weekend_sessions)"
        ).fetchone()[0])
        counts["weekend"] = weekend_raw - weekend_excluded

    add("stocks_duplicate_symbol_date", "pass" if counts["duplicate_symbol_date"] == 0 else "fail",
        "error", "dedupe_upsert", count=counts["duplicate_symbol_date"])

    labels = ["null_ohlc", "nonpositive_price", "inconsistent_ohlc", "negative_volume_money",
              "zero_volume_price_moves", "bad_preclose", "inverted_limits"]
    for label in labels:
        value = counts[label]
        add(f"stocks_{label}", "pass" if value == 0 else ("warning" if label in {"zero_volume_price_moves", "null_ohlc", "inconsistent_ohlc", "bad_preclose"} else "fail"),
            "warning" if label in {"zero_volume_price_moves", "null_ohlc", "inconsistent_ohlc", "bad_preclose"} else "error",
            "source_refresh_or_quarantine", count=value,
            note=("Legacy gap-filled/index pseudo-symbol rows can contain NULL or tiny rounding inconsistencies; review before treating as hard failures." if label in {"null_ohlc", "inconsistent_ohlc", "bad_preclose"} else None))

    add("stocks_weekend_dates", "pass" if counts["weekend"] == 0 else "warning", "warning",
        "source_refresh_or_quarantine", count=counts["weekend"],
        raw_weekend_rows=weekend_raw, excluded_historical_sessions=weekend_excluded,
        note="Weekend bars are warnings only when the date is not a verified historical weekend session; the raw count is retained for audit.")
    add("stocks_future_dates", "pass" if counts["future"] == 0 else "fail", "error",
        "delete_invalid_rows", count=counts["future"])
    add("stocks_before_canonical_floor", "pass" if counts["before_floor"] == 0 else "warning", "warning",
        "history_extension_rebuild", count=counts["before_floor"], floor=history_floor,
        note="Rows before the effective configured/current floor would be a deliberate history extension or a floor mismatch.")

    # Calendar gaps compared with trade_days, using dates collected in the
    # single stocks pass above instead of a NOT EXISTS rescan.
    if _table_exists(conn, "trade_days"):
        missing_calendar = conn.execute(
            """
            SELECT COUNT(*) FROM trade_days t
            WHERE t.date BETWEEN ?::DATE AND ?::DATE
              AND NOT EXISTS (
                  SELECT 1 FROM (SELECT unnest(?::DATE[]) AS d) s
                  WHERE s.d = t.date
              )
            """, [history_floor, target_date, stock_dates]
        ).fetchone()[0]
        add("stocks_trade_day_gaps", "pass" if int(missing_calendar) == 0 else "warning", "warning",
            "source_refresh", count=int(missing_calendar))

    # Cross consistency: valuation/stock date alignment and fundamentals publication date.
    if _table_exists(conn, "valuation"):
        orphan_raw, orphan_effective, halt_excluded = _valuation_orphan_stats(conn)
        add("valuation_orphan_rows", "pass" if orphan_effective == 0 else "warning", "warning",
            "rebuild_valuation_calendar", count=orphan_effective,
            raw_orphan_rows=orphan_raw, excluded_halt_sessions=halt_excluded,
            note="A valuation row with no same-day bar is expected on an explicit HALT session (no trading, no bar, carry-forward snapshot). Only non-HALT orphans are warnings.")
    if _table_exists(conn, "fundamentals"):
        bad_pub = conn.execute(
            "SELECT COUNT(*) FROM fundamentals WHERE publ_date IS NOT NULL AND TRY_STRPTIME(publ_date, '%Y%m%d') IS NULL"
        ).fetchone()[0]
        add("fundamentals_publdate_parseable", "pass" if int(bad_pub) == 0 else "warning", "warning",
            "normalize_publdate", count=int(bad_pub),
            note="publ_date uses compact YYYYMMDD text by design; TRY_CAST(DATE) is not the correct parser.")

    # Metadata hygiene.
    unknown_names = int(conn.execute(
        "SELECT COUNT(*) FROM stock_metadata WHERE stock_name IS NULL"
    ).fetchone()[0])
    dirty_names = conn.execute(
        "SELECT COUNT(*) FROM stock_metadata WHERE contains(stock_name, chr(0)) OR trim(stock_name)=''"
    ).fetchone()[0]
    add("stock_metadata_names_clean", "pass" if int(dirty_names) == 0 else "warning", "warning",
        "clean_metadata_names", count=int(dirty_names),
        unknown_name_rows=unknown_names,
        note="NULL is a valid unknown-name state, matching check_integrity's predicate; only NUL padding and blank strings are dirty. The NULL count is retained for audit.")
    blocks_total = len(symbols)
    blocks_ok = conn.execute(
        "SELECT COUNT(*) FROM stock_metadata WHERE symbol IN (" + ",".join(["?"] * len(symbols)) + ") AND blocks LIKE '%ZJHHY%'",
        symbols,
    ).fetchone()[0] if symbols else 0
    ratio = _pct(blocks_ok, blocks_total)
    add("stock_metadata_blocks_coverage", "pass" if ratio >= 0.95 else "fail", "error",
        "refresh_metadata_blocks", ratio=ratio, covered=int(blocks_ok), total=blocks_total)

    # Duplicate / negative / null exrights checks.
    if _table_exists(conn, "exrights"):
        ex_dup = conn.execute(
            "SELECT COUNT(*) FROM (SELECT symbol, date FROM exrights GROUP BY 1,2 HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        ex_bad = conn.execute(
            """
            SELECT COUNT(*) FROM exrights
            WHERE allotted_ps < 0 OR rationed_ps < 0 OR bonus_ps < 0 OR dividend < 0
               OR (exer_forward_a IS NOT NULL AND exer_forward_a <= 0)
            """
        ).fetchone()[0]
        add("exrights_duplicate_symbol_date", "pass" if int(ex_dup) == 0 else "warning", "warning",
            "dedupe_upsert", count=int(ex_dup))
        add("exrights_negative_or_bad_factors", "pass" if int(ex_bad) == 0 else "warning", "warning",
            "recompute_factors", count=int(ex_bad))

    # Serving view placeholders: zero-row PTrade-compatible files are expected for
    # legacy index pseudo-symbols, but should not be treated as real market data.
    return checks


def _export_inventory(export_dir: Path, conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> dict[str, Any]:
    if not export_dir.exists():
        return {"exists": False, "path": str(export_dir)}
    manifest_path = export_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    stock_files = list((export_dir / "stocks").glob("*.parquet"))
    valuation_files = list((export_dir / "valuation").glob("*.parquet"))
    fundamentals_files = list((export_dir / "fundamentals").glob("*.parquet"))
    exrights_files = list((export_dir / "exrights").glob("*.parquet"))
    one_m = list((export_dir / "stocks_1m").glob("*.parquet"))
    five_m = list((export_dir / "stocks_5m").glob("*.parquet"))
    adj = list(export_dir.glob("ptrade_adj_*.parquet"))
    stock_stems = {p.stem for p in stock_files}
    val_stems = {p.stem for p in valuation_files}
    return {
        "exists": True,
        "path": str(export_dir),
        "manifest": manifest,
        "file_counts": {
            "stocks": len(stock_files),
            "valuation": len(valuation_files),
            "fundamentals": len(fundamentals_files),
            "exrights": len(exrights_files),
            "stocks_1m": len(one_m),
            "stocks_5m": len(five_m),
            "ptrade_adj": len(adj),
        },
        "active_stock_files_present": len(set(symbols) & stock_stems),
        "active_valuation_files_present": len(set(symbols) & val_stems),
        "active_stock_files_coverage": _pct(len(set(symbols) & stock_stems), len(symbols)),
        "active_valuation_files_coverage": _pct(len(set(symbols) & val_stems), len(symbols)),
        "qmt_only_assets_preserved": {
            "stocks_1m": len(one_m),
            "stocks_5m": len(five_m),
            "ptrade_adj": len(adj),
        },
    }


def build_report(db_path: Path, export_dir: Path, target_date: str | None = None) -> dict[str, Any]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        target_date = target_date or _date_text(conn.execute("SELECT MAX(date) FROM stocks").fetchone()[0])
        assert target_date
        inventory = _table_inventory(conn)
        history_floor = _canonical_floor_text(inventory)
        stocks_start = (inventory.get("stocks") or {}).get("date_min")
        # Without a configured floor the effective floor IS the current stocks
        # minimum, so a rebuild cannot move the start earlier by itself.
        can_push_earlier = bool(stocks_start and str(stocks_start) > str(history_floor))
        meta = _metadata_universe(conn, target_date)
        symbols = _active_symbols(conn, target_date)
        domains: dict[str, Any] = {}
        for domain, table in DOMAINS.items():
            info = {"table": table, "present": table in inventory, "date": _date_coverage(inventory, table)}
            if table in {"stocks", "valuation", "fundamentals", "exrights"}:
                info["active_symbol_coverage"] = _symbol_table_coverage(conn, table, symbols, target_date)
            domains[domain] = info
        checks = _quality_checks(conn, target_date, symbols, history_floor)
        export = _export_inventory(export_dir, conn, symbols)
        errors = [c for c in checks if c["status"] == "fail" and c["severity"] == "error"]
        warnings = [c for c in checks if c["status"] != "pass" and c["severity"] == "warning"]
        report = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "canonical_db": str(db_path),
            "serving_view": str(export_dir),
            "target_date": target_date,
            "status": "fail" if errors else ("warning" if warnings else "pass"),
            "scope_note": "Local PTrade/SimTradeLab-compatible CN package health audit. It reports what is present locally; it cannot prove parity with a live PTrade platform entitlement.",
            "ptrade_domain_catalog": {
                "covered_locally": [
                    "stocks", "etf_or_fund_like_metadata_and_bars_when_available", "daily OHLCV",
                    "valuation", "fundamentals", "exrights", "benchmark", "trade_days",
                    "stock_metadata", "stock_status", "index_constituents", "minute_bars_serving_assets",
                ],
                "known_empty_tables": ["money_flow", "margin_trading", "lhb"],
                "not_claimable": [
                    "live PTrade platform entitlement completeness", "all ETF histories",
                    "all fund/share classes", "all fundamental fields from PTrade", "real-time data",
                    "tick/level-2 data", "corporate action announcements beyond exrights",
                ],
            },
            "table_inventory": inventory,
            "instrument_universe": {
                "metadata_rows": meta["metadata_rows"],
                "by_kind": meta["by_kind"],
                "active_by_kind": meta["active_by_kind"],
                "security_type_values": meta["security_type_values"],
                "listing_status_values": meta["listing_status_values"],
                "strict_active_universe": len(symbols),
                "strict_active_universe_note": "Mirrors check_integrity active CN stock universe; includes legacy 000xxx.SS index pseudo-symbols when security_type=1 metadata is present.",
            },
            "domains": domains,
            "quality_checks": checks,
            "quality_summary": {
                "checks": len(checks),
                "errors": len(errors),
                "warnings": len(warnings),
                "passes": sum(1 for c in checks if c["status"] == "pass"),
                "auto_repairable": sorted({c["auto_repair"] for c in checks if c["status"] != "pass"}),
            },
            "quality_predicates": {
                "weekend_sessions": "dayofweek IN (0,6) AND date NOT IN historical_weekend_sessions (verified 1991 real Saturday sessions)",
                "valuation_orphans": "no same-day stock bar AND the day is not an explicit HALT session",
                "metadata_names": "NUL padding or blank only; NULL is a valid unknown-name state (check_integrity parity)",
                "transparency": "raw counts and exclusion counts are retained on each check, so a pass cannot hide excluded rows",
            },
            "export_inventory": export,
            "history_extension": {
                "effective_floor": history_floor,
                "current_start": inventory.get("stocks", {}).get("date_min"),
                "current_end": inventory.get("stocks", {}).get("date_max"),
                "metadata_listed_date_min": inventory.get("stock_metadata", {}).get("listed_date_min"),
                "exrights_min": inventory.get("exrights", {}).get("date_min"),
                "can_push_earlier": can_push_earlier,
                "how": [
                    f"Use unified_data_manager.py --apply --full-history --history-start {history_floor} for an internet-first TDX/Mootdx/BaoStock rebuild; the integrity gate enforces the configured floor.",
                    "The bulk rebuild uses TDX hsjday.zip for long OHLCV history, Mootdx for XDXR/fundamentals, and BaoStock for valuation/status/calendar; QMT is only an optional local cross-check.",
                    "Keep exrights events before the floor when rebuilding: adjusted-price calculation for later bars needs historical corporate actions.",
                ],
                "caveats": [
                    "The effective floor is dynamic: SIMTRADE_CN_HISTORY_START when configured, otherwise the current stocks minimum date. A full rebuild is required to move it earlier.",
                    "Pre-2015 coverage and quality depend on source availability (TDX daily packages, BaoStock history, Mootdx XDXR/fundamentals); ETF and financial history are usually less complete than stock OHLCV.",
                ],
            },
            "repair_capabilities": {
                "implemented_now": [
                    "unified_data_manager.py: metadata NUL/blank name cleanup, serving metadata promotion, QMT cross-check append, trade_days/benchmark/valuation calendar carry-forward, staged export promotion, strict gate, PTrade smoke",
                    "check_integrity.py: fail-closed release gate for required tables, active universe, metadata hygiene, benchmark floor, blocks coverage, export file presence",
                    "data_health_report.py: read-only anomaly inventory and auto-repair classification",
                ],
                "requires_source_refresh": [
                    "wrong OHLC/close prices", "missing real trading days", "newer bars beyond source latest",
                    "missing fundamentals/valuation fields", "ETF universe extensions", "sparse 1990s domain coverage (source-limited)",
                ],
                "not_auto_repaired_safely": [
                    "fabricating values for missing market/fundamental data",
                    "choosing among conflicting authoritative sources without a trusted source",
                    "repairing corporate-action semantics without source events",
                ],
            },
        }
        return report
    finally:
        conn.close()


def _markdown(report: dict[str, Any]) -> str:
    lines = []
    lines.append(f"# 数据健康度报告（{report['target_date']}）")
    lines.append("")
    lines.append(f"- 生成时间：{report['generated_at']}")
    lines.append(f"- 总体状态：`{report['status']}`")
    lines.append(f"- 权威库：`{report['canonical_db']}`")
    lines.append(f"- 服务视图：`{report['serving_view']}`")
    lines.append(f"- 严格活跃股票 universe：{report['instrument_universe']['strict_active_universe']:,}")
    lines.append("")
    lines.append("> [!NOTE]")
    lines.append("> 本报告审计本地 PTrade/SimTradeLab 兼容数据包的覆盖与可检测错误；不能证明与线上 PTrade 平台权限/全量数据完全等价。")
    lines.append("")
    lines.append("## 数据域覆盖")
    lines.append("")
    lines.append("| 数据域 | 表 | 行数 | 日期范围 | 活跃标的覆盖 | 目标日覆盖 |")
    lines.append("|---|---:|---:|---|---:|---:|")
    for name, info in report["domains"].items():
        inv = report["table_inventory"].get(info["table"], {})
        rows = inv.get("rows", 0)
        d = info.get("date") or {}
        rng = f"{d.get('min') or '-'} → {d.get('max') or '-'}" if d.get("present") else "-"
        cov = info.get("active_symbol_coverage") or {}
        covtxt = f"{cov.get('coverage', 0):.2%}" if cov else "-"
        latesttxt = f"{cov.get('latest_coverage', 0):.2%}" if cov else "-"
        lines.append(f"| {name} | `{info['table']}` | {rows:,} | {rng} | {covtxt} | {latesttxt} |")
    lines.append("")
    lines.append("## 标的类别（按代码/元数据启发式）")
    lines.append("")
    lines.append("| 类别 | metadata 行数 | 活跃行数 |")
    lines.append("|---|---:|---:|")
    for kind in sorted(report["instrument_universe"]["by_kind"]):
        lines.append(f"| {kind} | {report['instrument_universe']['by_kind'][kind]:,} | {report['instrument_universe']['active_by_kind'].get(kind, 0):,} |")
    lines.append("")
    lines.append("## 服务视图文件")
    lines.append("")
    fc = report["export_inventory"].get("file_counts", {})
    lines.append("| 资产 | 文件数 |")
    lines.append("|---|---:|")
    for k, v in fc.items():
        lines.append(f"| {k} | {v:,} |")
    lines.append("")
    lines.append("## 质量检查")
    lines.append("")
    lines.append(f"- checks: {report['quality_summary']['checks']}")
    lines.append(f"- passes: {report['quality_summary']['passes']}")
    lines.append(f"- errors: {report['quality_summary']['errors']}")
    lines.append(f"- warnings: {report['quality_summary']['warnings']}")
    lines.append("")
    lines.append("| 检查 | 状态 | 严重度 | 数量/比例 | 自动修复方向 |")
    lines.append("|---|---|---|---|---|")
    for c in report["quality_checks"]:
        metric = c.get("count", c.get("ratio", ""))
        lines.append(f"| {c['name']} | `{c['status']}` | {c['severity']} | {metric} | `{c['auto_repair']}` |")
    lines.append("")
    ext = report["history_extension"]

    def domain_start(name: str) -> str:
        info = report["domains"].get(name) or {}
        return (info.get("date") or {}).get("min") or "-"

    lines.append("## 历史起点与可前推边界")
    lines.append("")
    lines.append(
        f"- 各数据域实际起点：日线 `{domain_start('daily_market_bars')}`、"
        f"估值 `{domain_start('valuation')}`、财务 `{domain_start('fundamentals')}`、"
        f"交易日历 `{domain_start('trade_days')}`、benchmark `{domain_start('benchmark')}`；"
        f"日线终点 `{ext['current_end']}`，有效历史下限 `{ext['effective_floor']}`。"
    )
    lines.append(f"- metadata 最早上市日：`{ext['metadata_listed_date_min']}`；exrights 最早事件：`{ext['exrights_min']}`。")
    if ext["can_push_earlier"]:
        lines.append(
            f"- 日线起点晚于有效下限，可以重建：`unified_data_manager.py --apply --full-history "
            f"--history-start {ext['effective_floor']}` 用互联网 TDX/Mootdx/BaoStock 全量源重建；不是增量补几天。"
        )
    else:
        lines.append(
            f"- 日线起点已到有效下限 `{ext['effective_floor']}`；`--full-history` 只能重建到该 floor，"
            "再往前需要更早的权威源。估值/财务/benchmark/ETF 等域起点不同，不受日线 floor 保证。"
        )
    lines.append("- QMT 本地 DAT 只作为可选交叉校验/补充；即使不刷新 QMT，也不应阻塞默认互联网更新或全历史重建。")
    lines.append("")
    lines.append("## 自动修复边界")
    lines.append("")
    lines.append("- 已实现：元数据清洗、日历/估值占位补齐、QMT 追加与交叉校验、分阶段导出、严格门禁、PTrade smoke。")
    lines.append("- 需要源刷新：错误 OHLC、缺失真实交易日、新增日期、缺失财务/估值因子、ETF 扩展、90 年代各域稀疏覆盖（受上游源起点限制）。")
    lines.append("- 不应自动修复：伪造行情/财务值、无权威依据时裁决冲突源、无公司行动事件时改复权语义。")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate comprehensive CN data health report")
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT))
    parser.add_argument("--target-date", default=None)
    parser.add_argument("--json-output", default=None)
    parser.add_argument("--markdown-output", default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    report = build_report(Path(args.db_path), Path(args.export_dir), args.target_date)
    if args.json_output:
        p = Path(args.json_output)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    if args.markdown_output:
        p = Path(args.markdown_output)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(_markdown(report), encoding="utf-8")
    if args.json or not (args.json_output or args.markdown_output):
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    if args.strict and report["status"] == "fail":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
