# -*- coding: utf-8 -*-
"""One-command maintenance for the single canonical SimTradeData dataset.

This module solves the local data-layer split:

* ``data/cn.duckdb`` is the authoritative relational store.
* ``data/cn`` is the PTrade/SimTradeLab-compatible serving view.
* QMT local DAT files and maintained OSS A-share tools are *sources*, never
  independent datasets.

The command is idempotent and dry-run by default.  With ``--apply`` it:

1. Runs the pre-publish data quality gate and CHECKPOINTs the WAL; no
   byte-level DuckDB/manifest snapshot is kept.
2. Repairs canonical metadata (NUL padding, missing industry blocks).
3. Uses internet/public sources first to discover the latest real bar date and
   fill active-symbol gaps. QMT local DAT is optional and used only as a
   cross-check/supplement fallback; its freshness never caps the target date.
4. Calls SmartRouter for remaining active-symbol gaps. The route table includes
   first-party public/EastMoney/BaoStock sources and optional AKShare/ADATA/
   efinance adapters from maintained GitHub projects.
5. Rebuilds a staged PTrade Parquet export from DuckDB, then atomically promotes
   only table-owned directories/files. QMT-only minute bars and adjustment
   caches in ``data/cn`` are preserved.
6. Runs the strict integrity gate and a PTrade storage/API smoke check. A failed
   gate prevents the serving view from being marked as published.

Usage:
    python scripts/unified_data_manager.py                 # full dry-run audit
    python scripts/unified_data_manager.py --apply         # internet-first update
    python scripts/unified_data_manager.py --apply --no-oss
    python scripts/unified_data_manager.py --apply --no-qmt
    python scripts/unified_data_manager.py --apply --full-history --history-start 1990-12-19
    python scripts/unified_data_manager.py --validate-only
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import logging
import os
import shutil
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
SIMDATA = HERE.parent
ROOT = SIMDATA.parents[1]
sys.path.insert(0, str(SIMDATA))

from simtradedata.importers.qmt_kline import decode_kline_dat, to_daily_frame  # noqa: E402
from simtradedata.utils.paths import DUCKDB_PATH  # noqa: E402
from simtradedata.writers.duckdb_writer import DuckDBWriter  # noqa: E402

LOGGER = logging.getLogger("unified_data_manager")
DB_PATH = Path(DUCKDB_PATH)
DATA_ROOT = SIMDATA / "data"
CN_DIR = DATA_ROOT / "cn"
META_DIR = CN_DIR / "metadata"
QMT_ROOT = Path(os.environ.get("SIMTRADE_QMT_ROOT", r"D:\君弘君智交易系统"))
QMT_DATADIR = QMT_ROOT / "datadir" if (QMT_ROOT / "datadir").is_dir() else QMT_ROOT
REPORT_DIR = SIMDATA / "ops" / "unified_data"
PUBLISH_STATE_PATH = REPORT_DIR / "latest_publish_state.json"
_FINGERPRINT_TABLES = (
    "stocks", "valuation", "fundamentals", "exrights",
    "benchmark", "trade_days", "stock_metadata",
    "stock_status", "index_constituents",
)
STOCK_COLS = [
    "date", "open", "close", "high", "low", "high_limit", "low_limit",
    "preclose", "volume", "money",
]
ACTIVE_PREFIXES = {
    "SS": ("600", "601", "603", "605", "688", "689"),
    "SZ": ("000", "001", "002", "003", "300", "301", "302"),
}
CN_FALLBACK_PREFIXES = {
    "000", "001", "002", "003", "300", "301", "302",
    "600", "601", "603", "605", "688", "689",
}
OSS_PROJECTS = {
    "akshare": "https://github.com/akfamily/akshare",
    "adata": "https://github.com/1nchaos/adata",
    "efinance": "https://github.com/Micro-sheep/efinance",
}
FIRST_PARTY_PROJECTS = {
    "baostock": "https://github.com/baostock/baostock",
    "mootdx": "https://github.com/mootdx/mootdx",
    "tushare_optional": "https://github.com/waditu/tushare",
}


# ─────────────────────────────────────────────────────────────────────────────
# Generic helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now()


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, pd.Timestamp, np.generic)):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    tmp.replace(path)


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


def _is_cn_symbol(symbol: str) -> bool:
    """Strict gate universe: standard CN A-share and legacy index-code prefixes."""
    if not isinstance(symbol, str) or "." not in symbol:
        return False
    code, suffix = symbol.split(".", 1)
    return len(code) == 6 and suffix in {"SZ", "SS"} and code[:3] in CN_FALLBACK_PREFIXES


def _is_cn_stock(symbol: str) -> bool:
    """Actual A-share stock universe, excluding SH index pseudo-symbols."""
    if not isinstance(symbol, str) or "." not in symbol:
        return False
    code, suffix = symbol.split(".", 1)
    return len(code) == 6 and code.isdigit() and code.startswith(ACTIVE_PREFIXES.get(suffix, ()))


def _latest_trade_day(conn: duckdb.DuckDBPyConnection) -> date:
    row = conn.execute("SELECT MAX(date) FROM stocks").fetchone()
    if row and row[0]:
        return pd.Timestamp(row[0]).date()
    return date(2015, 1, 5)


def _expected_latest_day(today: date | None = None) -> date:
    """Latest weekday no later than today; exchange holidays are source-driven."""
    day = today or _now().date()
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def _parse_iso_date(value: str | None, label: str) -> date | None:
    if not value:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {value}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{label} must be canonical YYYY-MM-DD: {value}")
    return parsed


def _canonical_floor(conn: duckdb.DuckDBPyConnection) -> date:
    """Effective released-data floor; used by calendar and history QA."""
    env_floor = _parse_iso_date(os.environ.get("SIMTRADE_CN_HISTORY_START"), "SIMTRADE_CN_HISTORY_START")
    if env_floor:
        return env_floor
    row = conn.execute("SELECT MIN(date) FROM stocks").fetchone()
    if row and row[0]:
        return min(pd.Timestamp(row[0]).date(), date(2015, 1, 5))
    return date(2015, 1, 5)


def _router_config(use_oss: bool) -> dict[str, Any]:
    from simtradedata.router.route_config import DEFAULT_ROUTE_TABLE

    config = {k: dict(v) for k, v in DEFAULT_ROUTE_TABLE.items()}
    config["daily_bars"] = dict(config["daily_bars"])
    sources = list(config["daily_bars"]["cn"])
    if not use_oss:
        sources = [s for s in sources if s not in OSS_PROJECTS]
    config["daily_bars"]["cn"] = sources
    return config


def discover_internet_latest(
    use_oss: bool,
    expected: date,
    lookback_days: int = 30,
) -> dict[str, Any]:
    """Discover the newest real daily bar date from a bounded probe set.

    This never fabricates bars. The expected weekday is only an upper bound; the
    returned source_latest is the newest date actually observed from network
    sources for liquid/index probe symbols.
    """
    from simtradedata.router.smart_router import SmartRouter

    probes = ["000001.SS", "000300.SS", "399001.SZ", "000001.SZ", "600519.SS"]
    start = (expected - timedelta(days=lookback_days)).isoformat()
    end = expected.isoformat()
    result: dict[str, Any] = {
        "enabled": True,
        "expected_date": expected.isoformat(),
        "lookback_days": lookback_days,
        "start": start,
        "end": end,
        "probes": probes,
        "oss_enabled": use_oss,
        "source_latest": None,
        "by_symbol": {},
        "errors": [],
    }
    config = _router_config(use_oss)
    observed: list[date] = []
    with SmartRouter(config=config) as router:
        for symbol in probes:
            try:
                raw = router.get_daily_bars(symbol, start, end)
                frame = _normalize_router_frame(raw, symbol, "target_probe")
                if frame.empty:
                    result["by_symbol"][symbol] = {"rows": 0, "last_date": None}
                    continue
                last = pd.Timestamp(frame["date"].max()).date()
                observed.append(last)
                result["by_symbol"][symbol] = {"rows": len(frame), "last_date": last.isoformat()}
            except Exception as exc:  # noqa: BLE001
                result["errors"].append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
    if observed:
        result["source_latest"] = max(observed).isoformat()
    result["status"] = "pass" if result["source_latest"] else "no_internet_bars"
    return result


def _ensure_ledger(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS unified_source_ledger (
            run_id VARCHAR,
            source VARCHAR,
            source_kind VARCHAR,
            symbol VARCHAR,
            rows_written BIGINT,
            first_date DATE,
            last_date DATE,
            checksum VARCHAR,
            status VARCHAR,
            detail VARCHAR,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (run_id, source, symbol)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS unified_runs (
            run_id VARCHAR PRIMARY KEY,
            mode VARCHAR,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR,
            target_date DATE,
            report_path VARCHAR,
            db_sha256 VARCHAR,
            notes VARCHAR
        )
    """)


def _ledger(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    source: str,
    kind: str,
    symbol: str,
    frame: pd.DataFrame,
    status: str,
    detail: str = "",
) -> None:
    if frame.empty:
        first = last = None
        rows = 0
        checksum = ""
    else:
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        first = dates.min().date() if len(dates) else None
        last = dates.max().date() if len(dates) else None
        rows = len(frame)
        checksum = hashlib.sha256(
            pd.util.hash_pandas_object(frame[["date", "open", "high", "low", "close", "volume", "money"]], index=False).values.tobytes()
        ).hexdigest()[:32]
    conn.execute("""
        INSERT OR REPLACE INTO unified_source_ledger
        (run_id, source, source_kind, symbol, rows_written, first_date, last_date, checksum, status, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, source, kind, symbol, rows, first, last, checksum, status, detail[:1000]])


# ─────────────────────────────────────────────────────────────────────────────
# Canonical metadata repair
# ─────────────────────────────────────────────────────────────────────────────

def repair_metadata(conn: duckdb.DuckDBPyConnection, apply: bool) -> dict[str, Any]:
    """Align canonical metadata with the already-patched PTrade serving view.

    The serving view is the contract consumed by SimTradeLab.  QMT patch jobs
    have already repaired its stock names, security types, listed dates and
    ZJHHY industry blocks.  Promoting those columns into DuckDB keeps the
    authoritative store and the PTrade-compatible export identical.
    """
    result: dict[str, Any] = {
        "apply": apply,
        "names_cleaned": 0,
        "blocks_filled": 0,
        "serving_rows": 0,
    }
    meta_path = META_DIR / "stock_metadata.parquet"
    if not meta_path.is_file():
        result["source_error"] = f"missing {meta_path}"
        return result

    source = pd.read_parquet(meta_path)
    result["serving_rows"] = len(source)
    if "symbol" not in source.columns:
        result["source_error"] = "serving metadata has no symbol column"
        return result

    cols = _columns(conn, "stock_metadata")
    if "stock_name" in cols:
        dirty = conn.execute("""
            SELECT COUNT(*) FROM stock_metadata
            WHERE contains(stock_name, chr(0)) OR trim(stock_name) = ''
        """).fetchone()[0]
        result["names_dirty_before"] = int(dirty)
        if apply and dirty:
            conn.execute("""
                UPDATE stock_metadata
                SET stock_name = NULLIF(trim(replace(stock_name, chr(0), '')), '')
            """)
            result["names_cleaned"] = int(dirty)

    # Serving-view rows can legitimately contain NUL-padded ST names. Clean the
    # in-memory copy before promoting it back into DuckDB, otherwise export can
    # reintroduce the same dirty names from the serving metadata source.
    if "stock_name" in source.columns:
        before_nul = int(source["stock_name"].astype("string").str.contains("\x00", na=False).sum())
        cleaned = source["stock_name"].astype("string").str.replace("\x00", "", regex=False).str.strip()
        source["stock_name"] = cleaned.mask(cleaned.eq(""), None)
        result["serving_names_nul_before"] = before_nul
        result["serving_names_cleaned"] = before_nul if apply else 0

    if "blocks" in cols:
        result["blocks_zjhhy_before"] = int(conn.execute("""
            SELECT COUNT(*) FROM stock_metadata WHERE blocks LIKE '%ZJHHY%'
        """).fetchone()[0])

    shared = [c for c in ("stock_name", "listed_date", "de_listed_date", "blocks", "security_type")
              if c in cols and c in source.columns]
    result["shared_columns"] = shared
    if not shared:
        return result

    src = source[["symbol", *shared]].copy()
    for col in shared:
        src[col] = src[col].astype("string")
    src = src.drop_duplicates("symbol", keep="last")
    conn.register("_serving_metadata", src)
    assignments = ", ".join(f"{c} = s.{c}" for c in shared)
    matched = conn.execute("""
        SELECT COUNT(*) FROM stock_metadata m
        JOIN _serving_metadata s USING (symbol)
    """).fetchone()[0]
    result["matched_rows"] = int(matched)
    if apply and matched:
        conn.execute(f"""
            UPDATE stock_metadata m
            SET {assignments}
            FROM _serving_metadata s
            WHERE m.symbol = s.symbol
        """)
        result["serving_columns_promoted"] = int(matched)
    conn.unregister("_serving_metadata")

    if "blocks" in cols:
        empty = conn.execute("""
            SELECT COUNT(*) FROM stock_metadata
            WHERE blocks IS NULL OR trim(CAST(blocks AS VARCHAR)) IN ('', 'None', 'nan', 'NULL')
        """).fetchone()[0]
        zjhhy = conn.execute("""
            SELECT COUNT(*) FROM stock_metadata WHERE blocks LIKE '%ZJHHY%'
        """).fetchone()[0]
        result["blocks_empty_after"] = int(empty)
        result["blocks_zjhhy_after"] = int(zjhhy)
        result["blocks_filled"] = max(0, int(zjhhy) - result.get("blocks_zjhhy_before", 0))
    return result


# ─────────────────────────────────────────────────────────────────────────────
# QMT local data
# ─────────────────────────────────────────────────────────────────────────────

def _qmt_daily_files(markets: Iterable[str] = ("SH", "SZ")) -> list[tuple[str, Path]]:
    """All QMT daily DAT files that can enrich the canonical store.

    The serving view contains stocks, funds and indices.  A-share prefixes are
    always included; non-A-share symbols are included only when the PTrade
    metadata whitelist already knows them.  This avoids importing QMT-only
    bonds/repo instruments while still preserving benchmark/index bars.
    """
    out: list[tuple[str, Path]] = []
    suffix_by_market = {"SH": "SS", "SZ": "SZ", "BJ": "BJ"}
    whitelist: set[str] | None = None
    meta_path = META_DIR / "stock_metadata.parquet"
    if meta_path.is_file():
        try:
            md = pd.read_parquet(meta_path, columns=["symbol", "security_type"])
            whitelist = set(
                md.loc[md["security_type"].isin(["stock", "fund", "index"]), "symbol"].astype(str)
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Could not read serving metadata whitelist: %s", exc)
    for market in markets:
        folder = QMT_DATADIR / market / "86400"
        if not folder.is_dir():
            continue
        suffix = suffix_by_market.get(market)
        for path in sorted(folder.glob("*.DAT")):
            code = path.stem[-6:]
            if not (len(code) == 6 and code.isdigit() and suffix):
                continue
            symbol = f"{code}.{suffix}"
            if _is_cn_stock(symbol) or (whitelist is not None and symbol in whitelist):
                out.append((symbol, path))
    return out


def _qmt_frame(path: Path, symbol: str, start: date, history_start: date) -> pd.DataFrame:
    """Decode one DAT file and keep rows from the canonical history window."""
    bars = decode_kline_dat(path)
    if bars.empty:
        return pd.DataFrame(columns=STOCK_COLS)
    frame = to_daily_frame(bars, symbol)
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    # QMT DAT is a rolling local cache. Import from the effective package floor,
    # not just from the DB's current tail, so benchmark/index history can also
    # be reconciled and backfilled when needed.
    frame = frame[frame["date"] >= pd.Timestamp(history_start)].copy()
    frame = frame[(frame["close"] > 0) & (frame["volume"] > 0) & (frame["money"] > 0)]
    for col in ("open", "close", "high", "low", "volume", "money"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["open", "close", "high", "low", "volume", "money"])
    frame = frame[(frame["high"] >= frame["low"]) &
                  (frame["close"] <= frame["high"] + 1e-6) &
                  (frame["close"] >= frame["low"] - 1e-6)]
    return frame.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)


def _existing_closes(conn: duckdb.DuckDBPyConnection, symbol: str, start: date) -> pd.DataFrame:
    """Existing closes for cross-checking; excludes future rows beyond the DB tail."""
    return conn.execute("""
        SELECT date, close FROM stocks
        WHERE symbol=? AND date>=? ORDER BY date
    """, [symbol, start]).fetchdf()


def _cross_check(incoming: pd.DataFrame, existing: pd.DataFrame, tolerance: float = 0.005) -> dict[str, Any]:
    if existing.empty or incoming.empty:
        return {"overlap": 0, "agree": 0, "conflict": 0, "ok": True, "reason": "no_overlap"}
    merged = incoming[["date", "close"]].merge(existing[["date", "close"]], on="date", suffixes=("_q", "_d"))
    if merged.empty:
        return {"overlap": 0, "agree": 0, "conflict": 0, "ok": True, "reason": "no_overlap"}
    denom = merged["close_d"].abs().replace(0, np.nan)
    rel = ((merged["close_q"] - merged["close_d"]).abs() / denom).fillna(np.inf)
    conflict = int((rel > tolerance).sum())
    ok = len(merged) < 20 or conflict <= max(2, int(len(merged) * 0.01))
    return {
        "overlap": len(merged),
        "agree": len(merged) - conflict,
        "conflict": conflict,
        "ok": ok,
        "max_rel_diff": float(rel.replace(np.inf, np.nan).max() or 0),
    }


def import_qmt_to_db(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    apply: bool,
    limit: int = 0,
) -> dict[str, Any]:
    """Merge QMT daily bars into DuckDB as append-only new dates."""
    # Cross-check only the overlap that already exists in DuckDB. New dates are
    # append-only; this avoids rejecting an entire QMT file because the local
    # DAT cache has more history than the current canonical DB.
    start = _latest_trade_day(conn)
    history_start = _canonical_floor(conn)
    files = _qmt_daily_files()
    if limit:
        files = files[:limit]
    stats: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    total_rows = 0
    source_dates: set[date] = set()

    # One bulk projection replaces the per-file SELECT that used to deserialize
    # ~4k existing rows into pandas once per DAT file. Stream it in bounded
    # 2048-row chunks and retain only the symbols we will visit, so the symbol
    # object column is never materialised for the whole stocks table.
    wanted = {sym for sym, _ in files}
    existing_parts: dict[str, list[pd.DataFrame]] = {}
    empty_existing = pd.DataFrame(columns=["date", "close"])
    if wanted:
        conn.execute("SELECT symbol, date, close FROM stocks WHERE date >= ?", [history_start])
        try:
            while True:
                chunk = conn.fetch_df_chunk(1)
                if chunk.empty:
                    break
                for sym, group in chunk.groupby("symbol", sort=False):
                    key = str(sym)
                    if key in wanted:
                        existing_parts.setdefault(key, []).append(group[["date", "close"]])
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("bulk existing-closes preload failed: %s", exc)
            existing_parts = {}
    existing_cache: dict[str, pd.DataFrame] = {
        sym: pd.concat(parts, ignore_index=True)
        for sym, parts in existing_parts.items()
    }
    del existing_parts

    pending_adds: list[pd.DataFrame] = []
    ledger_rows: list[tuple] = []

    def queue_ledger(sym: str, frame: pd.DataFrame, status: str, detail: str = "") -> None:
        # Mirrors _ledger's row shape, but is deferred so every symbol shares a
        # single INSERT. Upstream records source attempts even in dry-run, so
        # accumulation is not gated here; only the stocks INSERT is.
        if frame.empty:
            first = last = None
            rows = 0
            checksum = ""
        else:
            dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
            first = dates.min().date() if len(dates) else None
            last = dates.max().date() if len(dates) else None
            rows = len(frame)
            checksum = hashlib.sha256(
                pd.util.hash_pandas_object(
                    frame[["date", "open", "high", "low", "close", "volume", "money"]], index=False
                ).values.tobytes()
            ).hexdigest()[:32]
        ledger_rows.append((
            run_id, "qmt_local", "local_binary", sym, rows, first, last,
            checksum, status, detail[:1000],
        ))

    for symbol, path in files:
        try:
            frame = _qmt_frame(path, symbol, start, history_start)
            if frame.empty:
                stats["empty"] += 1
                continue
            source_dates.update(d.date() for d in pd.to_datetime(frame["date"]))
            existing = existing_cache.get(symbol, empty_existing)
            overlap_frame = frame[frame["date"] <= pd.Timestamp(start)].copy()
            check = _cross_check(overlap_frame, existing)
            if not check["ok"]:
                stats["conflict_skipped"] += 1
                if len(examples) < 20:
                    examples.append({"symbol": symbol, **check})
                queue_ledger(symbol, frame, "conflict_skipped", json.dumps(check, default=str))
                continue
            have = set(pd.to_datetime(existing["date"]).dt.date) if not existing.empty else set()
            add = frame[~pd.to_datetime(frame["date"]).dt.date.isin(have)].copy()
            if add.empty:
                stats["already_current"] += 1
                queue_ledger(symbol, add, "already_current")
                continue
            add = add[STOCK_COLS]
            if apply:
                batch = add.copy()
                batch.insert(0, "symbol", symbol)
                pending_adds.append(batch)
                # A per-file INSERT used to commit before the next file was read;
                # mirror that visibility so a repeated symbol is not queued twice.
                incoming = pd.DataFrame({
                    "date": pd.to_datetime(add["date"]).astype("datetime64[us]"),
                    "close": pd.to_numeric(add["close"], errors="coerce"),
                })
                prior = existing_cache.get(symbol)
                existing_cache[symbol] = (
                    pd.concat([prior, incoming], ignore_index=True) if prior is not None else incoming
                )
            stats["rows_written"] += len(add)
            total_rows += len(add)
            stats["symbols_written"] += 1
            queue_ledger(symbol, add, "written", json.dumps(check, default=str))
            if len(examples) < 20:
                examples.append({"symbol": symbol, "rows": len(add), **check})
        except Exception as exc:  # noqa: BLE001
            stats["error"] += 1
            if len(examples) < 20:
                examples.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})

    if apply and pending_adds:
        batch = pd.concat(pending_adds, ignore_index=True)
        conn.register("_qmt_add", batch)
        try:
            conn.execute("""
                INSERT INTO stocks
                (symbol, date, open, close, high, low, high_limit, low_limit, preclose, volume, money)
                SELECT symbol, date, open, close, high, low, high_limit, low_limit, preclose,
                       CAST(volume AS BIGINT), money
                FROM _qmt_add
                ON CONFLICT (symbol, date) DO NOTHING
            """)
        finally:
            conn.unregister("_qmt_add")
        _record_direct_changes(conn, "stocks", batch)
    # Upstream recorded source-ledger attempts even in dry-run; keep that
    # behaviour (only the stocks write itself is gated by ``apply``).
    if ledger_rows:
        dedup: dict[tuple[str, str, str], tuple] = {}
        for row in ledger_rows:
            dedup[(row[0], row[1], row[3])] = row
        ledger_frame = pd.DataFrame(list(dedup.values()), columns=[
            "run_id", "source", "source_kind", "symbol", "rows_written",
            "first_date", "last_date", "checksum", "status", "detail",
        ])
        ledger_frame["first_date"] = pd.to_datetime(ledger_frame["first_date"], errors="coerce")
        ledger_frame["last_date"] = pd.to_datetime(ledger_frame["last_date"], errors="coerce")
        conn.register("_qmt_ledger", ledger_frame)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO unified_source_ledger
                (run_id, source, source_kind, symbol, rows_written, first_date, last_date, checksum, status, detail)
                SELECT run_id, source, source_kind, symbol, rows_written,
                       CAST(first_date AS DATE), CAST(last_date AS DATE), checksum, status, detail
                FROM _qmt_ledger
            """)
        finally:
            conn.unregister("_qmt_ledger")
    return {
        "root": str(QMT_ROOT),
        "datadir": str(QMT_DATADIR),
        "files_seen": len(files),
        "start_date": str(start),
        "history_start": history_start.isoformat(),
        "source_last_date": max(source_dates).isoformat() if source_dates else None,
        "rows_written": total_rows,
        "stats": dict(stats),
        "examples": examples,
    }


def extend_calendar_from_stocks(conn: duckdb.DuckDBPyConnection, apply: bool, floor: date | None = None) -> dict[str, Any]:
    """Trade days are the union of actual bars and the existing calendar."""
    floor = floor or _canonical_floor(conn)
    rows = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT date FROM stocks WHERE date >= ?
            EXCEPT SELECT date FROM trade_days
        )
    """, [floor]).fetchone()[0]
    if apply and rows:
        conn.execute("""
            INSERT OR IGNORE INTO trade_days(date)
            SELECT DISTINCT date FROM stocks WHERE date >= ?
        """, [floor])
    return {"missing_dates": int(rows), "apply": apply, "floor": floor.isoformat()}


# ─────────────────────────────────────────────────────────────────────────────
# Canonical auxiliary tables / PTrade serving coverage
# ─────────────────────────────────────────────────────────────────────────────

def _ptrade_active_symbols(conn: duckdb.DuckDBPyConnection, target: date) -> list[str]:
    """Mirror check_integrity._active_cn_symbols for PTrade-serving coverage."""
    current_pool = None
    if _table_exists(conn, "stock_pool"):
        cols = _columns(conn, "stock_pool")
        if "last_seen_date" in cols:
            latest = conn.execute("SELECT MAX(last_seen_date) FROM stock_pool").fetchone()[0]
            if latest and str(latest)[:10] >= target.isoformat():
                current_pool = {r[0] for r in conn.execute(
                    "SELECT symbol FROM stock_pool WHERE last_seen_date >= ?", [target]
                ).fetchall()}

    cols = _columns(conn, "stock_metadata")
    name_expr = "stock_name" if "stock_name" in cols else "NULL"
    listed_expr = "listed_date" if "listed_date" in cols else "NULL"
    sec_expr = "security_type" if "security_type" in cols else "NULL"
    rows = conn.execute(f"""
        SELECT symbol, {listed_expr} AS listed_date, de_listed_date,
               {sec_expr} AS security_type, {name_expr} AS stock_name
        FROM stock_metadata ORDER BY symbol
    """).fetchall()
    has_security_type = any(r[3] not in (None, "") for r in rows)
    out = []
    for symbol, listed, delisted, sec, name in rows:
        listed_text = str(listed)[:10] if listed else ""
        delisted_text = str(delisted)[:10] if delisted else ""
        if listed_text and listed_text.lower() not in {"none", "null", "nat"} and listed_text > target.isoformat():
            continue
        if delisted_text and delisted_text.lower() not in {"none", "null", "nat"} and delisted_text <= target.isoformat():
            continue
        if name and str(name).strip("\x00").startswith("退市"):
            continue
        if current_pool is not None and symbol not in current_pool:
            continue
        if has_security_type:
            if sec != "1" or not isinstance(symbol, str) or not symbol.endswith((".SZ", ".SS")):
                continue
        elif not _is_cn_symbol(symbol):
            continue
        out.append(symbol)
    return sorted(set(out))


def ensure_serving_placeholders(apply: bool, target: date, symbols: list[str]) -> dict[str, Any]:
    """Write neutral PTrade-compatible files for strict-gate extra symbols.

    ``check_integrity`` uses the legacy CN prefix universe, which includes SH
    ``000xxx.SS`` index pseudo-symbols. ``DuckDBWriter`` exports the true
    A-share universe. Empty stocks/valuation Parquet files keep the serving
    view compatible with that gate without creating fake market data.
    """
    result: dict[str, Any] = {
        "apply": apply,
        "active_symbols": len(symbols),
        "extra_gate_symbols": 0,
        "stock_placeholders_written": 0,
        "valuation_placeholders_written": 0,
    }
    if not apply:
        return result
    exported = {p.stem for p in (CN_DIR / "stocks").glob("*.parquet")}
    extra = sorted(set(symbols) - exported)
    result["extra_gate_symbols"] = len(extra)
    stocks_dir = CN_DIR / "stocks"
    valuation_dir = CN_DIR / "valuation"
    stocks_dir.mkdir(parents=True, exist_ok=True)
    valuation_dir.mkdir(parents=True, exist_ok=True)
    empty_stock = pd.DataFrame({
        "date": pd.Series(dtype="datetime64[ns]"),
        "open": pd.Series(dtype="float64"),
        "close": pd.Series(dtype="float64"),
        "high": pd.Series(dtype="float64"),
        "low": pd.Series(dtype="float64"),
        "high_limit": pd.Series(dtype="float64"),
        "low_limit": pd.Series(dtype="float64"),
        "preclose": pd.Series(dtype="float64"),
        "volume": pd.Series(dtype="int64"),
        "money": pd.Series(dtype="float64"),
    })
    empty_valuation = pd.DataFrame({
        "date": pd.Series(dtype="datetime64[ns]"),
        "pe_ttm": pd.Series(dtype="float64"),
        "pb": pd.Series(dtype="float64"),
        "ps_ttm": pd.Series(dtype="float64"),
        "pcf": pd.Series(dtype="float64"),
        "roe": pd.Series(dtype="float64"),
        "roe_ttm": pd.Series(dtype="float64"),
        "roa": pd.Series(dtype="float64"),
        "roa_ttm": pd.Series(dtype="float64"),
        "naps": pd.Series(dtype="float64"),
        "total_shares": pd.Series(dtype="float64"),
        "a_floats": pd.Series(dtype="float64"),
        "turnover_rate": pd.Series(dtype="float64"),
    })
    for symbol in extra:
        stock_path = stocks_dir / f"{symbol}.parquet"
        valuation_path = valuation_dir / f"{symbol}.parquet"
        if not stock_path.exists():
            empty_stock.to_parquet(stock_path, index=False)
            result["stock_placeholders_written"] += 1
        if not valuation_path.exists():
            empty_valuation.to_parquet(valuation_path, index=False)
            result["valuation_placeholders_written"] += 1
    result["missing_examples"] = extra[:20]
    return result


def extend_valuation_calendar(conn: duckdb.DuckDBPyConnection, apply: bool, target: date) -> dict[str, Any]:
    """Carry the latest valuation snapshot forward to the package target date.

    Valuation fields are point-in-time snapshots. When QMT extends daily bars but
    no external valuation feed is available, carrying the last known snapshot
    forward preserves PTrade's per-symbol valuation files without inventing new
    factor values. The source/ledger report records this as a carry-forward.
    """
    result: dict[str, Any] = {"apply": apply, "target_date": target.isoformat()}
    if not _table_exists(conn, "valuation"):
        result["status"] = "no_valuation_table"
        return result
    cols = _columns(conn, "valuation")
    select_cols = [c for c in cols if c not in {"symbol", "date"}]
    value_exprs = ", ".join(f"v.{c}" for c in select_cols)
    insert_cols = ", ".join(["symbol", "date", *select_cols])
    row = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT symbol, MAX(date) AS last_date FROM valuation
            GROUP BY symbol HAVING MAX(date) < ?
        )
    """, [target]).fetchone()[0]
    result["symbols_needing_carry"] = int(row)
    if apply and row:
        conn.execute(f"""
            INSERT OR IGNORE INTO valuation ({insert_cols})
            SELECT v.symbol, ? AS date, {value_exprs}
            FROM valuation v
            JOIN (
                SELECT symbol, MAX(date) AS last_date FROM valuation GROUP BY symbol
            ) m ON m.symbol = v.symbol AND m.last_date = v.date
            WHERE v.date < ?
        """, [target, target])
    result["rows_written"] = int(row) if apply else 0
    return result


def extend_benchmark_calendar(conn: duckdb.DuckDBPyConnection, apply: bool, target: date, floor: date | None = None) -> dict[str, Any]:
    """Keep benchmark/trade_days aligned with the source-derived stocks calendar."""
    floor = floor or _canonical_floor(conn)
    result: dict[str, Any] = {"apply": apply, "target_date": target.isoformat(), "floor": floor.isoformat()}
    if _table_exists(conn, "benchmark"):
        rows = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT date FROM stocks WHERE date > (SELECT COALESCE(MAX(date), DATE '1900-01-01') FROM benchmark)
            )
        """).fetchone()[0]
        result["benchmark_dates_missing"] = int(rows)
        bench_cols = set(_columns(conn, "benchmark"))
        if apply and rows:
            index_expr = "s.close" if "close" in bench_cols else "b.close"
            high_expr = "s.high" if "high" in bench_cols else "b.high"
            low_expr = "s.low" if "low" in bench_cols else "b.low"
            open_expr = "s.open" if "open" in bench_cols else "b.open"
            volume_expr = "s.volume" if "volume" in bench_cols else "b.volume"
            money_expr = "s.money" if "money" in bench_cols else "b.money"
            conn.execute(f"""
                INSERT OR IGNORE INTO benchmark(date, open, high, low, close, volume, money)
                SELECT s.date, {open_expr}, {high_expr}, {low_expr}, {index_expr},
                       {volume_expr}, {money_expr}
                FROM stocks s
                LEFT JOIN benchmark b ON b.date = (SELECT MAX(date) FROM benchmark)
                WHERE s.date > (SELECT COALESCE(MAX(date), DATE '1900-01-01') FROM benchmark)
                  AND s.symbol = '000300.SS'
            """)
    if _table_exists(conn, "trade_days"):
        rows = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT date FROM stocks WHERE date >= ?
                EXCEPT SELECT date FROM trade_days
            )
        """, [floor]).fetchone()[0]
        result["trade_days_missing"] = int(rows)
        if apply and rows:
            conn.execute("""
                INSERT OR IGNORE INTO trade_days(date)
                SELECT DISTINCT date FROM stocks WHERE date >= ?
            """, [floor])
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Open-source / SmartRouter supplements
# ─────────────────────────────────────────────────────────────────────────────

def _active_symbols(conn: duckdb.DuckDBPyConnection, target: date) -> list[str]:
    rows = conn.execute("""
        SELECT m.symbol FROM stock_metadata m
        WHERE (m.de_listed_date IS NULL OR CAST(m.de_listed_date AS VARCHAR) > ?)
          AND NOT COALESCE(m.stock_name, '') LIKE '退市%'
        ORDER BY m.symbol
    """, [target.isoformat()]).fetchall()
    symbols = [r[0] for r in rows if _is_cn_stock(r[0])]
    if symbols:
        return symbols
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM stocks ORDER BY symbol"
    ).fetchall() if _is_cn_stock(r[0])]


def _normalize_router_frame(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STOCK_COLS)
    work = df.copy()
    work.columns = [str(c).lower() for c in work.columns]
    if "amount" not in work.columns and "money" in work.columns:
        work = work.rename(columns={"money": "amount"})
    if "amount" not in work.columns:
        work["amount"] = np.nan
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    for col in ("open", "high", "low", "close", "volume", "amount"):
        work[col] = pd.to_numeric(work.get(col), errors="coerce")
    work = work.dropna(subset=["date", "open", "high", "low", "close"])
    work = work[(work[["open", "high", "low", "close"]] > 0).all(axis=1)]
    work = work.sort_values("date").drop_duplicates("date", keep="last")
    out = pd.DataFrame({
        "date": work["date"],
        "open": work["open"], "close": work["close"], "high": work["high"], "low": work["low"],
        "high_limit": np.nan, "low_limit": np.nan, "preclose": np.nan,
        "volume": work["volume"].fillna(0).astype("int64"),
        "money": work["amount"],
    })
    return out[STOCK_COLS]


def supplement_from_router(
    conn: duckdb.DuckDBPyConnection,
    run_id: str,
    apply: bool,
    use_network: bool,
    use_oss: bool,
    target_override: date | None,
    max_symbols: int,
    lookback_days: int = 30,
) -> dict[str, Any]:
    """Internet-first daily-bar supplementation for active CN stock symbols.

    ``target_override`` is the newest real date discovered from network sources
    or supplied by ``--target-date``. QMT ledger dates are deliberately ignored
    here: local DAT freshness must not cap an internet-first update.
    """
    expected = _expected_latest_day()
    discovery: dict[str, Any] = {"enabled": use_network}
    source_latest: date | None = None
    if use_network and target_override is None:
        discovery = discover_internet_latest(use_oss, expected, lookback_days)
        if discovery.get("source_latest"):
            source_latest = date.fromisoformat(discovery["source_latest"])
    elif target_override is not None:
        source_latest = target_override
        discovery = {
            "enabled": use_network,
            "source": "explicit_target_date",
            "source_latest": source_latest.isoformat(),
            "expected_date": expected.isoformat(),
            "note": "Explicit target date is an upper bound; only real source bars are written.",
        }
    else:
        source_latest = _latest_trade_day(conn)
        discovery = {
            "enabled": False,
            "source": "database",
            "source_latest": source_latest.isoformat(),
            "note": "Network supplementation disabled; QMT/local DB state is authoritative for this run.",
        }

    db_latest = _latest_trade_day(conn)
    target = source_latest or db_latest
    fetch_start_floor = _canonical_floor(conn)
    symbols = _active_symbols(conn, target)
    latest_by_symbol = {
        r[0]: pd.Timestamp(r[1]).date()
        for r in conn.execute(
            "SELECT symbol, MAX(date) FROM stocks GROUP BY symbol"
        ).fetchall()
        if r[1] is not None
    }
    stale_rows: list[tuple[str, date]] = []
    for symbol in symbols:
        last = latest_by_symbol.get(symbol)
        if last is None:
            stale_rows.append((symbol, max(fetch_start_floor, target - timedelta(days=lookback_days))))
        elif last < target:
            stale_rows.append((symbol, last))
    if max_symbols:
        stale_rows = stale_rows[:max_symbols]
    result: dict[str, Any] = {
        "target_date": target.isoformat(),
        "db_latest_before": db_latest.isoformat(),
        "expected_date": expected.isoformat(),
        "stale_symbols": len(stale_rows),
        "enabled": use_network,
        "network_enabled": use_network,
        "oss_enabled": use_oss,
        "discovery": discovery,
        "sources": {
            "first_party": ["public", "eastmoney", "baostock", "mootdx", "tdx"],
            "oss_projects": OSS_PROJECTS if use_oss else {},
            "optional_projects": FIRST_PARTY_PROJECTS,
        },
        "written_rows": 0,
        "stats": Counter(),
        "examples": [],
    }
    if not stale_rows or not apply or not use_network:
        result["stats"]["dry_run_no_gap_or_network_disabled"] = len(stale_rows)
        if not use_network:
            result["note"] = ("Network supplementation disabled; QMT DAT and local sources are authoritative for this run. "
                              "Use the default command without --no-router to enable internet-first updates.")
        return result

    config = _router_config(use_oss)
    from simtradedata.router.smart_router import SmartRouter

    stats: Counter[str] = Counter()
    written_rows = 0
    # One bulk projection replaces the per-symbol SELECT that used to pull the
    # whole history into pandas once per stale symbol. Stream in bounded chunks
    # and keep only stale symbols so the symbol column stays small in memory.
    wanted = {sym for sym, _ in stale_rows}
    existing_parts: dict[str, list[pd.DataFrame]] = {}
    empty_existing = pd.DataFrame(columns=["date", "close"])
    conn.execute("SELECT symbol, date, close FROM stocks WHERE date >= ?", [fetch_start_floor])
    try:
        while True:
            chunk = conn.fetch_df_chunk(1)
            if chunk.empty:
                break
            for sym, group in chunk.groupby("symbol", sort=False):
                key = str(sym)
                if key in wanted:
                    existing_parts.setdefault(key, []).append(group[["date", "close"]])
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("bulk existing-closes preload failed: %s", exc)
        existing_parts = {}
    existing_cache: dict[str, pd.DataFrame] = {
        sym: pd.concat(parts, ignore_index=True)
        for sym, parts in existing_parts.items()
    }
    del existing_parts

    pending_adds: list[pd.DataFrame] = []
    ledger_rows: list[tuple] = []

    def queue_ledger(sym: str, frame: pd.DataFrame) -> None:
        # Mirrors _ledger's row shape, deferred into one shared INSERT.
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
        first = dates.min().date() if len(dates) else None
        last = dates.max().date() if len(dates) else None
        checksum = hashlib.sha256(
            pd.util.hash_pandas_object(
                frame[["date", "open", "high", "low", "close", "volume", "money"]], index=False
            ).values.tobytes()
        ).hexdigest()[:32]
        ledger_rows.append((
            run_id, "smart_router", "internet_public_or_oss", sym, len(frame),
            first, last, checksum, "written", "",
        ))

    with SmartRouter(config=config) as router:
        for symbol, last_date in stale_rows:
            # latest_by_symbol is keyed by symbol, not by date. Only symbols
            # that already have bars should start from the day after their
            # last bar; symbols with no bars keep the lookback floor.
            start = ((last_date + timedelta(days=1)).isoformat()
                     if symbol in latest_by_symbol else last_date.isoformat())
            end = target.isoformat()
            if start > end:
                continue
            try:
                raw = router.get_daily_bars(symbol, start, end)
                frame = _normalize_router_frame(raw, symbol, "smart_router")
                if frame.empty:
                    stats["empty"] += 1
                    continue
                existing = existing_cache.get(symbol, empty_existing)
                have = {d.date() for d in pd.to_datetime(existing["date"])}
                add = frame[~pd.to_datetime(frame["date"]).dt.date.isin(have)].copy()
                if add.empty:
                    stats["already_current"] += 1
                    continue
                batch = add.copy()
                batch.insert(0, "symbol", symbol)
                pending_adds.append(batch)
                # Mirror the visibility a per-symbol INSERT gave the next stale
                # row for the same symbol, so dates are not queued twice.
                incoming = pd.DataFrame({
                    "date": pd.to_datetime(add["date"]).astype("datetime64[us]"),
                    "close": pd.to_numeric(add["close"], errors="coerce"),
                })
                prior = existing_cache.get(symbol)
                existing_cache[symbol] = (
                    pd.concat([prior, incoming], ignore_index=True) if prior is not None else incoming
                )
                written_rows += len(add)
                stats["symbols_written"] += 1
                queue_ledger(symbol, add)
                if len(result["examples"]) < 20:
                    result["examples"].append({"symbol": symbol, "rows": len(add), "start": start, "end": end})
            except Exception as exc:  # noqa: BLE001
                stats["error"] += 1
                if len(result["examples"]) < 20:
                    result["examples"].append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})

    # Both flushes run only on this path (the dry-run / no-network guard above
    # returns early), so apply=False still performs no database writes.
    if pending_adds:
        batch = pd.concat(pending_adds, ignore_index=True)
        conn.register("_oss_add", batch)
        try:
            conn.execute("""
                INSERT INTO stocks
                (symbol, date, open, close, high, low, high_limit, low_limit, preclose, volume, money)
                SELECT symbol, date, open, close, high, low, high_limit, low_limit, preclose,
                       CAST(volume AS BIGINT), money FROM _oss_add
                ON CONFLICT (symbol, date) DO NOTHING
            """)
        finally:
            conn.unregister("_oss_add")
        _record_direct_changes(conn, "stocks", batch)
    if ledger_rows:
        dedup: dict[tuple[str, str, str], tuple] = {}
        for row in ledger_rows:
            dedup[(row[0], row[1], row[3])] = row
        ledger_frame = pd.DataFrame(list(dedup.values()), columns=[
            "run_id", "source", "source_kind", "symbol", "rows_written",
            "first_date", "last_date", "checksum", "status", "detail",
        ])
        ledger_frame["first_date"] = pd.to_datetime(ledger_frame["first_date"], errors="coerce")
        ledger_frame["last_date"] = pd.to_datetime(ledger_frame["last_date"], errors="coerce")
        conn.register("_oss_ledger", ledger_frame)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO unified_source_ledger
                (run_id, source, source_kind, symbol, rows_written, first_date, last_date, checksum, status, detail)
                SELECT run_id, source, source_kind, symbol, rows_written,
                       CAST(first_date AS DATE), CAST(last_date AS DATE), checksum, status, detail
                FROM _oss_ledger
            """)
        finally:
            conn.unregister("_oss_ledger")
    result["written_rows"] = written_rows
    result["stats"] = dict(stats)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Serving view export / promotion / validation
# ─────────────────────────────────────────────────────────────────────────────

def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


OWNED_EXPORT_DIRS = ["stocks", "exrights", "fundamentals", "valuation", "metadata"]


def _swap_slot(name: str, stamp: str) -> Path:
    """Temporary swap directory used only while a promote is in flight.

    No persistent rollback copy is kept: the serving view is fully derivable
    from the authoritative DuckDB, and the pre-publish quality gate blocks bad
    writes. The swap exists solely to make the multi-directory rename atomic;
    it is deleted on success and restored on failure.
    """
    return DATA_ROOT / f"_promote_swap_{name}_{stamp}"


def promote_staged_export(stage: Path, apply: bool) -> dict[str, Any]:
    """Replace only DB-owned PTrade directories/files; preserve QMT-only assets."""
    owned_dirs = OWNED_EXPORT_DIRS
    owned_files = ["manifest.json"]
    result: dict[str, Any] = {
        "stage": str(stage), "apply": apply, "owned_dirs": owned_dirs,
        "swaps_cleaned": 0,
    }
    if not apply:
        return result
    CN_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _now().strftime("%Y%m%d_%H%M%S%f")
    swaps: list[tuple[Path, Path]] = []
    try:
        for name in owned_dirs:
            src = stage / name
            if not src.is_dir():
                raise RuntimeError(f"staged export missing {name}")
            dst = CN_DIR / name
            if dst.exists():
                slot = _swap_slot(name, stamp)
                shutil.rmtree(slot, ignore_errors=True)
                dst.rename(slot)
                swaps.append((dst, slot))
            src.rename(dst)
        for name in owned_files:
            src = stage / name
            if src.is_file():
                dst = CN_DIR / name
                tmp = CN_DIR / f"{dst.name}.promote.tmp"
                shutil.copy2(src, tmp)
                tmp.replace(dst)
    except Exception:
        # Restore the previous view; leftover swaps are cleaned by the next
        # run's prune pass, so a failed promote never leaves a half-replaced
        # serving view *and* never leaves a permanent backup copy.
        for dst, slot in reversed(swaps):
            if dst.exists():
                shutil.rmtree(dst, ignore_errors=True)
            if slot.exists():
                slot.rename(dst)
        for _, slot in swaps:
            shutil.rmtree(slot, ignore_errors=True)
        raise
    for _, slot in swaps:
        shutil.rmtree(slot, ignore_errors=True)
    result["swaps_cleaned"] = len(swaps)
    return result


def prune_stale_artifacts(current_stage: Path | None = None) -> dict[str, Any]:
    """Remove derived leftovers: stale staging, historical backup dirs, swaps.

    Everything removed here is a copy of data derivable from the authoritative
    DuckDB (or an interrupted run's temporary swap), so no information is lost.
    DB-level safety comes from the pre-publish quality gate and idempotent
    upserts, not from byte-level snapshots.
    """
    removed_staging: list[str] = []
    removed_backups: list[str] = []
    removed_db_backups: list[str] = []
    for path in DATA_ROOT.glob("cn_staging_*"):
        if path.is_dir() and path != current_stage:
            shutil.rmtree(path, ignore_errors=True)
            removed_staging.append(path.name)
    for path in CN_DIR.glob("_backup_*"):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            removed_backups.append(path.name)
    for path in DATA_ROOT.glob("_promote_swap_*"):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            removed_staging.append(path.name)
    for pattern in ("cn.duckdb.unified_backup_*", "cn.duckdb.pre_full_history_*"):
        for path in DATA_ROOT.glob(pattern):
            if path.is_file():
                try:
                    path.unlink()
                    removed_db_backups.append(path.name)
                except OSError as exc:
                    LOGGER.warning("could not remove db backup %s: %s", path.name, exc)
    for path in CN_DIR.glob("*.bak"):
        try:
            path.unlink()
        except OSError:
            pass
    for pattern in ("manifest.pre_full_history_*.json", "manifest.unified_backup_*.json"):
        for path in CN_DIR.glob(pattern):
            try:
                path.unlink()
            except OSError:
                pass
    return {
        "staging_removed": removed_staging,
        "legacy_backups_removed": removed_backups,
        "db_backups_removed": removed_db_backups,
        "persistent_backups": 0,
    }


def _ptrade_smoke() -> dict[str, Any]:
    """Validate the PTrade serving contract without importing the full engine.

    SimTradeLab's storage module depends on optional runtime packages (for
    example pydantic) that are not required for the Parquet contract itself.
    When the engine module is importable we also read through its own storage
    layer; otherwise we fall back to direct Parquet reads of the same files.
    """
    lab_src = ROOT / "PTrade" / "SimTradeLab" / "src"
    if str(lab_src) not in sys.path:
        sys.path.insert(0, str(lab_src))
    engine: dict[str, Any] = {"status": "skipped", "reason": "not attempted"}
    try:
        storage = importlib.import_module("simtradelab.ptrade.storage")
        symbols = storage.list_stocks(str(CN_DIR))
        loaded = []
        for symbol in symbols[:5]:
            try:
                df = storage.load_stock(str(CN_DIR), symbol)
                loaded.append({
                    "symbol": symbol,
                    "rows": 0 if df is None else len(df),
                    "columns": [] if df is None else list(df.columns),
                    # The engine keeps `date` as a DatetimeIndex, not a column;
                    # counting it as a column would fail every real stock file.
                    "index_names": [] if df is None else [str(n) for n in (df.index.names or [])],
                })
            except Exception as exc:  # noqa: BLE001
                loaded.append({"symbol": symbol, "error": str(exc)})
        required = set(STOCK_COLS)

        def _schema_ok(item: dict[str, Any]) -> bool:
            cols = set(item.get("columns", []))
            if "date" in (item.get("index_names") or []):
                cols.add("date")
            return required.issubset(cols)

        schema_ok = all(_schema_ok(x) for x in loaded if "columns" in x)
        engine = {
            "status": "pass" if symbols and schema_ok else "fail",
            "symbols": len(symbols),
            "sample": loaded,
            "required_columns": sorted(required),
        }
    except Exception as exc:  # noqa: BLE001
        engine = {"status": "skipped", "reason": f"SimTradeLab import failed: {exc}"}

    stocks_dir = CN_DIR / "stocks"
    files = sorted(stocks_dir.glob("*.parquet"))
    sample: list[dict[str, Any]] = []
    for path in files[:5]:
        try:
            table = pq.ParquetFile(path)
            columns = list(table.schema_arrow.names)
            sample.append({
                "symbol": path.stem,
                "rows": table.metadata.num_rows,
                "columns": columns,
                "schema_ok": set(STOCK_COLS).issubset(columns),
            })
        except Exception as exc:  # noqa: BLE001
            sample.append({"symbol": path.stem, "error": str(exc), "schema_ok": False})
    manifest_path = CN_DIR / "manifest.json"
    manifest = {}
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            sample.append({"manifest": str(manifest_path), "error": str(exc), "schema_ok": False})
    direct_ok = bool(files) and all(x.get("schema_ok") for x in sample) and bool(manifest)
    engine_ok = engine.get("status") in {"pass", "skipped"}
    return {
        "status": "pass" if direct_ok and engine_ok else "fail",
        "engine": engine,
        "direct": {
            "status": "pass" if direct_ok else "fail",
            "stock_files": len(files),
            "sample": sample,
            "required_columns": sorted(STOCK_COLS),
            "manifest_version": manifest.get("version"),
            "manifest_date_range": manifest.get("date_range"),
        },
    }


def _remote_package_unchanged() -> bool:
    """True only when the TDX server confirms our cached package ETag (304).

    Keep this self-contained: importing download_tdx_day at manager load time
    would truncate the shared download log via its basicConfig(filemode='w').
    """
    import json
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    zip_path = DATA_ROOT / "downloads" / "hsjday.zip"
    meta_path = zip_path.with_name(zip_path.name + ".meta.json")
    if not zip_path.is_file() or not meta_path.is_file():
        return False
    try:
        etag = json.loads(meta_path.read_text(encoding="utf-8")).get("etag")
    except Exception:  # noqa: BLE001
        return False
    if not etag:
        return False
    req = Request("https://data.tdx.com.cn/vipdoc/hsjday.zip", method="HEAD")
    req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    req.add_header("If-None-Match", etag)
    try:
        with urlopen(req, timeout=20):
            return False  # 200 means the package changed
    except HTTPError as exc:
        return exc.code == 304
    except Exception:  # noqa: BLE001
        return False  # unknown -> rebuild conservatively


def _checkpoint_db() -> None:
    """Flush the WAL so read-only gate subprocesses see committed state."""
    try:
        conn = duckdb.connect(str(DB_PATH))
        try:
            conn.execute("CHECKPOINT")
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("checkpoint failed: %s", exc)


_INCREMENTAL_EXPORT_TABLES = ("stocks", "valuation", "fundamentals", "exrights")


def _record_direct_changes(
    conn: duckdb.DuckDBPyConnection, table: str, frame: pd.DataFrame
) -> int:
    """Mirror DuckDBWriter._record_symbol_changes for direct SQL writes.

    QMT/router supplementation writes straight into DuckDB. Without a
    change-log entry the incremental export hard-links the previous Parquet
    files and the newly inserted bars never reach the serving view.
    """
    if frame is None or frame.empty or not _table_exists(conn, "data_change_log"):
        return 0
    if "symbol" not in frame.columns or "date" not in frame.columns:
        return 0
    changes = frame[["symbol", "date"]].dropna().drop_duplicates().copy()
    if changes.empty:
        return 0
    changes["date"] = pd.to_datetime(changes["date"]).dt.date
    conn.register("_direct_change_rows", changes)
    try:
        conn.execute(f"""
            INSERT INTO data_change_log (table_name, symbol, date, changed_at)
            SELECT '{table}', symbol, date, CURRENT_TIMESTAMP
            FROM _direct_change_rows
            ON CONFLICT (table_name, symbol, date)
            DO UPDATE SET changed_at = EXCLUDED.changed_at
        """)
    finally:
        conn.unregister("_direct_change_rows")
    return len(changes)


def _db_table_state(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Per-table rows/max date, so unlogged direct writes force a rebuild."""
    state: dict[str, str] = {}
    for table in _INCREMENTAL_EXPORT_TABLES:
        if not _table_exists(conn, table):
            state[table] = "missing"
            continue
        try:
            rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            max_date = ""
            if "date" in _columns(conn, table):
                max_date = str(conn.execute(
                    f"SELECT CAST(MAX(date) AS VARCHAR) FROM {table}"
                ).fetchone()[0] or "")
            state[table] = f"{rows}:{max_date}"
        except Exception as exc:  # noqa: BLE001
            state[table] = f"error:{exc}"
    return state


def _incremental_export_plan() -> dict[str, Any]:
    """Decide whether staged export can reuse unchanged serving tables.

    Falls back to a full export whenever the safety chain is broken: no
    previous publish, an unreadable change log, DB changes that were never
    recorded, or a serving view modified outside this pipeline.
    """
    state = _read_publish_state()
    if not state or not state.get("published_at"):
        return {"mode": "full", "reason": "no previous publish state"}
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            fingerprint = _db_state_fingerprint(conn)
            table_state = _db_table_state(conn)
            placeholders = ", ".join("?" for _ in _INCREMENTAL_EXPORT_TABLES)
            changed = {
                row[0] for row in conn.execute(
                    f"SELECT DISTINCT table_name FROM data_change_log "
                    f"WHERE changed_at > ?::TIMESTAMP "
                    f"AND table_name IN ({placeholders})",
                    [state["published_at"], *_INCREMENTAL_EXPORT_TABLES],
                ).fetchall()
            }
            # A table whose rows/max date moved without a change-log entry
            # must be rebuilt, never hard-linked from the previous publish.
            previous_tables = state.get("table_state") or {}
            for table, value in table_state.items():
                if previous_tables.get(table) != value:
                    changed.add(table)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        return {"mode": "full", "reason": f"change lookup failed: {exc}"}

    if state.get("db_fingerprint") != fingerprint and not changed:
        return {"mode": "full", "reason": "DB changed without change-log entries"}
    if state.get("view_fingerprint") != _serving_view_fingerprint():
        return {"mode": "full", "reason": "serving view changed externally"}
    return {
        "mode": "incremental",
        "changed_tables": sorted(changed),
        "reused_tables": [t for t in _INCREMENTAL_EXPORT_TABLES if t not in changed],
    }


def _db_latest_date() -> str:
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            return str(conn.execute("SELECT MAX(date) FROM stocks").fetchone()[0] or "")
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return ""


def _rebuild_is_current(history_start: date | None, target_date: str) -> dict[str, Any] | None:
    """Return state when a full-history rebuild provably has nothing to do."""
    if history_start is None:
        return None
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            min_date, max_date = conn.execute(
                "SELECT MIN(date), MAX(date) FROM stocks"
            ).fetchone()
            quarters = conn.execute(
                "SELECT COUNT(*) FROM fundamentals_progress"
            ).fetchone()[0]
            sampled = conn.execute(
                "SELECT COUNT(*) FROM sampling_progress"
            ).fetchone()[0]
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return None
    if str(min_date or "") > history_start.isoformat():
        return None
    if str(max_date or "") < target_date:
        return None
    if quarters < 100 or sampled < 100:
        return None
    if not _remote_package_unchanged():
        return None
    return {
        "stocks_min": str(min_date),
        "stocks_max": str(max_date),
        "fundamentals_quarters": int(quarters),
        "sampled_months": int(sampled),
        "tdx_package": "unchanged (HTTP 304)",
    }


def _run_download_rebuild(history_start: date | None, full_history: bool) -> dict[str, Any]:
    """Run the existing internet-first bulk downloader in a fresh process.

    ``scripts/download.py`` is the sanctioned full-history path: TDX hsjday.zip
    supplies long daily OHLCV, Mootdx supplies XDXR/financial extras, and
    BaoStock supplies valuation/status/calendar data. It writes the same
    canonical DuckDB; this manager then exports/validates that single store.

    BaoStock stays in valuation-only mode even for a full-history rebuild.
    TDX already supplies 1990+ OHLCV and BaoStock valuation itself only
    reaches back to 1997, so ``--baostock-full`` added ~5k per-stock market
    fetches, up to 37 dividend queries per symbol, and duplicate
    calendar/benchmark writes without extending coverage.
    """
    import subprocess

    script = HERE / "download.py"
    cmd = [sys.executable, str(script)]
    if history_start:
        cmd.extend(["--history-start", history_start.isoformat()])
    env = os.environ.copy()
    if history_start:
        env["SIMTRADE_CN_HISTORY_START"] = history_start.isoformat()
    # Force UTF-8: the download/gate subprocesses print Chinese, and Windows'
    # locale decoder (GBK) can otherwise fail or yield None for the captured
    # streams. ``errors='replace'`` keeps the report readable either way.
    proc = subprocess.run(
        cmd, cwd=str(SIMDATA), env=env, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    return {
        "status": "pass" if proc.returncode == 0 else "fail",
        "returncode": proc.returncode,
        "command": cmd,
        "history_start": history_start.isoformat() if history_start else None,
        "full_history": full_history,
        "stdout_tail": (proc.stdout or "")[-4000:],
        "stderr_tail": (proc.stderr or "")[-4000:],
    }


def _integrity_report(
    target_date: date,
    export_dir: Path | None = None,
    history_start: date | None = None,
) -> dict[str, Any]:
    """Run the integrity gate in a fresh process/connection.

    DuckDB refuses a read-only connection while this process holds a writable
    connection to the same file. A subprocess also guarantees the gate observes
    committed state only.
    """
    import subprocess

    script = HERE / "check_integrity.py"
    out_path = REPORT_DIR / f"integrity_{_now().strftime('%Y%m%d_%H%M%S%f')}.json"
    cmd = [
        sys.executable, str(script),
        "--db-path", str(DB_PATH),
        "--market", "cn",
        "--target-date", target_date.isoformat(),
        "--json-output", str(out_path),
        "--json",
    ]
    env = os.environ.copy()
    if export_dir:
        cmd.extend(["--export-dir", str(export_dir)])
        cmd.extend([
            "--floor-stats", str(REPORT_DIR / "parquet_floor_stats.json"),
        ])
    if history_start:
        cmd.extend(["--history-start", history_start.isoformat()])
        env["SIMTRADE_CN_HISTORY_START"] = history_start.isoformat()
    # Force UTF-8: the download/gate subprocesses print Chinese, and Windows'
    # locale decoder (GBK) can otherwise fail or yield None for the captured
    # streams. ``errors='replace'`` keeps the report readable either way.
    proc = subprocess.run(
        cmd, cwd=str(SIMDATA), env=env, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        raise RuntimeError(f"integrity gate failed to execute: {(proc.stderr or '')[-2000:]}")
    return json.loads(out_path.read_text(encoding="utf-8"))


def _data_quality_gate(target_date: date, allow_issues: bool) -> dict[str, Any]:
    """Run the pre-publish data quality gate in a fresh process.

    This is deliberately separate from the post-publish health audit: a bad
    write must be caught *before* export/promotion, not merely reported after
    dirty rows have already reached the serving view.
    """
    import subprocess

    script = HERE / "data_quality_gate.py"
    gate_path = REPORT_DIR / "data_quality_gate.json"
    md_path = REPORT_DIR / "data_quality_gate.md"
    cmd = [
        sys.executable, str(script),
        "--db-path", str(DB_PATH),
        "--export-dir", str(CN_DIR),
        "--target-date", target_date.isoformat(),
        "--baseline", str(REPORT_DIR / "data_quality_baseline.json"),
        "--json-output", str(gate_path),
        "--markdown-output", str(md_path),
    ]
    if allow_issues:
        cmd.append("--allow-issues")
    proc = subprocess.run(
        cmd, cwd=str(SIMDATA), env=os.environ.copy(), capture_output=True,
        text=True, encoding="utf-8", errors="replace",
    )
    result: dict[str, Any] = {
        "status": "pass" if proc.returncode == 0 else "fail",
        "returncode": proc.returncode,
        "command": cmd,
        "json_path": str(gate_path),
        "markdown_path": str(md_path),
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }
    if gate_path.is_file():
        try:
            result.update(json.loads(gate_path.read_text(encoding="utf-8")))
        except Exception as exc:  # noqa: BLE001
            result["report_error"] = str(exc)
    return result


def _write_quality_baseline(report: dict[str, Any]) -> None:
    """Promote this run's clean gate result to the comparison baseline."""
    gate_path = REPORT_DIR / "data_quality_gate.json"
    if not gate_path.is_file():
        return
    try:
        gate = json.loads(gate_path.read_text(encoding="utf-8"))
        baseline = {
            "generated_at": report.get("finished_at"),
            "target_date": gate.get("target_date"),
            "errors": gate["summary"]["errors"],
            "warnings": gate["summary"]["warnings"],
            "metrics": {
                c["name"]: {
                    "count": c.get("count"),
                    "ratio": c.get("ratio"),
                    "status": c["status"],
                }
                for c in gate["checks"]
            },
        }
        _atomic_write_json(REPORT_DIR / "data_quality_baseline.json", baseline)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("could not update data quality baseline: %s", exc)


def _health_report(
    target_date: date, history_start: date | None = None
) -> dict[str, Any]:
    """Run the read-only health audit in a fresh process/connection."""
    import subprocess

    script = HERE / "data_health_report.py"
    out_path = REPORT_DIR / "data_health_latest.json"
    md_path = REPORT_DIR / "data_health_latest.md"
    cmd = [
        sys.executable, str(script),
        "--db-path", str(DB_PATH),
        "--export-dir", str(CN_DIR),
        "--target-date", target_date.isoformat(),
        "--json-output", str(out_path),
        "--markdown-output", str(md_path),
    ]
    env = os.environ.copy()
    if history_start:
        env["SIMTRADE_CN_HISTORY_START"] = history_start.isoformat()
    # Force UTF-8: the download/gate subprocesses print Chinese, and Windows'
    # locale decoder (GBK) can otherwise fail or yield None for the captured
    # streams. ``errors='replace'`` keeps the report readable either way.
    proc = subprocess.run(
        cmd, cwd=str(SIMDATA), env=env, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )
    result = {
        "status": "pass" if proc.returncode == 0 else "fail",
        "returncode": proc.returncode,
        "command": cmd,
        "json_path": str(out_path),
        "markdown_path": str(md_path),
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }
    if out_path.exists():
        try:
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            result["health_status"] = payload.get("status")
            result["summary"] = payload.get("summary")
        except Exception as exc:
            result["parse_error"] = str(exc)
    return result


def _write_status(apply: bool, report: dict[str, Any]) -> None:
    if not apply:
        return
    status = {
        "published": report["status"] == "pass",
        "status": report["status"],
        "generated_at": _now().isoformat(timespec="seconds"),
        "target_date": report.get("target_date"),
        "report_path": report.get("report_path"),
        "canonical_db": str(DB_PATH),
        "serving_view": str(CN_DIR),
        "contract": "ptrade-simtradelab-v1",
    }
    _atomic_write_json(REPORT_DIR / "latest_status.json", status)


def _db_state_fingerprint(conn: duckdb.DuckDBPyConnection) -> str:
    """Hash table sizes and last dates; stable under repeated no-op rewrites."""
    parts = []
    for table in _FINGERPRINT_TABLES:
        if not _table_exists(conn, table):
            parts.append(f"{table}:missing")
            continue
        try:
            rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            max_date = ""
            if "date" in _columns(conn, table):
                max_date = str(conn.execute(
                    f"SELECT CAST(MAX(date) AS VARCHAR) FROM {table}"
                ).fetchone()[0] or "")
            parts.append(f"{table}:{rows}:{max_date}")
        except Exception as exc:  # noqa: BLE001
            parts.append(f"{table}:error:{exc}")
    # INSERT OR REPLACE over the (table,symbol,date) PK keeps this count
    # stable for no-op writes, while new (symbol,date) pairs still change it.
    if _table_exists(conn, "data_change_log"):
        parts.append("changes:" + str(conn.execute(
            "SELECT COUNT(*) FROM data_change_log"
        ).fetchone()[0]))
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]


def _serving_view_fingerprint() -> str:
    """Cheap view check; deliberately ignores export timestamps."""
    parts = []
    for sub in ("stocks", "fundamentals", "valuation", "exrights"):
        directory = CN_DIR / sub
        count = len(list(directory.glob("*.parquet"))) if directory.is_dir() else 0
        parts.append(f"{sub}:{count}")
    manifest = CN_DIR / "manifest.json"
    if manifest.is_file():
        try:
            meta = json.loads(manifest.read_text(encoding="utf-8"))
            rng = meta.get("date_range") or {}
            parts.append(
                f"manifest:{meta.get('mode')}:{meta.get('version')}:"
                f"{rng.get('start')}:{rng.get('end')}"
            )
        except Exception:  # noqa: BLE001
            parts.append("manifest:unreadable")
    else:
        parts.append("manifest:missing")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]


def _read_publish_state() -> dict[str, Any]:
    try:
        return json.loads(PUBLISH_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _write_publish_state(report: dict[str, Any]) -> None:
    try:
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            db_fp = _db_state_fingerprint(conn)
            table_state = _db_table_state(conn)
        finally:
            conn.close()
        _atomic_write_json(PUBLISH_STATE_PATH, {
            "db_fingerprint": db_fp,
            "table_state": table_state,
            "view_fingerprint": _serving_view_fingerprint(),
            "target_date": report.get("target_date"),
            "report_path": report.get("report_path"),
            "health_status": (report.get("health_report") or {}).get("health_status"),
            "published_at": report.get("finished_at"),
        })
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("Could not persist publish state: %s", exc)


def _reusable_publish(conn: duckdb.DuckDBPyConnection, target_date: str) -> dict[str, Any] | None:
    """Return the previous publish state when nothing could have changed."""
    state = _read_publish_state()
    if not state:
        return None
    try:
        status = json.loads((REPORT_DIR / "latest_status.json").read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None
    if status.get("status") != "pass" or not status.get("published"):
        return None
    if state.get("report_path") and not Path(state["report_path"]).is_file():
        return None
    if str(state.get("target_date")) != str(target_date):
        return None
    if state.get("db_fingerprint") != _db_state_fingerprint(conn):
        return None
    if state.get("view_fingerprint") != _serving_view_fingerprint():
        return None
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> dict[str, Any]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _now().strftime("%Y%m%d_%H%M%S")
    run_id = f"unified_{stamp}"
    report_path = REPORT_DIR / f"{run_id}.json"
    apply = bool(args.apply and not args.validate_only)
    explicit_target = _parse_iso_date(args.target_date, "--target-date")
    history_start = _parse_iso_date(args.history_start, "--history-start")
    if args.full_history and history_start is None:
        history_start = date(1990, 12, 19)
    if history_start:
        os.environ["SIMTRADE_CN_HISTORY_START"] = history_start.isoformat()
    use_network = not args.no_router
    use_oss = not args.no_oss
    use_qmt = not args.no_qmt
    report: dict[str, Any] = {
        "run_id": run_id,
        "mode": "apply" if apply else "dry-run" if not args.validate_only else "validate-only",
        "started_at": _now().isoformat(timespec="seconds"),
        "canonical_db": str(DB_PATH),
        "serving_view": str(CN_DIR),
        "qmt_root": str(QMT_ROOT),
        "status": "running",
        "report_path": str(report_path),
        "oss_projects": OSS_PROJECTS,
        "first_party_projects": FIRST_PARTY_PROJECTS,
        "source_policy": {
            "internet_first": True,
            "network_enabled": use_network,
            "oss_enabled": use_oss,
            "qmt_enabled": use_qmt,
            "qmt_role": "optional fallback cross-check/supplement; freshness does not cap target",
            "explicit_target_date": explicit_target.isoformat() if explicit_target else None,
            "history_start": history_start.isoformat() if history_start else None,
            "full_history": bool(args.full_history),
        },
    }

    # A no-op publish still used to back up 1.9GB, re-export 22M rows into
    # 22k parquet files, swap five directories and re-run every gate. When
    # neither the canonical DB nor the serving view changed since the last
    # successful publish, none of that work can produce a different result.
    skip_state = None
    effective_target = ""
    if apply and not getattr(args, "full_history", False) and not getattr(args, "force_publish", False):
        try:
            ro_conn = duckdb.connect(str(DB_PATH), read_only=True)
            try:
                effective_target = (
                    explicit_target.isoformat() if explicit_target
                    else str(ro_conn.execute("SELECT MAX(date) FROM stocks").fetchone()[0] or "")
                )
                skip_state = _reusable_publish(ro_conn, effective_target)
            finally:
                ro_conn.close()
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).debug("publish reuse check skipped: %s", exc)

    if skip_state:
        report.update({
            "status": "pass",
            "skipped": True,
            "skip_reason": "canonical DB and serving view unchanged since last successful publish",
            "reused_report": skip_state.get("report_path"),
            "reused_published_at": skip_state.get("published_at"),
            "target_date": effective_target,
            "integrity_after": {"status": "pass", "checks": [], "reused": True},
            "ptrade_smoke": _ptrade_smoke(),
            "health_report": {
                "status": "pass",
                "skipped": True,
                "reused": True,
                "health_status": skip_state.get("health_status") or "warning",
                "reason": "identical DB/serving view; previous audit reused",
            },
            "failed_error_checks": [],
            "finished_at": _now().isoformat(timespec="seconds"),
        })
        _atomic_write_json(report_path, report)
        _write_status(apply, report)
        return report

    if apply:
        # Drop derived leftovers (staging dirs, historical view/DB backups,
        # .bak files) before this run creates its own staging directory.
        report["artifact_prune"] = prune_stale_artifacts()
        # Flush the WAL so the read-only gate subprocesses see committed state.
        # No byte-level DB snapshot is kept: safety comes from the pre-publish
        # data quality gate and idempotent upserts.
        _checkpoint_db()

    # Full-history rebuild is bulk and internet-first. It must run before the
    # manager opens DuckDB because download.py owns its own DB connections.
    if apply and args.full_history:
        expected_target = (
            explicit_target.isoformat() if explicit_target
            else _db_latest_date() or ""
        )
        current_state = None
        if not getattr(args, "force_rebuild", False):
            current_state = _rebuild_is_current(history_start, expected_target)
        if current_state:
            report["download_rebuild"] = {
                "status": "pass",
                "skipped": True,
                "reason": "DB already covers the floor/target, ledgers complete, TDX package unchanged (304)",
                "history_start": history_start.isoformat() if history_start else None,
                "full_history": True,
                **current_state,
            }
        else:
            report["download_rebuild"] = _run_download_rebuild(history_start, True)
        if report["download_rebuild"]["status"] != "pass":
            report["status"] = "fail"
            report["finished_at"] = _now().isoformat(timespec="seconds")
            report["failed_error_checks"] = ["download_rebuild"]
            _atomic_write_json(report_path, report)
            _write_status(apply, report)
            return report

    conn = duckdb.connect(str(DB_PATH))
    gate_symbols: list[str] = []
    try:
        _ensure_ledger(conn)
        conn.execute("INSERT OR REPLACE INTO unified_runs VALUES (?,?,?,?,?,?,?,?,?)", [
            run_id, report["mode"], _now(), None, "running", None, str(report_path), None, "",
        ])
        before_latest = _latest_trade_day(conn)
        floor = _canonical_floor(conn)
        report["before"] = {
            "latest_stocks_date": before_latest.isoformat(),
            "expected_latest_weekday": _expected_latest_day().isoformat(),
            "history_floor": floor.isoformat(),
            "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2),
        }
        if not args.validate_only:
            report["metadata_repair"] = repair_metadata(conn, apply)
            report["router_supplement"] = supplement_from_router(
                conn, run_id, apply, use_network, use_oss, explicit_target,
                args.max_router_symbols, args.lookback_days,
            )
            if use_qmt:
                report["qmt_import"] = import_qmt_to_db(conn, run_id, apply, args.limit)
            else:
                report["qmt_import"] = {
                    "enabled": False,
                    "role": "optional fallback skipped by --no-qmt",
                    "files_seen": 0,
                    "rows_written": 0,
                }
            target = _latest_trade_day(conn)
            report["calendar"] = extend_calendar_from_stocks(conn, apply, floor)
            report["benchmark_calendar"] = extend_benchmark_calendar(conn, apply, target, floor)
            report["valuation_calendar"] = extend_valuation_calendar(conn, apply, target)
        target = _latest_trade_day(conn)
        gate_symbols = _ptrade_active_symbols(conn, target)
        report["target_date"] = target.isoformat()
        report["after"] = {
            "latest_stocks_date": target.isoformat(),
            "min_stocks_date": str(conn.execute("SELECT MIN(date) FROM stocks").fetchone()[0] or ""),
            "max_valuation_date": str(conn.execute("SELECT MAX(date) FROM valuation").fetchone()[0] or ""),
            "max_benchmark_date": str(conn.execute("SELECT MAX(date) FROM benchmark").fetchone()[0] or ""),
            "max_trade_days_date": str(conn.execute("SELECT MAX(date) FROM trade_days").fetchone()[0] or ""),
            "db_size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2),
        }
    finally:
        conn.close()

    # DuckDB allows only one process/configuration to hold this database file.
    # All gates and writers below run only after the writable connection closes.
    # The old baseline gate inspected the serving view about to be replaced,
    # and its result never affected the verdict (only integrity_after does).
    # It doubled gate cost: two independent 22k-file parquet footer scans on
    # --history-start runs. Keep the report key for consumers, skip the work.
    report["baseline_integrity"] = {
        "status": "skipped",
        "reason": "redundant: integrity_after gates the published view",
    }

    if apply and not args.validate_only:
        # Gate the canonical DB *before* anything is exported or promoted.
        # A post-publish audit can only complain after dirty rows shipped.
        quality_gate = _data_quality_gate(
            target, getattr(args, "allow_data_quality_issues", False)
        )
        report["data_quality_gate"] = quality_gate
        if quality_gate["status"] != "pass":
            report["status"] = "fail"
            report["failed_error_checks"] = ["data_quality_gate"]
            report["finished_at"] = _now().isoformat(timespec="seconds")
            report["health_report"] = {
                "status": "skipped", "reason": "data quality gate blocked the publish",
            }
            report["integrity_after"] = {"status": "skipped", "checks": []}
            report["ptrade_smoke"] = {"status": "skipped"}
            _atomic_write_json(report_path, report)
            _write_status(apply, report)
            return report

        stage = DATA_ROOT / f"cn_staging_{stamp}"
        rebuild = report.get("download_rebuild") or {}
        if getattr(args, "full_export", False):
            export_plan: dict[str, Any] = {"mode": "full", "reason": "--full-export"}
        elif rebuild and not rebuild.get("skipped"):
            export_plan = {"mode": "full", "reason": "full-history rebuild"}
        else:
            export_plan = _incremental_export_plan()
        report["export_plan"] = export_plan

        writer = DuckDBWriter(db_path=str(DB_PATH))
        try:
            if export_plan["mode"] == "incremental":
                writer.export_to_parquet(
                    str(stage), market="cn",
                    base_dir=str(CN_DIR),
                    changed_tables=set(export_plan["changed_tables"]),
                )
            else:
                writer.export_to_parquet(str(stage), market="cn")
        finally:
            # Surface export-path degradations instead of losing them in logs.
            report["export_degradations"] = list(getattr(writer, "export_degradations", []))
            writer.close()
        report["staged_export"] = str(stage)
        report["promotion"] = promote_staged_export(stage, apply=True)
        shutil.rmtree(stage, ignore_errors=True)
        report["serving_placeholders"] = ensure_serving_placeholders(True, target, gate_symbols)

    report["integrity_after"] = _integrity_report(
        target, CN_DIR if CN_DIR.is_dir() else None, history_start
    )
    report["ptrade_smoke"] = _ptrade_smoke()
    health = _health_report(target, history_start) if apply and not args.validate_only else {
        "status": "pass",
        "skipped": True,
        "reason": "health report is generated for full-history apply/publish runs",
    }
    report["health_report"] = health
    failed_errors = [c for c in report["integrity_after"].get("checks", [])
                     if c.get("status") == "fail" and c.get("severity") == "error"]
    smoke_ok = report["ptrade_smoke"].get("status") == "pass"
    health_ok = health.get("status") == "pass" and (
        health.get("skipped") is True
        or health.get("health_status") in {"pass", "warning"}
    )
    report["status"] = "pass" if not failed_errors and smoke_ok and health_ok else "fail"
    report["failed_error_checks"] = [c["name"] for c in failed_errors]
    report["finished_at"] = _now().isoformat(timespec="seconds")

    conn = duckdb.connect(str(DB_PATH))
    try:
        conn.execute("""
            UPDATE unified_runs SET finished_at=?, status=?, target_date=?, report_path=?, notes=?
            WHERE run_id=?
        """, [_now(), report["status"], target, str(report_path),
              "strict_gate=" + report["status"], run_id])
    finally:
        conn.close()
    if apply and report["status"] == "pass":
        # A real full-history rebuild establishes a new baseline: clear the
        # per-row change marks (which otherwise number in the millions) and
        # redirect older delta clients to a baseline package. A no-op rebuild
        # (skipped) must not move the baseline.
        rebuild = report.get("download_rebuild") or {}
        full_rebuild = bool(getattr(args, "full_history", False)) and not rebuild.get("skipped")
        baseline_writer = DuckDBWriter(str(DB_PATH))
        try:
            if full_rebuild:
                cleared = baseline_writer.mark_baseline(str(report.get("target_date") or ""))
                report["baseline_marked"] = {
                    "version": report.get("target_date"),
                    "cleared_change_log_rows": cleared,
                }
            else:
                report["pruned_change_log_rows"] = baseline_writer.prune_change_log()
        finally:
            baseline_writer.close()
    _atomic_write_json(report_path, report)
    _write_status(apply, report)
    if apply and report["status"] == "pass":
        _write_publish_state(report)
        _write_quality_baseline(report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Internet-first unified SimTradeData maintenance; QMT is optional fallback")
    parser.add_argument("--apply", action="store_true", help="Actually mutate DB and serving view")
    parser.add_argument("--validate-only", action="store_true", help="Only run integrity and PTrade smoke checks")
    parser.add_argument("--no-oss", action="store_true", help="Disable optional AKShare/ADATA/efinance adapters; first-party internet sources remain enabled")
    parser.add_argument("--no-router", action="store_true", help="Disable all network SmartRouter supplementation/target discovery")
    parser.add_argument("--no-qmt", action="store_true", help="Skip optional local QMT DAT cross-check/supplement")
    parser.add_argument("--target-date", default=None, help="Explicit expected latest date YYYY-MM-DD; only real source bars are written")
    parser.add_argument("--lookback-days", type=int, default=30, help="Probe/backfill lookback window for internet target discovery")
    parser.add_argument("--history-start", default=None, help="Set SIMTRADE_CN_HISTORY_START for this run (YYYY-MM-DD)")
    parser.add_argument("--full-history", action="store_true", help="Run internet-first bulk TDX/Mootdx/BaoStock rebuild before publish; defaults history start to 1990-12-19")
    parser.add_argument("--limit", type=int, default=0, help="Limit QMT DAT files (for testing)")
    parser.add_argument("--max-router-symbols", type=int, default=0, help="Limit network-supplemented symbols")
    parser.add_argument("--json", action="store_true", help="Print full JSON report")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when final status is not pass")
    parser.add_argument("--force-publish", action="store_true", help="Re-run backup/export/gates even when nothing changed")
    parser.add_argument("--force-rebuild", action="store_true", help="Run the bulk download pipeline even when a full-history rebuild is already current")
    parser.add_argument("--full-export", action="store_true", help="Rebuild every serving parquet instead of reusing unchanged tables")
    parser.add_argument("--allow-data-quality-issues", action="store_true", help="Publish despite data-quality gate errors (report still records them)")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    # One writer at a time: backup, export, promotion and the gate subprocesses
    # all touch the same DuckDB file and the serving view.
    from simtradedata.utils.process_lock import ProcessLock

    with ProcessLock(str(DATA_ROOT / ".unified.lock")):
        report = run(args)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    else:
        print("=" * 78)
        print("SimTradeData Unified Data Manager")
        print("=" * 78)
        print(f"run_id:       {report['run_id']}")
        print(f"mode:         {report['mode']}")
        print(f"status:       {report['status'].upper()}")
        print(f"target date:  {report.get('target_date')}")
        print(f"report:       {report['report_path']}")
        if "qmt_import" in report:
            q = report["qmt_import"]
            print(f"QMT files:    {q['files_seen']:,}, rows: {q['rows_written']:,}, source last: {q['source_last_date']}")
        if "router_supplement" in report:
            r = report["router_supplement"]
            print(f"router gaps:  {r['stale_symbols']:,}, rows: {r['written_rows']:,}, target: {r.get('target_date')}")
        if "download_rebuild" in report:
            d = report["download_rebuild"]
            print(f"bulk rebuild: {d['status']}, start: {d.get('history_start')}")
        if report.get("failed_error_checks"):
            print("failed checks: " + ", ".join(report["failed_error_checks"]))
        print("=" * 78)
    if args.strict and report["status"] != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
