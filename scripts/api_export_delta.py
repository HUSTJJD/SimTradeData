#!/usr/bin/env python
"""Export table-level delta package for customer data sync."""

import argparse
import datetime as dt
import hashlib
import json
import tarfile
import tempfile
from pathlib import Path

import duckdb

from check_integrity import _halted_symbols_on, _stock_universe
from export_parquet import _resolve_db
from simtradedata.writers.duckdb_writer import cn_stock_filter_sql


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"invalid date: {value}") from exc


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _latest_stock_date(conn: duckdb.DuckDBPyConnection) -> dt.date:
    value = conn.execute("SELECT MAX(date) FROM stocks").fetchone()[0]
    if value is None:
        raise SystemExit("stocks table is empty")
    if isinstance(value, dt.datetime):
        return value.date()
    return value


def _count_required_symbols_by_date(
    conn: duckdb.DuckDBPyConnection,
    *,
    table: str,
    dates: list[dt.date],
    symbols: list[str],
) -> dict[dt.date, int]:
    """One grouped query for all candidate dates, instead of one scan per date.

    ``dates``/``symbols`` are parallel lists of the exact required
    (date, symbol) pairs from the active/halted universe. Joining against them
    preserves the fail-closed rule (COUNT(DISTINCT symbol) must equal the number
    of required symbols); the Python side then compares the per-date counts.
    """
    if not dates:
        return {}
    rows = conn.execute(
        f"""
        WITH required AS (
            SELECT unnest(?::DATE[]) AS date, unnest(?::VARCHAR[]) AS symbol
        )
        SELECT s.date, COUNT(DISTINCT s.symbol) AS symbol_count
        FROM {table} s
        JOIN required r
          ON r.date = s.date AND r.symbol = s.symbol
        WHERE s.date IN (SELECT DISTINCT date FROM required)
        GROUP BY s.date
        """,
        [dates, symbols],
    ).fetchall()
    return {
        (value.date() if isinstance(value, dt.datetime) else value): int(count)
        for value, count in rows
    }


def _latest_complete_date(conn: duckdb.DuckDBPyConnection, market: str) -> dt.date | None:
    latest = _latest_stock_date(conn)
    if market != "cn":
        return latest

    recent_dates = conn.execute(
        """
        SELECT date
        FROM stocks
        GROUP BY date
        ORDER BY date DESC
        LIMIT 60
        """
    ).fetchall()
    candidates = [
        value.date() if isinstance(value, dt.datetime) else value
        for (value,) in recent_dates
    ]

    # Resolve the active/halted universe once per candidate (small metadata
    # tables), then batch the two big COUNT(DISTINCT symbol) checks into one
    # grouped query per table instead of two full scans per candidate round.
    required_by_date: dict[dt.date, list[str]] = {}
    empty_universe_date: dt.date | None = None
    for candidate_date in candidates:
        candidate_text = candidate_date.isoformat()
        active_symbols, _ = _stock_universe(conn, market, candidate_text)
        halted_symbols = _halted_symbols_on(conn, candidate_text)
        required_symbols = [
            symbol for symbol in active_symbols if symbol not in halted_symbols
        ]
        if not required_symbols:
            empty_universe_date = candidate_date
            break
        required_by_date[candidate_date] = required_symbols

    if not required_by_date:
        return empty_universe_date

    pair_dates: list[dt.date] = []
    pair_symbols: list[str] = []
    for candidate_date, required_symbols in required_by_date.items():
        pair_dates.extend([candidate_date] * len(required_symbols))
        pair_symbols.extend(required_symbols)

    stocks_counts = _count_required_symbols_by_date(
        conn, table="stocks", dates=pair_dates, symbols=pair_symbols
    )
    valuation_counts = _count_required_symbols_by_date(
        conn, table="valuation", dates=pair_dates, symbols=pair_symbols
    )
    for candidate_date, required_symbols in required_by_date.items():
        expected = len(required_symbols)
        if (
            stocks_counts.get(candidate_date, 0) == expected
            and valuation_counts.get(candidate_date, 0) == expected
        ):
            return candidate_date

    return empty_universe_date


def _copy_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    table: str,
    sql: str,
    output_dir: Path,
) -> dict:
    output = output_dir / f"{table}.parquet"
    conn.execute(
        f"COPY ({sql}) TO '{_sql_path(output)}' (FORMAT PARQUET, CODEC 'ZSTD')"
    )
    # The Parquet footer records the exact row count COPY just wrote; reading
    # it avoids executing the export query a second time for COUNT(*).
    rows = conn.execute(
        "SELECT COALESCE(SUM(num_rows), 0) FROM parquet_file_metadata(?)",
        [str(output)],
    ).fetchone()[0]
    return {
        "table": table,
        "path": output.name,
        "rows": int(rows),
        "size": output.stat().st_size,
        "sha256": _sha256(output),
    }


def _write_manifest(path: Path, manifest: dict) -> None:
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_archive(output: Path, package_dir: Path, files: list[str]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, "w:gz") as tar:
        for name in files:
            tar.add(package_dir / name, arcname=name)


def _write_manifest_archive(output: Path, manifest: dict) -> None:
    with tempfile.TemporaryDirectory(prefix="simtradedata-delta-") as tmp:
        package_dir = Path(tmp)
        _write_manifest(package_dir / "manifest.json", manifest)
        _write_archive(output, package_dir, ["manifest.json"])


def _write_busy_manifest_archive(
    output: Path,
    *,
    market: str,
    last_sync: dt.date,
    retry_after: int,
) -> None:
    _write_manifest_archive(output, {
        "package_format": "simtradedata_api_delta_v1",
        "schema_version": 1,
        "market": market,
        "from_version": last_sync.isoformat(),
        "to_version": None,
        "up_to_date": False,
        "fallback_to_baseline": False,
        "pipeline_busy": True,
        "retry_after": retry_after,
        "tables": [],
    })


def export_api_delta(
    *,
    market: str,
    db_path: Path,
    last_sync: dt.date,
    output: Path,
    max_days: int,
    retry_after: int = 300,
) -> None:
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
    except duckdb.IOException as exc:
        message = str(exc)
        # DuckDB 1.5.6 on Windows reports a busy file as "File is already open
        # in <pid>" rather than the older lock-phrasing; the localized Chinese
        # prefix varies with the OS code page, so match the stable English tail.
        busy_markers = (
            "Could not set lock",
            "Conflicting lock",
            "File is already open in",
        )
        if not any(marker in message for marker in busy_markers):
            raise
        _write_busy_manifest_archive(
            output,
            market=market,
            last_sync=last_sync,
            retry_after=retry_after,
        )
        return

    with conn:
        latest = _latest_complete_date(conn, market)
        if latest is None:
            _write_busy_manifest_archive(
                output,
                market=market,
                last_sync=last_sync,
                retry_after=retry_after,
            )
            return

        base_manifest = {
            "package_format": "simtradedata_api_delta_v1",
            "schema_version": 1,
            "market": market,
            "from_version": last_sync.isoformat(),
            "to_version": latest.isoformat(),
            "up_to_date": last_sync >= latest,
            "fallback_to_baseline": False,
            "pipeline_busy": False,
            "retry_after": None,
            "tables": [],
        }

        with tempfile.TemporaryDirectory(prefix="simtradedata-delta-") as tmp:
            package_dir = Path(tmp)

            if last_sync >= latest:
                _write_manifest(package_dir / "manifest.json", base_manifest)
                _write_archive(output, package_dir, ["manifest.json"])
                return

            # A full-history rebuild clears the change log and establishes a
            # new baseline. Clients older than that baseline cannot be served
            # an incremental delta, so redirect them to the baseline package.
            baseline = None
            try:
                row = conn.execute(
                    "SELECT value FROM version_info WHERE key='baseline_version'"
                ).fetchone()
                if row and row[0]:
                    baseline = dt.date.fromisoformat(str(row[0]))
            except Exception:  # noqa: BLE001
                baseline = None
            if baseline and last_sync < baseline:
                manifest = {
                    **base_manifest,
                    "fallback_to_baseline": True,
                    "reason": "baseline_required",
                    "baseline_version": baseline.isoformat(),
                }
                _write_manifest(package_dir / "manifest.json", manifest)
                _write_archive(output, package_dir, ["manifest.json"])
                return

            if (latest - last_sync).days > max_days:
                manifest = {
                    **base_manifest,
                    "fallback_to_baseline": True,
                    "reason": "delta_window_too_large",
                }
                _write_manifest(package_dir / "manifest.json", manifest)
                _write_archive(output, package_dir, ["manifest.json"])
                return

            start = last_sync.isoformat()
            end = latest.isoformat()
            # 拼进 SQL 前显式断言类型：last_sync 经 _parse_date(date.fromisoformat)
            # 校验，latest 来自 duckdb DATE 列，两者都必须是 dt.date，isoformat()
            # 输出才必然无注入面（_copy_table 不支持参数化）。
            assert isinstance(last_sync, dt.date) and isinstance(latest, dt.date)

            # 与全量导出对齐：CN 包只含真实 A 股（security_type='1' 或旧库前缀回退），
            # 指数/ETF/BJ 行不进 API 数据包。stock_metadata 与全量包一致不过滤：
            # 指数行是有意注入的，客户端盘前选股依赖。
            stocks_filter = f"AND {cn_stock_filter_sql('stocks.symbol')} " if market == "cn" else ""
            valuation_filter = f"AND {cn_stock_filter_sql('valuation.symbol')} " if market == "cn" else ""
            exrights_filter = f"AND {cn_stock_filter_sql('e.symbol')} " if market == "cn" else ""

            tables = [
                _copy_table(
                    conn,
                    table="stocks",
                    sql=(
                        "SELECT * FROM stocks "
                        f"WHERE date > DATE '{start}' AND date <= DATE '{end}' "
                        f"{stocks_filter}ORDER BY symbol, date"
                    ),
                    output_dir=package_dir,
                ),
                _copy_table(
                    conn,
                    table="valuation",
                    sql=(
                        "SELECT * FROM valuation "
                        f"WHERE date > DATE '{start}' AND date <= DATE '{end}' "
                        f"{valuation_filter}ORDER BY symbol, date"
                    ),
                    output_dir=package_dir,
                ),
                _copy_table(
                    conn,
                    table="stock_status",
                    sql=(
                        # DB 中 symbols 存的是 JSON 字符串，导出为 list 与全量包格式对齐。
                        # 全量导出：客户 redis 权威部署会重写历史状态，窗口增量到不了
                        # 客户端；全表 zstd 约 1.7MB，随 delta 重发可接受。
                        "SELECT date, status_type, symbols::JSON::VARCHAR[] AS symbols "
                        "FROM stock_status "
                        "ORDER BY date, status_type"
                    ),
                    output_dir=package_dir,
                ),
                _copy_table(
                    conn,
                    table="exrights",
                    sql=(
                        # 按 data_change_log 标记的 symbol 全量导出（长表），客户端
                        # 覆盖对应 per-symbol 文件。权威部署每天全量标记，
                        # 其他部署按东财新事件增量标记。
                        "SELECT e.symbol, e.date::TIMESTAMP_NS AS date, "
                        "allotted_ps, rationed_ps, rationed_px, bonus_ps, "
                        "COALESCE(e.dividend, e.bonus_ps) AS dividend, "
                        "exer_forward_a, exer_forward_b, exer_backward_a, exer_backward_b "
                        "FROM exrights e "
                        "WHERE e.symbol IN ("
                        "  SELECT symbol FROM data_change_log "
                        "  WHERE table_name = 'exrights' AND symbol IS NOT NULL "
                        f"    AND (date > DATE '{start}' OR changed_at::DATE > DATE '{start}')"
                        ") "
                        f"{exrights_filter}ORDER BY e.symbol, e.date"
                    ),
                    output_dir=package_dir,
                ),
                _copy_table(
                    conn,
                    table="stock_metadata",
                    sql="SELECT * FROM stock_metadata ORDER BY symbol",
                    output_dir=package_dir,
                ),
            ]

            manifest = {**base_manifest, "tables": tables}
            _write_manifest(package_dir / "manifest.json", manifest)
            _write_archive(
                output,
                package_dir,
                [
                    "manifest.json",
                    "stocks.parquet",
                    "valuation.parquet",
                    "stock_status.parquet",
                    "stock_metadata.parquet",
                    "exrights.parquet",
                ],
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", choices=["cn", "us"], default="cn")
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--last-sync", required=True, help="Last local version, YYYY-MM-DD")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-days", type=int, default=60)
    parser.add_argument("--retry-after", type=int, default=300)
    args = parser.parse_args()

    export_api_delta(
        market=args.market,
        db_path=args.db or Path(_resolve_db(args.market)),
        last_sync=_parse_date(args.last_sync),
        output=args.output,
        max_days=args.max_days,
        retry_after=args.retry_after,
    )


if __name__ == "__main__":
    main()
