"""Regression tests for the additive long-history index asset."""

from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "build_index_history.py"


def _load():
    spec = importlib.util.spec_from_file_location("build_index_history_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _seed(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE stocks (
                date DATE, symbol VARCHAR,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                volume DOUBLE, money DOUBLE,
                PRIMARY KEY (symbol, date)
            )
            """
        )
        rows = []
        for symbol, start in (("000001.SS", "1990-12-19"), ("000300.SS", "2005-01-04")):
            base = date.fromisoformat(start)
            for offset in range(3):
                rows.append((base + timedelta(days=offset), symbol,
                             1.0, 2.0, 0.5, 1.5, 100.0, 1000.0))
        con.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        con.close()


def test_dry_run_is_read_only(tmp_path):
    mod = _load()
    db = tmp_path / "cn.duckdb"
    _seed(db)
    export = tmp_path / "metadata" / "index_history.parquet"
    report = mod.build(db, export, apply=False)
    assert report["mode"] == "dry-run"
    assert report["earliest_bar"] == "1990-12-19"
    assert report["rows"] == 6
    assert not export.exists()
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert "index_history" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    finally:
        con.close()


def test_apply_is_idempotent_and_keeps_pre_inception_bars_out(tmp_path):
    mod = _load()
    db = tmp_path / "cn.duckdb"
    _seed(db)
    export = tmp_path / "metadata" / "index_history.parquet"
    first = mod.build(db, export, apply=True)
    assert first["written_rows"] == 6
    assert first["parquet_written"] is True
    assert pq.ParquetFile(export).metadata.num_rows == 6
    assert mod.build(db, export, apply=True)["written_rows"] == 6

    con = duckdb.connect(str(db), read_only=True)
    try:
        assert con.execute(
            "SELECT MIN(date), COUNT(DISTINCT symbol) FROM index_history"
        ).fetchone() == (date(1990, 12, 19), 2)
        # CSI300 keeps its real inception; no 1990 rows are invented for it.
        assert con.execute(
            "SELECT MIN(date) FROM index_history WHERE symbol='000300.SS'"
        ).fetchone()[0] == date(2005, 1, 4)
    finally:
        con.close()
