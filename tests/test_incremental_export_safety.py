"""Direct DuckDB writes must never leave the incremental export serving stale bars."""

import importlib.util
import json
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "udm_export_safety_test", ROOT / "scripts" / "unified_data_manager.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_record_direct_changes_marks_rows_and_is_idempotent(tmp_path):
    mod = _load()
    db = tmp_path / "cn.duckdb"
    con = duckdb.connect(str(db))
    try:
        con.execute("""
            CREATE TABLE data_change_log (
                table_name VARCHAR, symbol VARCHAR, date DATE,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name, symbol, date)
            )
        """)
        frame = pd.DataFrame({
            "symbol": ["000001.SZ", "000001.SZ", "000002.SZ"],
            "date": ["2026-11-25", "2026-11-26", "2026-11-26"],
        })
        assert mod._record_direct_changes(con, "stocks", frame) == 3
        assert mod._record_direct_changes(con, "stocks", frame) == 3
        rows = con.execute(
            "SELECT table_name, COUNT(*) FROM data_change_log GROUP BY 1"
        ).fetchall()
        assert rows == [("stocks", 3)]
    finally:
        con.close()


def test_incremental_plan_rebuilds_table_with_unlogged_rows(tmp_path, monkeypatch):
    """Regression: a bare INSERT plus a change-log row on another table used
    to leave `stocks` out of changed_tables, so its old Parquet was reused."""
    mod = _load()
    db = tmp_path / "cn.duckdb"
    con = duckdb.connect(str(db))
    try:
        for table in ("stocks", "valuation", "fundamentals", "exrights"):
            con.execute(f"CREATE TABLE {table} (symbol VARCHAR, date DATE)")
        con.execute("""
            CREATE TABLE data_change_log (
                table_name VARCHAR, symbol VARCHAR, date DATE,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name, symbol, date)
            )
        """)
        con.execute(
            "INSERT INTO stocks VALUES ('000001.SZ', DATE '2026-11-26')"
        )
    finally:
        con.close()

    view = tmp_path / "cn"
    view.mkdir()
    (view / "manifest.json").write_text(json.dumps({
        "mode": "full", "version": "v1",
        "date_range": {"start": "1990-12-19", "end": "2026-11-26"},
    }), encoding="utf-8")
    monkeypatch.setattr(mod, "DB_PATH", db)
    monkeypatch.setattr(mod, "CN_DIR", view)
    monkeypatch.setattr(mod, "PUBLISH_STATE_PATH", tmp_path / "state.json")

    ro = duckdb.connect(str(db), read_only=True)
    try:
        db_fp = mod._db_state_fingerprint(ro)
        table_state = mod._db_table_state(ro)
    finally:
        ro.close()
    mod._atomic_write_json(tmp_path / "state.json", {
        "db_fingerprint": db_fp,
        "table_state": table_state,
        "view_fingerprint": mod._serving_view_fingerprint(),
        "published_at": "2000-01-01T00:00:00",
        "target_date": "2026-11-26",
    })

    # Unlogged direct write + a change-log entry on a different table.
    con = duckdb.connect(str(db))
    try:
        con.execute(
            "INSERT INTO stocks VALUES ('000002.SZ', DATE '2026-11-26')"
        )
        con.execute(
            "INSERT INTO data_change_log VALUES "
            "('valuation', '000001.SZ', DATE '2026-11-26', CURRENT_TIMESTAMP)"
        )
    finally:
        con.close()

    plan = mod._incremental_export_plan()
    assert plan["mode"] == "incremental"
    assert "stocks" in plan["changed_tables"]
    assert "valuation" in plan["changed_tables"]
