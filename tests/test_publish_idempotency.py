"""Regression tests for the no-op publish fast path."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "publish_idempotency_test", ROOT / "scripts" / "unified_data_manager.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


def _setup(tmp_path, monkeypatch):
    mod = _load_module()
    db = tmp_path / "cn.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE stocks (symbol VARCHAR, date DATE)")
    con.execute("INSERT INTO stocks VALUES ('000001.SZ', '2026-11-26')")
    con.execute(
        "CREATE TABLE data_change_log (table_name VARCHAR, symbol VARCHAR, date DATE, "
        "changed_at TIMESTAMP, PRIMARY KEY (table_name, symbol, date))"
    )
    con.close()

    view = tmp_path / "cn"
    for sub in ("stocks", "fundamentals", "valuation", "exrights"):
        (view / sub).mkdir(parents=True)
        (view / sub / "000001.SZ.parquet").write_bytes(b"x")
    manifest = {
        "mode": "full",
        "version": "2026-11-26",
        "market": "CN",
        "date_range": {"start": "1990-12-19", "end": "2026-11-26"},
        "export_date": "2026-11-26 18:27:33",
    }
    (view / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    ops = tmp_path / "ops"
    ops.mkdir()
    prev = ops / "unified_prev.json"
    prev.write_text("{}", encoding="utf-8")
    (ops / "latest_status.json").write_text(json.dumps({
        "published": True,
        "status": "pass",
        "target_date": "2026-11-26",
        "report_path": str(prev),
    }), encoding="utf-8")

    monkeypatch.setattr(mod, "DB_PATH", db)
    monkeypatch.setattr(mod, "CN_DIR", view)
    monkeypatch.setattr(mod, "REPORT_DIR", ops)
    monkeypatch.setattr(mod, "PUBLISH_STATE_PATH", ops / "latest_publish_state.json")
    monkeypatch.setattr(mod, "_ptrade_smoke", lambda: {"status": "pass"})
    return mod, db, view, ops


def _write_state(mod, db, ops, target="2026-11-26"):
    con = duckdb.connect(str(db), read_only=True)
    try:
        db_fp = mod._db_state_fingerprint(con)
    finally:
        con.close()
    mod._atomic_write_json(ops / "latest_publish_state.json", {
        "db_fingerprint": db_fp,
        "view_fingerprint": mod._serving_view_fingerprint(),
        "target_date": target,
        "report_path": str(ops / "unified_prev.json"),
        "health_status": "warning",
        "published_at": "2026-11-26T18:27:47",
    })


def _args(**overrides):
    base = dict(
        apply=True, validate_only=False, target_date="2026-11-26",
        history_start=None, full_history=False, no_router=True,
        no_oss=True, no_qmt=True, max_router_symbols=0,
        lookback_days=30, limit=0, force_publish=False,
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_view_fingerprint_ignores_export_timestamp(tmp_path, monkeypatch):
    mod, _, view, _ = _setup(tmp_path, monkeypatch)
    before = mod._serving_view_fingerprint()

    manifest = json.loads((view / "manifest.json").read_text(encoding="utf-8"))
    manifest["export_date"] = "2030-01-01 00:00:00"
    (view / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    assert mod._serving_view_fingerprint() == before


def test_view_fingerprint_detects_missing_files(tmp_path, monkeypatch):
    mod, _, view, _ = _setup(tmp_path, monkeypatch)
    before = mod._serving_view_fingerprint()

    (view / "stocks" / "000001.SZ.parquet").unlink()

    assert mod._serving_view_fingerprint() != before


def test_db_fingerprint_stable_under_noop_rewrite_but_changes_with_rows(tmp_path, monkeypatch):
    mod, db, _, _ = _setup(tmp_path, monkeypatch)
    # Seed one change row first: a brand-new (symbol,date) pair is a real
    # change and SHOULD move the fingerprint.
    con = duckdb.connect(str(db))
    con.execute(
        "INSERT INTO data_change_log VALUES ('stocks','000001.SZ','2026-11-26', CURRENT_TIMESTAMP)"
    )
    con.close()
    con = duckdb.connect(str(db), read_only=True)
    try:
        before = mod._db_state_fingerprint(con)
    finally:
        con.close()

    # Rewriting that same row must NOT change the fingerprint.
    con = duckdb.connect(str(db))
    con.execute(
        "INSERT OR REPLACE INTO data_change_log VALUES ('stocks','000001.SZ','2026-11-26', CURRENT_TIMESTAMP)"
    )
    con.execute(
        "INSERT OR REPLACE INTO data_change_log VALUES ('stocks','000001.SZ','2026-11-26', CURRENT_TIMESTAMP)"
    )
    con.close()
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert mod._db_state_fingerprint(con) == before
    finally:
        con.close()

    con = duckdb.connect(str(db))
    con.execute("INSERT INTO stocks VALUES ('000002.SZ', '2026-11-26')")
    con.close()
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert mod._db_state_fingerprint(con) != before
    finally:
        con.close()


def test_run_skips_all_publish_work_when_nothing_changed(tmp_path, monkeypatch):
    mod, db, view, ops = _setup(tmp_path, monkeypatch)
    _write_state(mod, db, ops)

    calls = []
    monkeypatch.setattr(mod, "_write_publish_state", lambda *a, **k: calls.append("state"))

    report = mod.run(_args())

    assert report["status"] == "pass"
    assert report["skipped"] is True
    assert report["target_date"] == "2026-11-26"
    assert report["integrity_after"]["reused"] is True
    assert calls == []
    assert (ops / "latest_publish_state.json").is_file()
    assert json.loads((ops / "latest_status.json").read_text(encoding="utf-8"))["status"] == "pass"
    # No 1.9GB backup, no staged export, no promotion on the fast path.
    assert report.get("db_backup") is None
    assert report.get("staged_export") is None
    assert report.get("promotion") is None
    assert not list(tmp_path.glob("cn_staging_*"))


def test_reusable_publish_rejects_stale_or_failed_state(tmp_path, monkeypatch):
    mod, db, view, ops = _setup(tmp_path, monkeypatch)
    _write_state(mod, db, ops)

    con = duckdb.connect(str(db), read_only=True)
    try:
        assert mod._reusable_publish(con, "2026-11-26") is not None
        assert mod._reusable_publish(con, "2026-11-27") is None
    finally:
        con.close()

    (ops / "latest_status.json").write_text(
        json.dumps({"published": False, "status": "fail"}), encoding="utf-8"
    )
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert mod._reusable_publish(con, "2026-11-26") is None
    finally:
        con.close()
