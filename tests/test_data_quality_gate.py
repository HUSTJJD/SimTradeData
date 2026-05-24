"""The publish gate must block dirty data and any regression vs last publish."""
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "dq_gate_test", ROOT / "scripts" / "data_quality_gate.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _check(name, status="pass", severity="error", **details):
    return {"name": name, "status": status, "severity": severity,
            "auto_repair": "source_refresh_or_quarantine", **details}


def _report(checks):
    errors = [c for c in checks if c["status"] == "fail" and c["severity"] == "error"]
    warnings = [c for c in checks if c["status"] != "pass" and c["severity"] == "warning"]
    return {
        "canonical_db": "cn.duckdb",
        "target_date": "2026-11-26",
        "quality_checks": checks,
        "quality_summary": {
            "checks": len(checks), "errors": len(errors),
            "warnings": len(warnings),
            "passes": sum(1 for c in checks if c["status"] == "pass"),
            "auto_repairable": [],
        },
        "table_inventory": {"stocks": {"rows": 10, "date_min": "1990-12-19",
                                       "date_max": "2026-11-26"}},
        "instrument_universe": {},
        "domains": {},
    }


def test_clean_db_passes_and_gate_report_is_readable(tmp_path):
    mod = _load()
    checks = [
        _check("stocks_duplicate_symbol_date"),
        _check("stocks_null_ohlc", "warning", "warning", count=2702),
        _check("stock_metadata_blocks_coverage", ratio=0.975),
    ]
    gate = mod.evaluate(_report(checks), None, allow_issues=False)
    assert gate["status"] == "pass"
    assert gate["blocking_reasons"] == []
    assert gate["regressions"] == []
    assert gate["warning_checks"][0]["count"] == 2702
    assert len(gate["checks"]) == 3
    text = mod._markdown(gate)
    assert "数据质量门禁" in text and "stocks_null_ohlc" in text
    p = tmp_path / "gate.json"
    p.write_text(json.dumps(gate, ensure_ascii=False, indent=2), encoding="utf-8")
    assert json.loads(p.read_text(encoding="utf-8"))["status"] == "pass"


def test_error_severity_blocks_even_without_baseline():
    mod = _load()
    checks = [
        _check("stocks_duplicate_symbol_date"),
        _check("stocks_nonpositive_price", "fail", "error", count=3),
    ]
    gate = mod.evaluate(_report(checks), None, allow_issues=False)
    assert gate["status"] == "fail"
    assert len(gate["blocking_checks"]) == 1
    assert gate["blocking_checks"][0]["name"] == "stocks_nonpositive_price"
    assert gate["waived"] is False


def test_new_dirty_rows_regress_against_baseline():
    mod = _load()
    checks = [
        _check("stocks_null_ohlc", "warning", "warning", count=2702),
        _check("stocks_inconsistent_ohlc", "warning", "warning", count=99),
    ]
    baseline = {
        "errors": 0, "warnings": 2,
        "metrics": {
            "stocks_null_ohlc": {"count": 2702, "ratio": None, "status": "warning"},
            "stocks_inconsistent_ohlc": {"count": 90, "ratio": None, "status": "warning"},
        },
    }
    gate = mod.evaluate(_report(checks), baseline, allow_issues=False)
    assert gate["status"] == "fail"
    assert gate["regressions"] == [
        {"name": "stocks_inconsistent_ohlc", "metric": "count",
         "before": 90, "after": 99},
    ]


def test_coverage_ratio_drop_regresses():
    mod = _load()
    checks = [_check("stock_metadata_blocks_coverage", "fail", "error", ratio=0.93)]
    baseline = {
        "errors": 0, "warnings": 0,
        "metrics": {"stock_metadata_blocks_coverage": {"count": None,
                                                      "ratio": 0.975,
                                                      "status": "pass"}},
    }
    gate = mod.evaluate(_report(checks), baseline, allow_issues=False)
    assert gate["status"] == "fail"
    assert any(r["metric"] == "ratio" for r in gate["regressions"])


def test_allow_issues_waives_but_still_records():
    mod = _load()
    checks = [_check("stocks_future_dates", "fail", "error", count=5)]
    gate = mod.evaluate(_report(checks), None, allow_issues=True)
    assert gate["status"] == "fail"
    assert gate["effective_status"] == "pass"
    assert gate["waived"] is True
    assert len(gate["blocking_checks"]) == 1


def test_manager_does_not_export_or_promote_when_gate_fails(tmp_path, monkeypatch):
    import argparse

    spec = importlib.util.spec_from_file_location(
        "udm_qgate_test", ROOT / "scripts" / "unified_data_manager.py"
    )
    manager = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(manager)
    from simtradedata.writers.duckdb_writer import DuckDBWriter

    db = tmp_path / "cn.duckdb"
    writer = DuckDBWriter(str(db))
    writer.close()

    view = tmp_path / "cn"
    (view / "stocks").mkdir(parents=True)
    (view / "manifest.json").write_text(json.dumps({
        "mode": "full", "version": "2026-11-26",
        "date_range": {"start": "1990-12-19", "end": "2026-11-26"},
    }), encoding="utf-8")
    ops = tmp_path / "ops"
    ops.mkdir()
    for attr, value in (
        ("DB_PATH", db), ("CN_DIR", view), ("DATA_ROOT", tmp_path),
        ("REPORT_DIR", ops), ("PUBLISH_STATE_PATH", ops / "latest_publish_state.json"),
    ):
        monkeypatch.setattr(manager, attr, value)

    sentinel = (view / "stocks" / "000001.SZ.parquet")
    sentinel.write_bytes(b"untouched")
    monkeypatch.setattr(
        manager, "_data_quality_gate",
        lambda *a, **k: {"status": "fail", "blocking_reasons": ["injected dirty row"]},
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("export/promotion must not run when the gate fails")

    monkeypatch.setattr(manager, "promote_staged_export", forbidden)
    monkeypatch.setattr(manager, "_ptrade_smoke", lambda: {"status": "pass"})
    args = argparse.Namespace(**dict(
        apply=True, validate_only=False, target_date="2026-11-26",
        history_start=None, full_history=False, no_router=True, no_oss=True,
        no_qmt=True, max_router_symbols=0, lookback_days=30, limit=0,
        force_publish=True, force_rebuild=False, full_export=False,
        allow_data_quality_issues=False,
    ))
    report = manager.run(args)

    assert report["status"] == "fail"
    assert report["failed_error_checks"] == ["data_quality_gate"]
    assert "staged_export" not in report
    assert "promotion" not in report
    assert sentinel.read_bytes() == b"untouched"


def test_cli_returns_nonzero_on_dirty_db(tmp_path):
    import subprocess
    import sys

    from simtradedata.writers.duckdb_writer import DuckDBWriter

    db = tmp_path / "cn.duckdb"
    writer = DuckDBWriter(str(db))
    try:
        writer.conn.execute(
            "INSERT INTO stocks (symbol, date, open, close, high, low, volume, money) "
            "VALUES ('000001.SZ', DATE '2026-11-26', -1.0, -1.0, -1.0, -1.0, 100, 100.0)"
        )
    finally:
        writer.close()
    view = tmp_path / "cn"
    view.mkdir()
    gate_json = tmp_path / "gate.json"
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "data_quality_gate.py"),
         "--db-path", str(db), "--export-dir", str(view),
         "--target-date", "2026-11-26", "--baseline", str(tmp_path / "none.json"),
         "--json-output", str(gate_json)],
        capture_output=True, text=True, cwd=str(ROOT),
    )
    assert result.returncode == 1, result.stdout + result.stderr
    gate = json.loads(gate_json.read_text(encoding="utf-8"))
    assert gate["status"] == "fail"
    names = [c["name"] for c in gate["blocking_checks"]]
    assert "stocks_nonpositive_price" in names
