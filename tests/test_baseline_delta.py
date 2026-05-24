"""Delta endpoint must redirect clients older than a full-rebuild baseline."""
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "api_delta_baseline_test", ROOT / "scripts" / "api_export_delta.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _make_db(tmp_path, baseline=None):
    db = tmp_path / "cn.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE version_info (key VARCHAR PRIMARY KEY, value VARCHAR)")
    if baseline:
        con.execute("INSERT INTO version_info VALUES ('baseline_version', ?)", [baseline])
    con.close()
    return db


def _run(mod, tmp_path, baseline, last_sync, monkeypatch, max_days=365):
    db = _make_db(tmp_path, baseline)
    monkeypatch.setattr(mod, "_latest_complete_date", lambda conn, market: date(2026, 11, 26))
    captured = {}

    def fake_archive(output, package_dir, names):
        captured.update(json.loads((package_dir / "manifest.json").read_text(encoding="utf-8")))

    monkeypatch.setattr(mod, "_write_archive", fake_archive)
    mod.export_api_delta(
        market="cn", db_path=db, last_sync=last_sync,
        output=tmp_path / "delta.tar", max_days=max_days,
    )
    return captured


def test_baseline_forces_fallback(tmp_path, monkeypatch):
    mod = _load()
    manifest = _run(mod, tmp_path, "2026-11-20", date(2026, 11, 10), monkeypatch)
    assert manifest["fallback_to_baseline"] is True
    assert manifest["reason"] == "baseline_required"
    assert manifest["baseline_version"] == "2026-11-20"
    assert manifest["tables"] == []


def test_client_at_or_after_baseline_is_not_redirected(tmp_path, monkeypatch):
    mod = _load()
    manifest = _run(mod, tmp_path, "2026-11-20", date(2026, 11, 22), monkeypatch, max_days=1)
    assert manifest["fallback_to_baseline"] is True
    assert manifest["reason"] == "delta_window_too_large"


def test_up_to_date_client_is_unaffected(tmp_path, monkeypatch):
    mod = _load()
    manifest = _run(mod, tmp_path, "2026-11-20", date(2026, 11, 26), monkeypatch)
    assert manifest["up_to_date"] is True
    assert manifest["fallback_to_baseline"] is False
