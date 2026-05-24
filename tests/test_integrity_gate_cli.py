"""CI gate: exercise check_integrity.py --strict against a synthetic DuckDB.

The 2 GB canonical database never enters CI. This test builds the same
minimal schema used by the integrity unit tests, runs the real CLI, and checks
both the pass path and a fail-closed negative case. It is collected by the
existing offline ``pytest tests/`` CI stage without any network access.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb

from tests.test_check_integrity import create_db

ROOT = Path(__file__).resolve().parents[1]


def _run_gate(db: Path, tmp_path: Path, label: str):
    report_path = tmp_path / f"integrity-gate-{label}.json"
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.check_integrity",
         "--db-path", str(db), "--target-date", "2026-06-24",
         "--strict", "--json-output", str(report_path)],
        cwd=str(ROOT), capture_output=True, text=True, errors="replace", timeout=180,
    )
    assert report_path.is_file(), proc.stdout + proc.stderr
    return proc, json.loads(report_path.read_text(encoding="utf-8"))


def test_integrity_gate_cli_passes_on_clean_db(tmp_path):
    db = tmp_path / "clean.duckdb"
    create_db(db)
    proc, report = _run_gate(db, tmp_path, "clean")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert report["status"] == "pass"


def test_integrity_gate_cli_fails_on_missing_valuation(tmp_path):
    db = tmp_path / "dirty.duckdb"
    create_db(db)
    con = duckdb.connect(str(db))
    try:
        con.execute("DELETE FROM valuation WHERE symbol='000001.SZ'")
    finally:
        con.close()
    proc, report = _run_gate(db, tmp_path, "dirty")
    # A missing active valuation row is an error-severity completeness failure;
    # --strict must turn it into a non-zero exit code.
    failed_errors = [c for c in report.get("checks", [])
                     if c.get("status") == "fail" and c.get("severity") == "error"]
    assert proc.returncode == 1, (proc.returncode, report.get("status"))
    assert failed_errors, report.get("status")
