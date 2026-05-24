"""Regression tests for the configured CN history floor."""

import json

import duckdb
import pandas as pd
import pytest

from scripts import check_integrity as integrity
from simtradedata.config import field_mappings as fm


def _create_floor_db(
    path, *, metadata_date="20050430", status_date="20050430",
    benchmark_date="2005-05-09",
):
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE stocks (symbol VARCHAR, date DATE)")
        conn.execute("CREATE TABLE valuation (symbol VARCHAR, date DATE)")
        conn.execute(
            "CREATE TABLE stock_metadata (symbol VARCHAR, de_listed_date DATE, security_type VARCHAR, blocks VARCHAR)"
        )
        conn.execute(
            "CREATE TABLE benchmark (date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, money DOUBLE)"
        )
        conn.execute("CREATE TABLE index_constituents (date VARCHAR, index_code VARCHAR, symbols VARCHAR)")
        conn.execute("CREATE TABLE stock_status (date VARCHAR, status_type VARCHAR, symbols VARCHAR)")
        conn.execute("INSERT INTO stocks VALUES ('000001.SZ', '2005-05-09')")
        conn.execute("INSERT INTO valuation VALUES ('000001.SZ', '2005-05-09')")
        conn.execute(
            "INSERT INTO stock_metadata VALUES ('000001.SZ', '2900-01-01', '1', '{\"ZJHHY\": []}')"
        )
        conn.execute(
            "INSERT INTO benchmark VALUES (?, 1, 1, 1, 1, 1, 1)",
            [benchmark_date],
        )
        conn.execute(
            "INSERT INTO index_constituents VALUES (?, '399005.SZ', ?)",
            [metadata_date, json.dumps(["000001.SZ"])],
        )
        conn.execute(
            "INSERT INTO stock_status VALUES (?, 'HALT', ?)",
            [status_date, json.dumps(["000001.SZ"])],
        )
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("metadata_date", "status_date", "expect_floor_fail"),
    [
        ("20050430", "20050430", True),
        ("20050509", "20050509", False),
    ],
)
def test_integrity_history_floor_date_tables(
    tmp_path, monkeypatch, metadata_date, status_date, expect_floor_fail
):
    db_path = tmp_path / "cn.duckdb"
    _create_floor_db(db_path, metadata_date=metadata_date, status_date=status_date)
    monkeypatch.setattr(integrity, "CN_HISTORY_START", "2005-05-09")

    report = integrity.check_integrity(str(db_path), target_date="2005-05-09")

    assert (report["status"] == "fail") is expect_floor_fail
    floor_failed = {
        check["name"]
        for check in report["checks"]
        if check["status"] == "fail" and check["name"].endswith("_history_floor")
    }
    assert bool(floor_failed) is expect_floor_fail
    if expect_floor_fail:
        assert "stock_status_history_floor" in floor_failed


def test_integrity_rejects_pre_floor_export_metadata(tmp_path, monkeypatch):
    db_path = tmp_path / "cn.duckdb"
    _create_floor_db(db_path, metadata_date="20050509", status_date="20050509")
    export_dir = tmp_path / "export"
    (export_dir / "stocks").mkdir(parents=True)
    (export_dir / "valuation").mkdir()
    (export_dir / "metadata").mkdir()
    # The export floor checks read only the date column from these files.
    pd.DataFrame({"date": pd.to_datetime(["2005-05-09"])}).to_parquet(
        export_dir / "stocks" / "000001.SZ.parquet", index=False
    )
    pd.DataFrame({"date": pd.to_datetime(["2005-05-09"])}).to_parquet(
        export_dir / "valuation" / "000001.SZ.parquet", index=False
    )
    pd.DataFrame({"symbol": ["000001.SZ"]}).to_parquet(
        export_dir / "metadata" / "stock_metadata.parquet", index=False
    )
    pd.DataFrame({"date": pd.to_datetime(["2005-05-09"])}).to_parquet(
        export_dir / "metadata" / "benchmark.parquet", index=False
    )
    pd.DataFrame({"date": ["20050430"]}).to_parquet(
        export_dir / "metadata" / "stock_status.parquet", index=False
    )
    (export_dir / "manifest.json").write_text(
        json.dumps({"version": "2005-05-09", "date_range": {"end": "2005-05-09"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(integrity, "CN_HISTORY_START", "2005-05-09")

    report = integrity.check_integrity(
        str(db_path), target_date="2005-05-09", export_dir=str(export_dir)
    )

    failed = {check["name"] for check in report["checks"] if check["status"] == "fail"}
    assert "stock_status_export_history_floor" in failed


def test_benchmark_floor_follows_configured_history_start(monkeypatch):
    # CSI300 (the default index) only exists from 2005-01-04. A 1990 floor is
    # clamped to that inception instead of failing an impossible requirement.
    monkeypatch.setattr(fm, "CN_HISTORY_START", "1990-12-19")
    assert fm.effective_benchmark_history_floor() == "2005-01-04"
    assert fm.benchmark_history_ok("2005-01-04")
    assert not fm.benchmark_history_ok("2015-01-05")

    monkeypatch.setattr(fm, "CN_HISTORY_START", None)
    assert fm.effective_benchmark_history_floor() == "2015-01-05"
    assert fm.benchmark_history_ok("2015-01-05")


def test_benchmark_floor_not_clamped_for_long_history_index(monkeypatch):
    """A deployment on the SSE Composite keeps the full 1990 floor."""
    monkeypatch.setattr(fm, "CN_HISTORY_START", "1990-12-19")
    monkeypatch.setitem(fm.BENCHMARK_CONFIG, "default_index", "000001.SS")
    assert fm.effective_benchmark_history_floor() == "1990-12-19"
    assert fm.benchmark_history_ok("1990-12-19")
    assert not fm.benchmark_history_ok("2005-01-04")


def test_full_history_benchmark_gate_accepts_index_inception(tmp_path, monkeypatch):
    """The one-shot full-history floor must not fail on CSI300's 2005 start."""
    db_path = tmp_path / "cn.duckdb"
    _create_floor_db(db_path, benchmark_date="2005-01-04")
    monkeypatch.setattr(integrity, "CN_HISTORY_START", "1990-12-19")

    report = integrity.check_integrity(str(db_path), target_date="2005-05-09")

    check = next(
        c for c in report["checks"] if c["name"] == "benchmark_history_coverage"
    )
    assert check["status"] == "pass"
    assert check["expected"] == "<= 2005-01-04"
