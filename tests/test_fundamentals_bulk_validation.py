"""Bulk fundamentals writes must keep the upstream per-company validation."""

import pandas as pd
import pytest

from simtradedata.validators import DataQualityError
from simtradedata.writers.duckdb_writer import DuckDBWriter


def test_bulk_marks_source_roa_as_not_derived(tmp_path):
    writer = DuckDBWriter(str(tmp_path / "x.duckdb"))
    try:
        df = pd.DataFrame({
            "symbol": ["000001.SZ"],
            "end_date": ["2024-03-31"],
            "publ_date": ["20240430"],
            "roa": [3.5],
            "roe": [12.0],
        })
        assert writer.write_fundamentals_bulk(df) == 1
        roa, derived = writer.conn.execute(
            "SELECT roa, roa_derived FROM fundamentals"
        ).fetchone()
        assert roa == pytest.approx(3.5)
        # Source ROA must not be recomputed by compute_derived_fundamentals().
        assert derived is False
    finally:
        writer.close()


def test_bulk_rejects_conflicting_quarters_without_announcement_date(tmp_path):
    writer = DuckDBWriter(str(tmp_path / "x.duckdb"))
    try:
        df = pd.DataFrame({
            "symbol": ["000001.SZ", "000001.SZ"],
            "end_date": ["2024-03-31", "2024-03-31"],
            "roa": [1.0, 2.0],
        })
        with pytest.raises(DataQualityError, match="Conflicting quarters"):
            writer.write_fundamentals_bulk(df)
    finally:
        writer.close()


def test_bulk_rejects_duplicate_quarters_with_same_announcement_date(tmp_path):
    writer = DuckDBWriter(str(tmp_path / "x.duckdb"))
    try:
        df = pd.DataFrame({
            "symbol": ["000001.SZ", "000001.SZ"],
            "end_date": ["2024-03-31", "2024-03-31"],
            "publ_date": ["20240430", "20240430"],
            "roe": [10.0, 99.0],
        })
        with pytest.raises(DataQualityError, match="same announcement date"):
            writer.write_fundamentals_bulk(df)
    finally:
        writer.close()


def test_bulk_matches_per_symbol_path_and_records_changes(tmp_path):
    """Bulk keeps result parity with the upstream per-company writer."""
    frame = pd.DataFrame({
        "code": ["000001", "000002", "600000"],
        "end_date": pd.to_datetime(["2006-06-30"] * 3),
        "publ_date": ["20060831"] * 3,
        "roe": [1.0, 2.0, 3.0],
        "roa": [0.5, 1.5, 2.5],
    })
    per_symbol = DuckDBWriter(str(tmp_path / "per.duckdb"))
    bulk = DuckDBWriter(str(tmp_path / "bulk.duckdb"))
    try:
        for code, group in frame.groupby("code"):
            symbol = f"{code}.SZ" if code.startswith("0") else f"{code}.SS"
            part = group.drop(columns=["code"]).set_index("end_date")
            per_symbol.write_fundamentals(symbol, part)

        work = frame.copy()
        work["symbol"] = [
            f"{c}.SZ" if c.startswith("0") else f"{c}.SS" for c in work["code"]
        ]
        assert bulk.write_fundamentals_bulk(work.drop(columns=["code"])) == 3

        columns = "symbol, date, publ_date, roe, roa"
        left = per_symbol.conn.execute(
            f"SELECT {columns} FROM fundamentals ORDER BY symbol"
        ).fetchall()
        right = bulk.conn.execute(
            f"SELECT {columns} FROM fundamentals ORDER BY symbol"
        ).fetchall()
        assert left == right
        assert bulk.conn.execute(
            "SELECT COUNT(*) FROM data_change_log WHERE table_name='fundamentals'"
        ).fetchone()[0] == 3
    finally:
        per_symbol.close()
        bulk.close()
