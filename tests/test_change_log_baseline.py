"""Regression tests for baseline marking and change-log retention."""
from datetime import datetime, timedelta

from simtradedata.writers.duckdb_writer import DuckDBWriter


def _seed(writer, ages_days):
    for i, age in enumerate(ages_days):
        when = datetime.now() - timedelta(days=age)
        writer.conn.execute(
            "INSERT OR REPLACE INTO data_change_log VALUES (?, ?, ?, ?)",
            ["stocks", f"{i:06d}.SZ", "2026-11-26", when],
        )


def test_mark_baseline_clears_change_log_and_records_version(tmp_path):
    writer = DuckDBWriter(str(tmp_path / "cn.duckdb"))
    try:
        _seed(writer, [0, 1, 2, 40])
        assert writer.conn.execute("SELECT COUNT(*) FROM data_change_log").fetchone()[0] == 4

        removed = writer.mark_baseline("2026-11-26")

        assert removed == 4
        assert writer.conn.execute("SELECT COUNT(*) FROM data_change_log").fetchone()[0] == 0
        assert writer.conn.execute(
            "SELECT value FROM version_info WHERE key='baseline_version'"
        ).fetchone()[0] == "2026-11-26"
        # Idempotent: a second call changes nothing.
        assert writer.mark_baseline("2026-11-27") == 0
    finally:
        writer.close()


def test_prune_change_log_keeps_recent_rows(tmp_path):
    writer = DuckDBWriter(str(tmp_path / "cn.duckdb"))
    try:
        writer.mark_baseline("2026-11-01")
        _seed(writer, [0, 1, 10, 40, 90])

        removed = writer.prune_change_log(retention_days=30)

        assert removed == 2
        remaining = writer.conn.execute(
            "SELECT COUNT(*) FROM data_change_log"
        ).fetchone()[0]
        assert remaining == 3
        assert writer.prune_change_log(retention_days=0) == 0
    finally:
        writer.close()
