"""Export-path degradations must be visible in the publish report, not silent."""

from simtradedata.writers.duckdb_writer import DuckDBWriter


def test_index_failure_is_recorded(tmp_path):
    writer = DuckDBWriter(str(tmp_path / "degradation.duckdb"))
    try:
        assert writer.export_degradations == []
        # A missing table makes CREATE INDEX fail for real (no mocking needed).
        writer._index_export_temp_table("definitely_missing_export_table")
        assert len(writer.export_degradations) == 1
        event = writer.export_degradations[0]
        assert event["kind"] == "index"
        assert event["table"] == "definitely_missing_export_table"
        assert event["error"]
    finally:
        writer.close()


def test_each_writer_instance_has_its_own_degradation_list(tmp_path):
    first = DuckDBWriter(str(tmp_path / "first.duckdb"))
    second = DuckDBWriter(str(tmp_path / "second.duckdb"))
    try:
        first.export_degradations.append({"kind": "index", "table": "x", "error": "boom"})
        assert second.export_degradations == []
    finally:
        first.close()
        second.close()
