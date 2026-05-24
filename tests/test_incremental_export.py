"""Incremental export must reuse unchanged tables without mutating the live view."""
from pathlib import Path

from simtradedata.writers.duckdb_writer import DuckDBWriter

TABLES = ("stocks", "exrights", "fundamentals", "valuation", "metadata")


def _seed_view(base: Path) -> dict[str, bytes]:
    blobs = {}
    for table in TABLES:
        (base / table).mkdir(parents=True)
        for i in range(3):
            data = f"{table}-{i}".encode()
            (base / table / f"{i:06d}.SZ.parquet").write_bytes(data)
            blobs[f"{table}/{i:06d}.SZ.parquet"] = data
    return blobs


def _stub_exports(writer, calls):
    writer.compute_derived_fundamentals = lambda: None
    writer._write_manifest = lambda output_path, market="cn": None

    def make(name):
        def stub(output_dir, market="cn", **kwargs):
            calls.append(name)
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            (Path(output_dir) / f"{name}.parquet").write_bytes(b"rebuilt")
        return stub

    for name in ("_export_stocks_batch", "_export_exrights_batch",
                 "_export_fundamentals_batch", "_export_valuation_batch",
                 "_export_metadata"):
        setattr(writer, name, make(name))


def test_incremental_reuses_unchanged_tables_and_rebuilds_changed(tmp_path):
    base = tmp_path / "cn"
    blobs = _seed_view(base)
    writer = DuckDBWriter(str(tmp_path / "db.duckdb"))
    calls = []
    _stub_exports(writer, calls)
    stage = tmp_path / "stage"
    try:
        writer.export_to_parquet(
            str(stage), market="cn", base_dir=str(base),
            changed_tables={"stocks"},
        )
    finally:
        writer.close()

    assert calls == ["_export_stocks_batch", "_export_metadata"]
    # Changed table: directory rebuilt, no stale base files leaked in.
    assert sorted(p.name for p in (stage / "stocks").glob("*.parquet")) == [
        "_export_stocks_batch.parquet"
    ]
    # Unchanged tables: byte-identical to the live view.
    for table in ("exrights", "fundamentals", "valuation", "metadata"):
        if table == "metadata":
            assert (stage / table / "_export_metadata.parquet").read_bytes() == b"rebuilt"
            continue
        for i in range(3):
            rel = f"{table}/{i:06d}.SZ.parquet"
            assert (stage / rel).read_bytes() == blobs[rel]
    # The live view must not have been written through a hard link.
    for rel, data in blobs.items():
        assert (base / rel).read_bytes() == data


def test_full_export_ignores_base_and_rebuilds_every_table(tmp_path):
    base = tmp_path / "cn"
    _seed_view(base)
    writer = DuckDBWriter(str(tmp_path / "db.duckdb"))
    calls = []
    _stub_exports(writer, calls)
    stage = tmp_path / "stage"
    try:
        writer.export_to_parquet(str(stage), market="cn")
    finally:
        writer.close()

    assert calls == [
        "_export_stocks_batch", "_export_exrights_batch",
        "_export_fundamentals_batch", "_export_valuation_batch",
        "_export_metadata",
    ]
    for table in TABLES:
        assert (stage / table).is_dir()


def test_reuse_falls_back_to_copy_when_hardlink_fails(tmp_path, monkeypatch):
    import os

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.parquet").write_bytes(b"payload")
    dst = tmp_path / "dst"

    def no_link(_src, _dst):
        raise OSError("cross-device")

    monkeypatch.setattr(os, "link", no_link)
    count = DuckDBWriter._reuse_parquet_tree(src, dst)
    assert count == 1
    assert (dst / "a.parquet").read_bytes() == b"payload"
