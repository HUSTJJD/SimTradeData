"""No persistent backup copies; promote swaps are temporary and cleaned."""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "udm_artifacts_test", ROOT / "scripts" / "unified_data_manager.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _make_stage(path: Path, tag: str):
    for name in ("stocks", "exrights", "fundamentals", "valuation", "metadata"):
        (path / name).mkdir(parents=True)
        (path / name / "x.parquet").write_bytes(tag.encode())
    (path / "manifest.json").write_text('{"tag": "%s"}' % tag, encoding="utf-8")
    return path


def test_promote_leaves_no_backup_copies_and_rolls_back_on_failure(tmp_path, monkeypatch):
    mod = _load()
    view = tmp_path / "cn"
    _make_stage(view, "old")
    (view / "stocks_1m").mkdir()
    (view / "stocks_1m" / "keep.parquet").write_bytes(b"qmt-only")
    monkeypatch.setattr(mod, "CN_DIR", view)
    monkeypatch.setattr(mod, "DATA_ROOT", tmp_path)

    for i in range(3):
        stage = _make_stage(tmp_path / f"stage_{i}", f"new{i}")
        result = mod.promote_staged_export(stage, apply=True)
        assert (view / "stocks" / "x.parquet").read_bytes() == f"new{i}".encode()
        assert result["swaps_cleaned"] == 5
        assert (view / "stocks_1m" / "keep.parquet").read_bytes() == b"qmt-only"
        # No persistent backup/swap dirs anywhere.
        assert not list(view.glob("_backup_*"))
        assert not list(tmp_path.glob("_promote_swap_*"))
        assert not list(view.glob("*.bak"))
        assert not list(view.glob("*.promote.tmp"))

    # Broken stage rolls back to the previous view.
    broken = tmp_path / "broken"
    (broken / "stocks").mkdir(parents=True)
    (broken / "stocks" / "x.parquet").write_bytes(b"partial")
    try:
        mod.promote_staged_export(broken, apply=True)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected promote to fail on missing owned dirs")
    assert (view / "stocks" / "x.parquet").read_bytes() == b"new2"
    assert not list(tmp_path.glob("_promote_swap_*"))


def test_prune_removes_staging_all_backups_swaps_and_db_copies(tmp_path, monkeypatch):
    mod = _load()
    view = tmp_path / "cn"
    view.mkdir()
    stale = tmp_path / "cn_staging_20200101_000000"
    (stale / "stocks").mkdir(parents=True)
    (stale / "stocks" / "a.parquet").write_bytes(b"stale")
    for name in ("_backup_unified_stocks_20200101_000000",
                 "_backup_qmt_full_20200101_000000",
                 "_backup_unified_stocks_latest"):
        d = view / name
        d.mkdir()
        (d / "a.parquet").write_bytes(b"old")
    swap = tmp_path / "_promote_swap_stocks_20200101_000000"
    swap.mkdir()
    (swap / "a.parquet").write_bytes(b"swap")
    dbbak = tmp_path / "cn.duckdb.unified_backup_20200101_000000"
    dbbak.write_bytes(b"db")
    (view / "manifest.json.unified.bak").write_bytes(b"bak")
    (view / "manifest.pre_full_history_20200101_000000.json").write_bytes(b"{}")
    keep = view / "manifest.json"
    keep.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(mod, "CN_DIR", view)
    monkeypatch.setattr(mod, "DATA_ROOT", tmp_path)

    out = mod.prune_stale_artifacts()

    assert "cn_staging_20200101_000000" in out["staging_removed"]
    assert "cn.duckdb.unified_backup_20200101_000000" in out["db_backups_removed"]
    assert set(out["legacy_backups_removed"]) == {
        "_backup_unified_stocks_20200101_000000",
        "_backup_qmt_full_20200101_000000",
        "_backup_unified_stocks_latest",
    }
    assert out["persistent_backups"] == 0
    assert not stale.exists() and not swap.exists() and not dbbak.exists()
    assert not list(view.glob("_backup_*"))
    assert not list(view.glob("*.bak"))
    assert keep.read_text(encoding="utf-8") == "{}"
