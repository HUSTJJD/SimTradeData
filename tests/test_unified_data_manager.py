from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


def test_oss_normalize_accepts_chinese_akshare_columns():
    mod = _load("oss_fetchers_test", ROOT / "simtradedata" / "fetchers" / "oss_fetchers.py")
    raw = pd.DataFrame({
        "日期": ["2026-11-24", "2026-11-25"],
        "开盘": [10.0, 10.5],
        "收盘": [10.2, 10.7],
        "最高": [10.4, 10.9],
        "最低": [9.9, 10.3],
        "成交量": [100, 120],
        "成交额": [1020.0, 1284.0],
    })
    out = mod._normalize_daily(raw, "2026-11-24", "2026-11-25")
    assert list(out.columns) == mod._STANDARD_COLUMNS
    assert len(out) == 2
    assert out.iloc[-1]["close"] == 10.7


def test_oss_route_table_contains_optional_supplements():
    mod = _load("route_config_test", ROOT / "simtradedata" / "router" / "route_config.py")
    chain = mod.DEFAULT_ROUTE_TABLE["daily_bars"]["cn"]
    assert chain[:3] == ["public", "eastmoney", "baostock"]
    assert {"akshare", "adata", "efinance"}.issubset(chain)
    assert "akshare" in mod.FETCHER_REGISTRY


def test_cross_check_rejects_systematic_conflict(tmp_path):
    import duckdb

    manager = _load("unified_manager_test", ROOT / "scripts" / "unified_data_manager.py")
    con = duckdb.connect(str(tmp_path / "x.duckdb"))
    try:
        incoming = pd.DataFrame({
            "date": pd.date_range("2026-01-01", periods=30),
            "close": [10.0] * 30,
        })
        existing = pd.DataFrame({
            "date": pd.date_range("2026-01-01", periods=30),
            "close": [11.0] * 30,
        })
        status = manager._cross_check(incoming, existing)
        assert status["ok"] is False
        assert status["conflict"] == 30
    finally:
        con.close()


def test_cross_check_allows_small_isolated_noise():
    manager = _load("unified_manager_test2", ROOT / "scripts" / "unified_data_manager.py")
    dates = pd.date_range("2026-01-01", periods=100)
    incoming = pd.DataFrame({"date": dates, "close": [10.0] * 100})
    closes = [10.0] * 100
    closes[10] = 10.06
    existing = pd.DataFrame({"date": dates, "close": closes})
    status = manager._cross_check(incoming, existing)
    assert status["ok"] is True
    assert status["conflict"] == 1


def test_stock_schema_contract_is_ptrade_ten_columns():
    manager = _load("unified_manager_test3", ROOT / "scripts" / "unified_data_manager.py")
    assert manager.STOCK_COLS == [
        "date", "open", "close", "high", "low", "high_limit", "low_limit",
        "preclose", "volume", "money",
    ]


def test_parser_separates_network_oss_qmt_and_history_flags():
    manager = _load("unified_manager_test4", ROOT / "scripts" / "unified_data_manager.py")
    args = manager.build_parser().parse_args([
        "--no-oss", "--no-qmt", "--target-date", "2026-11-26",
        "--full-history", "--history-start", "1990-12-19",
    ])
    assert args.no_oss is True
    assert args.no_router is False  # --no-oss must not disable first-party internet
    assert args.no_qmt is True
    assert args.target_date == "2026-11-26"
    assert args.full_history is True
    assert args.history_start == "1990-12-19"


def test_router_config_removes_only_oss_sources():
    manager = _load("unified_manager_test5", ROOT / "scripts" / "unified_data_manager.py")
    config = manager._router_config(False)
    chain = config["daily_bars"]["cn"]
    assert chain[:3] == ["public", "eastmoney", "baostock"]
    assert not (set(manager.OSS_PROJECTS) & set(chain))


def test_full_history_rebuild_passes_history_start(monkeypatch):
    manager = _load("unified_manager_test6", ROOT / "scripts" / "unified_data_manager.py")
    captured = {}

    class _Proc:
        returncode = 0
        stdout = "ok"
        stderr = ""

    def _fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env", {})
        captured["cwd"] = kwargs.get("cwd")
        return _Proc()

    monkeypatch.setattr("subprocess.run", _fake_run)
    result = manager._run_download_rebuild(manager.date(1990, 12, 19), True)
    assert result["status"] == "pass"
    # BaoStock stays valuation-only: TDX owns 1990+ OHLCV, and full mode
    # only adds per-stock market/dividend fetches without extending coverage.
    assert "--baostock-full" not in captured["cmd"]
    assert captured["cmd"][-2:] == ["--history-start", "1990-12-19"]
    assert captured["env"]["SIMTRADE_CN_HISTORY_START"] == "1990-12-19"


def test_integrity_subprocess_passes_history_start(monkeypatch, tmp_path):
    manager = _load("unified_manager_test7", ROOT / "scripts" / "unified_data_manager.py")
    captured = {}

    class _Proc:
        returncode = 0
        stdout = "{}"
        stderr = ""

    def _fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env", {})
        out_index = cmd.index("--json-output") + 1
        Path(cmd[out_index]).write_text(
            "{\"status\":\"pass\",\"checks\":[]}", encoding="utf-8"
        )
        return _Proc()

    monkeypatch.setattr("subprocess.run", _fake_run)
    monkeypatch.setattr(manager, "REPORT_DIR", tmp_path)
    monkeypatch.setattr(manager, "_now", lambda: manager.datetime(2026, 11, 26, 16, 0, 0))
    result = manager._integrity_report(
        manager.date(2026, 11, 26), None, manager.date(1990, 12, 19)
    )
    assert result["status"] == "pass"
    assert captured["cmd"][-2:] == ["--history-start", "1990-12-19"]
    assert captured["env"]["SIMTRADE_CN_HISTORY_START"] == "1990-12-19"


def test_download_rebuild_forces_utf8_and_tolerates_none_streams(monkeypatch):
    """Windows GBK decoding of Chinese subprocess output used to kill the reader
    thread, leaving stdout/stderr as None and crashing the manager."""
    manager = _load("unified_manager_test8", ROOT / "scripts" / "unified_data_manager.py")
    captured = {}

    class _Proc:
        returncode = 0
        stdout = None
        stderr = None

    def _fake_run(cmd, **kwargs):
        captured.update(kwargs)
        return _Proc()

    monkeypatch.setattr("subprocess.run", _fake_run)
    result = manager._run_download_rebuild(manager.date(1990, 12, 19), True)

    assert result["status"] == "pass"
    assert result["stdout_tail"] == ""
    assert result["stderr_tail"] == ""
    assert captured.get("encoding") == "utf-8"
    assert captured.get("errors") == "replace"
