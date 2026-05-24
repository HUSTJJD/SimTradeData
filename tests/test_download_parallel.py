"""Offline checks for the network-only fetch parallelisation."""

from __future__ import annotations

import importlib.util
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "download_efficient.py"


def _load():
    spec = importlib.util.spec_from_file_location("download_efficient_under_test", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def _fetch(symbol: str) -> str:
    time.sleep(0.05)
    if symbol == "bad":
        raise RuntimeError("boom")
    return symbol.upper()


def test_parallel_fetch_preserves_results_and_isolates_errors():
    mod = _load()
    out = mod._parallel_fetch(_fetch, ["a", "b", "c", "d", "bad"], 4)
    assert {k: v for k, v in out.items() if k != "bad"} == {
        "a": "A", "b": "B", "c": "C", "d": "D"
    }
    assert isinstance(out["bad"], RuntimeError)


def test_workers_one_is_equivalent_and_sequential_fallback():
    mod = _load()
    symbols = ["a", "b", "bad"]
    serial = mod._parallel_fetch(_fetch, symbols, 1)
    parallel = mod._parallel_fetch(_fetch, symbols, 3)
    assert [type(serial[s]) for s in symbols] == [type(parallel[s]) for s in symbols]
    assert [serial[s] for s in symbols if s != "bad"] == [parallel[s] for s in symbols if s != "bad"]
    assert mod._parallel_fetch(_fetch, [], 4) == {}


def test_parallel_fetch_is_faster_than_serial():
    mod = _load()
    symbols = [f"s{i}" for i in range(8)]
    start = time.perf_counter()
    mod._parallel_fetch(_fetch, symbols, 1)
    serial = time.perf_counter() - start
    start = time.perf_counter()
    mod._parallel_fetch(_fetch, symbols, 4)
    parallel = time.perf_counter() - start
    assert parallel < serial * 0.8, (serial, parallel)
