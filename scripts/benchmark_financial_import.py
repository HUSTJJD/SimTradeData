"""Compare financial import paths using a local ZIP and temporary databases.

The benchmark is fully offline and reproducible: it copies one cached
``gpcw*.zip`` into a temporary directory, parses it, and times the individual
vs. bulk DuckDB write of the same rows while asserting the two paths produce
identical data. ``--md-output`` writes a versioned report suitable for Git;
``--json-output`` writes the raw measurements.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import platform
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from simtradedata.fetchers.mootdx_affair_fetcher import MootdxAffairFetcher
from simtradedata.utils.code_utils import convert_to_ptrade_code
from simtradedata.writers.duckdb_writer import DuckDBWriter

ROOT = Path(__file__).resolve().parents[1]
# Historical publish logs are observational evidence only: they were not
# produced under identical conditions and must not be presented as a controlled
# A/B comparison. Their hashes are recorded so the observation stays auditable.
HISTORICAL_LOGS = (
    ("full export", ROOT / "ops" / "unified_data" / "path_full.log"),
    ("incremental export", ROOT / "ops" / "unified_data" / "path_incremental2.log"),
)


def benchmark(package: Path, companies: int = 100) -> dict:
    with tempfile.TemporaryDirectory(prefix="simtrade-financial-benchmark-") as temporary:
        root = Path(temporary)
        shutil.copyfile(package, root / package.name)
        fetcher = MootdxAffairFetcher(str(root))
        started = time.perf_counter()
        raw = fetcher.parse_local(package.name)
        parse = time.perf_counter() - started
        started = time.perf_counter()
        frame = fetcher._convert_to_ptrade_format(raw)
        conversion = time.perf_counter() - started
        frames = {
            convert_to_ptrade_code(str(code), "qstock"): group.drop(columns="code").set_index("end_date")
            for code, group in list(frame.groupby("code"))[:companies]
        }
        timings: dict[str, float] = {}
        with DuckDBWriter(str(root / "benchmark.duckdb")) as writer:
            writer.conn.execute("DELETE FROM fundamentals")
            writer.conn.execute("DELETE FROM data_change_log")
            for mode in ("individual", "batch"):
                writer.conn.execute("DELETE FROM fundamentals")
                writer.conn.execute("DELETE FROM data_change_log")
                writer.begin()
                started = time.perf_counter()
                if mode == "individual":
                    rows = sum(writer.write_fundamentals(symbol, data)
                               for symbol, data in frames.items())
                else:
                    rows = 0
                    items = list(frames.items())
                    for offset in range(0, len(items), 500):
                        rows += writer.write_fundamentals_batch(dict(items[offset:offset + 500]))
                writer.commit()
                timings[mode] = time.perf_counter() - started
                if mode == "individual":
                    writer.conn.execute("CREATE TEMP TABLE baseline AS SELECT * FROM fundamentals")
                else:
                    difference = writer.conn.execute(
                        "SELECT COUNT(*) FROM ((SELECT * FROM baseline EXCEPT ALL SELECT * FROM fundamentals) "
                        "UNION ALL (SELECT * FROM fundamentals EXCEPT ALL SELECT * FROM baseline))"
                    ).fetchone()[0]
                    if difference:
                        raise AssertionError("Batch import differs from individual import")
        return {
            "package": package.name,
            "package_sha256": hashlib.sha256(package.read_bytes()).hexdigest(),
            "package_bytes": package.stat().st_size,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "python": platform.python_version(),
            "duckdb_version": duckdb.__version__,
            "companies": len(frames),
            "rows": rows,
            "parse_seconds": round(parse, 4),
            "conversion_seconds": round(conversion, 4),
            "write_seconds": {k: round(v, 4) for k, v in timings.items()},
            "speedup": round(timings["individual"] / timings["batch"], 3),
            "data_equal": True,
        }


def _historical_observations() -> list[str]:
    lines = [
        "## Historical observations (not a controlled A/B)",
        "",
        "The publish pipeline logs below were captured on 2026-11-27 under",
        "different workloads. The audit recorded ~507 s for a full export and",
        "~27 s for an incremental export reusing 7,714 / 5,621 / 5,245 / 5,813",
        "Parquet files. These are observations, not a reproducible speedup claim.",
        "",
    ]
    for label, path in HISTORICAL_LOGS:
        if path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
            lines.append(f"- {label}: `{path.name}` (sha256 `{digest}…`, {path.stat().st_size} bytes)")
        else:
            lines.append(f"- {label}: log not present on this machine")
    lines.append("")
    lines.append("The reproducible measurement above is the individual-vs-bulk")
    lines.append("financial import; re-run the documented command to refresh it.")
    return lines


def _markdown(result: dict) -> str:
    write = result["write_seconds"]
    lines = [
        "# Performance baseline",
        "",
        "Reproducible, offline measurement of the financial import optimisation.",
        "",
        f"- Generated (UTC): `{result['generated_at']}`",
        f"- Input: `{result['package']}` ({result['package_bytes']:,} bytes, sha256 `{result['package_sha256']}`)",
        f"- Runtime: Python {result['python']} / duckdb {result['duckdb_version']}",
        f"- Companies: {result['companies']}, rows written: {result['rows']}",
        "",
        "| Stage | Seconds |",
        "|---|---:|",
        f"| parse_local | {result['parse_seconds']} |",
        f"| _convert_to_ptrade_format | {result['conversion_seconds']} |",
        f"| write_fundamentals (individual) | {write['individual']} |",
        f"| write_fundamentals_batch | {write['batch']} |",
        "",
        f"- Speedup (individual / batch): **{result['speedup']}×**",
        f"- Data equality asserted: `{result['data_equal']}`",
        "",
        "Reproduce:",
        "",
        "```bash",
        "uv run --frozen python scripts/benchmark_financial_import.py \\",
        "  data/mootdx_financial/gpcw20260930.zip --companies 100 \\",
        "  --md-output docs/benchmarks/PERFORMANCE.md",
        "```",
        "",
    ]
    return "\n".join(lines + _historical_observations()) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("package", type=Path)
    parser.add_argument("--companies", type=int, default=100)
    parser.add_argument("--json-output", type=Path, default=None)
    parser.add_argument("--md-output", type=Path, default=None)
    args = parser.parse_args()
    if args.companies <= 0:
        parser.error("--companies must be positive")
    if not args.package.is_file():
        parser.error(f"missing benchmark package: {args.package}")
    logging.basicConfig(level=logging.ERROR)
    result = benchmark(args.package, args.companies)
    payload = json.dumps(result, ensure_ascii=False, indent=2)
    print(payload)
    for target, content in ((args.json_output, payload + "\n"),
                            (args.md_output, _markdown(result) if args.md_output else None)):
        if target and content is not None:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
