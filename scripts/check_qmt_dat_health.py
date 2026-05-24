# -*- coding: utf-8 -*-
"""Read-only freshness/decodability probe for the local QMT DAT files.

This is deliberately NOT part of ``check_data_sources.py``: that probe drives
SmartRouter network sources, while QMT local ``*.DAT`` files are optional
cross-check inputs handled by ``unified_data_manager.py``. It never imports
xtdata / the QMT SDK, never writes to the canonical DuckDB, and never contacts
the network.

Usage:
    python scripts/check_qmt_dat_health.py
    python scripts/check_qmt_dat_health.py --qmt-root D:\\path --json-output ops/unified_data/qmt_dat_health.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import struct
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from simtradedata.importers.qmt_kline import decode_kline_dat

_KLINE_NAME = re.compile(r"^\d{6}$")

DEFAULT_ROOT = Path(r"D:\君弘君智交易系统")
TZ8 = timezone(timedelta(hours=8))
MIN_TIME = 946684800  # 2000-01-01 UTC, same floor as qmt_kline.py
RECORD_BYTES = {7001: 1264, 7002: 664, 7003: 920, 7004: 56,
                7005: 64, 7006: 416, 7007: 416, 7008: 344}
PERIOD_DIRS = {"daily": "86400", "5m": "300", "1m": "60"}
SAMPLE_FILES = 20
MIN_ONE_MINUTE_FILES = 100


def _last_stamp(path: Path) -> str | None:
    """Decode the last 64-byte K-line record's timestamp without reading the file."""
    try:
        if path.stat().st_size < 72:
            return None
        with path.open("rb") as fh:
            fh.seek(-64, 2)
            tail = fh.read(64)
        stamp = struct.unpack_from("<9I", tail, 0)[0]
        if stamp < MIN_TIME:
            return None
        return datetime.fromtimestamp(stamp, TZ8).replace(tzinfo=None).isoformat(sep=" ")
    except Exception:  # noqa: BLE001
        return None


def _daily_last_bar(path: Path) -> str | None:
    """Daily DATs use the 64-byte K-line layout, not the minute timestamp layout."""
    try:
        frame = decode_kline_dat(path)
        if frame is None or frame.empty:
            return None
        # decode_kline_dat returns `datetime`; to_daily_frame renames it to `date`.
        column = "datetime" if "datetime" in frame.columns else "date"
        return str(frame[column].iloc[-1])[:10]
    except Exception:  # noqa: BLE001
        return None


def _scan_kline(market_dir: Path, period_dir: str, is_daily: bool = False) -> dict[str, Any]:
    directory = market_dir / period_dir
    result: dict[str, Any] = {
        "directory": str(directory), "exists": directory.is_dir(),
        "files": 0, "non_kline_files": 0, "zero_size": 0, "bad_record_size": 0,
        "newest_mtime": None, "sampled_last_bar": None, "undecodable_samples": 0,
    }
    if not directory.is_dir():
        return result
    newest_mtime = 0.0
    newest_paths: list[tuple[float, Path]] = []
    with os.scandir(directory) as entries:
        for entry in entries:
            if not entry.name.upper().endswith(".DAT"):
                continue
            # QMT mixes auxiliary per-symbol tables (code_<table>.DAT) into the
            # same 86400 folder; those are not K-line files and are counted
            # separately instead of being reported as corrupt bars.
            if not _KLINE_NAME.match(Path(entry.name).stem):
                result["non_kline_files"] += 1
                continue
            result["files"] += 1
            try:
                size = entry.stat().st_size
            except OSError:
                result["bad_record_size"] += 1
                continue
            if size == 0:
                result["zero_size"] += 1
                continue
            if size < 64 or size % 64 not in (0, 8):
                result["bad_record_size"] += 1
            mtime = entry.stat().st_mtime
            newest_mtime = max(newest_mtime, mtime)
            newest_paths.append((mtime, Path(entry.path)))
    if newest_mtime:
        result["newest_mtime"] = datetime.fromtimestamp(newest_mtime).isoformat(timespec="seconds")
    newest_paths.sort(reverse=True)
    stamps: list[str] = []
    for _, path in newest_paths[:SAMPLE_FILES]:
        stamp = _daily_last_bar(path) if is_daily else _last_stamp(path)
        if stamp:
            stamps.append(stamp)
        else:
            result["undecodable_samples"] += 1
    if stamps:
        result["sampled_last_bar"] = max(stamps)
    return result


def _scan_finance(finance_dir: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "directory": str(finance_dir), "exists": finance_dir.is_dir(),
        "files": 0, "divisible": 0, "undivisible": 0, "unknown_table": 0,
        "by_table": {}, "newest_mtime": None,
    }
    if not finance_dir.is_dir():
        return result
    newest_mtime = 0.0
    with os.scandir(finance_dir) as entries:
        for entry in entries:
            if not entry.name.upper().endswith(".DAT"):
                continue
            result["files"] += 1
            try:
                tid = int(Path(entry.name).stem.split("_")[-1])
            except ValueError:
                result["unknown_table"] += 1
                continue
            width = RECORD_BYTES.get(tid)
            if width is None:
                result["unknown_table"] += 1
                continue
            size = entry.stat().st_size
            key = str(tid)
            bucket = result["by_table"].setdefault(key, {"files": 0, "divisible": 0})
            bucket["files"] += 1
            if size % width == 0:
                result["divisible"] += 1
                bucket["divisible"] += 1
            else:
                result["undivisible"] += 1
            newest_mtime = max(newest_mtime, entry.stat().st_mtime)
    if newest_mtime:
        result["newest_mtime"] = datetime.fromtimestamp(newest_mtime).isoformat(timespec="seconds")
    return result


def build(qmt_root: Path) -> dict[str, Any]:
    datadir = qmt_root / "datadir" if (qmt_root / "datadir").is_dir() else qmt_root
    report: dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "qmt_root": str(qmt_root), "datadir": str(datadir),
        "root_exists": qmt_root.is_dir(),
        "kline": {}, "finance": {}, "errors": [], "warnings": [],
    }
    if not qmt_root.is_dir():
        report["errors"].append("missing_root")
        report["status"] = "fail"
        return report

    for market in ("SH", "SZ"):
        market_dir = datadir / market
        for label, period_dir in PERIOD_DIRS.items():
            key = f"{market}/{label}"
            scanned = _scan_kline(market_dir, period_dir, is_daily=(label == "daily"))
            report["kline"][key] = scanned
            if not scanned["exists"]:
                report["errors"].append(f"missing_kline_dir:{key}")
            if scanned["bad_record_size"]:
                report["errors"].append(f"bad_record_size:{key}:{scanned['bad_record_size']}")
            if scanned["undecodable_samples"]:
                report["errors"].append(f"undecodable_samples:{key}:{scanned['undecodable_samples']}")
            if label == "1m" and scanned["files"] < MIN_ONE_MINUTE_FILES:
                report["warnings"].append(
                    f"insufficient_1m:{key}:{scanned['files']} files (true 1m bars were never downloaded in the QMT client)"
                )
        report["finance"][market] = _scan_finance(datadir / "Finance" / market / "86400")
        if report["finance"][market]["undivisible"]:
            report["errors"].append(
                f"undivisible_finance:{market}:{report['finance'][market]['undivisible']}"
            )

    report["status"] = "fail" if report["errors"] else ("warning" if report["warnings"] else "pass")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qmt-root", type=Path, default=Path(os.environ.get("SIMTRADE_QMT_ROOT", str(DEFAULT_ROOT))))
    parser.add_argument("--json-output", type=Path, default=None)
    args = parser.parse_args()
    report = build(args.qmt_root)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    print(payload)
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(payload + "\n", encoding="utf-8")
    return 1 if report["status"] == "fail" else 0


if __name__ == "__main__":
    sys.exit(main())
