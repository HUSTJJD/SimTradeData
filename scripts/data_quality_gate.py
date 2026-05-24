# -*- coding: utf-8 -*-
"""Pre-publish data quality gate for the canonical DuckDB.

Wraps the existing health audit with three things a plain report cannot do:

* error-severity findings block the publish (nothing is exported/promoted);
* any metric getting worse than the last successful publish is a regression
  and also blocks, so a bad write cannot quietly add dirty rows;
* a machine- and human-readable report makes the DB observable instead of a
  black box, including per-check counts and examples.

Exit code 0 = publish may proceed, 1 = blocked.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_health_report import _json_default, build_report  # noqa: E402

DEFAULT_DB = ROOT / "data" / "cn.duckdb"
DEFAULT_EXPORT = ROOT / "data" / "cn"
DEFAULT_BASELINE = ROOT / "ops" / "unified_data" / "data_quality_baseline.json"


def _count(check: dict[str, Any]) -> int | None:
    """Comparable dirty-row count for a check, or None when not count-based."""
    value = check.get("count")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ratio(check: dict[str, Any]) -> float | None:
    value = check.get("ratio")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric(check: dict[str, Any]) -> dict[str, Any]:
    """Snapshot the comparable part of a check for the baseline."""
    return {"count": _count(check), "ratio": _ratio(check), "status": check["status"]}


def evaluate(report: dict[str, Any], baseline: dict[str, Any] | None,
             allow_issues: bool) -> dict[str, Any]:
    checks = report["quality_checks"]
    summary = report["quality_summary"]
    blocking = [
        c for c in checks if c["status"] == "fail" and c["severity"] == "error"
    ]
    warnings = [
        c for c in checks if c["status"] != "pass" and c["severity"] == "warning"
    ]

    regressions: list[dict[str, Any]] = []
    if baseline:
        before = baseline.get("metrics", {})
        for check in checks:
            name = check["name"]
            old = before.get(name)
            if not old:
                continue
            now_count, old_count = _count(check), old.get("count")
            now_ratio, old_ratio = _ratio(check), old.get("ratio")
            if (now_count is not None and old_count is not None
                    and now_count > old_count):
                regressions.append({"name": name, "metric": "count",
                                    "before": old_count, "after": now_count})
            elif (now_ratio is not None and old_ratio is not None
                  and now_ratio < old_ratio - 1e-9):
                regressions.append({"name": name, "metric": "ratio",
                                    "before": old_ratio, "after": now_ratio})
        if summary["errors"] > baseline.get("errors", 0):
            regressions.append({"name": "quality_summary.errors",
                                "metric": "count",
                                "before": baseline.get("errors", 0),
                                "after": summary["errors"]})

    reasons = []
    if blocking:
        reasons.append(f"{len(blocking)} error-severity checks failed")
    if regressions:
        reasons.append(f"{len(regressions)} metrics regressed vs baseline")
    status = "fail" if reasons else "pass"
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "canonical_db": str(report["canonical_db"]),
        "target_date": report["target_date"],
        "status": status,
        "waived": bool(allow_issues and reasons),
        "effective_status": "pass" if allow_issues else status,
        "blocking_reasons": reasons,
        "summary": dict(summary),
        "blocking_checks": blocking,
        "warning_checks": warnings,
        "regressions": regressions,
        "baseline": {
            "errors": baseline.get("errors"), "warnings": baseline.get("warnings"),
        } if baseline else None,
        "checks": checks,
        "table_inventory": report.get("table_inventory", {}),
        "instrument_universe": report.get("instrument_universe", {}),
        "domains": report.get("domains", {}),
    }


def _markdown(gate: dict[str, Any]) -> str:
    s = gate["summary"]
    lines = [
        "# 数据质量门禁（发布前）",
        "",
        f"- 生成时间：{gate['generated_at']}",
        f"- 目标日：{gate['target_date']}",
        f"- 结论：`{gate['status']}`" + ("（已豁免）" if gate["waived"] else ""),
        f"- 检查：{s['checks']} 项｜通过 {s['passes']}｜error {s['errors']}｜warning {s['warnings']}",
        "",
        "> 该门禁在导出与发布之前对权威库执行：error 级脏数据、或比上次发布更差，都会阻断发布。",
        "",
    ]
    if gate["blocking_reasons"]:
        lines += ["## 阻断原因", ""] + [f"- {r}" for r in gate["blocking_reasons"]] + [""]
    if gate["regressions"]:
        lines += ["## 回归项", "", "| 检查 | 指标 | 上次 | 本次 |", "|---|---|---:|---:|"]
        for r in gate["regressions"]:
            lines.append(f"| {r['name']} | {r['metric']} | {r['before']} | {r['after']} |")
        lines.append("")
    lines += ["## 全部检查", "", "| 检查 | 状态 | 严重度 | 数量/比例 |", "|---|---|---|---|"]
    for c in gate["checks"]:
        metric = c.get("count", c.get("ratio", ""))
        lines.append(f"| {c['name']} | `{c['status']}` | {c['severity']} | {metric} |")
    lines += ["", "## 库内容盘点", "", "| 表 | 行数 | 日期范围 |", "|---|---:|---|"]
    for name, item in sorted(gate["table_inventory"].items()):
        lo = item.get("date_min") or "—"
        hi = item.get("date_max") or "—"
        lines.append(f"| {name} | {item.get('rows', '')} | {lo} → {hi} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-publish data quality gate")
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT))
    parser.add_argument("--target-date", default=None)
    parser.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    parser.add_argument("--json-output", default=None)
    parser.add_argument("--markdown-output", default=None)
    parser.add_argument("--update-baseline", action="store_true")
    parser.add_argument("--allow-issues", action="store_true")
    args = parser.parse_args()

    report = build_report(Path(args.db_path), Path(args.export_dir), args.target_date)
    baseline = None
    base_path = Path(args.baseline)
    if base_path.is_file():
        try:
            baseline = json.loads(base_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            baseline = None

    gate = evaluate(report, baseline, args.allow_issues)
    if args.json_output:
        p = Path(args.json_output)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(gate, ensure_ascii=False, indent=2,
                                default=_json_default), encoding="utf-8")
    if args.markdown_output:
        p = Path(args.markdown_output)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(_markdown(gate), encoding="utf-8")
    print(json.dumps({
        "status": gate["status"], "effective_status": gate["effective_status"],
        "summary": gate["summary"], "blocking_reasons": gate["blocking_reasons"],
        "regressions": gate["regressions"],
    }, ensure_ascii=False, indent=2, default=_json_default))

    if args.update_baseline and not gate["blocking_reasons"]:
        new_baseline = {
            "generated_at": gate["generated_at"],
            "target_date": gate["target_date"],
            "errors": gate["summary"]["errors"],
            "warnings": gate["summary"]["warnings"],
            "metrics": {c["name"]: _metric(c) for c in gate["checks"]},
        }
        base_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = base_path.with_name(base_path.name + ".tmp")
        tmp.write_text(json.dumps(new_baseline, ensure_ascii=False, indent=2,
                                  default=_json_default), encoding="utf-8")
        tmp.replace(base_path)

    return 0 if gate["effective_status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
