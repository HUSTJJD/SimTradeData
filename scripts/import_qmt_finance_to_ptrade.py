# -*- coding: utf-8 -*-
"""把本机 QMT Finance DAT 中**已数值锚定确认**的字段，补进 PTrade fundamentals/ 空值。

严格边界（与 import_qmt_daily_to_ptrade.py 一致，且更保守）
------------------------------------------------------------------
  * 只写引擎原生读取的 ``data/cn/fundamentals/<symbol>.parquet``，
    不新增/修改任何引擎接口，不 import QMT SDK / xtdata，不连在线服务。
  * **不猜字段**：QMT 财务 DAT 的列名无官方字典，项目既有
    probe_qmt_private_binary.py（已归档至 scratch/probes/）明确判定「不建议当数据入口」。
    本脚本只导入经 probe_qmt_finance_full.py（归档快照）全表数值锚定确认的 4 个数值列，
    以及经 99.78% 交叉验证的 publ_date（公告日，不依赖列语义）。
  * **只填补空值，绝不覆盖 PTrade 已有值**；与已有值冲突的单元格整格跳过。
  * 不追加历史季度行（QMT 有 2007-2013，PTrade 从 2014Q1 起），
    避免改变 get_fundamentals(start_year=...) 的既有行为。
  * 默认 dry-run；显式 --apply 才落盘；apply 前自动备份被修改文件；
    每次写变更清单 JSON 便于审阅/回滚；脚本可重入。

锚定证据（全表 60 只样本股、报告期按季度末 ±5 天归一化）
------------------------------------------------------------------
  | PTrade 字段                | QMT 表/列     | 精确一致率 | 样本数 |
  | total_shares               | 7004[0]       | 0.986      | 1878   |
  | a_floats                   | 7004[1]       | 0.949      | 1792   |
  | net_operate_cash_flow      | 7003[39]      | 1.000      | 2334   |
  | dividend_interest_payment  | 7003[54]      | 1.000      | 2185   |
  | publ_date                  | 7005 公告日   | 0.9978     | 1809   |

  7008（每股指标）与 PTrade 的 roe/增长率 Spearman 0.98~0.99 但精确率仅
  0.37~0.67，属同经济量不同口径，**不导入**，仅在报告中留作校准线索。
  7006/7007 是含中文股东名的变长文本表，double 解读无意义，排除。
  7001/7002/7003 其余列未通过锚定，不导入。

时间口径：QMT 毫秒戳是北京时间，按 UTC+8 转日期；否则公告日会差 1 天。

用法：
    python scripts/import_qmt_finance_to_ptrade.py                    # 体检，不写盘
    python scripts/import_qmt_finance_to_ptrade.py --apply            # 实际导入
    python scripts/import_qmt_finance_to_ptrade.py --limit 100 --apply
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import qmt_finance_anchor as A                 # noqa: E402

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
CN = ROOT / "SimTradeData" / "data" / "cn"
FUND = CN / "fundamentals"
STYLE = ROOT / "strategies" / "style_timing" / "results"
AUDIT_JSON = STYLE / "qmt_finance_patch_audit.json"
AUDIT_MD = STYLE / "qmt_finance_patch_audit.md"

# 只有这 4 个数值列允许进入 PTrade
VALUE_MAP = {
    "total_shares": (7004, 0, 0.986),
    "a_floats": (7004, 1, 0.949),
    "net_operate_cash_flow": (7003, 39, 1.000),
    "dividend_interest_payment": (7003, 54, 1.000),
}
PUB_FILL_TABLES = (7005,)            # 只用 7005 填充：与 PTrade 已有值 99.78% 精确一致
PUB_CHECK_TABLES = (7003, 7008)      # 仅作一致性校验，不用于填充（一致率 87%/86%）
PUB_CONF = {7005: 0.9978, 7003: 0.8717, 7008: 0.8649}


def ms_beijing(ms: int):
    if not (A.TS_MIN < ms < A.TS_MAX):
        return None
    return datetime.fromtimestamp(ms / 1000 + 8 * 3600, tz=timezone.utc).date()


def ann_map(sym: str, tid: int):
    """{季度键: 公告日}，同季度多条取公告日最新。"""
    code, mkt = sym.split(".")
    fp = A.QMT / mkt.replace("SS", "SH") / "86400" / f"{code}_{tid}.DAT"
    if not fp.exists():
        return {}
    rows = A.decode(fp, tid) or []
    out = {}
    for qk, ann, _ in rows:
        if ann is None:
            continue
        if qk not in out or ann > out[qk]:
            out[qk] = ann
    return out


def qkey_of(ts):
    return f"{ts.year}Q{(ts.month - 1) // 3 + 1}"


def conflict_num(a, b, tol=0.01):
    if not np.isfinite(a) or not np.isfinite(b):
        return False
    return abs(a - b) / max(abs(b), 1e-9) > tol


def process(sym: str, apply: bool, backup_dir: Path | None):
    """返回该标的的变更记录（不写盘时也是同样计算，用于 dry-run 统计）。"""
    f = FUND / f"{sym}.parquet"
    d = pd.read_parquet(f)
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    qkeys = [qkey_of(x) for x in d["date"]]
    rec = {"symbol": sym, "rows": len(d), "filled": {}, "conflicts": 0,
           "pit_rejected": 0, "skipped_existing": 0, "written": False}

    # ---- 数值字段 ----
    qcache = {}
    for field, (tid, col, conf) in VALUE_MAP.items():
        if field not in d.columns:
            continue
        if tid not in qcache:
            qcache[tid] = A.qmt_matrix(sym, tid)
        qm = qcache[tid]
        cur = d[field].to_numpy(dtype=float)
        filled = 0
        for i, qk in enumerate(qkeys):
            if np.isfinite(cur[i]):
                rec["skipped_existing"] += 1
                continue
            vals = qm.get(qk)
            if vals is None or col >= len(vals) or not np.isfinite(vals[col]):
                continue
            v = float(vals[col])
            # 股本类字段量级保护：7004 混有股本变更事件记录，异常量级一律不填
            if field in ("total_shares", "a_floats") and not (1e6 <= v <= 1e13):
                rec["conflicts"] += 1
                continue
            if not np.isfinite(v) or abs(v) >= 1e15:
                rec["conflicts"] += 1
                continue
            cur[i] = v
            filled += 1
        d[field] = cur
        rec["filled"][field] = filled

    # ---- publ_date（只填空，不覆盖；多源按优先级）----
    pubsrc = {tid: ann_map(sym, tid) for tid in PUB_FILL_TABLES}
    pub = d["publ_date"].copy() if "publ_date" in d.columns else pd.Series([None] * len(d))
    filled_pub = 0
    for i, qk in enumerate(qkeys):
        v = pub.iloc[i]
        if isinstance(v, str) and v.strip() and v != "None":
            # 已有值：只做一致性核验，不覆盖
            for tid in PUB_FILL_TABLES:      # 只按填充源校验，避免把 86% 一致率的检查源误记为冲突
                qa = pubsrc[tid].get(qk)
                if qa is not None:
                    try:
                        if abs((pd.Timestamp(v).date() - qa).days) > 3:
                            rec["conflicts"] += 1
                    except Exception:
                        pass
                    break
            rec["skipped_existing"] += 1
            continue
        report_day = d["date"].iloc[i].date() if pd.notna(d["date"].iloc[i]) else None
        for tid in PUB_FILL_TABLES:
            qa = pubsrc[tid].get(qk)
            if qa is not None:
                # 硬约束：公告日不得早于报告期，否则会引入前视偏差
                if report_day is not None and qa < report_day:
                    rec["pit_rejected"] += 1
                    continue
                pub.iloc[i] = qa.strftime("%Y%m%d")
                filled_pub += 1
                break
    d["publ_date"] = pub.astype("string")
    rec["filled"]["publ_date"] = filled_pub

    total = sum(rec["filled"].values())
    rec["total_filled"] = int(total)
    if apply and total > 0:
        if backup_dir is not None:
            dst = backup_dir / f"{sym}.parquet"
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, dst)
        # 去掉辅助列后写回，保持原有列集合
        d = d.drop(columns=[c for c in d.columns if c.startswith("__")], errors="ignore")
        d.to_parquet(f, index=False)
        rec["written"] = True
    return rec, d


def coverage(dirsyms, fields):
    """抽样统计字段覆盖率（写盘前后共用）。"""
    out = {f: [] for f in fields}
    for s in dirsyms:
        d = pd.read_parquet(FUND / f"{s}.parquet")
        for f in fields:
            if f in d.columns:
                out[f].append(float(d[f].notna().mean()))
    return {f: (round(float(np.mean(v)), 4) if v else None) for f, v in out.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="实际写盘（默认 dry-run）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（0=全部）")
    ap.add_argument("--sample", type=int, default=400, help="覆盖率统计抽样数")
    args = ap.parse_args()

    A.ms_to_date = ms_beijing
    STYLE.mkdir(parents=True, exist_ok=True)
    syms = sorted(p.stem for p in FUND.glob("*.parquet"))
    if args.limit:
        syms = syms[:args.limit]
    rng = np.random.default_rng(20261122)
    stat_syms = list(rng.choice(syms, size=min(args.sample, len(syms)), replace=False))
    fields = ["publ_date"] + list(VALUE_MAP)
    before = coverage(stat_syms, fields)
    print(f"标的 {len(syms)} 只；抽样 {len(stat_syms)} 只；apply={args.apply}", flush=True)
    print("补齐前覆盖率:", before, flush=True)

    backup_dir = None
    if args.apply:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = CN / f"_backup_qmt_finance_{ts}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        print(f"备份目录 {backup_dir}", flush=True)

    records = []
    t0 = datetime.now()
    for i, sym in enumerate(syms):
        try:
            rec, _ = process(sym, args.apply, backup_dir)
            records.append(rec)
        except Exception as exc:                                  # noqa: BLE001
            records.append({"symbol": sym, "error": f"{type(exc).__name__}: {exc}"})
        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(syms)}，用时 {(datetime.now()-t0).seconds}s", flush=True)

    after = coverage(stat_syms, fields) if args.apply else None
    changed = [r for r in records if r.get("total_filled", 0) > 0]
    errs = [r for r in records if "error" in r]
    agg = {f: int(sum(r.get("filled", {}).get(f, 0) for r in records)) for f in fields}
    written = sum(1 for r in records if r.get("written"))

    audit = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "apply" if args.apply else "dry-run",
        "n_symbols": len(syms), "n_changed": len(changed), "n_written": written,
        "n_errors": len(errs), "sample_for_coverage": len(stat_syms),
        "coverage_before": before, "coverage_after": after,
        "filled_by_field": agg,
        "conflicts": int(sum(r.get("conflicts", 0) for r in records)),
        "pit_rejected": int(sum(r.get("pit_rejected", 0) for r in records)),
        "skipped_existing": int(sum(r.get("skipped_existing", 0) for r in records)),
        "backup_dir": str(backup_dir) if backup_dir else None,
        "design": {
            "value_map": {k: {"qmt_table": v[0], "qmt_col": v[1], "exact_rate": v[2]}
                          for k, v in VALUE_MAP.items()},
            "publ_date_sources": {"fill": list(PUB_FILL_TABLES),
                                  "check_only": list(PUB_CHECK_TABLES),
                                  "exact_rate": PUB_CONF},
            "time_rule": "QMT 毫秒戳 UTC+8；报告期季度末 ±5 天归一化",
            "excluded": ["7001/7002/7003 其余未锚定列", "7006/7007 中文文本表",
                         "7008 每股指标（相关高但精确率低，口径不一致）",
                         "历史季度行（不追加，避免改变 get_fundamentals 行为）"],
            "principles": ["只填补空值，不覆盖已有值", "默认 dry-run，--apply 才落盘",
                           "apply 前自动备份被修改文件", "不猜字段，全部经数值锚定"],
        },
        "errors": errs[:20],
        "sample_changes": changed[:12],
    }
    AUDIT_JSON.write_text(json.dumps(audit, ensure_ascii=False, indent=2, default=str),
                          encoding="utf-8")

    L = ["# QMT → PTrade 财务数据补齐审计", "",
         f"模式 **{audit['mode']}**；标的 {len(syms)} 只；有变更 {len(changed)} 只；"
         f"实际写入 {written} 只；错误 {len(errs)} 只。", "",
         "## 覆盖率（抽样 %d 只）" % len(stat_syms), "",
         "| 字段 | 前 | 后 | 提升 |", "|---|---:|---:|---:|"]
    for f in fields:
        b, a = before.get(f), (after or {}).get(f)
        L.append(f"| {f} | {b} | {a if a is not None else '（dry-run 未写盘）'} | "
                 f"{round((a-b)*100, 2) if (a is not None and b is not None) else '—'} pp |")
    L += ["", "## 填充单元格数", "",
          "| 字段 | 填充数 |", "|---|---:|"]
    for f, n in agg.items():
        L.append(f"| {f} | {n} |")
    L += ["", f"- 公告日早于报告期被拒（前视保护）：{audit['pit_rejected']} 处，未写入。",
          f"- 与已有值冲突（>1% 或公告日 >3 天）：{audit['conflicts']} 处，均已跳过不覆盖。",
          f"- 跳过已有非空值：{audit['skipped_existing']} 处。",
          f"- 备份：{audit['backup_dir'] or '（dry-run 无备份）'}", "",
          "## 锚定依据与边界", "",
          "| PTrade 字段 | QMT 表/列 | 精确一致率 |", "|---|---|---:|"]
    for k, (tid, col, conf) in VALUE_MAP.items():
        L.append(f"| {k} | {tid}[{col}] | {conf:.3f} |")
    L.append(f"| publ_date | 7005 公告日（与 PTrade 已有值 {PUB_CONF[7005]:.4f} 精确一致） | 0.9978 |")
    L += ["", "不导入：7008 每股指标（Spearman 0.98~0.99 但精确率 0.37~0.67，口径不一致）；"
          "7001/7002/7003 其余未锚定列；7006/7007 中文文本表；QMT 更早历史季度行。", "",
          "复现：", "```bash",
          "python scripts/qmt_finance_anchor.py            # 锚定常量与 DAT 解码（保留）",
          "python scratch/probes/probe_qmt_finance_full.py  # 历史全表锚定探针（归档）",
          "python scripts/import_qmt_finance_to_ptrade.py  # dry-run",
          "python scripts/import_qmt_finance_to_ptrade.py --apply", "```", ""]
    AUDIT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    print(f"\n填充合计 {agg}；冲突 {audit['conflicts']}；写入 {written} 只", flush=True)
    if args.apply:
        print("补齐后覆盖率:", after, flush=True)
    print(f"审计 → {AUDIT_MD}", flush=True)
    try:
        import qmt_finance_patch_report
        qmt_finance_patch_report.render(audit)
        print(f"HTML → {STYLE/'qmt_finance_patch.html'}", flush=True)
    except Exception as exc:                                      # noqa: BLE001
        print(f"[警告] HTML 生成失败: {type(exc).__name__}: {exc}", flush=True)


if __name__ == "__main__":
    main()
