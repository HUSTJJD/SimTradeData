# -*- coding: utf-8 -*-
"""只读探针：QMT Finance DAT 的列能否用 PTrade 已知字段「数值锚定」出来。

背景
------------------------------------------------------------------
QMT 财务 DAT 是裸 int64 毫秒时间戳 + double 记录，缺失哨兵
0x7FEFFFFFFFFFFFFF。项目已有 probe_qmt_private_binary.py 判定「能读，
但字段顺序/单位无官方字典，不建议当数据入口」。

本探针不推翻该结论，而是补上它缺的一步：**不猜字段**，用 PTrade
fundamentals/ 里已经可信的数值作为锚点，反推 QMT 每一列对应哪个字段。
只有「在多个股票、多个报告期上唯一且高置信匹配」的列才允许进入后续导入。

边界（与既有 import_qmt_daily_to_ptrade.py 一致）
------------------------------------------------------------------
  * 只读，不写任何 parquet，不修改引擎接口，不 import QMT SDK / xtdata。
  * 7006/7007 是含中文股东名的变长文本表，double 解读无意义，直接排除。
  * 输出报告 JSON：PTrade/SimTradeData/scripts/qmt_finance_anchor_report.json

用法：python scripts/qmt_finance_anchor.py
"""
from __future__ import annotations

import json
import math
import struct
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
QMT = Path(r"D:\君弘君智交易系统\datadir\Finance")
CN = ROOT / "SimTradeData" / "data" / "cn"
OUT = ROOT / "SimTradeData" / "scripts" / "qmt_finance_anchor_report.json"

# 由「文件字节数被整除 + 合法时间戳位置间隔」实测确定（非推测）
RECORD_BYTES = {7001: 1264, 7002: 664, 7003: 920, 7004: 56,
                7005: 64, 7006: 416, 7007: 416, 7008: 344}
N_TS = {7002: 3, 7003: 3}
SKIP_TABLES = {7006, 7007}          # 变长文本表
SENTINEL = 0x7FEFFFFFFFFFFFFF
TS_MIN, TS_MAX = 631152000000, 1893456000000   # 1990-01-01 ~ 2030-01-01

# 官方字段名候选（QMT/vendor/xtquant_big_convert/.../market_bigqmt.py）
FIELD_DICT = {
    7001: "ASHAREBALANCESHEET", 7002: "ASHAREINCOME",
    7003: "ASHARECASHFLOW", 7004: "CAPITALSTRUCTURE",
    7005: "SHAREHOLDER", 7008: "PERSHAREINDEX",
}

# PoC 先锚定三个「短记录表」：字段少、字段名最可信、最可能与 PTrade 指标同源。
# 7001/7002/7003 各有 80~156 列，二阶段再扩展（避免 PoC 阶段 O(股票×季度×列×字段) 爆内存）。
ANCHOR_TABLES = (7004, 7005, 7008)
N_SAMPLE = 24


def ms_to_date(ms: int):
    if not (TS_MIN < ms < TS_MAX):
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date()


def quarter_key(ms: int):
    """把 QMT 的『季度末前 1 天』(12-30/03-30/06-29/09-29) 归一化到季度键。"""
    d = ms_to_date(ms)
    if d is None:
        return None
    cands = []
    for y in (d.year - 1, d.year, d.year + 1):
        for q, (m, day) in enumerate([(3, 31), (6, 30), (9, 30), (12, 31)], start=1):
            end = datetime(y, m, day, tzinfo=timezone.utc).date()
            cands.append((abs((d - end).days), (y, q)))
    delta, key = min(cands, key=lambda x: x[0])
    return f"{key[0]}Q{key[1]}" if delta <= 5 else None   # 字符串键，避免 tuple 被 pandas 当复合索引


def pick_dates(ts_list):
    """从 2~3 个时间戳里挑报告期（能归一化到季度末）与公告日。"""
    report = None
    for t in ts_list:
        if quarter_key(t) is not None:
            report = t
            break
    if report is None:
        return None, None
    others = [t for t in ts_list if t != report and ms_to_date(t) is not None]
    ann = max(others) if others else report
    return quarter_key(report), ms_to_date(ann)


def decode(path: Path, tid: int):
    b = path.read_bytes()
    rs = RECORD_BYTES[tid]
    k = N_TS.get(tid, 2)
    n = len(b) // rs
    if n * rs != len(b):
        return None
    rows = []
    for i in range(n):
        o = i * rs
        ts = [struct.unpack_from("<Q", b, o + j * 8)[0] for j in range(k)]
        qkey, ann = pick_dates(ts)
        if qkey is None:
            continue
        vals = []
        for j in range(k, rs // 8):
            u = struct.unpack_from("<Q", b, o + j * 8)[0]
            if u == SENTINEL:
                vals.append(np.nan)
            else:
                v = struct.unpack_from("<d", b, o + j * 8)[0]
                vals.append(v if math.isfinite(v) and abs(v) < 1e15 else np.nan)
        rows.append((qkey, ann, vals))
    return rows


def qmt_matrix(sym: str, tid: int):
    """返回 {(季度键): {col: value}}，同季度多条取公告日最新的一条。"""
    code, mkt = sym.split(".")
    folder = QMT / mkt.replace("SS", "SH").replace("SZ", "SZ") / "86400"
    p = folder / f"{code}_{tid}.DAT"
    if not p.exists():
        return {}
    rows = decode(p, tid)
    if not rows:
        return {}
    from datetime import date as _date
    best = {}
    for qk, ann, vals in rows:
        key = ann or _date.min
        if qk not in best or key > best[qk][0]:
            best[qk] = (key, vals)
    return {qk: v[1] for qk, v in best.items()}


def ptr_panel(sym: str):
    p = CN / "fundamentals" / f"{sym}.parquet"
    if not p.exists():
        return None
    d = pd.read_parquet(p)
    d["date"] = pd.to_datetime(d["date"])
    d["qkey"] = [f"{x.year}Q{(x.month - 1) // 3 + 1}" for x in d["date"]]
    return d.set_index("qkey")


def main():
    md = pd.read_parquet(CN / "metadata" / "stock_metadata.parquet")
    stocks = md.loc[md.security_type == "stock", "symbol"].tolist()
    rng = np.random.default_rng(20261122)
    rng.shuffle(stocks)
    sample = []
    for s in stocks:
        if (CN / "fundamentals" / f"{s}.parquet").exists():
            sample.append(s)
        if len(sample) >= N_SAMPLE:
            break
    print(f"样本股票 {len(sample)} 只", flush=True)

    # 收集 (PTrade字段, QMT表, QMT列) -> [(qmt_val, ptr_val, sym, qkey)]
    pairs = defaultdict(list)
    pcols = None
    for i, sym in enumerate(sample):
        pp = ptr_panel(sym)
        if pp is None:
            continue
        if pcols is None:
            pcols = [c for c in pp.columns
                     if c not in ("date", "publ_date", "qkey", "end_date")
                     and pp[c].dtype != "object"
                     and str(pp[c].dtype) != "null"
                     and pp[c].notna().sum() >= 3]
        for tid in ANCHOR_TABLES:
            if tid in SKIP_TABLES:
                continue
            qm = qmt_matrix(sym, tid)
            for qk, vals in qm.items():
                if qk not in pp.index:
                    continue
                row = pp.loc[qk]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[-1]
                for col, qv in enumerate(vals):
                    if not np.isfinite(qv):
                        continue
                    for pf in pcols:
                        pv = row.get(pf, np.nan)
                        if pd.isna(pv) or not np.isfinite(float(pv)):
                            continue
                        pairs[(pf, tid, col)].append((qv, float(pv)))
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(sample)}", flush=True)
    print(f"配对组合 {len(pairs)} 个", flush=True)

    # 评分
    results = []
    for (pf, tid, col), arr in pairs.items():
        if len(arr) < 12:
            continue
        q = np.array([a[0] for a in arr], dtype=float)
        p = np.array([a[1] for a in arr], dtype=float)
        m = np.isfinite(q) & np.isfinite(p) & (p != 0)
        if m.sum() < 10:
            continue
        q, p = q[m], p[m]
        rel = np.abs(q - p) / np.maximum(np.abs(p), 1e-9)
        exact = float((rel < 1e-4).mean())
        ratio = q / p
        # 固定比例一致性（允许单位差异：1 / 0.01 / 100 / 10000）
        ratio_score = 0.0
        best_k = None
        for k in (0.01, 0.1, 1.0, 10.0, 100.0, 10000.0, 1e4, 1e8):
            r = float((np.abs(ratio - k) / max(k, 1e-9) < 1e-4).mean())
            if r > ratio_score:
                ratio_score, best_k = r, k
        try:
            corr = float(pd.Series(q).corr(pd.Series(p), method="spearman"))
        except Exception:
            corr = float("nan")
        results.append({"ptr_field": pf, "qmt_table": tid, "qmt_col": col,
                        "n": int(len(arr)), "exact_rate": round(exact, 4),
                        "ratio_k": best_k, "ratio_rate": round(ratio_score, 4),
                        "spearman": round(corr, 4) if np.isfinite(corr) else None,
                        "qmt_median": round(float(np.median(q)), 4),
                        "ptr_median": round(float(np.median(p)), 4)})

    df = pd.DataFrame(results)
    # 每个 PTrade 字段取最佳匹配，并要求唯一（第二名明显更差）
    best = []
    if len(df):
        df["score"] = df["exact_rate"] * 2 + df["ratio_rate"] + df["spearman"].fillna(0).clip(0, 1)
        for pf, g in df.groupby("ptr_field"):
            g = g.sort_values("score", ascending=False)
            top = g.iloc[0]
            second = g.iloc[1]["score"] if len(g) > 1 else 0.0
            best.append({**top.to_dict(), "margin": round(float(top["score"] - second), 4),
                         "n_candidates": int(len(g))})
    best = sorted(best, key=lambda x: -x["score"])
    confident = [b for b in best
                 if (b["exact_rate"] >= 0.80 or b["ratio_rate"] >= 0.80)
                 and (b["margin"] >= 0.30 or b["n_candidates"] == 1)]

    out = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sample_stocks": len(sample), "sample_symbols": sample[:10],
        "record_bytes": RECORD_BYTES, "skip_tables": sorted(SKIP_TABLES),
        "n_pair_combos": len(pairs),
        "ptr_fields": pcols,
        "best_match_per_field": best,
        "confident_matches": confident,
        "verdict": ("可导入字段数 %d / %d；仅这些列允许进入后续补齐，"
                    "其余列语义未确认，不导入。" % (len(confident), len(best))),
        "notes": [
            "报告期用『季度末容差 ±5 天』归一化，QMT 惯例为季度末前 1 个交易日。",
            "7006/7007 是含中文股东名的变长文本表，double 解读无意义，已排除。",
            "匹配判据：精确率或固定比例率 ≥0.80 且相对第二名有 ≥0.30 分差（或唯一候选）。",
            "本探针只读，不写任何 parquet；字段字典仅作候选名集合，未用于猜测列顺序。",
        ],
    }
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n=== 高置信锚定 {len(confident)} 个 ===")
    for b in confident:
        print(f"  {b['ptr_field']:<32} <- {b['qmt_table']}[{b['qmt_col']:>3}]  "
              f"exact {b['exact_rate']:.2f} ratio {b['ratio_rate']:.2f} "
              f"corr {b['spearman']} n={b['n']}")
    print(f"\n报告 → {OUT}")


if __name__ == "__main__":
    main()
