# -*- coding: utf-8 -*-
"""QMT -> PTrade 本地回测数据的统一补齐入口。

严格边界（与仓库既有 importer 约定一致）：
  * 只写引擎原生读取的目录（stocks/、stocks_1m/、metadata/、根级 parquet），
    不新增/修改任何引擎接口，不 import QMT SDK / xtdata，不改引擎代码。
  * 标的域严格限定 stock_metadata.security_type in {stock, fund, index}，
    绝不把 QMT 独有的债券/回购/伪日线标的灌进 PTrade。
  * 只做「填补空值 / 修正末端截断 bar / 追加新日期」，不覆盖任何已有历史行
    （唯一例外：2026-11-04 盘中截断 bar，有跨源证据，且单独备份）。
  * 默认 dry-run；只有显式 --apply 才落盘。apply 前逐文件备份。
  * 可重入：已补齐的标的会被快速跳过。多进程 worker 内自足解码 + 备份 + 写盘。

模块：
  daily        日线 stocks/：修正 2026-11-04 截断 bar + 追加 11-05~11-20
  minute       分钟 stocks_1m/：补 ETF/指数 5m bar + 追加末端
  blocks       stock_metadata.blocks：QMT 行业/地域/概念/指数板块映射
  constituents index_constituents：QMT 指数权重与成分股板块（当前快照一期）
  adj_post     后复权缓存 ffill 到新交易日，避免 fq='post' 末端断层
  manifest     同步 manifest.json 的数据末日（version.parquet 作为导出标识不动）

不可补（在报告中给证据）：valuation / exrights / ptrade_adj_pre / stock_status /
Level-2 与龙虎榜（非 PTrade 数据，引擎无通道）。

用法：
    python scripts/patch_all_from_qmt.py                                  # 全模块 dry-run
    python scripts/patch_all_from_qmt.py --modules daily,blocks --limit 50
    python scripts/patch_all_from_qmt.py --apply --workers 8             # 实际写盘
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent           # .../PTrade/SimTradeData/scripts
SIMDATA = HERE.parents[0]                       # .../PTrade/SimTradeData
PTRADE = HERE.parents[1]                        # .../PTrade
ROOT = HERE.parents[2]                          # .../QuantificationResearch
QMT_DATADIR = Path("D:/君弘君智交易系统/datadir")
CN = SIMDATA / "data" / "cn"
STOCKS = CN / "stocks"
STOCKS_1M = CN / "stocks_1m"
META = CN / "metadata"
RESULT_DIR = PTRADE / "strategies" / "style_timing" / "results"

sys.path.insert(0, str(ROOT / "QMT" / "research"))
from qmt_dat import decode_kline_dat, to_daily_frame  # noqa: E402

sys.path.insert(0, str(SIMDATA))
from simtradedata.importers.qmt_kline import decode_kline_dat as decode_5m  # noqa: E402

FIELDS = ["date", "open", "close", "high", "low", "high_limit", "low_limit",
          "preclose", "volume", "money"]
MINUTE_COLS = ["datetime", "open", "high", "low", "close", "volume", "money"]
TRUNC_VOL_RATIO = 0.85
PRICE_TOL = 1e-4
NEW_DAYS_START = pd.Timestamp("2026-11-05")
MODULES = ["daily", "minute", "blocks", "constituents", "adj_post", "manifest"]


# ═══════════════════════════════════════════════════════════════════════════
# 公共
# ═══════════════════════════════════════════════════════════════════════════
def whitelist() -> list[str]:
    md = pd.read_parquet(META / "stock_metadata.parquet", columns=["symbol", "security_type"])
    return md.loc[md.security_type.isin(["stock", "fund", "index"]), "symbol"].tolist()


def qmt_path(symbol: str, period: str) -> Path | None:
    code, mkt = symbol.split(".")
    folder = QMT_DATADIR / mkt.replace("SS", "SH") / period
    p = folder / f"{code}.DAT"
    return p if p.exists() else None


def save_backup(target: Path, backup_root: Path, module: str) -> None:
    if not target.exists():
        return
    rel = target.relative_to(CN)
    dst = backup_root / module / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copy2(target, dst)


def qmt_daily(path: Path) -> pd.DataFrame:
    d = to_daily_frame(decode_kline_dat(path))
    if d.empty:
        return d
    d = d[(d["close"] > 0) & (d["volume"] > 0)].copy()
    d.index = pd.to_datetime(d.index).normalize()
    d.index.name = "date"
    return d


def ptr_daily(symbol: str) -> pd.DataFrame:
    p = STOCKS / f"{symbol}.parquet"
    if not p.exists():
        return pd.DataFrame()
    d = pd.read_parquet(p)
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    return d.dropna(subset=["date"]).set_index("date").sort_index()


def limit_ratios(p: pd.DataFrame) -> tuple[float | None, float | None]:
    """从 PTrade 原文件推断涨跌停比例；ETF/指数为 NaN 时返回 None。"""
    if "high_limit" not in p or "preclose" not in p:
        return None, None
    s = p[["high_limit", "low_limit", "preclose"]].dropna().tail(60)
    s = s[(s.preclose > 0) & (s.high_limit > 0) & (s.low_limit > 0)]
    if len(s) < 5:
        return None, None
    hi = float((s.high_limit / s.preclose - 1).median())
    lo = float((1 - s.low_limit / s.preclose).median())
    return hi, lo


def to_ptr_frame(q: pd.DataFrame) -> pd.DataFrame:
    """QMT 日线 -> PTrade 10 列契约。涨跌停价与 preclose 在拼接后统一重算。"""
    out = pd.DataFrame(index=q.index)
    for f in ["open", "close", "high", "low"]:
        out[f] = q[f].astype(float)
    out["volume"] = q["volume"].astype("int64")
    out["money"] = q["amount"].astype(float)
    out["preclose"] = np.nan
    out["high_limit"] = np.nan
    out["low_limit"] = np.nan
    out = out.reset_index()
    out["date"] = pd.to_datetime(out["date"])
    return out[FIELDS]


# ═══════════════════════════════════════════════════════════════════════════
# M1 日线
# ═══════════════════════════════════════════════════════════════════════════
def daily_one(task: tuple) -> dict:
    symbol, dat, apply, backup = task
    rec = {"symbol": symbol, "action": "skip"}
    try:
        q = qmt_daily(Path(dat))
        if q.empty:
            rec["action"] = "empty_dat"
            return rec
        p = ptr_daily(symbol)
        if p.empty:
            rec["action"] = "no_ptr_file"
            return rec
        if p.index.max() >= pd.Timestamp("2026-11-20"):
            rec["action"] = "already_patched"
            return rec
        last = p.index.max()
        if last not in q.index:
            rec["action"] = "last_bar_missing_in_qmt"
            return rec

        # 末根截断判定：成交量明显偏低，或收盘价不一致
        pv = float(p.loc[last, "volume"]) if "volume" in p else 0.0
        qv = float(q.loc[last, "volume"])
        pc, qc = float(p.loc[last, "close"]), float(q.loc[last, "close"])
        vr = pv / qv if qv > 0 else np.nan
        truncated = bool((np.isfinite(vr) and vr < TRUNC_VOL_RATIO)
                         or abs(pc / qc - 1) > PRICE_TOL)
        rec.update({"last_bar": str(last)[:10], "volume_ratio": round(float(vr), 4)
                    if np.isfinite(vr) else None,
                    "price_diff_pct": round((pc / qc - 1) * 100, 4) if qc else None,
                    "truncated": truncated})

        # 复权口径对齐：取最近 20 个重叠日（排除末根）的 PTR/QMT 收盘比中位数
        cmp_dates = q.index.intersection(p.index)
        cmp_dates = cmp_dates[cmp_dates < last][-20:]
        if len(cmp_dates) < 5:
            rec["action"] = "insufficient_overlap"
            return rec
        ratios = (pd.to_numeric(p.loc[cmp_dates, "close"], errors="coerce")
                  / pd.to_numeric(q.loc[cmp_dates, "close"], errors="coerce")).dropna()
        k = float(ratios.median())
        spread = float(ratios.std() / abs(k)) if k else np.inf
        rec.update({"scale_factor": round(k, 8), "scale_spread": round(spread, 6),
                    "overlap_checked": len(cmp_dates)})
        if not np.isfinite(k) or k <= 0 or spread > 0.02:
            rec["action"] = "unstable_scale_skip"
            return rec

        cutoff = last if truncated else p.index.max()
        keep = p[p.index < cutoff].reset_index() if truncated else p.reset_index()
        new = to_ptr_frame(q)
        new = new[new["date"] >= cutoff].copy() if truncated else new[new["date"] > last].copy()
        if new.empty:
            rec["action"] = "no_new_bar"
            return rec
        if abs(k - 1.0) > 1e-6:
            for f in ["open", "high", "low", "close"]:
                new[f] = pd.to_numeric(new[f], errors="coerce") * k
        out = pd.concat([keep[FIELDS], new[FIELDS]], ignore_index=True)
        out = out.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)
        # 新增行的 preclose / 涨跌停价按最终拼接后的 close 序列统一重算；
        # seam = len(keep) 精确定位第一根新增行，绝不重算任何 PTrade 已有行
        seam = len(keep)
        hi_r, lo_r = limit_ratios(p)
        for i in range(seam, len(out)):
            prev = float(out.loc[i - 1, "close"]) if i > 0 else np.nan
            out.loc[i, "preclose"] = prev
            if np.isfinite(prev) and hi_r is not None:
                out.loc[i, "high_limit"] = round(prev * (1 + hi_r), 2)
                out.loc[i, "low_limit"] = round(prev * (1 - lo_r), 2)
        ret = out["close"].pct_change().abs()
        near = ret.iloc[max(0, seam - 1): seam + 2].dropna()
        seam_max = float(near.max()) if len(near) else 0.0
        rec.update({"action": "patch_truncated" if truncated else "append_only",
                    "added_rows": len(new), "out_rows": len(out),
                    "out_end": str(out["date"].max())[:10],
                    "seam_max_ret": round(seam_max, 6), "seam_ok": bool(seam_max < 0.21)})
        if not rec["seam_ok"]:
            rec["action"] = "seam_failed_skip"
            return rec
        if apply:
            target = STOCKS / f"{symbol}.parquet"
            save_backup(target, Path(backup), "daily")
            out.to_parquet(target, index=False)
    except Exception as exc:                                             # noqa: BLE001
        rec["action"] = "error"
        rec["error"] = f"{type(exc).__name__}: {exc}"
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# M2 分钟线
# ═══════════════════════════════════════════════════════════════════════════
def minute_one(task: tuple) -> dict:
    symbol, dat, apply, backup = task
    rec = {"symbol": symbol, "action": "skip"}
    try:
        f = decode_5m(Path(dat))
        if f.empty:
            rec["action"] = "empty_dat"
            return rec
        f["datetime"] = pd.to_datetime(f["datetime"])
        f = (f.drop_duplicates("datetime", keep="last")
              .sort_values("datetime").reset_index(drop=True))[MINUTE_COLS]
        target = STOCKS_1M / f"{symbol}.parquet"
        if target.exists():
            old = pd.read_parquet(target)
            if "datetime" not in old.columns:
                rec["action"] = "bad_existing_schema"
                return rec
            old["datetime"] = pd.to_datetime(old["datetime"])
            last = old["datetime"].max()
            add = f[f["datetime"] > last]
            if add.empty:
                rec["action"] = "no_new_bar"
                rec["bars"] = len(old)
                return rec
            # keep='first' 保证绝不覆盖本地已有 bar
            merged = (pd.concat([old, add], ignore_index=True)
                      .drop_duplicates("datetime", keep="first")
                      .sort_values("datetime").reset_index(drop=True))
            rec.update({"action": "append", "added_bars": len(add),
                        "existing_bars": len(old), "bars": len(merged),
                        "end": str(merged["datetime"].max())[:16]})
            frame = merged
        else:
            rec.update({"action": "create", "bars": len(f),
                        "end": str(f["datetime"].max())[:16]})
            frame = f
        if apply:
            save_backup(target, Path(backup), "minute")
            frame.to_parquet(target, index=False)
    except Exception as exc:                                             # noqa: BLE001
        rec["action"] = "error"
        rec["error"] = f"{type(exc).__name__}: {exc}"
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# M3 行业/板块 blocks
# ═══════════════════════════════════════════════════════════════════════════
def norm_code(raw: str) -> str | None:
    """QMT 代码 -> PTrade 代码（600000.SH -> 600000.SS）。"""
    s = raw.strip()
    if not s:
        return None
    m = re.match(r"^(SH|SZ)(\d{6})$", s)
    if m:
        return f"{m.group(2)}.{'SS' if m.group(1) == 'SH' else 'SZ'}"
    m = re.match(r"^(\d{6})\.(SH|SZ|SS|BJ)$", s)
    if m:
        return None if m.group(2) == "BJ" else (
            f"{m.group(1)}.{'SS' if m.group(2) == 'SH' else m.group(2)}")
    return None


def read_members(path: Path) -> list[str]:
    try:
        txt = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    out = []
    for tok in re.split(r"[,;\s]+", txt):
        c = norm_code(tok)
        if c:
            out.append(c)
    return out


def industry_code_map() -> dict[str, str]:
    """industry.txt 的证监会编号 -> 规范化名称（含一级与二级）。"""
    out = {}
    for line in (PTRADE / "industry.txt").read_text(encoding="utf-8", errors="replace").splitlines():
        p = line.split("\t")
        if len(p) >= 3 and re.match(r"^[A-Z]\d{5}$", p[1].strip()):
            name = p[2].strip()
            key = re.sub(r"[、，,。\s]", "", name).rstrip("业")
            out[key] = p[1].strip()
    return out


def build_blocks(apply: bool, backup: Path) -> dict:
    md_path = META / "stock_metadata.parquet"
    md = pd.read_parquet(md_path)
    valid = set(md.loc[md.security_type.isin(["stock", "fund", "index"]), "symbol"])
    cmap = industry_code_map()
    blocks: dict[str, dict[str, list]] = {}
    src_stats: Counter = Counter()

    def add(codes, key, value):
        n = 0
        for c in codes:
            if c not in valid:
                continue
            b = blocks.setdefault(c, {})
            b.setdefault(key, [])
            if value not in b[key]:
                b[key].append(value)
                n += 1
        return n

    # 证监会一级 + 二级（优先二级编号，回退一级）
    for level, folder in [("CSRC1", "证监会行业板块"), ("CSRC2", "证监会二级行业板块")]:
        d = QMT_DATADIR / "Sector" / folder
        if not d.is_dir():
            continue
        for p in sorted(d.iterdir()):
            if not p.is_file() or p.name.endswith(".xml"):
                continue
            name = re.sub(r"^CSRC[12]", "", p.name)
            key = re.sub(r"[、，,。\s]", "", name).rstrip("业")
            code = cmap.get(key)
            tag = f"{code}.XBHS" if code else f"{level}_{name}"
            src_stats[folder] += add(read_members(p), "HY" if level == "CSRC2" else "ZJHHY",
                                     [tag, name])
    # IndustryData.txt 是 8 类分类的合集（CSRC1/CSRC2/SW1/SW2/SW3/HKSW1-3），
    # 必须按前缀分派；港股分类与证监会二级（已由目录写入 HY）一律跳过。
    idata = QMT_DATADIR / "Industry" / "IndustryData.txt"
    idata_skipped = Counter()
    if idata.is_file():
        for line in idata.read_text(encoding="utf-8", errors="replace").splitlines():
            if "," not in line:
                continue
            head, *rest = line.split(",")
            m = re.match(r"^(CSRC1|CSRC2|SW1|SW2|SW3|HKSW\d)(.*)$", head.strip())
            if not m:
                idata_skipped["unknown"] += 1
                continue
            kind, name = m.group(1), m.group(2).strip()
            if kind.startswith("HKSW") or kind == "CSRC2":
                idata_skipped[kind] += 1
                continue
            members = [x for r in rest if (x := norm_code(r))]
            if kind == "CSRC1":
                key = re.sub(r"[、，,。\s]", "", name).rstrip("业")
                code = cmap.get(key)
                tag = f"{code}.XBHS" if code else f"CSRC1_{name}"
                src_stats["IndustryData.txt"] += add(members, "ZJHHY", [tag, name])
            else:
                src_stats["IndustryData.txt"] += add(members, kind, [f"{kind}_{name}", name])
    for k, v in idata_skipped.items():
        src_stats[f"IndustryData_skip_{k}"] = v
    # 申万一级 / 地域 / 概念 / 指数成分
    for folder, key, prefix in [("申万一级行业板块", "SW1", "SW1_"),
                                ("地域", "DY", "DY_"),
                                ("D概念", "GN", ""),
                                ("T概念", "GN", ""),
                                ("指数成分股板块", "INDEX", "")]:
        d = QMT_DATADIR / "Sector" / folder
        if not d.is_dir():
            continue
        for p in sorted(d.iterdir()):
            if not p.is_file() or p.name.endswith(".xml"):
                continue
            nm = p.name
            tag = f"{prefix}{nm}" if prefix else nm
            src_stats[folder] += add(read_members(p), key, [tag, nm])

    # 只填空值，不覆盖任何已有 blocks
    empty = md["blocks"].isna() | (md["blocks"].astype(str).str.strip().isin(["", "None", "nan"]))
    filled = 0
    new_blocks = md["blocks"].copy()
    for i in md.index[empty]:
        sym = str(md.at[i, "symbol"])
        if sym in blocks:
            new_blocks.at[i] = json.dumps(blocks[sym], ensure_ascii=False)
            filled += 1
    hy_n = int(sum(1 for b in blocks.values() if b.get("HY")))
    rec = {"module": "blocks", "source_memberships": dict(src_stats),
           "symbols_with_blocks": len(blocks), "symbols_with_HY": hy_n,
           "rows_total": len(md), "rows_empty_before": int(empty.sum()),
           "rows_filled": filled,
           "sample": dict(list(blocks.items())[:2])}
    if apply and filled:
        save_backup(md_path, backup, "blocks")
        md["blocks"] = new_blocks
        md.to_parquet(md_path, index=False)
    rec["applied"] = bool(apply and filled)
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# M4 指数成分（当前快照一期，绝不伪造历史）
# ═══════════════════════════════════════════════════════════════════════════
def index_name_to_symbol() -> dict[str, str]:
    """PTrade/index.txt 指数名称 -> 引擎代码（000xxx.SS / 399xxx.SZ）。"""
    out = {}
    for line in (PTRADE / "index.txt").read_text(encoding="utf-8", errors="replace").splitlines():
        p = line.split("\t")
        if len(p) >= 3 and re.match(r"^\d{6}$", p[1].strip()):
            code = p[1].strip()
            out[p[2].strip()] = f"{code}.{'SZ' if code.startswith('399') else 'SS'}"
    return out


def build_constituents(apply: bool, backup: Path) -> dict:
    path = META / "index_constituents.parquet"
    old = pd.read_parquet(path)
    snapshot = "20261120"
    existing_dates = set(old["date"].astype(str))
    if snapshot in existing_dates:
        return {"module": "constituents", "action": "already_exists", "rows_added": 0}
    have = set(whitelist())
    rows: dict[str, list[str]] = {}

    def put(idx_code: str, members: list[str]):
        c = sorted({x for x in members if x in have})
        if len(c) >= 5:
            # 权重文件与板块文件可能对同一指数给出不同成分，取并集
            rows[idx_code] = sorted(set(rows.get(idx_code, []) + c))

    w = QMT_DATADIR / "Weight" / "sectorWeightData.txt"
    n_weight = 0
    if w.is_file():
        for line in w.read_text(encoding="utf-8", errors="replace").splitlines():
            parts = line.split(";")
            if len(parts) < 3:
                continue
            raw = parts[0].strip()
            m = re.match(r"^(SH|SZ)(\d{6})$", raw)
            if m:
                idx = f"{m.group(2)}.{'SS' if m.group(1) == 'SH' else 'SZ'}"
            elif re.match(r"^\d{6}$", raw):
                idx = f"{raw}.{'SZ' if raw.startswith('399') else 'SS'}"
            else:
                continue
            mem = [norm_code(t.split(",")[0]) for t in parts[1:] if "," in t]
            put(idx, [x for x in mem if x])
            n_weight += 1
    # 指数成分股板块：文件名是 6 位指数代码或中文名；中文名经 index.txt 映射
    name_map = index_name_to_symbol()
    d = QMT_DATADIR / "Sector" / "指数成分股板块"
    n_sector = n_named = 0
    if d.is_dir():
        for p in sorted(d.iterdir()):
            if not p.is_file() or p.name.endswith(".xml"):
                continue
            if re.match(r"^\d{6}$", p.name):
                idx = f"{p.name}.{'SZ' if p.name.startswith('399') else 'SS'}"
            elif p.name in name_map:
                idx = name_map[p.name]
                n_named += 1
            else:
                continue
            put(idx, read_members(p))
            n_sector += 1
    if not rows:
        return {"module": "constituents", "action": "no_source", "rows_added": 0}
    add = pd.DataFrame({"date": [snapshot] * len(rows),
                        "index_code": list(rows),
                        "symbols": [np.array(v, dtype=object) for v in rows.values()]})
    rec = {"module": "constituents", "action": "append_snapshot", "snapshot": snapshot,
           "weight_lines": n_weight, "sector_files": n_sector,
           "named_sector_mapped": n_named, "index_name_dict": len(name_map),
           "indices_added": len(rows), "rows_added": len(add),
           "rows_before": len(old),
           "avg_members": round(float(np.mean([len(v) for v in rows.values()])), 1),
           "sample_indices": sorted(rows)[:8],
           "note": "QMT 只有当前快照；仅追加 2026-11-20 一期，历史时点查询仍走原有月末快照，不引入历史成分前视偏差"}
    if apply:
        save_backup(path, backup, "constituents")
        pd.concat([old, add], ignore_index=True).to_parquet(path, index=False)
    rec["applied"] = bool(apply)
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# M5 后复权缓存 ffill
# ═══════════════════════════════════════════════════════════════════════════
def build_adj_post(apply: bool, backup: Path) -> dict:
    path = CN / "ptrade_adj_post.parquet"
    d = pd.read_parquet(path, columns=["date", "adj_a", "adj_b", "symbol"])
    d["date"] = pd.to_datetime(d["date"])
    last = d["date"].max()
    td = pd.read_parquet(META / "trade_days.parquet")
    days = pd.to_datetime(td["date"], errors="coerce").dropna().sort_values()
    new_days = days[(days > last) & (days <= pd.Timestamp("2026-11-20"))]
    if new_days.empty:
        return {"module": "adj_post", "action": "no_new_days", "rows_added": 0}
    wl = set(whitelist())
    tail = (d[d.symbol.isin(wl)].sort_values("date").groupby("symbol", as_index=False)
            .last()[["symbol", "adj_a", "adj_b"]])
    grid = pd.MultiIndex.from_product([tail.symbol, new_days], names=["symbol", "date"]).to_frame(index=False)
    add = grid.merge(tail, on="symbol", how="left")[["date", "adj_a", "adj_b", "symbol"]]
    add = add[add.adj_a.notna()]
    rec = {"module": "adj_post", "action": "ffill", "old_last": str(last)[:10],
           "new_days": len(new_days), "symbols": int(tail.shape[0]),
           "rows_added": len(add), "rows_before": len(d)}
    if apply and len(add):
        save_backup(path, backup, "adj_post")
        out = pd.concat([d, add], ignore_index=True)
        out.to_parquet(path, index=False)
    rec["applied"] = bool(apply and len(add))
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# M6 manifest
# ═══════════════════════════════════════════════════════════════════════════
def sync_manifest(apply: bool, backup: Path) -> dict:
    p = CN / "manifest.json"
    m = json.loads(p.read_text(encoding="utf-8"))
    old_end = m.get("date_range", {}).get("end")
    rec = {"module": "manifest", "old_end": old_end, "new_end": "2026-11-20",
           "action": "update" if old_end != "2026-11-20" else "already_current"}
    if apply and old_end != "2026-11-20":
        save_backup(p, backup, "manifest")
        m.setdefault("date_range", {})["end"] = "2026-11-20"
        m["export_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        m["description"] = (m.get("description", "") +
                            " | QMT full patch 2026-11-22 (stocks/1m/blocks/constituents/adj_post)")
        p.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
    rec["applied"] = bool(apply and old_end != "2026-11-20")
    return rec


# ═══════════════════════════════════════════════════════════════════════════
# 编排
# ═══════════════════════════════════════════════════════════════════════════
def run_pool(func, tasks, workers, desc):
    out = []
    if workers > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futs = [pool.submit(func, t) for t in tasks]
            for i, f in enumerate(as_completed(futs), 1):
                out.append(f.result())
                if i % 1000 == 0:
                    print(f"  {desc} {i}/{len(tasks)}", flush=True)
    else:
        for i, t in enumerate(tasks, 1):
            out.append(func(t))
            if i % 1000 == 0:
                print(f"  {desc} {i}/{len(tasks)}", flush=True)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="QMT -> PTrade 统一数据补齐")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--modules", default=",".join(MODULES))
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args(argv)

    mods = [m.strip() for m in args.modules.split(",") if m.strip() in MODULES]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = CN / f"_backup_qmt_full_{ts}"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    wl = whitelist()
    if args.limit:
        wl = wl[: args.limit]
    print(f"模式：{'APPLY' if args.apply else 'DRY-RUN'}  模块：{mods}  白名单：{len(wl)}  备份：{backup}", flush=True)

    audit = {"mode": "apply" if args.apply else "dry-run", "timestamp": ts,
             "backup_dir": str(backup), "whitelist": len(wl), "modules": {}}
    skipped = []

    if "daily" in mods or "minute" in mods:
        daily_tasks, minute_tasks, no_dat = [], [], 0
        for s in wl:
            pd_path = qmt_path(s, "86400")
            pm_path = qmt_path(s, "300")
            if pd_path is None and pm_path is None:
                no_dat += 1
                continue
            if "daily" in mods and pd_path:
                daily_tasks.append((s, str(pd_path), args.apply, str(backup)))
            if "minute" in mods and pm_path:
                minute_tasks.append((s, str(pm_path), args.apply, str(backup)))
        skipped.append(f"QMT 86400/300 均无 DAT 的白名单标的：{no_dat} 只（多为退市股/指数）")

    if "daily" in mods:
        print(f"M1 日线：{len(daily_tasks)} 只", flush=True)
        recs = run_pool(daily_one, daily_tasks, args.workers, "daily")
        c = Counter(r["action"] for r in recs)
        bad = [r for r in recs if r["action"] in ("error", "seam_failed_skip", "unstable_scale_skip")]
        audit["modules"]["daily"] = {
            "n": len(recs), "actions": dict(c),
            "added_rows": int(sum(r.get("added_rows", 0) for r in recs)),
            "truncated_fixed": int(c.get("patch_truncated", 0)),
            "seam_failed": int(c.get("seam_failed_skip", 0)),
            "scale_factors": {"min": min((r.get("scale_factor", 1) for r in recs), default=1),
                              "max": max((r.get("scale_factor", 1) for r in recs), default=1),
                              "non_unit": sum(1 for r in recs
                                              if abs(r.get("scale_factor", 1) - 1) > 1e-4)},
            "bad": bad[:20],
            "sample": [r for r in recs if r["action"] in ("patch_truncated", "append_only")][:5]}

    if "minute" in mods:
        print(f"M2 分钟线：{len(minute_tasks)} 只", flush=True)
        recs = run_pool(minute_one, minute_tasks, args.workers, "minute")
        c = Counter(r["action"] for r in recs)
        audit["modules"]["minute"] = {
            "n": len(recs), "actions": dict(c),
            "created": int(c.get("create", 0)), "appended": int(c.get("append", 0)),
            "added_bars": int(sum(r.get("added_bars", 0) + (r.get("bars", 0)
                                                            if r["action"] == "create" else 0)
                                  for r in recs)),
            "errors": [r for r in recs if r["action"] == "error"][:10]}

    for name, fn in [("blocks", build_blocks), ("constituents", build_constituents),
                     ("adj_post", build_adj_post), ("manifest", sync_manifest)]:
        if name in mods:
            print(f"M{['blocks', 'constituents', 'adj_post', 'manifest'].index(name) + 3} {name}", flush=True)
            audit["modules"][name] = fn(args.apply, backup)

    audit["skipped"] = skipped
    audit["not_patchable"] = [
        {"item": "valuation/*.parquet", "reason": "QMT 无估值数据：Finance 是财报私有二进制、EP 只有 config 无数值",
         "ptrade_end": "2026-05-22"},
        {"item": "exrights/*.parquet + ptrade_adj_pre", "reason": "QMT DividData 为 LevelDB 私有格式，仓库既有裁决「不认、不猜字段」；pre 前复权基准随末日变化，不做 ffill 近似",
         "ptrade_end": "2025-10 / 2026-05-26"},
        {"item": "stock_status.parquet", "reason": "QMT 无 ST/HALT 历史状态明文序列（仅 1 个当前快照板块文件）",
         "ptrade_end": "2026-05-22"},
        {"item": "龙虎榜 / 主力资金流 / Level-2", "reason": "非 PTrade 数据，SimTradeLab 无数据通道；longhubang.db3 属 QMT 侧离线资产"},
        {"item": "真 1m K 线", "reason": "QMT 60 周期目录仅 1 个样本文件；stocks_1m 装的是 5m bar，1m 语义失真"},
    ]
    out = RESULT_DIR / "qmt_full_patch_audit.json"
    out.write_text(json.dumps(audit, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("\n=== 汇总 ===")
    for k, v in audit["modules"].items():
        print(f"  {k}: {json.dumps({a: b for a, b in v.items() if a not in ('sample', 'bad', 'errors')}, ensure_ascii=False)[:400]}")
    print(f"审计：{out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
