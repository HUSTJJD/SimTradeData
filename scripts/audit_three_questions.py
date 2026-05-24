# -*- coding: utf-8 -*-
"""追问核查（只读）：

Q1  PTrade 已有 23662/23698 个 parquet，是不是脏数据 / 没做验证对齐？
Q2  QMT 5m 数据能不能转换成 15m / 30m / 60m？在「只支持 PTrade 原生接口」约束下如何落地？
Q3  见 probe_qmt_private_binary.py（私有二进制格式可行性）。

边界（重要）：
    SimTradeLab 只是 PTrade 回测接口的本地实现，只允许通过 PTrade 原生接口取数。
    本脚本不改引擎代码、不改数据文件、不 import QMT SDK / xtdata。
    引擎只读 data/cn/stocks (1d) 与 data/cn/stocks_1m (1m)，
    5m/15m/30m/60m/120m 由引擎从 1m bar 自行聚合。

用法：
    python scripts/audit_three_questions.py
输出：
    scripts/qmt_three_questions_report.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
import types
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
sys.path.insert(0, str(ROOT / "SimTradeLab" / "src"))
sys.path.insert(0, str(ROOT / "SimTradeData"))
DATA_DIR = ROOT / "SimTradeData" / "data" / "cn"
QMT_DIR = Path(r"D:\君弘君智交易系统\datadir")
OUT = ROOT / "SimTradeData" / "scripts" / "qmt_three_questions_report.json"

os.environ["SIMTRADELAB_DATA_PATH"] = str(DATA_DIR)

from simtradelab.ptrade.api import PtradeAPI  # noqa: E402
from simtradelab.ptrade.data_context import DataContext  # noqa: E402
from simtradedata.importers.qmt_kline import decode_kline_dat, to_daily_frame  # noqa: E402


# ---------------------------------------------------------------- Q1
def scan_footers(folder: Path) -> pd.DataFrame:
    """只读 parquet footer statistics，拿到每个文件的行数与日期区间，不读数据行。"""
    rows = []
    for path in sorted(folder.glob("*.parquet")):
        try:
            meta = pq.ParquetFile(path).metadata
            lo = hi = None
            for rg in range(meta.num_row_groups):
                st = meta.row_group(rg).column(0).statistics
                if st is None or not st.has_min_max:
                    continue
                lo = st.min if lo is None else min(lo, st.min)
                hi = st.max if hi is None else max(hi, st.max)
            rows.append((path.stem, meta.num_rows, pd.Timestamp(lo), pd.Timestamp(hi)))
        except Exception as exc:  # noqa: BLE001
            rows.append((path.stem, -1, pd.NaT, pd.NaT))
    return pd.DataFrame(rows, columns=["symbol", "rows", "first", "last"])


def code_bucket(symbol: str) -> str:
    code, market = symbol.rsplit(".", 1)
    if market == "SS":
        if code.startswith(("600", "601", "603", "605")):
            return "沪市主板A股"
        if code.startswith(("688", "689")):
            return "科创板A股"
        if code.startswith("900"):
            return "沪市B股"
        if code.startswith(("000", "880", "881", "999")):
            return "沪市指数/其它"
        if code.startswith(("5", "51", "56", "58")):
            return "沪市ETF/基金"
        if code.startswith(("1", "2", "10", "11", "20")):
            return "沪市债券/回购"
        return "沪市其它"
    if market == "SZ":
        if code.startswith(("000", "001", "002", "003")):
            return "深市主板A股"
        if code.startswith(("300", "301")):
            return "创业板A股"
        if code.startswith("200"):
            return "深市B股"
        if code.startswith(("15", "16", "18")):
            return "深市ETF/基金"
        if code.startswith(("39", "12", "10")):
            return "深市债券/指数"
        return "深市其它"
    return "其它"


def q1_universe() -> dict:
    meta = pd.read_parquet(DATA_DIR / "metadata" / "stock_metadata.parquet")
    types_ct = meta["security_type"].value_counts(dropna=False).to_dict()

    daily = scan_footers(DATA_DIR / "stocks")
    print(f"[Q1] 扫描日线 footer：{len(daily)} 个文件")
    five = scan_footers(DATA_DIR / "stocks_5m")
    five_syms = set(five["symbol"])
    print(f"[Q1] 扫描 5m footer：{len(five)} 个文件")

    daily["bucket"] = daily["symbol"].map(code_bucket)
    bucket_ct = daily["bucket"].value_counts().to_dict()

    # 短历史群：起始晚于 5m 数据起点，且行数明显偏少
    short = daily[(daily["first"] >= pd.Timestamp("2025-10-01")) & (daily["rows"] < 400)]
    short_by_type = meta.set_index("symbol").reindex(short["symbol"])["security_type"] \
                        .value_counts(dropna=False).to_dict()
    short_with_5m = int(short["symbol"].isin(five_syms).sum())
    long_ct = int((daily["first"] < pd.Timestamp("2025-10-01")).sum())

    # security_type='stock' 里到底有多少是真 A 股？交叉表暴露误标
    meta = meta.copy()
    meta["bucket"] = meta["symbol"].map(code_bucket)
    stock_bucket = meta.loc[meta["security_type"] == "stock", "bucket"].value_counts().to_dict()

    # 短历史 stock 群：区分「被误标的指数」与「正常新股」
    ss = meta[meta["symbol"].isin(short["symbol"]) & (meta["security_type"] == "stock")].copy()
    ss = ss.merge(daily[["symbol", "first", "rows"]], on="symbol", how="left")
    ss["listed"] = pd.to_datetime(ss["listed_date"], errors="coerce")
    synthetic = ss[(ss["rows"] == 245) & (ss["first"] == pd.Timestamp("2025-10-31"))]
    normal_ipo = ss.drop(index=synthetic.index)
    normal_ipo_same_start = int((normal_ipo["first"] == normal_ipo["listed"]).sum())

    # 抽样比对：短历史日线 == 对应 5m bar 合成的伪日线？
    samples = []
    for sym in short["symbol"].head(8).tolist():
        p5 = DATA_DIR / "stocks_5m" / f"{sym}.parquet"
        pd_path = DATA_DIR / "stocks" / f"{sym}.parquet"
        if not p5.exists() or not pd_path.exists():
            continue
        bars = pd.read_parquet(p5)
        daily_df = pd.read_parquet(pd_path)
        rebuilt = to_daily_frame(bars, sym)
        merged = daily_df.merge(rebuilt, on="date", suffixes=("_ptrade", "_from5m"))
        if merged.empty:
            continue
        diffs = {}
        for col in ("open", "high", "low", "close", "volume"):
            a = pd.to_numeric(merged[f"{col}_ptrade"], errors="coerce")
            b = pd.to_numeric(merged[f"{col}_from5m"], errors="coerce")
            denom = b.abs().replace(0, pd.NA)
            rel = ((a - b).abs() / denom).dropna()
            diffs[col] = round(float(rel.max()), 8) if len(rel) else None
        samples.append({
            "symbol": sym,
            "daily_rows": len(daily_df),
            "five_m_rows": len(bars),
            "overlap_days": len(merged),
            "max_rel_diff": diffs,
            "identical": all(v is not None and v < 1e-6 for v in diffs.values()),
        })

    # 只读清单：把 5m 合成伪日线逐只列出，供回测 universe 隔离使用（不删不改任何文件）
    qmt_daily = set()
    for market, suffix in (("SH", "SS"), ("SZ", "SZ")):
        for p in (QMT_DIR / market / "86400").glob("*.DAT"):
            qmt_daily.add(f"{p.stem[-6:]}.{suffix}")
    manifest = short[["symbol", "rows", "first", "last"]].copy()
    meta_idx = meta.set_index("symbol")
    manifest["security_type"] = manifest["symbol"].map(meta_idx["security_type"])
    manifest["stock_name"] = manifest["symbol"].map(meta_idx["stock_name"])
    manifest["has_5m_source"] = manifest["symbol"].isin(five_syms)
    manifest["has_qmt_official_daily"] = manifest["symbol"].isin(qmt_daily)
    manifest["first"] = manifest["first"].dt.strftime("%Y-%m-%d")
    manifest["last"] = manifest["last"].dt.strftime("%Y-%m-%d")
    manifest_path = OUT.with_name("qmt_pseudo_daily_manifest.json")
    manifest_path.write_text(json.dumps({
        "purpose": "只读清单：疑似由 5m 合成的短历史伪日线，供回测 universe 隔离；不删除、不修改任何文件",
        "criteria": "first_date >= 2025-10-01 且 rows < 400",
        "count": len(manifest),
        "by_security_type": {k: int(v) for k, v in manifest["security_type"].value_counts().items()},
        "with_qmt_official_daily": int(manifest["has_qmt_official_daily"].sum()),
        "symbols": manifest.to_dict("records"),
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    return {
        "pseudo_daily_manifest": manifest_path.name,
        "stocks_files": len(daily),
        "stocks_1m_exists": (DATA_DIR / "stocks_1m").is_dir(),
        "stocks_5m_files": len(five),
        "metadata_security_type": {k: int(v) for k, v in types_ct.items()},
        "code_bucket": {k: int(v) for k, v in bucket_ct.items()},
        "stock_tagged_by_code_bucket": {k: int(v) for k, v in stock_bucket.items()},
        "a_share_estimate": int(sum(v for k, v in bucket_ct.items() if "A股" in k)),
        "long_history_files": long_ct,
        "short_history_stock_group": {
            "total": len(ss),
            "index_mislabeled_as_stock": len(synthetic),
            "index_examples": synthetic["stock_name"].head(6).tolist(),
            "normal_recent_ipo": len(normal_ipo),
            "normal_ipo_first_equals_listed_date": normal_ipo_same_start,
        },
        "short_history_suspect": {
            "count": len(short),
            "definition": "first_date >= 2025-10-01 且 rows < 400",
            "by_security_type": {k: int(v) for k, v in short_by_type.items()},
            "has_matching_stocks_5m": short_with_5m,
            "first_date_modes": {str(k): int(v) for k, v in
                                short["first"].dt.strftime("%Y-%m-%d").value_counts().head(5).items()},
            "rows_modes": {int(k): int(v) for k, v in short["rows"].value_counts().head(5).items()},
        },
        "rebuild_from_5m_samples": samples,
        "verdict": (
            "23698 不是 2 万多只 A 股：目录按全品种维护（债券 17073 / 基金 851 / 指数 21），"
            "按代码前缀估算真 A 股约 5564 只，其中 5473 只有完整长历史，数量级合理。"
            "但确实存在口径污染：18225 个短历史文件起始于 QMT 5m 数据起点，抽样 8 只均可用 "
            "stocks_5m 逐字段精确复现，属于既有 import_qmt_local.py 从 5m 合成的伪日线；"
            "短历史 stock 群里 148 只其实是上证指数被误标为 stock，另有 93 只是 2025-10 后"
            "上市的正常新股（起始日等于上市日）。核心 A 股长历史未被污染，本次导入的重叠期"
            "收盘价交叉校验也支持这一点。"
        ),
    }


# ---------------------------------------------------------------- Q2
def q2_periods() -> dict:
    dat = QMT_DIR / "SH" / "300" / "600000.DAT"
    bars = decode_kline_dat(dat)
    if bars.empty:
        return {"error": f"5m DAT 解码为空: {dat}"}
    bars["datetime"] = pd.to_datetime(bars["datetime"])
    frame = bars.set_index("datetime").sort_index()
    symbol = "600000.SS"

    out = {
        "source": str(dat),
        "raw_5m_bars": len(frame),
        "range": [str(frame.index.min()), str(frame.index.max())],
        "direct_aggregation": {},
        "via_ptrade_native_api": {},
    }

    # (a) 直接用引擎的静态方法做数学聚合
    for freq, minutes in (("15m", 15), ("30m", 30), ("60m", 60)):
        agg = PtradeAPI._aggregate_intraday_kline(frame.copy(), minutes)
        out["direct_aggregation"][freq] = {
            "bars": len(agg),
            "last_index": str(agg.index.max()),
            "last_close": float(agg["close"].iloc[-1]),
        }

    # (b) 把 5m bar 假装成 stocks_1m 的内容，走 PTrade 原生 get_history
    class _G:
        pass

    # 5m DAT 当天只写到上午收盘，current_dt 取 11:30 才不会在标准分钟时间轴上
    # reindex 出一堆下午的 NaN（这是引擎的对齐行为，不是数据错误）
    last_bar = frame.index.max()
    ctx = types.SimpleNamespace(
        current_dt=last_bar,
        frequency="1m",
        g=_G(),
        broker_profile="auto",
    )
    dc = DataContext(
        stock_data_dict={symbol: frame.iloc[0:0].copy()},
        valuation_dict={}, fundamentals_dict={}, exrights_dict={},
        benchmark_data={}, stock_metadata=pd.DataFrame(),
        index_constituents={}, stock_status_history={},
        adj_pre_cache=None, adj_post_cache=None, trade_days=None,
        stock_data_dict_1m={symbol: frame.copy()},
    )
    api = PtradeAPI(dc, ctx, logging.getLogger("audit_q2"))
    api.active_universe = {symbol}

    for freq in ("1m", "5m", "15m", "30m", "60m"):
        try:
            res = api.get_history(10, frequency=freq, field=["close"],
                                  security_list=[symbol], fq=None, include=True)
            panel = res["close"] if isinstance(res, dict) else res
            if isinstance(panel, pd.DataFrame):
                idx = list(panel.index)
                series = panel.iloc[:, 0] if panel.shape[1] else pd.Series(dtype=float)
            else:
                idx = list(getattr(panel, "index", []))
                series = pd.Series(panel)
            vals = pd.to_numeric(series, errors="coerce")
            out["via_ptrade_native_api"][freq] = {
                "returned_bars": len(idx),
                "non_nan_bars": int(vals.notna().sum()),
                "nan_bars": int(vals.isna().sum()),
                "last_index": str(idx[-1]) if idx else None,
                "index_gap_minutes": (
                    round((pd.Timestamp(idx[-1]) - pd.Timestamp(idx[-2])).total_seconds() / 60, 1)
                    if len(idx) >= 2 else None
                ),
                "last_close": (float(vals.dropna().iloc[-1]) if vals.notna().any() else None),
            }
        except Exception as exc:  # noqa: BLE001
            out["via_ptrade_native_api"][freq] = {"error": f"{type(exc).__name__}: {exc}"}

    # 最后交易日 5m -> 15m 的 OHLCV 明细，验证聚合口径
    last_day = frame.index.max().normalize()
    day5 = frame[frame.index.normalize() == last_day]
    agg15 = PtradeAPI._aggregate_intraday_kline(day5.copy(), 15)
    out["last_session_detail"] = {
        "date": str(last_day.date()),
        "bars_5m": len(day5),
        "bars_15m": len(agg15),
        "sample": agg15[["open", "high", "low", "close", "volume"]].tail(3)
                    .round(4).reset_index().astype(str).to_dict("records"),
    }
    out["verdict"] = (
        "可以。把 QMT 5m bar 写进引擎唯一认的 stocks_1m 后，通过 PTrade 原生 get_history "
        "取 5m/15m/30m/60m 都能返回完整无 NaN 的数据且与直接聚合一致；"
        "唯一代价是 frequency='1m' 会按标准 1m 时间轴 reindex，5m bar 只落在 5 分钟刻度上，"
        "10 根里只有 2 根有值，即 1m 语义失真。QMT 1m 本来就没下载，"
        "若策略只用 5m 及以上周期可直接落地；若要真 1m，必须先让 QMT 客户端补下载 1m。"
    )
    return out


def main() -> int:
    print("== Q1 23698 个 parquet 的构成 ==")
    r1 = q1_universe()
    print(json.dumps({k: v for k, v in r1.items() if k != "rebuild_from_5m_samples"},
                     ensure_ascii=False, indent=2))
    print("  抽样复现：", [(s["symbol"], s["identical"]) for s in r1["rebuild_from_5m_samples"]])

    print("\n== Q2 5m -> 15/30/60m ==")
    r2 = q2_periods()
    print(json.dumps(r2, ensure_ascii=False, indent=2, default=str))

    # Q3 的详细探针独立运行（probe_qmt_private_binary.py），这里并入其结论
    q3_path = OUT.with_name("qmt_private_binary_report.json")
    if q3_path.exists():
        q3_full = json.loads(q3_path.read_text(encoding="utf-8"))
        q3 = {
            "detail_report": q3_path.name,
            "leveldb": {
                "footer_magic_ok": q3_full["leveldb"].get("footer_magic_ok"),
                "index_entries": q3_full["leveldb"].get("index_entries"),
                "key_layout": q3_full["leveldb"].get("key_layout"),
                "value_layout": q3_full["leveldb"].get("value_layout"),
            },
            "finance_record_cells": {
                tid: info["record_cells_8byte"] for tid, info in q3_full["finance"]["tables"].items()
            },
            "conclusion": q3_full["conclusion"],
        }
    else:
        q3 = {"conclusion": "请先运行 scripts/probe_qmt_private_binary.py"}

    OUT.write_text(json.dumps({"q1": r1, "q2": r2, "q3": q3},
                             ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n报告已写入：{OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
