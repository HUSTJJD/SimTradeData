# -*- coding: utf-8 -*-
"""只读体检：本机 QMT 客户端 datadir 的下载完备度，以及与 PTrade 本地回测引擎数据目录的对齐情况。

边界（重要）：
    SimTradeLab 只是 PTrade 回测接口的本地实现，只允许通过 PTrade 原生接口
    (get_history / get_price / get_fundamentals ...) 取数。引擎只读
        data/cn/stocks        -> 1d 日线
        data/cn/stocks_1m     -> 1m 分钟线（5m/15m/30m/60m 由引擎从 1m bar 自行聚合）
    不存在 stocks_5m 之类的独立目录，也不允许在引擎里接 QMT SDK / xtdata。
    本脚本不 import 任何 QMT SDK，不修改引擎代码，只解析 QMT 落盘的 DAT 文件做体检。

用法：
    python scripts/audit_qmt_local.py
    python scripts/audit_qmt_local.py --json scripts/qmt_audit_report.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from simtradedata.importers.qmt_kline import decode_kline_dat  # noqa: E402

DEFAULT_ROOT = Path(r"D:\君弘君智交易系统")
DEFAULT_DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data")

# QMT datadir 周期目录 -> 引擎口径
PERIOD_LABEL = {
    "86400": "1d",
    "300": "5m",
    "60": "1m",
    "900": "15m",
    "1800": "30m",
    "3600": "60m",
    "0": "tick/其它",
}
MARKETS = ("SH", "SZ", "BJ", "HK")

# A 股代码口径（用于判断"全市场股票是否下载到位"）
def is_a_share(market: str, code: str) -> bool:
    if market == "SH":
        return code.startswith(("600", "601", "603", "605", "688", "689"))
    if market == "SZ":
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if market == "BJ":
        return code.startswith(("4", "8", "9"))
    return False


def symbol_of(market: str, code: str) -> str:
    return f"{code}.{'SS' if market == 'SH' else 'SZ' if market == 'SZ' else 'BJ' if market == 'BJ' else 'HK'}"


def scan_dat_dir(folder: Path) -> tuple[int, int, list[str]]:
    """返回 (DAT 数, 总字节, 文件名列表)。"""
    count, total, names = 0, 0, []
    if not folder.is_dir():
        return count, total, names
    with os.scandir(folder) as it:
        for entry in it:
            if entry.is_file() and entry.name.lower().endswith(".dat"):
                count += 1
                total += entry.stat().st_size
                names.append(entry.name)
    return count, total, names


def sample_names(names: list[str], n: int) -> list[str]:
    if not names or n <= 0:
        return []
    names = sorted(names)
    step = max(1, len(names) // n)
    picked = names[::step][:n]
    if names[-1] not in picked:
        picked[-1] = names[-1]
    return picked


def decode_sample(folder: Path, names: list[str], n: int) -> dict:
    """抽样解码，得到记录数 / 起止时间 / 最新日期分布。"""
    result = {"sampled": 0, "empty": 0, "bars_min": None, "bars_max": None,
              "first_min": None, "last_max": None, "last_dates": [], "samples": []}
    firsts, lasts, bar_counts = [], [], []
    for name in sample_names(names, n):
        try:
            frame = decode_kline_dat(folder / name)
        except Exception as exc:  # 文件写到一半 / 格式异常
            result["samples"].append({"file": name, "error": str(exc)})
            continue
        result["sampled"] += 1
        if frame.empty:
            result["empty"] += 1
            result["samples"].append({"file": name, "bars": 0})
            continue
        first = pd.Timestamp(frame["datetime"].iloc[0])
        last = pd.Timestamp(frame["datetime"].iloc[-1])
        firsts.append(first)
        lasts.append(last)
        bar_counts.append(len(frame))
        result["last_dates"].append(last.strftime("%Y-%m-%d"))
        result["samples"].append({
            "file": name, "bars": len(frame),
            "first": str(first), "last": str(last),
            "last_close": float(frame["close"].iloc[-1]),
        })
    if bar_counts:
        result["bars_min"] = min(bar_counts)
        result["bars_max"] = max(bar_counts)
        result["first_min"] = str(min(firsts))
        result["last_max"] = str(max(lasts))
        result["last_date_counter"] = dict(Counter(result["last_dates"]).most_common(8))
    result.pop("last_dates", None)
    return result


def audit_qmt(root: Path, samples: int) -> dict:
    datadir = root / "datadir" if (root / "datadir").is_dir() else root
    report = {"qmt_root": str(root), "datadir": str(datadir), "exists": datadir.is_dir(), "klines": {},
              "aux": {}, "code_coverage": {}}
    if not report["exists"]:
        return report

    all_a_share: set[str] = set()
    for market in MARKETS:
        market_dir = datadir / market
        if not market_dir.is_dir():
            report["klines"][market] = {"missing": True}
            continue
        periods = sorted([p.name for p in market_dir.iterdir() if p.is_dir()])
        report["klines"][market] = {"periods": {}}
        for period in periods:
            folder = market_dir / period
            count, total, names = scan_dat_dir(folder)
            entry = {
                "label": PERIOD_LABEL.get(period, period),
                "files": count,
                "bytes": total,
                "sample": decode_sample(folder, names, samples) if count else None,
            }
            codes = [Path(n).stem[-6:] for n in names]
            entry["a_share_files"] = sum(1 for c in codes if is_a_share(market, c))
            entry["non_a_share_files"] = count - entry["a_share_files"]
            if entry["label"] == "1d":
                all_a_share.update(symbol_of(market, c) for c in codes if is_a_share(market, c))
            report["klines"][market]["periods"][period] = entry

    # 辅助数据
    for name in ("Finance", "DividData", "Sector", "Industry", "Weight", "increase", "EP", "CC"):
        folder = datadir / name
        if not folder.is_dir():
            report["aux"][name] = {"missing": True}
            continue
        files = list(folder.rglob("*"))
        files = [f for f in files if f.is_file()]
        report["aux"][name] = {"files": len(files), "bytes": sum(f.stat().st_size for f in files)}

    finance = datadir / "Finance"
    if finance.is_dir():
        table_ids = Counter()
        for f in finance.rglob("*.DAT"):
            table_ids[f.stem.rsplit("_", 1)[-1]] += 1
        report["aux"]["Finance"]["table_ids"] = dict(table_ids.most_common(20))
        report["aux"]["Finance"]["sample_names"] = [str(f.relative_to(finance)) for f in list(finance.rglob("*.DAT"))[:5]]

    report["code_coverage"]["a_share_symbols_in_1d"] = len(all_a_share)
    report["qmt_a_share_1d"] = sorted(all_a_share)
    return report


def audit_ptrade(data_root: Path, samples: int) -> dict:
    """检查引擎实际读取的目录，确认接口口径下的现有覆盖。"""
    cn = data_root / "cn" if (data_root / "cn").is_dir() else data_root
    out = {"data_root": str(data_root), "resolved": str(cn), "exists": cn.is_dir(), "folders": {}}
    if not out["exists"]:
        return out
    for folder in ("stocks", "stocks_1m", "stocks_5m", "exrights", "fundamentals", "metadata"):
        p = cn / folder
        out["folders"][folder] = {"exists": p.is_dir(),
                                  "files": len(list(p.glob("*.parquet"))) if p.is_dir() else 0}

    manifest = cn / "manifest.json"
    if manifest.is_file():
        try:
            out["manifest"] = json.loads(manifest.read_text(encoding="utf-8"))
        except Exception:
            pass

    stocks = sorted((cn / "stocks").glob("*.parquet"))
    out["stocks_symbols"] = [p.stem for p in stocks]
    picked = sample_names([p.name for p in stocks], samples)
    tails, heads, cols = [], [], set()
    for name in picked:
        try:
            df = pd.read_parquet(cn / "stocks" / name)
        except Exception:
            continue
        cols.update(df.columns)
        if "date" in df.columns and not df.empty:
            s = pd.to_datetime(df["date"])
            heads.append(str(s.min()))
            tails.append(str(s.max()))
    out["stocks_columns"] = sorted(cols)
    out["stocks_first_min"] = min(heads) if heads else None
    out["stocks_last_max"] = max(tails) if tails else None
    out["stocks_last_tail_counter"] = dict(Counter(tails).most_common(8))

    meta = cn / "metadata" / "stock_metadata.parquet"
    if meta.is_file():
        try:
            out["metadata_rows"] = len(pd.read_parquet(meta, columns=["symbol"]))
        except Exception:
            pass
    return out


def build_verdict(q: dict, p: dict, today: str) -> list[str]:
    lines = []
    lines.append("== 审查结论（按 PTrade 原生回测接口口径）==")
    lines.append("引擎只读 data/cn/stocks (1d) 与 data/cn/stocks_1m (1m)；"
                 "5m/15m/30m/60m 由引擎从 1m bar 聚合，QMT 的 5m DAT 不能充当 1m。")

    def kline(market, period):
        return (q.get("klines", {}).get(market, {}).get("periods", {}).get(period, {}))

    d_sh, d_sz = kline("SH", "86400"), kline("SZ", "86400")
    m_sh, m_sz = kline("SH", "300"), kline("SZ", "300")
    one_sh, one_sz = kline("SH", "60"), kline("SZ", "60")
    lines.append(f"- 日线 1d：SH {d_sh.get('files',0)} / SZ {d_sz.get('files',0)} 个 DAT，"
                 f"其中 A 股 {d_sh.get('a_share_files',0)} + {d_sz.get('a_share_files',0)} 只；"
                 f"抽样最新 {d_sh.get('sample',{}).get('last_max') if d_sh.get('sample') else None} / "
                 f"{d_sz.get('sample',{}).get('last_max') if d_sz.get('sample') else None}（今天 {today}）。")
    lines.append(f"- 5 分钟 5m：SH {m_sh.get('files',0)} / SZ {m_sz.get('files',0)} 个 DAT —— "
                 "已下载，但引擎没有 stocks_5m 目录，不能直接喂给回测接口。")
    lines.append(f"- 1 分钟 1m：SH {one_sh.get('files',0)} / SZ {one_sz.get('files',0)} 个 DAT —— "
                 "几乎为空，分钟级回测（含 5m 及以上聚合周期）暂不可用。")
    for period, label in (("900", "15m"), ("1800", "30m"), ("3600", "60m")):
        has = any(kline(m, period) for m in ("SH", "SZ"))
        lines.append(f"- {label}：{'QMT 有目录但需核对' if has else 'QMT 无对应周期目录，未下载'}。")
    bj_missing = q.get("klines", {}).get("BJ", {}).get("missing", True)
    lines.append(f"- BJ（北交所）：{'目录不存在，未下载' if bj_missing else '目录存在'}。")

    fin = q.get("aux", {}).get("Finance", {})
    div = q.get("aux", {}).get("DividData", {})
    lines.append(f"- 财务 Finance：{fin.get('files',0)} 个 DAT（表编号 {list(fin.get('table_ids',{}))[:6]} ...），"
                 "是 QMT 私有二进制，现有 importer 只认 csv/parquet/xlsx，无法直接入库。")
    lines.append(f"- 除权 DividData：{div.get('files',0)} 个 LevelDB .ldb，同样不是逐只分红表，"
                 "PTrade 的 fq 复权所需 exrights 暂无法从这里补齐。")
    lines.append(f"- 板块 Sector：{q.get('aux',{}).get('Sector',{}).get('files',0)} 个文件；"
                 f"行业 Industry：{q.get('aux',{}).get('Industry',{}).get('files',0)} 个文件（非 PTrade 行情接口范畴）。")

    ptrade_last = p.get("stocks_last_max")
    lines.append(f"- 现有 PTrade 数据：stocks {p.get('folders',{}).get('stocks',{}).get('files',0)} 只，"
                 f"抽样最新 {ptrade_last}；stocks_1m {p.get('folders',{}).get('stocks_1m',{}).get('files',0)} 只。")

    qmt_syms = set(q.get("qmt_a_share_1d", []))
    ptrade_syms = set(p.get("stocks_symbols", []))
    new_syms = qmt_syms - ptrade_syms
    only_ptrade = ptrade_syms - qmt_syms
    lines.append(f"- 标的对齐：QMT 日线 A 股 {len(qmt_syms)} 只，PTrade 已有 {len(ptrade_syms)} 只；"
                 f"QMT 独有 {len(new_syms)} 只（示例 {sorted(new_syms)[:5]}），"
                 f"PTrade 独有 {len(only_ptrade)} 只（示例 {sorted(only_ptrade)[:5]}）。")
    return lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="只读审查 QMT 本地行情下载完备度与 PTrade 数据对齐")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="QMT 客户端安装目录")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA, help="PTrade 引擎 data 目录")
    parser.add_argument("--samples", type=int, default=12, help="每个周期目录抽样解码文件数")
    parser.add_argument("--json", type=Path, default=None, help="把完整报告另存为 JSON")
    args = parser.parse_args(argv)

    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    q = audit_qmt(args.root, args.samples)
    p = audit_ptrade(args.data, args.samples)
    verdict = build_verdict(q, p, today)

    print(f"QMT 数据目录：{q['datadir']}（存在={q['exists']}）")
    print(f"PTrade 数据目录：{p['resolved']}（存在={p['exists']}）\n")
    for market, info in q.get("klines", {}).items():
        if info.get("missing"):
            print(f"  {market}: 目录不存在（未下载）")
            continue
        for period, entry in info["periods"].items():
            s = entry.get("sample") or {}
            print("  {:<2}/{:<6} {:<5} 文件 {:>6}  A股 {:>6}  抽样 {:>2} 根数 {}-{}  末日 {}".format(
                market, period, entry["label"], entry["files"], entry["a_share_files"],
                s.get("sampled", 0), s.get("bars_min"), s.get("bars_max"), s.get("last_max")))
    print("\n辅助数据：", json.dumps(q.get("aux", {}), ensure_ascii=False)[:1200])
    print("\n" + "\n".join(verdict))

    if args.json:
        payload = {"generated_at": str(pd.Timestamp.now()), "qmt": q, "ptrade": p, "verdict": verdict}
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\n完整报告已写入 {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
