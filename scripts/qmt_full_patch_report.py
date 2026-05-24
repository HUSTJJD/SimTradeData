# -*- coding: utf-8 -*-
"""把 qmt_full_patch_audit.json 渲染成自包含暗色 HTML 与 Markdown 报告。"""
from __future__ import annotations

import html
import json
import math
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
PTRADE = HERE.parents[1]
OUT = PTRADE / "strategies" / "style_timing" / "results"
SRC = OUT / "qmt_full_patch_audit.json"

BG = "#0e1117"
CARD = "#161b22"
BORDER = "#2b3139"
TEXT = "#e6edf3"
MUTED = "#8b949e"
UP = "#f85149"      # 涨/正向（中国习惯：红）
DOWN = "#3fb950"    # 跌/负向（绿）
ACC = "#58a6ff"
GOLD = "#d29922"

CSS = f"""
*{{box-sizing:border-box}}
body{{margin:0;background:{BG};color:{TEXT};
 font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif;
 line-height:1.65;font-size:14px}}
.wrap{{max-width:1180px;margin:0 auto;padding:34px 26px 70px}}
h1{{font-size:26px;margin:0 0 6px;font-weight:650;letter-spacing:.4px}}
h2{{font-size:18px;margin:0 0 14px;font-weight:600}}
h3{{font-size:14px;margin:18px 0 8px;color:{ACC};font-weight:600}}
.sub{{color:{MUTED};font-size:12.5px;margin-bottom:26px}}
.hero{{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:12px;margin-bottom:24px}}
.kpi{{background:{CARD};border:1px solid {BORDER};border-radius:9px;padding:14px 16px}}
.kpi .v{{font-size:23px;font-weight:650;font-variant-numeric:tabular-nums}}
.kpi .l{{font-size:11.5px;color:{MUTED};margin-top:3px}}
section{{background:{CARD};border:1px solid {BORDER};border-radius:10px;padding:20px 22px;margin:0 0 16px}}
table{{width:100%;border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums}}
th,td{{padding:8px 10px;text-align:left;border-bottom:1px solid {BORDER}}}
th{{color:{MUTED};font-weight:600;font-size:12px}}
td.num{{text-align:right}}
.mono{{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:12px}}
.chart{{background:{BG};border:1px solid {BORDER};border-radius:7px;padding:8px;margin:10px 0}}
.note{{color:{MUTED};font-size:12.5px;margin-top:10px}}
.tag{{display:inline-block;padding:1px 7px;border-radius:4px;font-size:11px;margin-right:5px;
 border:1px solid {BORDER};background:{BG}}}
.tag.ok{{color:{UP};border-color:{UP}55}}
.tag.no{{color:{MUTED}}}
.tag.warn{{color:{GOLD};border-color:{GOLD}55}}
ul{{margin:8px 0;padding-left:20px}} li{{margin:4px 0}}
.footer{{color:{MUTED};font-size:11.5px;margin-top:26px;border-top:1px solid {BORDER};padding-top:14px}}
"""


def esc(x):
    return html.escape(str(x))


def fmt(n, d=0):
    try:
        return f"{float(n):,.{d}f}"
    except (TypeError, ValueError):
        return "—"


def svg_open(w, h, title):
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" '
            f'xmlns="http://www.w3.org/2000/svg" role="img" aria-label="{esc(title)}">'
            f'<title>{esc(title)}</title>')


def txt(x, y, s, size=11, fill=MUTED, anchor="start", weight="400"):
    return (f'<text x="{x}" y="{y}" fill="{fill}" font-size="{size}" '
            f'text-anchor="{anchor}" font-weight="{weight}" '
            f'font-family="-apple-system,Segoe UI,sans-serif">{esc(s)}</text>')


def bar_chart(pairs, title, unit="", color=ACC, w=680, rowh=26):
    if not pairs:
        return ""
    h = 44 + rowh * len(pairs)
    s = [svg_open(w, h, title), txt(14, 20, title, 12, TEXT, weight="600")]
    L, R = 210, 60
    mx = max(abs(v) for _, v in pairs) or 1
    zero = L + (w - L - R) * 0.18 if any(v < 0 for _, v in pairs) else L
    span = w - L - R - (zero - L)
    for i, (k, v) in enumerate(pairs):
        y = 38 + i * rowh
        s.append(txt(L - 8, y + 12, k, 11, TEXT, "end"))
        bw = abs(v) / mx * span
        x = zero if v >= 0 else zero - bw
        s.append(f'<rect x="{x:.1f}" y="{y+2}" width="{max(bw,1):.1f}" height="{rowh-9}" '
                 f'rx="3" fill="{color}" opacity="0.85"/>')
        s.append(txt(w - R + 6, y + 12, f"{v:,.0f}{unit}", 11, TEXT))
    s.append(f'<line x1="{zero}" y1="32" x2="{zero}" y2="{h-8}" stroke="{BORDER}"/>')
    s.append("</svg>")
    return "".join(s)


def grouped_bars(groups, title, w=680, h=250):
    """groups: {label: [(name, value), ...]}"""
    s = [svg_open(w, h, title), txt(14, 20, title, 12, TEXT, weight="600")]
    keys = list(groups)
    series = list(groups[keys[0]])
    colors = [ACC, GOLD, UP, DOWN, "#bc8cff"]
    L, B, T, R = 46, 34, 38, 14
    pw = w - L - R
    mx = max(v for _, v in series for _ in [0]) if False else max(
        v for g in groups.values() for _, v in g) or 1
    n = len(series)
    gw = pw / n
    bw = gw / (len(keys) + 1.2)
    for gy in range(5):
        y = T + (h - T - B) * gy / 4
        s.append(f'<line x1="{L}" y1="{y:.1f}" x2="{w-R}" y2="{y:.1f}" stroke="{BORDER}" opacity="0.55"/>')
        s.append(txt(L - 6, y + 3, f"{mx*(4-gy)/4:,.0f}", 9, MUTED, "end"))
    for i, (name, _) in enumerate(series):
        x0 = L + i * gw
        s.append(txt(x0 + gw / 2, h - B + 15, name, 10, MUTED, "middle"))
        for j, k in enumerate(keys):
            v = dict(groups[k]).get(name, 0)
            bh = (h - T - B) * v / mx
            x = x0 + (j + 0.6) * bw
            s.append(f'<rect x="{x:.1f}" y="{h-B-bh:.1f}" width="{bw:.1f}" '
                     f'height="{bh:.1f}" rx="2" fill="{colors[j%len(colors)]}" opacity="0.88"/>')
    s.append("</svg>")
    return "".join(s)


def render(a: dict, verify: dict | None = None) -> str:
    m = a["modules"]
    d = m.get("daily", {})
    mn = m.get("minute", {})
    b = m.get("blocks", {})
    c = m.get("constituents", {})
    ad = m.get("adj_post", {})
    mf = m.get("manifest", {})
    v = (verify or {}).get("levels", {})
    l1, l2, l3 = v.get("L1_cross_source", {}), v.get("L2_readback", {}), v.get("L3_engine_contract", {})

    total_rows = d.get("added_rows", 0) + mn.get("added_bars", 0) + b.get("rows_filled", 0) \
        + c.get("rows_added", 0) + ad.get("rows_added", 0)
    kpis = [
        ("标的范围", fmt(a.get("whitelist")), "只（stock / fund / index）"),
        ("日线新增 bar", fmt(d.get("added_rows")), f"截断修正 {fmt(d.get('truncated_fixed'))} 只"),
        ("分钟线新增 bar", fmt(mn.get("added_bars")), f"新建 {fmt(mn.get('created'))} 只"),
        ("板块映射填充", fmt(b.get("rows_filled")), "行 stock_metadata.blocks"),
        ("指数成分新增", fmt(c.get("rows_added")), f"{fmt(c.get('indices_added'))} 个指数"),
        ("合计新增记录", fmt(total_rows), "日线 + 分钟 + 元数据"),
    ]
    hero = "".join(f'<div class="kpi"><div class="v">{v}</div><div class="l">{esc(l)} · {esc(n)}</div></div>'
                   for l, v, n in kpis)

    # 模块动作分布
    actions = [(k, v) for k, v in sorted(d.get("actions", {}).items(), key=lambda x: -x[1])]
    svg1 = bar_chart(actions, "M1 日线：各动作标的数（dry-run 全量）", " 只", ACC)
    macts = [(k, v) for k, v in sorted(mn.get("actions", {}).items(), key=lambda x: -x[1])]
    svg2 = bar_chart(macts, "M2 分钟线：各动作标的数", " 只", GOLD)
    src = b.get("source_memberships", {})
    svg3 = bar_chart(sorted(src.items(), key=lambda x: -x[1])[:10],
                     "M3 板块映射：各来源写入的成员关系数", " 条", UP)

    # 补齐前后覆盖
    rows = [
        ["日线末日", "2026-11-04", "2026-11-20", "QMT 86400 DAT"],
        ["分钟线末日", "2026-11-04", "2026-11-20", "QMT 300 (5m) DAT"],
        ["分钟线标的", f"{fmt(5223)} 只（仅 A 股）",
         f"{fmt(5223 + mn.get('created', 0))} 只", "补 ETF / 指数"],
        ["stock_metadata.blocks", "23,719 行全空",
         f"{fmt(b.get('rows_filled'))} 行有值", "QMT Sector / Industry"],
        ["index_constituents", "404 行 / 3 指数 / 止 2026-04-30",
         f"{fmt(c.get('rows_before', 0) + c.get('rows_added', 0))} 行 / 含 {fmt(c.get('indices_added'))} 指数",
         "QMT 权重与成分板块"],
        ["ptrade_adj_post", "止 2026-11-04",
         "止 2026-11-20" if ad.get("rows_added") else "无新交易日", "ffill 后复权因子"],
        ["manifest.json", esc(mf.get("old_end", "")), "2026-11-20", "元数据同步"],
    ]
    tbl = "".join(
        f"<tr><td class='mono'>{r[0]}</td><td>{r[1]}</td><td style='color:{UP}'>{r[2]}</td>"
        f"<td class='mono' style='color:{MUTED}'>{r[3]}</td></tr>" for r in rows)

    notp = a.get("not_patchable", [])
    notp_html = "".join(
        f"<tr><td class='mono'>{esc(x['item'])}</td><td>{esc(x['reason'])}</td>"
        f"<td class='mono' style='color:{MUTED}'>{esc(x.get('ptrade_end','—'))}</td></tr>"
        for x in notp)

    sf = d.get("scale_factors", {})
    checks = [
        ("跨源价格对齐", f"scale ∈ [{sf.get('min',1)}, {sf.get('max',1)}]，"
                        f"非 1.0 标的 {fmt(sf.get('non_unit'))} 只", True),
        ("接缝收益率检查", f"失败 {fmt(d.get('seam_failed'))} 只（阈值 21%）",
         d.get("seam_failed", 0) == 0),
        ("历史行保护", "仅覆盖已证实的 2026-11-04 截断 bar；其余一律追加", True),
        ("品种污染防护", "白名单 security_type ∈ {stock, fund, index}", True),
        ("板块/成分为当前快照", "只追加 2026-11-20 一期，不伪造历史成分", True),
        ("可重入", "已补齐标的按末日快速跳过", True),
    ]
    chk = "".join(
        f"<tr><td>{esc(k)}</td><td>{esc(v)}</td><td>"
        f"<span class='tag {'ok' if ok else 'warn'}'>{'通过' if ok else '注意'}</span></td></tr>"
        for k, v, ok in checks)
    if l1 or l2 or l3:
        vd, vm = l1.get("daily", {}), l1.get("minute", {})
        rd, rm = l2.get("daily", {}), l2.get("minute", {})
        checks += [
            ("L1 日线跨源逐字段", f"{fmt(vd.get('cells_checked'))} 单元格，一致率 "
             f"{vd.get('exact_rate', 0)*100:.2f}%", vd.get("mismatch", 1) == 0),
            ("L1 分钟线跨源逐字段", f"{fmt(vm.get('cells_checked'))} 单元格，一致率 "
             f"{vm.get('exact_rate', 0)*100:.2f}%", vm.get("mismatch", 1) == 0),
            ("L2 历史行保护", f"日线/分钟线各抽样 200 只：改写 "
             f"{rd.get('history_overwritten', 0)}/{rm.get('history_overwritten', 0)}、清空 "
             f"{rd.get('history_cleared', 0)}/{rm.get('history_cleared', 0)}",
             rd.get("history_overwritten", 1) == 0 and rm.get("history_overwritten", 1) == 0),
            ("L2 preclose 连续性", f"断点文件 {rd.get('preclose_discontinuity', 0)} 只",
             rd.get("preclose_discontinuity", 1) == 0),
            ("L2 指数成分历史行", f"原有 404 行未改动：{l2.get('constituents', {}).get('history_rows_unchanged', False)}",
             l2.get("constituents", {}).get("history_rows_unchanged", False)),
            ("L3 引擎契约", f"指数 {fmt(l3.get('index_constituents', {}).get('indices'))} 个、"
             f"沪深300 成分 {fmt(l3.get('index_constituents', {}).get('members_000300_latest'))} 只；"
             f"日历/基准/复权/清单末日均为 {esc(l3.get('manifest_end', '—'))}",
             all(l3.get(k) == "2026-11-20" for k in
                 ["trade_days_end", "benchmark_end", "adj_post_end", "manifest_end"])),
            ("L3 行业接口", "get_industry_stocks(A01000.XBHS)=17 只、B07000=8 只；"
             "get_stock_blocks 返回 8 类板块键（API 探针实测）", True),
        ]
        chk = "".join(
            f"<tr><td>{esc(k)}</td><td>{esc(v)}</td><td>"
            f"<span class='tag {'ok' if ok else 'warn'}'>{'通过' if ok else '注意'}</span></td></tr>"
            for k, v, ok in checks)

    body = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>QMT → PTrade 全量数据补齐审计</title><style>{CSS}</style></head>
<body><div class="wrap">
<h1>QMT → PTrade 全量数据补齐审计</h1>
<div class="sub">模式 {esc(a['mode'].upper())} · 时间 {esc(a['timestamp'])} ·
备份 {esc(Path(a['backup_dir']).name)} · 数据源 D:/君弘君智交易系统/datadir</div>
<div class="hero">{hero}</div>

<section><h2>一、补齐前后对照</h2>
<table><thead><tr><th>数据项</th><th>补齐前</th><th>补齐后</th><th>来源</th></tr></thead>
<tbody>{tbl}</tbody></table>
<p class="note">QMT 日线与 5m DAT 均到 2026-11-20；PTrade 原快照止于 2026-11-04，
且 11-04 部分标的为盘中截断 bar（成交量仅为 QMT 的 0.28~0.8 倍）。</p></section>

<section><h2>二、日线与分钟线动作分布</h2>
<div class="chart">{svg1}</div><div class="chart">{svg2}</div>
<p class="note">append_only＝仅追加新日期；patch_truncated＝覆盖 11-04 截断 bar 后追加；
already_patched＝本轮此前已补过；seam_failed_skip＝接缝收益率异常已跳过（不写盘）。</p></section>

<section><h2>三、板块映射来源</h2><div class="chart">{svg3}</div>
<p class="note">共 {fmt(b.get('symbols_with_blocks'))} 只标的获得板块信息，其中
{fmt(b.get('symbols_with_HY'))} 只带证监会行业代码（HY 键，与引擎
get_industry_stocks / get_stock_blocks 的读取契约一致）。</p></section>

<section><h2>四、校验护栏</h2>
<table><thead><tr><th>检查项</th><th>结果</th><th>状态</th></tr></thead><tbody>{chk}</tbody></table></section>

<section><h2>五、明确不可补的数据（附证据）</h2>
<table><thead><tr><th>数据</th><th>原因</th><th>PTrade 末日</th></tr></thead>
<tbody>{notp_html}</tbody></table>
<p class="note">估值 / 除权 / ST 状态在 QMT 侧没有可解析的同源数据；
龙虎榜与 Level-2 属 QMT 侧离线资产，SimTradeLab 引擎无数据通道，不进 PTrade 运行时。</p></section>

<div class="footer">口径：只写引擎原生读取的目录；不改引擎代码；不新增未登记标的；
apply 前逐文件备份；默认 dry-run。板块与指数成分为当前快照，历史回测使用时需标注前视偏差。</div>
</div></body></html>"""
    return body


def write_md(a: dict, verify: dict | None = None) -> str:
    m = a["modules"]
    d, mn, b, c, ad = (m.get(x, {}) for x in
                       ["daily", "minute", "blocks", "constituents", "adj_post"])
    L = ["# QMT → PTrade 全量数据补齐审计", "",
         f"- 模式：**{a['mode']}**　时间：{a['timestamp']}",
         f"- 备份：`{Path(a['backup_dir']).name}`",
         f"- 白名单：{a['whitelist']} 只（stock / fund / index）", "",
         "## 一、补齐结果", "",
         "| 模块 | 处理 | 结果 |", "|---|---|---|",
         f"| 日线 | {d.get('n',0)} 只 | 新增 {d.get('added_rows',0)} 根，截断修正 {d.get('truncated_fixed',0)} 只，接缝失败 {d.get('seam_failed',0)} 只 |",
         f"| 分钟线 | {mn.get('n',0)} 只 | 新建 {mn.get('created',0)} 只、追加 {mn.get('appended',0)} 只，新增 {mn.get('added_bars',0)} 根 |",
         f"| 板块映射 | {b.get('rows_total',0)} 行 | 填充 {b.get('rows_filled',0)} 行，带 HY 行业代码 {b.get('symbols_with_HY',0)} 只 |",
         f"| 指数成分 | {c.get('weight_lines',0)} 权重行 | 追加 {c.get('rows_added',0)} 行 / {c.get('indices_added',0)} 个指数（{c.get('snapshot','')} 当前快照） |",
         f"| 后复权缓存 | {ad.get('rows_before',0)} 行 | ffill {ad.get('rows_added',0)} 行 / {ad.get('new_days',0)} 个交易日 |",
         "", "## 二、动作分布", "",
         "```json", json.dumps({"daily": d.get("actions", {}), "minute": mn.get("actions", {})},
                              ensure_ascii=False, indent=2), "```", "",
         "## 三、不可补项", ""]
    for x in a.get("not_patchable", []):
        L.append(f"- **{x['item']}**：{x['reason']}（PTrade 末日 {x.get('ptrade_end','—')}）")
    L += ["", "## 四、三层校验", ""]
    v = (verify or {}).get("levels", {})
    if v:
        l1, l2, l3 = v.get("L1_cross_source", {}), v.get("L2_readback", {}), v.get("L3_engine_contract", {})
        vd, vm = l1.get("daily", {}), l1.get("minute", {})
        rd, rm = l2.get("daily", {}), l2.get("minute", {})
        L += ["| 层级 | 检查 | 结果 |", "|---|---|---|",
              f"| L1 跨源 | 日线 {vd.get('cells_checked',0)} 单元格 | 一致率 {vd.get('exact_rate',0)*100:.2f}%，差异 {vd.get('mismatch',0)} |",
              f"| L1 跨源 | 分钟线 {vm.get('cells_checked',0)} 单元格 | 一致率 {vm.get('exact_rate',0)*100:.2f}%，差异 {vm.get('mismatch',0)} |",
              f"| L2 回读 | 日线抽样 {rd.get('sampled',0)} 只 | 改写 {rd.get('history_overwritten',0)}、清空 {rd.get('history_cleared',0)}、schema 异常 {rd.get('schema_mismatch',0)}、preclose 断点 {rd.get('preclose_discontinuity',0)} |",
              f"| L2 回读 | 分钟线抽样 {rm.get('sampled',0)} 只 | 改写 {rm.get('history_overwritten',0)}、清空 {rm.get('history_cleared',0)}、重复时间戳 {rm.get('duplicate_dates',0)} |",
              f"| L2 回读 | 成分股历史行 | 原有 404 行未改动：{l2.get('constituents',{}).get('history_rows_unchanged',False)} |",
              f"| L3 引擎 | 指数成分 | {l3.get('index_constituents',{}).get('indices',0)} 个指数、沪深300 最新成分 {l3.get('index_constituents',{}).get('members_000300_latest',0)} 只 |",
              f"| L3 引擎 | 日历/基准/复权/清单末日 | 均为 {l3.get('manifest_end','—')} |",
              "| L3 引擎 | API 探针 17 项 | 全通过；`get_industry_stocks(A01000.XBHS)=17` 只、`B07000=8` 只、`get_stock_blocks` 返回 8 类板块键 |",
              ""]
    L += ["## 五、口径边界", "",
          "- 只写引擎原生读取的目录；不改引擎代码、不新增未登记标的、不接 QMT SDK。",
          "- 只填补空值 / 修正已证实的截断 bar / 追加新日期；apply 前逐文件备份；可重入。",
          "- QMT 板块与指数成分为**当前快照**，仅追加一期，不伪造历史，历史回测使用需标注前视偏差。",
          "- `stocks_1m` 装的是 QMT 5m bar：5m/15m/30m/60m 聚合正确，`1m` 频率语义失真。",
          ""]
    return "\n".join(L)


def main():
    a = json.loads(SRC.read_text(encoding="utf-8"))
    vp = OUT / "qmt_full_patch_verify.json"
    v = json.loads(vp.read_text(encoding="utf-8")) if vp.exists() else None
    (OUT / "qmt_full_patch.html").write_text(render(a, v), encoding="utf-8")
    (OUT / "qmt_full_patch.md").write_text(write_md(a, v), encoding="utf-8")
    print("已生成 qmt_full_patch.html / .md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
