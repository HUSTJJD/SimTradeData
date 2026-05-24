# -*- coding: utf-8 -*-
"""把 qmt_finance_patch_audit.json 渲染成自包含暗色 HTML（内联 SVG，无外部依赖）。"""
from __future__ import annotations

import html
import json
from pathlib import Path

BG, PANEL, BORDER = "#0d1117", "#161b22", "#30363d"
TEXT, MUTED = "#e6edf3", "#8b949e"
UP, DOWN, ACCENT, WARN, GRID = "#f85149", "#3fb950", "#58a6ff", "#d29922", "#21262d"
STYLE = Path(r"D:\learning\QuantificationResearch\PTrade\strategies\style_timing\results")


def esc(s):
    return html.escape(str(s), quote=True)


def svg_grouped_bars(labels, before, after, title, unit="%"):
    """覆盖率前/后分组柱状。after 为 None 时只画 before（dry-run）。"""
    w, h = 1100, 320
    L, R, T, B = 74, 150, 54, 56
    vals = [v for v in before + [x for x in after if x is not None] if v is not None]
    hi = max(vals + [0.1]) * 1.15
    Y0 = h - B
    s = [f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" role="img" '
         f'aria-label="{esc(title)}" style="font-family:ui-monospace,Menlo,monospace">',
         f'<rect width="{w}" height="{h}" fill="{PANEL}" rx="8"/>',
         f'<text x="24" y="28" font-size="13" fill="{TEXT}" font-weight="600">{esc(title)}</text>']
    for i in range(0, 6):
        v = hi * i / 5
        Y = Y0 - v / hi * (Y0 - T)
        s.append(f'<line x1="{L}" y1="{Y:.1f}" x2="{w-R}" y2="{Y:.1f}" stroke="{GRID}"/>')
        s.append(f'<text x="{L-8}" y="{Y+3:.1f}" font-size="9" fill="{MUTED}" '
                 f'text-anchor="end">{v*100:.0f}{unit}</text>')
    s.append(f'<line x1="{L}" y1="{Y0}" x2="{w-R}" y2="{Y0}" stroke="{MUTED}"/>')
    gw = (w - L - R) / max(1, len(labels))
    for i, lab in enumerate(labels):
        x0 = L + i * gw
        b = before[i] if i < len(before) and before[i] is not None else 0.0
        a = after[i] if i < len(after) and after[i] is not None else None
        bw = gw * (0.30 if a is not None else 0.44)
        hb = b / hi * (Y0 - T)
        s.append(f'<rect x="{x0+gw*0.10:.1f}" y="{Y0-hb:.1f}" width="{bw:.1f}" '
                 f'height="{max(1,hb):.1f}" fill="{MUTED}" opacity=".75" rx="2"/>')
        s.append(f'<text x="{x0+gw*0.10+bw/2:.1f}" y="{Y0-hb-5:.1f}" font-size="9" '
                 f'fill="{MUTED}" text-anchor="middle">{b*100:.1f}</text>')
        if a is not None:
            ha = a / hi * (Y0 - T)
            xa = x0 + gw * 0.52
            s.append(f'<rect x="{xa:.1f}" y="{Y0-ha:.1f}" width="{bw:.1f}" '
                     f'height="{max(1,ha):.1f}" fill="{ACCENT}" opacity=".9" rx="2"/>')
            s.append(f'<text x="{xa+bw/2:.1f}" y="{Y0-ha-5:.1f}" font-size="9" '
                     f'fill="{ACCENT}" text-anchor="middle">{a*100:.1f}</text>')
            s.append(f'<text x="{x0+gw/2:.1f}" y="{Y0+16:.1f}" font-size="9.5" '
                     f'fill="{UP if a>b else MUTED}" text-anchor="middle">+{(a-b)*100:.1f}pp</text>')
        s.append(f'<text x="{x0+gw/2:.1f}" y="{Y0+34:.1f}" font-size="9" fill="{MUTED}" '
                 f'text-anchor="middle">{esc(lab)}</text>')
    s.append(f'<rect x="{w-R+10}" y="{T}" width="12" height="12" fill="{MUTED}" rx="2"/>')
    s.append(f'<text x="{w-R+26}" y="{T+10}" font-size="10" fill="{TEXT}">补齐前</text>')
    if any(x is not None for x in after):
        s.append(f'<rect x="{w-R+10}" y="{T+22}" width="12" height="12" fill="{ACCENT}" rx="2"/>')
        s.append(f'<text x="{w-R+26}" y="{T+32}" font-size="10" fill="{TEXT}">补齐后</text>')
    s.append("</svg>")
    return "".join(s)


def svg_simple_bars(data, title, color=ACCENT, pct=True, h=260):
    if not data:
        return ""
    w = 1100
    L, R, T, B = 250, 40, 52, 44
    keys = list(data)
    vals = [float(data[k]) for k in keys]
    hi = max(vals + [0.001]) * 1.15
    s = [f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" role="img" '
         f'aria-label="{esc(title)}" style="font-family:ui-monospace,Menlo,monospace">',
         f'<rect width="{w}" height="{h}" fill="{PANEL}" rx="8"/>',
         f'<text x="24" y="28" font-size="13" fill="{TEXT}" font-weight="600">{esc(title)}</text>']
    rowh = (h - T - B) / max(1, len(keys))
    for i, (k, v) in enumerate(zip(keys, vals)):
        y = T + i * rowh
        bw = v / hi * (w - L - R)
        s.append(f'<text x="{L-10}" y="{y+rowh*0.62:.1f}" font-size="10.5" fill="{TEXT}" '
                 f'text-anchor="end">{esc(k)}</text>')
        s.append(f'<rect x="{L}" y="{y+rowh*0.18:.1f}" width="{max(1,bw):.1f}" '
                 f'height="{rowh*0.55:.1f}" fill="{color}" opacity=".85" rx="2"/>')
        lab = f"{v*100:.2f}%" if pct else f"{v:,.0f}"
        s.append(f'<text x="{L+bw+8:.1f}" y="{y+rowh*0.62:.1f}" font-size="10" '
                 f'fill="{color}">{lab}</text>')
    s.append("</svg>")
    return "".join(s)


def render(audit):
    fields = ["publ_date"] + list(audit["design"]["value_map"])
    before = [audit["coverage_before"].get(f) for f in fields]
    after_all = audit.get("coverage_after") or {}
    after = [after_all.get(f) for f in fields]

    vm = audit["design"]["value_map"]
    conf = {k: v["exact_rate"] for k, v in vm.items()}
    psrc = audit["design"]["publ_date_sources"]
    er = psrc.get("exact_rate", {})
    conf["publ_date (7005 公告日)"] = float(er.get("7005", er.get(7005, 0.9978)))
    filled = audit["filled_by_field"]

    kpis = [
        ("模式", audit["mode"], "默认 dry-run，--apply 才落盘"),
        ("处理标的", str(audit["n_symbols"]), f"有变更 {audit['n_changed']} 只"),
        ("实际写入", str(audit["n_written"]), f"错误 {audit['n_errors']} 只"),
        ("填充单元格", f"{sum(filled.values()):,}",
         f"冲突 {audit['conflicts']} / PIT 拒绝 {audit.get('pit_rejected', 0)}"),
        ("publ_date 覆盖", f"{before[0]*100:.1f}%",
         f"补齐后 {(after[0] or before[0])*100:.1f}%"),
        ("锚定字段", str(len(vm) + 1), "含 publ_date；不猜字段"),
    ]
    card = "".join(f'<div class="kpi"><div class="k-label">{esc(k)}</div>'
                   f'<div class="k-val">{esc(v)}</div><div class="k-sub">{esc(s)}</div></div>'
                   for k, v, s in kpis)

    rows = []
    for k, v in vm.items():
        rows.append(f"<tr><td class='mono'>{esc(k)}</td><td class='mono'>{v['qmt_table']}[{v['qmt_col']}]</td>"
                    f"<td class='num'>{v['exact_rate']:.3f}</td>"
                    f"<td class='num'>{filled.get(k,0):,}</td></tr>")
    rows.append(f"<tr><td class='mono'>publ_date</td><td class='mono'>7005 公告日 (UTC+8)</td>"
                f"<td class='num'>0.9978</td><td class='num'>{filled.get('publ_date',0):,}</td></tr>")

    excluded = audit["design"]["excluded"]
    excl_html = "".join(f"<li>{esc(x)}</li>" for x in excluded)

    doc = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>QMT → PTrade 财务数据补齐审计</title>
<style>
:root{{--bg:{BG};--panel:{PANEL};--border:{BORDER};--text:{TEXT};--muted:{MUTED};
--up:{UP};--down:{DOWN};--accent:{ACCENT};--warn:{WARN}}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--text);line-height:1.6;
font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}}
.wrap{{max-width:1200px;margin:0 auto;padding:36px 24px 64px}}
h1{{font-size:26px;margin:0 0 6px}}h2{{font-size:15px;margin:0 0 14px;font-weight:600}}
.sub{{color:var(--muted);font-size:13px;margin-bottom:26px}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:30px}}
.kpi{{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
.k-label{{font-size:11px;color:var(--muted)}}.k-val{{font-size:22px;font-weight:650;margin:6px 0 2px}}
.k-sub{{font-size:10.5px;color:var(--muted)}}
section{{background:var(--panel);border:1px solid var(--border);border-radius:12px;
padding:18px 18px 14px;margin-bottom:20px}}
.chart{{width:100%;overflow-x:auto}}.note{{font-size:12px;color:var(--muted);margin:10px 2px 0}}
.tbl{{width:100%;border-collapse:collapse;font-size:12px}}
.tbl th{{text-align:left;color:var(--muted);font-weight:500;padding:8px 6px;border-bottom:1px solid var(--border)}}
.tbl td{{padding:7px 6px;border-bottom:1px solid {GRID}}}
.tbl tr:last-child td{{border-bottom:none}}
.mono{{font-family:ui-monospace,Menlo,monospace;font-size:11px}}.num{{text-align:right}}
.concl{{border-left:3px solid var(--accent)}}.concl p{{font-size:13px;margin:0 0 12px}}
.concl p:last-child{{margin-bottom:0}}
.warn{{border-left:3px solid var(--warn)}}
ul{{margin:6px 0 0 18px;padding:0;color:var(--muted);font-size:12px}}
li{{margin:4px 0}}
footer{{color:var(--muted);font-size:11px;text-align:center;margin-top:28px}}
</style></head><body><div class="wrap">
<h1>QMT → PTrade 财务数据补齐审计</h1>
<div class="sub">{esc(audit['generated'])} · 抽样 {audit['sample_for_coverage']} 只 ·
只填补空值、不覆盖已有值 · 全部字段经数值锚定</div>
<div class="kpis">{card}</div>
<section class="concl"><h2>结论摘要</h2>
<p><b>补齐了什么：</b>从 QMT Finance DAT 中经数值锚定确认的
<b>4 个数值列</b>与<b>不依赖列语义的 publ_date 公告日</b>，填补 PTrade
<code>fundamentals/</code> 的空值。共填充 <b>{sum(filled.values()):,}</b> 个单元格，
冲突 {audit['conflicts']} 处全部跳过，未覆盖任何已有值。</p>
<p><b>为什么不补更多：</b>QMT 财务 DAT 无字段字典，项目既有探针明确判定
「能读但不建议当数据入口」。7008 每股指标与 PTrade 的 roe/增长率
Spearman 0.98~0.99，但精确率仅 0.37~0.67（同经济量、不同口径），
直接导入会污染 <code>get_fundamentals</code>；因此只导入精确一致的列。</p>
<p><b>关键口径：</b>QMT 毫秒戳是北京时间，须 UTC+8；否则公告日系统性差 1 天
（实测修正后 7005 与 PTrade publ_date 有 <b>99.78%</b> 完全相等）。</p></section>
<section><h2>一 · 覆盖率提升（抽样 {audit['sample_for_coverage']} 只）</h2>
<div class="chart">{svg_grouped_bars(fields, before, after, '字段非空覆盖率：补齐前 vs 补齐后')}</div>
<p class="note">{'dry-run 未写盘，仅显示补齐前覆盖率；执行 --apply 后才会出现补齐后柱。' if not audit.get('coverage_after') else '补齐后为实际回读统计。'}</p></section>
<section><h2>二 · 锚定精确一致率（只导入 ≥0.90 的列）</h2>
<div class="chart">{svg_simple_bars(conf, 'QMT 列与 PTrade 字段的精确一致率（全表 60 只样本）', color=ACCENT)}</div>
<p class="note">判据：精确率 ≥0.90 且相对次优候选有明显分差；固定比例 1/0.01/100 等已测试，
未发现单位差异。7008 的 roe / 增长率精确率 0.37~0.67，未进入导入。</p></section>
<section><h2>三 · 填充单元格数</h2>
<div class="chart">{svg_simple_bars({k: float(v) for k,v in filled.items()},
  '各字段实际填充的单元格数', color=WARN, pct=False)}</div></section>
<section><h2>四 · 字段映射与填充明细</h2>
<div class="chart"><table class="tbl">
<tr><th>PTrade 字段</th><th>QMT 来源</th><th>精确一致率</th><th>填充数</th></tr>
{''.join(rows)}</table></div>
<p class="note">7003 = 现金流量表（112 数值列），7004 = 股本结构（5 数值列），
7005 = 股东户数（6 数值列）。记录长度由文件字节数整除与时间戳间隔实测确定。</p></section>
<section class="warn"><h2>五 · 明确不导入的部分与边界</h2><ul>{excl_html}</ul>
<p class="note">原则：{esc(' / '.join(audit['design']['principles']))}。</p>
<p class="note">时间口径：{esc(audit['design']['time_rule'])}。
备份：{esc(audit.get('backup_dir') or '（dry-run 无备份）')}。</p></section>
<footer>脚本：qmt_finance_anchor.py / probe_qmt_finance_full.py（归档快照） /
import_qmt_finance_to_ptrade.py · 审计：qmt_finance_patch_audit.json / .md ·
数据源：D:/君弘君智交易系统/datadir/Finance</footer>
</div></body></html>"""
    (STYLE / "qmt_finance_patch.html").write_text(doc, encoding="utf-8")
    return STYLE / "qmt_finance_patch.html"


if __name__ == "__main__":
    with open(STYLE / "qmt_finance_patch_audit.json", encoding="utf-8") as f:
        render(json.load(f))
