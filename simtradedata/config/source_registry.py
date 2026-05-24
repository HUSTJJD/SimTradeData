# -*- coding: utf-8 -*-
"""GitHub「A股数据」检索记录，以及本地最终采用的数据源。

检索日 2026-11-02。GitHub Search API，关键词「A股数据」「A股 行情 财务」
「akshare baostock」，按 star 降序合并去重。只记录和取数有关的仓库。
选股软件、投研 Agent 不采用它们的接口。

本地不复制这些仓库的代码。采用它们实测还在用的公开接口，自己请求，
两源对得上才写入最终列。
"""

# star, repo, 它实际拿数的地方
RANKED = (
    (11376, "simonlin1212/a-stock-data", "腾讯K线, 新浪三表, BaoStock估值, 通达信盘后包"),
    (5343, "1nchaos/adata", "新浪/腾讯行情, 东财仅作补充"),
    (3926, "mpquant/Ashare", "新浪与腾讯双源, 日线与分钟线"),
    (3100, "hello245m/free-stockdb", "日K、分钟K、ETF分钟, 本地缓存后复权"),
    (1621, "khscience/OSkhQuant", "BaoStock / Tushare 落到本地库"),
    (917, "rootSunc/CNEquity", "本地数据湖, 财务与行情分数据集"),
)

# 更高 star 但不是数据源本身，不从这里取数：
# akfamily/akshare 23047 是上面这些接口的包装。
# waditu/tushare 15462 要 token，本机没有则不用。

# 最终口径
FINAL = {
    "daily_bars": "腾讯不复权，按日期往前翻页。与新浪不复权收盘价相对差超过 0.5% 的日期不采用。",
    "minute_bars": "腾讯 1/5/15/30/60 分钟，不复权。只有最近一个窗口，不是十年库。",
    "fund_bars": "场内基金与 ETF 走腾讯日线，代码规则与股票相同。",
    "fundamentals": "新浪现金流量表、利润表。股利支付和经营现金流写入 PTrade 字段名。扣非没有这一行就留空。",
}
