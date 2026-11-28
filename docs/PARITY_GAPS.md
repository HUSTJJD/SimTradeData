# 数据完备性边界（按 PTrade 官方回测契约收窄）

原则：**PTrade 官方回测不提供、本地 SimTradeLab 也没有读取通道的数据，本项目不再补齐、不再维护、不列为待办**。
本文不是「已完全对齐 PTrade」的声明；逐字段口径仍须用目标券商版本的真实返回样本验收。

## 一、官方回测真正的数据契约

**本地离线回测**（`SimTradeLab/src/simtradelab/ptrade/storage.py`）只打开：

- `stocks/{symbol}.parquet`（10 列日线）、`valuation/`、`fundamentals/`、`exrights/`、`stocks_1m/`
- `metadata/` 下的 `stock_metadata` / `benchmark` / `trade_days` / `index_constituents` / `stock_status` / `version`
- 根级 `ptrade_adj_pre/post.parquet`（缺失时引擎自行重建）
- `manifest.json` 只用于根目录判定与校验，**不作为行情数据读取**

`LazyDataDict` 的加载映射只有 `stock / stock_1m / valuation / fundamentals / exrights` 五种；`data_server.py` 只请求 4 个 metadata 名。

**云端 PTrade API**（`docs/reference/PTrade_API_mini_Reference.md`）：

- 回测/研究可用：`get_history`、`get_price`、`get_fundamentals`（9 类财务表）、`get_stock_info`、`get_stock_blocks`、`get_index_stocks`、`get_stock_exrights`、`get_industry_stocks`、`get_stock_status`、交易日历。
- **K 线接口限制**：`get_history` / `get_price` 只能取 **2005 年后**的数据（参考文档 `:1790`、`:1985`）；`get_trade_days` 的 `start_date` 最早可到 **1990 年**（`:1554`），这是日历上限，不是行情上限。
- 仅交易模块/需 L2 权限、官方回测不可用：`get_snapshot`、`get_gear_price`、`get_individual_entrust`、`get_individual_transaction`、`get_tick_direction`、`get_etf_info` 等；SimTradeLab 对这些统一 `NotImplementedError`。
- 主仓研究侧口径更窄：`PTrade/adapters/simtradedata.py` 只读 `stocks`（OHLCV+money）、`valuation.turnover_rate`、`benchmark`（OHLCV+money）。

## 二、本机已闭环（都在官方契约内或属于工程链）

| 项 | 落地方式 | 证据 |
| --- | --- | --- |
| 主仓研究资产门禁失败 | `.venv` 回到矩阵登记的 Python 3.11.18、`pydantic==1.10.7` 入 dev 依赖、主仓门禁子进程输出 `errors="replace"` | `python -I tools/check_research_assets.py` 退出 0；`docs/OPERATIONS.md` 记录 `uv sync --frozen --python 3.11.18` |
| 引擎级 PTrade smoke 被跳过 / 误判 fail | 补齐 pydantic，修正 `date` 作为 DatetimeIndex 的 schema 判定 | `_ptrade_smoke()` overall/engine/direct 全 `pass` |
| 数据健康报告 7 项 warning | 3 项有据的判定口径修正（1991 历史周六、HALT 停牌日估值、NULL=合法未知名，报告保留 raw/excluded 计数）+ 4 项真脏数据可逆隔离 / 哨兵改 NULL | `scripts/repair_data_quality.py --apply`；`status=pass, errors=0, warnings=0`；`quarantine_*` 可按 batch 恢复 |
| 历史财务字段大面积 NULL | `scripts/refresh_finvalue_from_cache.py` 从缓存季度包按生产解析器只回填 NULL（223 列，不覆盖非空、不追加季度行） | 探针列 NULL 从 214,228 降到 127,476；43 个 585 列旧格式包跳过并逐包记录 |
| floor 门禁语义误导 | pass 明确只表示「无 floor 之前的发布行、无非法日期」，新增 `coverage_to_floor` | `scripts/check_integrity.py` |
| 正式性能基准 / CI 完整性门禁 | `docs/benchmarks/PERFORMANCE.md`（可复跑）；`tests/test_integrity_gate_cli.py` 在合成库上跑真实 `--strict` 正负例 | 196 项离线测试全绿 |
| 导出降级、下载取数并行 | `export_degradations` 显式上报；`SIMTRADE_DOWNLOAD_WORKERS` 只并行纯网络取数，DB 写留主线程 | 对应单测 |
| QMT DAT 健康探针 | `scripts/check_qmt_dat_health.py` 只读检查日线/5m/财务 DAT（本机日任务）。真 1m 不在探针范围内 | 财务 DAT 42,156/42,156 整除 |

## 三、明确不支持（不再补齐、不维护、不列为待办）

| 数据 / 能力 | 依据 |
| --- | --- |
| 龙虎榜 `lhb`、主力资金流 `money_flow`、两融余额 `margin_trading` | 官方 API 基线全文无这些数据接口（「融资融券」都是交易/标的券/保证金接口）；SimTradeLab 源码与 `quant_core` 无读取路径；[patch_all_from_qmt.py:660](../scripts/patch_all_from_qmt.py) 原文「非 PTrade 数据，SimTradeLab 无数据通道」。三张表保持为空，已有联网补 DB 脚本只是数据源工具，不是回测通道 |
| `index_history` / 1990 年代附加指数资产 | 官方无此概念；`metadata/index_history.parquet` 引擎不读。已随本轮收窄删除脚本、表、Parquet、健康域与门禁项 |
| Level-2 逐笔委托/成交、快照、五档、分时成交 | 官方仅交易模块且需 L2 权限；本地 `_unsupported_api` / `NotImplementedError` |
| 真 1m K 线 | QMT 客户端未下载 1m；`stocks_1m/` 装的是 5m bar，1m 语义失真（[patch_all_from_qmt.py:661](../scripts/patch_all_from_qmt.py)）。不反推、不补齐 |
| ETF 申赎 / 历史 PCF、沪市连续 PCF | `get_etf_info` 等仅交易模块；`etf_pcf/` 无 reader；最新清单不能前填到历史日期 |
| 银行/券商/保险专用财报、429 个未标注 FINVALUE 位置 | 需带标签的授权财报源 / 字段字典；合计项不能拆成明细 |
| 90 年代 ETF、指数诞生前的 benchmark 行情 | 品种当时不存在；CSI300 诞生于 2005-01-04，不造诞生前点位 |
| `get_stock_blocks` 的历史成分、`get_snapshot` 的实时态 | 官方自身警告「回测取不到匹配回测日期的数据」；本地只提供当前快照并标注边界 |

**物理不可能**：从 5m OHLCV 反推真实 1m；从普通分笔反推 L2 委托编号；把 FINVALUE 合计项拆成明细；把零值当缺失或把缺失填零；在 GitHub 托管 runner 上检查本机 QMT 路径。

## 四、90 年代数据的准确定位

- `trade_days` 从 1990-12-19 起与官方 `get_trade_days` 的 1990 下限一致，**保留**。
- `stocks/` 的 1990–2004 日线是**本地离线包的扩展**：官方 K 线接口只给 2005 年后数据；本地 `get_price` / `get_history` 的 parquet 切片没有年份下限，显式传 1990 起可返回这些行。但**主回测循环**的 `date_range` 由 `benchmark.parquet`（默认 CSI300，2005-01-04 起）过滤生成，`start_date` / `end_date` 落在 2005 年前会得到空区间而直接返回。因此 90 年代日线服务的是因子研究与直接取数，**不是官方回测区间能力**，不得据此宣称与线上 PTrade 等价。`manifest.json` 的 `date_range.start` 与 `version.parquet` 只用于清单/版本/门禁，不进引擎读取。
- `fundamentals` / `valuation` / `exrights` 的 90 年代覆盖取决于上游源，稀疏属实，不再为「全口径 90 年代」新增资产。

## 五、后续验收只针对契约内数据

1. 任何补齐都必须落在第一节列出的读取路径里，并保留 `derived_from`、单位、公告可见时点与口径标注。
2. 用 `data_health_report.py` / `check_integrity.py --strict` / `data_quality_gate.py` 复跑，并显式断言 `warnings == 0`（`--strict` 不会因 warning 失败）。
3. 逐字段对照目标 PTrade 版本真实返回：名称、类型、代码后缀、单位、频率、时区、日期边界、缺失值行为与返回形状。
4. 契约外的数据不再进入缺口台账；如官方版本新增接口，先补读取通道再谈数据。
