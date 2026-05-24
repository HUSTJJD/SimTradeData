中文 | [English](README.md) | [Deutsch](README_de.md)

# SimTradeData

[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-red.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-1.2.0-brightgreen.svg)](CHANGELOG.md)
[![DuckDB](https://img.shields.io/badge/Storage-DuckDB-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Parquet](https://img.shields.io/badge/Export-Parquet-50ABF1)](https://parquet.apache.org/)
[![Code Style: Black](https://img.shields.io/badge/Code%20Style-Black-000000.svg)](https://github.com/psf/black)
[![uv](https://img.shields.io/badge/Packaging-uv-261230)](https://docs.astral.sh/uv/)

为[SimTradeLab](https://github.com/kay-ou/SimTradeLab)和SimTradeDesk准备本地回测数据：支持A股、ETF和美股，多源下载到DuckDB，再导出为共享Parquet数据包。

## 用网页管理数据（推荐）

需要Python 3.10+、[uv](https://docs.astral.sh/uv/getting-started/installation/)和Node.js 20+（含npm）。在项目根目录执行：
---

<div align="center">

### 推荐组合：SimTradeData + SimTradeLab

**对齐 PTrade 数据口径 | A股+美股 | 回测速度提升20倍以上**

[![SimTradeLab](https://img.shields.io/badge/SimTradeLab-量化回测框架-blue?style=for-the-badge)](https://github.com/kay-ou/SimTradeLab)

**无需修改PTrade策略代码** | **极速本地回测** | **零成本解决方案**

</div>

---

## 核心特性

### 高效存储架构
- **DuckDB 中间存储**: 高性能列式数据库，支持 SQL 查询和增量更新
- **Parquet 导出格式**: 压缩高效，跨平台兼容，适合大规模数据分析
- **自动增量更新**: 智能识别已下载数据，仅更新增量部分

### 数据完整性
- **市场数据**: OHLCV 日线数据，含涨跌停价、前收盘价
- **估值指标**: PE/PB/PS/PCF/换手率/总股本/流通股
- **财务数据**: 23个季度财务指标 + TTM指标自动计算
- **除权除息**: 分红、送股、配股数据（含前复权因子）
- **元数据**: 股票信息、交易日历、指数成分股、ST/停牌状态
- **美股支持**: 6000+ 美股普通股，S&P 500 / NASDAQ-100 指数成分股

### 数据质量保障
- **自动验证**: 写入前自动验证数据完整性
- **导出时计算**: 涨跌停价、TTM指标等在导出时计算，确保数据一致性
- **详细日志**: 完整的错误日志和警告信息

## 统一数据层（推荐日常入口）

本地回测只应维护**一份权威数据**：`data/cn.duckdb` 是唯一关系型权威库，`data/cn/` 是唯一的 PTrade/SimTradeLab 兼容服务视图。QMT 本地 DAT、BaoStock/Mootdx/TDX/EastMoney 以及可选开源数据源都只是补充/校验来源，不再形成第二份数据集。`data/cn_2/` 仅视为历史遗留；`_backup_*` 与 DB 字节级快照已全部清理，服务视图可由权威库完整重建。

日线 Parquet 固定为 PTrade 10 列：

```text
date, open, close, high, low, high_limit, low_limit, preclose, volume, money
```

一条命令完成：发布前数据质量门禁 → 元数据对齐 → 互联网源目标日发现/活跃标的补源 → 可选 QMT DAT 跨源对账补充 → 交易日历/基准/估值服务覆盖补齐 → 分阶段导出并选择性原子替换 → 严格完整性门禁 → PTrade 存储契约 smoke check。默认 **internet-first**：QMT 本地 DAT 只是最后的可选校验/补充来源，不刷新 QMT 不会阻塞更新；`--no-qmt` 可完全跳过。默认 dry-run，不写数据；只有 `--apply` 才落盘。

数据正确性由**发布前质量门禁**保证：19 项检查里 error 级脏数据、或比上次发布更差，都会在导出/发布之前阻断；门禁报告与基线写入 `ops/unified_data/data_quality_gate.json` / `data_quality_baseline.json`。不再保留 DB 或服务视图的字节级快照：所有导出都可由权威库重建，写入是幂等 upsert。需要临时回滚时 `promote_staged_export` 使用一次性交换目录，成功即删。

```bash
# 全量 dry-run 审计（默认不修改任何数据）
uv run python scripts/unified_data_manager.py --json

# 自动扩充到互联网源当前可用的最新真实交易日并发布服务视图
uv run python scripts/unified_data_manager.py --apply --json --strict

# 指定期望目标日（只写源里真实存在的 bar，不伪造）
uv run python scripts/unified_data_manager.py --apply --json --strict --target-date 2026-11-26

# 仅用第一方互联网源，跳过可选开源库；不会关闭 public/eastmoney/baostock
uv run python scripts/unified_data_manager.py --apply --json --strict --no-oss

# 完全跳过可选 QMT 本地 DAT（QMT 未刷新/未安装时的推荐路径）
uv run python scripts/unified_data_manager.py --apply --json --strict --no-qmt

# 从 A 股起始附近做互联网全历史重建（TDX + Mootdx + BaoStock）
uv run python scripts/unified_data_manager.py --apply --json --strict \
  --full-history --history-start 1990-12-19

# 完全跳过网络 SmartRouter 补源（纯本地校验/发布路径）
uv run python scripts/unified_data_manager.py --apply --json --strict --no-router

# 只跑校验，不改数据
uv run python scripts/unified_data_manager.py --validate-only --json --strict

# 输出数据健康度报告（覆盖、时间范围、错误检测、自动修复边界）
uv run python scripts/data_health_report.py \
  --json-output ops/unified_data/data_health_latest.json \
  --markdown-output ops/unified_data/data_health_latest.md
```

健康度报告是只读审计：覆盖股票/ETF/指数等标的类型、日线/估值/财务/除权除息/日历/状态等数据域、活跃标的覆盖率、时间范围、服务视图文件数，以及可检测的错误数据（重复主键、NULL OHLC、OHLC 不一致、非正价格、负成交量/成交额、异常日期、元数据名称/行业覆盖等），并标注哪些问题可自动修复、哪些必须刷新数据源或全量重建。它不会伪造数据，也不会静默改写权威库。

QMT 本地数据根目录默认 `D:\君弘君智交易系统`，可用环境变量覆盖；但默认流程不依赖 QMT 的新鲜度：管理器先用少量指数/流动性探针从互联网源发现最新真实 bar 日期，再补齐活跃标的，最后才把本地 QMT DAT 作为可选交叉校验/补充。`--no-qmt` 适合 QMT 客户端长期不更新的环境。

```bash
# Windows PowerShell
$env:SIMTRADE_QMT_ROOT="D:\君弘君智交易系统"
```

可选维护中的 GitHub A 股数据工具（按需安装，未安装会自动跳过，不影响主流程）：

```bash
uv pip install akshare adata efinance
```

路由优先级：`public → eastmoney → baostock → akshare → adata → efinance → mootdx`（上游 TDX 源保留为最后一层本地兜底）。`--no-oss` 只移除可选 OSS，不会关闭第一方互联网源；`--no-router` 才关闭所有网络补源。所有来源都写入同一个 DuckDB；QMT DAT 只做可选交叉校验/补充，与已有收盘价按 0.5% 容差对账，系统性冲突会跳过并记录。估值不是 QMT 可提供的字段：管理器会对无估值文件的活跃标的写入 NULL 占位行，并把最后已知估值快照按 PTrade 文件契约前推到目标日，不会伪造因子值。

全历史模式通过 `--full-history` 调用 `scripts/download.py` 的互联网批量源：TDX `hsjday.zip` 提供长历史日线，Mootdx 提供 XDXR/财务，BaoStock 提供估值/状态/日历。默认 `--history-start 1990-12-19`（A 股起始附近），并为本进程设置 `SIMTRADE_CN_HISTORY_START`；`scripts/download.py --history-start` 会把同一 floor 传给 TDX/Mootdx/BaoStock，严格门禁通过 `--history-start` 按该 floor 检查 DB 和导出视图（benchmark 覆盖要求会钳制到默认指数自身的起始日，默认沪深 300 为 `2005-01-04`，不要求指数提供诞生前的行情；若把 `default_index` 换成上证综指则仍按 1990 floor 严格要求），发布流程自动重跑健康报告。早于 floor 的 exrights 公司行动会保留，因为 floor 之后的复权因子计算依赖历史公司行动；QMT 本地 DAT 不作为 1990–2014 全市场行情/财务/ETF 历史源。1990–2014 的 ETF、财务、估值覆盖取决于上游源，报告不会声称与线上 PTrade 权限完全等价。

每次运行会生成机器可读报告到 `ops/unified_data/unified_*.json`，最近一次发布状态写入 `ops/unified_data/latest_status.json`。`--apply` 会先 CHECKPOINT WAL 并跑发布前质量门禁，不再复制 DB 或视图快照；导出采用 staging 目录，只替换 DB 拥有的 `stocks/`、`exrights/`、`fundamentals/`、`valuation/`、`metadata/`、`manifest.json`，保留 QMT 独有的 `stocks_1m/`、`stocks_5m/`、`ptrade_adj_*.parquet`。临时目录（staging / promote 交换目录）在成功或下次运行前自动清理，不堆积备份副本。

## 生成的数据结构

```
data/
├── cn.duckdb          # DuckDB 数据库 - A股（下载源）
├── us.duckdb             # DuckDB 数据库 - 美股（下载源）
└── export/                      # 导出的 Parquet 文件（按市场分目录）
    ├── cn/                      # A股导出
    │   ├── stocks/              # 日线行情（每只股票一个文件）
    │   │   ├── 000001.SZ.parquet
    │   │   └── 600519.SS.parquet
    │   ├── exrights/            # 除权除息事件
    │   ├── fundamentals/        # 季度财务数据（含TTM）
    │   ├── valuation/           # 估值指标（日频）
    │   ├── metadata/            # 元数据
    │   └── manifest.json
    └── us/                      # 美股导出
        ├── stocks/
        │   ├── AAPL.US.parquet
        │   └── MSFT.US.parquet
        ├── exrights/
        ├── fundamentals/
        ├── valuation/
        ├── metadata/
        └── manifest.json
```

## 前置条件

- **Python**: 3.10 或更高版本
- **uv**: [安装指南](https://docs.astral.sh/uv/getting-started/installation/)
- **仅本地 CI 需要**：Node.js 20+ 和 npm；无需运行 `npm install`
- **网络**: 下载数据需要网络（A 股数据建议使用中国大陆网络）

## 快速开始

### 方式一：直接下载现成数据（推荐）

从 [Releases](https://github.com/kay-ou/SimTradeData/releases) 下载最新数据：

- **A股**: `data-cn-v*` release → 解压到 `data/cn/`
- **美股**: `data-us-v*` release → 解压到 `data/us/`

```bash
npm run ui
```

默认从 **http://127.0.0.1:8765** 启动，端口占用时自动选择下一个；以终端显示的地址为准。需要固定端口时可选：

```bash
npm run ui -- --port 8767
```

1. 点击“更新回测数据”：股票和ETF统一补齐，无需逐只选择。
2. 已有有效数据自动跳过；下载时显示实时进度，可取消，失败后可继续补齐。
3. 股票和ETF分别显示已保存数量及日期范围；季度财务和ETF申赎清单位于“其他数据”。
4. 在“导出回测数据包”生成并下载CN或US包，解压到对应应用的数据目录；不需要在页面输入导出命令或路径。

默认使用 `data/cn.duckdb`，可通过 `--data-dir /path/to/data` 指定其他目录。更新判断来自实际数据，不保存下载任务历史。启动时自动安装前端依赖、按需构建，并由uv同步Python依赖。端口从8765开始自动选择可用值，无需手填；以终端显示的地址为准。界面右上角可切换中文、English和Deutsch，首次跟随浏览器语言，之后记住手动选择。

完整操作、日期范围、质量修补、数据包导出与数据源健康检查见[网页使用指南](docs/WEB_DATA_ADMIN.md)。网页管理沪深股票、ETF日线、季度财务、深市历史PCF和交易日历，并可导出CN/US数据包；美股下载仍使用命令行。

## 直接使用现成数据

在[Releases](https://github.com/kay-ou/SimTradeData/releases)下载 `data-cn-v*` 或 `data-us-v*`，将Parquet数据分别解压到SimTradeLab的 `data/cn/` 或 `data/us/`。发布包不是可直接更新的DuckDB数据库。

## 命令行下载与导出

仅使用命令行时安装Python和uv，执行 `uv sync` 后：

```bash
uv run python scripts/download.py
uv run python scripts/export_parquet.py
```

默认导出到 `data/export/cn/`。美股、独立来源和导出参数见[命令行指南](docs/CLI_GUIDE.md)。不要同时通过网页和命令行更新同一个数据库。

## 数据覆盖与文档

日线批量来源为通达信官方历史包；Mootdx支持股票与ETF按需历史查询，BaoStock提供估值、状态和日历等补充，东方财富为部分接口备用，美股使用yfinance。财务已有223个公开字段映射，但历史证券池、公告时点、L2和部分PCF仍有边界，不能视为完整PTrade等价。

| 想做什么 | 去哪里 |
| --- | --- |
| 下载、查看和修补本地数据 | [网页指南](docs/WEB_DATA_ADMIN.md) |
| 美股、命令行和Parquet导出 | [命令行指南](docs/CLI_GUIDE.md) |
| 确认可回测范围和缺失能力 | [数据覆盖](docs/PTRADE_DATA_COVERAGE.md) |
| 缺口台账：已闭环/需外部源/物理不可能 | [缺口台账](docs/PARITY_GAPS.md) |
| 可复跑的性能基准 | [性能基准](docs/benchmarks/PERFORMANCE.md) |
| CI、自动检查、发布和故障恢复 | [运维说明](docs/OPERATIONS.md) |
| 字段规范、财务和PCF详情 | [完整文档导航](docs/README.md) |

第三方接口会延迟、修订或不可用；健康样本通过不等于全市场历史完整。ETF普通价格回测不需要申赎清单。

版本变更见[CHANGELOG](CHANGELOG.md)。项目采用[AGPL-3.0](LICENSE)。

## 支持项目


如果项目对你有帮助，欢迎支持。

| WeChat Pay | Alipay |
|:---:|:---:|
| <img src="docs/sponsor/WechatPay.png?raw=true" alt="WeChat Pay" width="200"> | <img src="docs/sponsor/AliPay.png?raw=true" alt="Alipay" width="200"> |

