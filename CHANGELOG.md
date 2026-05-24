# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- `scripts/repair_data_quality.py`: reversible, dry-run-by-default quality repair. Invalid bars move into `quarantine_*` tables (restorable by batch id), zero sentinels become NULL (old values kept in `quarantine_row_patches`), and 51 verified 1991 Saturday sessions are recorded in `historical_weekend_sessions`. No market value is ever invented
- `scripts/check_qmt_dat_health.py`: read-only local QMT daily/5m/1m/Finance DAT freshness and decodability probe, registered as a host-local daily task in `ops/data-quality-tasks.yaml`; explicitly reports the known "true 1m bars were never downloaded" warning
- `scripts/import_qmt_lhb.py`: fills the empty `lhb` table with 229,171 rows aggregated from local QMT seat detail (2006-09-01 → 2026-11-20), with `derived_from` and `ptrade_semantics_verified: false` in the report
- `scripts/refresh_finvalue_from_cache.py`: offline, idempotent backfill of historical NULL fundamentals cells from cached `gpcw*.zip` packs (NULL-only, no overwrite, no new quarter rows). 43 legacy 585-column packs are skipped with an explicit parse-error record
- `scripts/benchmark_financial_import.py --md-output` writes the versioned `docs/benchmarks/PERFORMANCE.md`; `tests/test_integrity_gate_cli.py` runs the real `check_integrity.py --strict` gate on a synthetic DuckDB (positive + fail-closed negative case) in offline CI
- `tests/test_build_index_history.py` covers dry-run, idempotency and per-index inception; `tests/test_export_degradations.py` covers export degradation reporting; `tests/test_download_parallel.py` verifies the network-only fetch parallelisation preserves results, isolates errors and is faster than serial (end-to-end online speedup is NOT claimed). `docs/PARITY_GAPS.md` records what is closed locally, what needs an authorised external source, and what is physically impossible
- `docs/PTRADE_PARQUET_FORMAT.md` §10.1 and `README_de.md` document the additive 1990s index asset, full-history floor, QMT cross-check role and quality-gate boundaries
- `scripts/build_index_history.py`: idempotent, dry-run-by-default publisher for a 1990s-capable index-history asset (`index_history` table + `metadata/index_history.parquet`). Long-lived index bars already in `stocks` (SSE Composite since 1990-12-19, SZSE Component since 1991-04-03) become usable without changing the CSI300 default benchmark, inventing pre-inception bars or polluting per-symbol stock files; the health report lists it as its own data domain
- `scripts/unified_data_manager.py`: one-command, idempotent maintenance for the single canonical CN dataset. It runs a pre-publish data quality gate (no DB/manifest byte-level snapshots), aligns canonical metadata with the PTrade serving view, cross-checks QMT local DAT bars into `data/cn.duckdb`, repairs calendar/benchmark/valuation serving coverage, optionally supplements through SmartRouter, stages and selectively promotes the PTrade Parquet export, then runs strict integrity and storage smoke gates
- Optional maintained OSS A-share adapters (`akshare`, `adata`, `efinance`) registered in SmartRouter as lazy best-effort supplements; missing packages are skipped without breaking first-party routes
- `unified_source_ledger` and `unified_runs` audit tables plus machine-readable reports under `ops/unified_data/`
- Tests for OSS normalization, route table, QMT cross-check conflict handling and the PTrade 10-column schema contract
- `scripts/data_health_report.py`: read-only data health audit with coverage by instrument class/data domain, date ranges, serving-view inventory, anomaly detection and auto-repair classification; outputs JSON and Markdown reports under `ops/unified_data/`
- Internet-first target discovery in `unified_data_manager.py`: bounded SmartRouter probes determine the newest real network bar date, QMT freshness no longer caps the target, and `--no-oss` now disables only optional OSS adapters while first-party internet routes remain enabled
- `--no-qmt`, `--target-date`, `--lookback-days`, `--history-start` and `--full-history` flags; full-history mode invokes the sanctioned TDX/Mootdx/BaoStock bulk downloader with a default A-share-inception floor of `1990-12-19`
- Tests for source-policy flag separation and OSS-only route filtering
- Full-history rebuild plumbing: `scripts/download.py --history-start` propagates `SIMTRADE_CN_HISTORY_START` into TDX, Mootdx and BaoStock full downloads; strict integrity receives the same floor through `--history-start`; apply/publish runs refresh the health report automatically

### Changed
- CN benchmark history floor is dynamic: legacy packages require `2015-01-05`, while a configured `SIMTRADE_CN_HISTORY_START` requires benchmark coverage back to that full-history floor; shorter benchmark exports still fail closed
- XDXR/exrights conversion keeps pre-floor corporate actions even when daily OHLCV is trimmed, because adjusted-price calculation needs historical events
- Integrity tests run the POSIX `run_daily.sh` subprocess case on Windows Git Bash via MSYS path conversion and a `flock` stub; they skip only when bash itself is unavailable
- Unified manager is internet-first by default; QMT local DAT is optional fallback cross-check/supplement. README examples and health-report history guidance now reflect QMT-independent updates and configurable full-history floors

### Fixed
- `download_efficient.py` 的季度财务下载把纯网络取数放进线程池（`SIMTRADE_DOWNLOAD_WORKERS`，默认 4，设 1 回退串行）；DB 读写、事务与公共报表缓存仍留在主线程，避免 DuckDBWriter 线程安全问题
- 数据健康报告的三类结构性伪警改为有据判定，并在每项检查里保留 `raw_*` / `excluded_*` 计数：1991 年 51 个真实周六交易时段（213 根带成交的 bar）不再被当作错位日期；HALT 停牌日的估值快照不再被当作 orphan；`NULL` 股票名称与 `check_integrity.py` 对齐为合法未知状态。配合可逆隔离，19 项检查从 `warning / 7 warnings` 变为 `pass / 0 warnings`
- 引擎级 PTrade smoke 的 schema 判定把 `date` 作为 DatetimeIndex 计入（原来真实股票文件会被误判 fail）
- `DuckDBWriter.export_degradations` 把分区/索引失败从静默或仅日志升级为发布报告中的显式事件
- 主仓 `tools/check_research_assets.py` 的子进程输出改 `errors="replace"`，消除 Windows GBK 输出导致的 `UnicodeDecodeError`
- 引擎级 PTrade smoke 不再被跳过：`pydantic==1.10.7` 纳入 dev 依赖（与 `PTrade/constraints-ptrade-py311.txt` 同 pin），`docs/OPERATIONS.md` 明确 `uv sync --frozen --python 3.11.18` 的宿主 venv 契约；缺 pydantic 时 `simtradelab.ptrade.storage` 无法导入，smoke 会静默退化成只做直接 Parquet 检查
- 修正历史 floor 门禁的语义误导：`check_integrity.py` 的 pass 现在明确只表示「没有早于 floor 的发布行、没有非法日期」，并新增 `coverage_to_floor` 字段区分「无脏行」与「覆盖到 floor」；健康度报告的 `can_push_earlier` 不再写死为 True，日期段落按各数据域实际起点分别列出，移除「2015 前历史」的过时描述。同步修正 `version.parquet` 的 `start_date` 文档、统一管理器关于字节级备份的失真描述、每日源探针中 QMT 本地 DAT 的适用范围注释，并删除 `download_efficient.py` 未使用的导入
- 恢复上游 Python/CI 契约：`requires-python` 回到 `>=3.10,<4.0`，移除与 uv 冲突的 Poetry dev 组/第三方镜像及无用 `lxml` 依赖，恢复 `tqdm`/`pyarrow` 下界
- 美股 CLI 恢复共享非阻塞 `ProcessLock`，删除本地 `filelock` 重定义（无限阻塞 + 退出删锁文件）
- 默认日线路由把上游 `mootdx`/TDX 恢复为最后兜底；`fundamentals`/`minute_bars` 恢复 mootdx 首源、`public` 作为补充；恢复被删的 fallback 测试与精确路由断言
- `write_fundamentals_bulk()` 复用 `_prepare_fundamentals` 的冲突检测、验证与 `roa_derived` 标记，不再静默 last-wins
- 统一管理器的直接 SQL 写入补记 `data_change_log`，增量发布按表指纹强制重建未登记变更的表
- 全历史重建恢复 BaoStock 全区间月度采样与指数成分回填；断点台账只在查询成功时记账，网络异常下次重试
- 可选 OSS 源全部缺失时 `_try_fetch` 抛 `DataSourceError`，不再返回「成功但为空」
- 每日源健康监控纳入 `public` 且保留全部上游源；修正 README/CHANGELOG 中关于 DB 备份与「完全兼容 PTrade」的失真描述，README 行尾归一化为 LF
- 38 个一次性调查/探针脚本与 5 份生成报告移出版本控制，归档到本地 `scratch/`（`probes/` + `reports/`，已忽略）；`ops/unified_data/*.md` 不再入库，运行时报告照常生成。`scripts/probe_qmt_finance_anchor.py` 是 QMT 财务导入的真实依赖，改名为正式模块 `scripts/qmt_finance_anchor.py` 并保留；`cleanup_artifacts.py` / `full_history_preflight.py` 两个可复用运维脚本也保留
- exrights 导出在 `dividend` 为空时用 `bonus_ps` 回填（部分权威数据源只提供 `bonus_ps`，两者在现有数据上完全一致）；覆盖全量 per-symbol、batch、delta 和 API delta 四条导出路径，避免 SimTradeLab 分红缓存读不到现金分红
- 统一 exrights 事件局部字段契约：`allotted_ps`=送股/转增、`rationed_ps`=配股、`rationed_px`=配股价、`bonus_ps`=税前现金分红、`dividend` 为 `bonus_ps` 别名。修正 TDX 字段映射（`songzhuangu→allotted_ps`、`fenhong→bonus_ps`）、BaoStock 把转增并入 `allotted_ps`、东财补齐配股/分红列；`write_exrights` 对缺失列补 0 并回填 `dividend`

### Changed
- Valuation export prefers daily fields (`roe`, `roa`, `naps`, `total_shares`, `float_value`) over quarterly fundamentals enrichment when present
- `valuation` table gains `a_shares` (A股股本) and `float_value` (流通市值) daily columns; `write_valuation` only overwrites the columns a source provides, preserving daily fields
- Valuation export derives `a_floats` as the customer snapshot's market cap over the trading day's close (`float_value / close`) instead of carrying the quarterly fundamentals share count forward. The engine computes 流通市值 as `close × a_floats` at runtime and ignores the stored `float_value` column, so a quarterly share count made the returned value differ from the platform's own figure and step on report days (observed: 002226.SZ 2026-06-30, +51% overnight). With this, the engine's value equals the stored `float_value` — i.e. redis — on every row that has a snapshot; rows without one leave `a_floats` NULL rather than inventing a figure from quarterly data

## [1.2.1] - 2026-03-26

### Fixed
- Valuation export: replace `ASOF JOIN fundamentals` with `LEFT JOIN LATERAL` to preserve early-period valuation rows when no fundamentals match exists
- Affects both single-symbol and batch export paths in `DuckDBWriter`

## [1.2.0] - 2026-03-13 - Smart Data Source Router / 智能数据源路由

### Added
- SmartRouter unified data access layer / SmartRouter 统一数据访问层
- Auto-selects best data source by data type and market / 自动根据数据类型和市场选择最佳数据源
- Static priority + circuit breaker health-aware, auto fallback on failure / 静态优先级 + 熔断器健康感知，主源失败自动 fallback
- 13 data types: daily bars, XDXR, fundamentals, valuation, money flow, LHB, margin trading, etc. / 支持 13 种数据类型
- EastMoney as A-share daily bars fallback source / EastMoney 作为 A 股日线 fallback
- Output column normalization: consistent column structure regardless of source / 输出列标准化

## [1.1.0] - 2026-03-10 - TDX Fast Import Integration / TDX 快速导入集成

### Added
- `--tdx-download` flag: auto-download TDX official daily data package and import / 自动下载 TDX 官方日线包并导入
- `--tdx-source` flag: import TDX daily data from local ZIP file or directory / 从本地 ZIP 文件导入
- First-time 6,000+ stocks OHLCV reduced from hours to minutes / 首次下载 6000+ 只股票从数小时缩短到几分钟
- TDX import runs as Phase 0 before Mootdx phase / TDX 导入作为 Phase 0 在 Mootdx 之前执行

### Fixed
- Corporate actions not downloading after TDX bulk import / TDX 导入后除权除息无法补充下载
- Corporate actions now check per-symbol independently of OHLCV incremental logic / 除权除息按个股检查

## [0.6.0] - 2026-02-08 - US Stock Support / 美股数据支持

### Added
- yfinance data source: 6,000+ US common stocks / yfinance 数据源，6000+ 只美股
- US stock ticker format `AAPL.US`, consistent with A-shares `{code}.{market}` / 美股代码格式
- Separate database `data/us.duckdb` / 独立数据库
- 5-phase download: stock list -> bulk OHLCV -> financials+valuation -> metadata+corporate actions -> global data
- `yf.download()` batch market data (50 per batch) / 批量获取行情
- S&P 500 / NASDAQ-100 index constituents (Wikipedia) / 指数成分股
- Incremental updates via `get_max_date()` / 增量更新

## [0.5.0] - 2026-02-01 - Unified Download Command / 统一下载命令

### Added
- `scripts/download.py` unified download entry point / 统一下载入口
- Automatic orchestration of Mootdx and BaoStock sources / 自动编排数据源
- Optimized incremental detection: skips all stocks in seconds when no new trading days / 优化增量检测
- Financial data incremental via remote file hash / 财务数据增量检测
- Index constituents incremental: tracks downloaded months / 指数成分股增量

### Fixed
- Mootdx Affair API return value handling / Mootdx Affair API 返回值处理
- DuckDB `changes()` function compatibility / DuckDB changes() 兼容性
- Auto-filters empty rows for suspended stocks / 停牌股票空行过滤

## [0.4.0] - 2026-01-30 - DuckDB + Parquet Architecture / DuckDB + Parquet 架构

### Changed
- Storage migrated from HDF5 to DuckDB + Parquet / 存储从 HDF5 迁移到 DuckDB + Parquet

### Added
- Limit-up/down price calculation (computed at export from preclose) / 涨跌停价计算
- TTM metric calculation (SQL window functions at export) / TTM 指标计算
- Corporate action data download / 除权除息数据下载
- Share capital data (total_shares/a_floats) / 股本数据
- Optimized incremental update logic / 优化增量更新逻辑

## [0.3.0] - 2025-11-24 - Quality & Architecture Optimization / 质量与架构优化

### Added
- Market cap field calculation / 市值字段计算
- Data validator / 数据验证器
- Extracted BaseFetcher base class / 提取 BaseFetcher 基类

### Fixed
- TTM metric calculation / TTM 指标计算

## [0.2.0] - 2025-11-22 - Performance Optimization / 性能优化

### Added
- Unified data fetching, reducing API calls by 33% / 统一数据获取
- Optimized HDF5 write logic / 优化 HDF5 写入逻辑

## [0.1.0] - 2024-11-14 - Initial Release / 初始版本

### Added
- Basic data download functionality / 基础数据下载功能
- BaoStock data source integration / BaoStock 数据源集成
