# 数据完备性缺口与验收边界

本文是 2026-11-27 数据链路审计后的**缺口台账**，不是「已完全对齐 PTrade」的声明。
字段数量增加、警告清零或新资产落地都不等于与线上 PTrade 权限一比一；
逐字段口径仍须用目标券商版本的真实返回样本验收。

## 一、本机已闭环（有可复跑证据）

| 缺口 | 落地方式 | 证据 |
| --- | --- | --- |
| 主仓研究资产门禁失败 | `.venv` 回到矩阵登记的 Python 3.11.18；`pydantic==1.10.7` 入 dev 依赖并写进锁；主仓 `check_research_assets.py` 子进程输出改 `errors="replace"` | `python -I tools/check_research_assets.py` 退出 0；`docs/OPERATIONS.md` 记录 `uv sync --frozen --python 3.11.18` |
| 引擎级 PTrade smoke 被跳过 / 误判 fail | 补齐 pydantic；修正 smoke 对 `date` 作为 DatetimeIndex 的 schema 判定 | `_ptrade_smoke()` 返回 overall/engine/direct 全 `pass` |
| 数据健康报告 7 项 warning | 3 项判定口径修正（1991 历史周六、HALT 停牌日估值、NULL=合法未知名，均在报告里保留 raw/excluded 计数）+ 4 项真脏数据可逆隔离 / 哨兵改 NULL | `scripts/repair_data_quality.py --apply`；健康报告 `status=pass, errors=0, warnings=0`；`quarantine_*` 表可按 batch 恢复 |
| 90 年代「默认 benchmark」缺口 | 新增附加资产 `index_history` 表 + `metadata/index_history.parquet`，把已存在的上证综指（1990-12-19 起）、深证成指（1991-04-03 起）暴露出来；不改默认 CSI300、不造指数诞生前行情 | `scripts/build_index_history.py`；`docs/PTRADE_PARQUET_FORMAT.md` §10.1；发布流程自动刷新 |
| QMT 本地 DAT 无持续健康检查 | 新增只读 `scripts/check_qmt_dat_health.py`（日线/5m/1m/财务 DAT 新鲜度、可解码性、整除性），登记为 `ops/data-quality-tasks.yaml` 本机日任务 | 探针报告：日线/5m 末根 2026-11-20、财务 42,156/42,156 整除；1m 仅 1 文件标为已知 warning |
| `lhb` 空表 | 用 QMT 本地 `longhubang.db3` 的席位明细离线聚合灌入 229,171 行（2006-09-01 → 2026-11-20，5,188 标的） | `scripts/import_qmt_lhb.py --apply`；报告标注 `derived_from` 与 `ptrade_semantics_verified: false` |
| 历史财务字段大面积 NULL | 从本机缓存季度包按生产解析器回填 223 列的 NULL 单元格（不覆盖非空、不追加季度行） | `scripts/refresh_finvalue_from_cache.py --apply`；`trading_assets` 等探针列 NULL 从 214,228 降到 127,476 |
| 正式性能基准缺失 | `scripts/benchmark_financial_import.py` 支持 `--md-output`，生成可版本化 `docs/benchmarks/PERFORMANCE.md`（含输入 sha256、可复跑命令、历史日志仅作观测） | 报告内 `data_equal=true`、individual/batch 实测对比 |
| CI 不检查完整性门禁 | 新增 `tests/test_integrity_gate_cli.py`：合成 DuckDB 上跑真实 `check_integrity.py --strict` 的正/负例，自动被现有离线 CI 收集 | 2 passed |
| 分区/索引失败静默回退 | `DuckDBWriter.export_degradations` 收集降级事件并写入发布报告 | `simtradedata/writers/duckdb_writer.py`、`scripts/unified_data_manager.py` |
| floor 门禁语义误导 | pass 明确只表示「无 floor 之前的发布行、无非法日期」，新增 `coverage_to_floor` 区分覆盖 | `scripts/check_integrity.py` |
| 文档滞后 | 德文 README 补齐全历史/QMT/健康报告/边界；`docs/OPERATIONS.md` 补环境契约 | 本仓库文件 |

## 二、需要联网或外部授权源（本机无数据，禁止伪造）

| 缺口 | 需要什么 | 为什么不能在本机补 |
| --- | --- | --- |
| `money_flow`（按单大小净流入）、`margin_trading`（两融余额） | 东财公开接口或授权行情/两融源 | 本机 `capitalFlow.db3` 为 0 字节；`rzrkpy.dat` 是「代码→拼音」字典，不是余额；`akshare` 未安装且同样需联网 |
| 真 1m K 线 | 在 QMT 客户端下载 1m 后再导入 | `SH/SZ/60` 各仅 1 个 `000001.DAT`；`stocks_1m` 实际由 5m bar 填充，不能反推真实 1m |
| Level-2 逐笔委托/成交 | 授权 L2 行情（毫秒时间、委托/成交编号、撤单标记、通道号） | 普通 TDX 分笔只有 `HH:MM` 与方向值，缺全部 L2 标识 |
| 银行/券商/保险专用财报字段 | 带标签的 XBRL/行业财报源 | FINVALUE 429 个位置无字段字典；合计项无法拆成 PTrade 明细 |
| 沪市及连续历史 PCF、申赎代码、成分停牌状态 | 交易所按日期的历史公告端点 | 本机只有 2 个 XML 样本；最新清单不能前填到历史日期 |
| 2015–2025 季度财务包的 127,476 行 NULL | 联网重新下载 659 列新格式 `gpcw*.zip`，或兼容 585 列旧格式的解析器 | 本机缓存这些季度是 585 列旧格式，当前 mootdx 解析器要求 659 列，43 个包无法解析；已跳过并记录，不猜列 |
| 逐字段口径验收 | 目标 PTrade 版本的真实返回样本 | 字段数量与覆盖率不是一比一验收标准 |

## 二之二、本机可做但未执行（有明确工程理由）

| 项 | 为什么没有执行 | 要做时需要什么 |
| --- | --- | --- |
| QMT 财务锚点从 24 样本扩到全量（9,334 标的 × 42,156 个 DAT） | 计算可行但**上限不变**：`qmt_finance_full_anchor_report.json` 的 60 样本/6 表已只有 3 个 confident 字段，`VALUE_MAP` 最多仍是 `publ_date + 4` 个数值列；扩全量只提高同一小批列的统计置信度，不能生成任何新 PTrade 字段语义 | 先把 `scratch/probes/probe_qmt_finance_full.py` 提升为正式脚本并产出可版本化报告；再按 QMT 官方字段字典扩充锚定，而不是靠相关性猜列 |
| `import_qmt_finance_to_ptrade.py --apply`（QMT 财务补丁写服务视图） | 该脚本直接改写 `data/cn/fundamentals/*.parquet` 并在 `data/cn/` 下建 `_backup_qmt_finance_*`，**不写权威库**；执行会造成 DB 与服务视图不一致，且下次发布即被覆盖，违反「`data/cn.duckdb` 唯一权威、视图可重建」的架构 | 先把补丁目标改成 `cn.duckdb` 的 `fundamentals` 表（只填空值、冲突整格跳过、保留 PIT 校验），再由统一管理器重新导出视图 |
| 2015–2025 季度财务包的 127,476 行 NULL | 本机缓存是 585 列旧格式，当前 mootdx 解析器要求 659 列 | 联网重新下载新格式包，或提供兼容 585 列的旧格式解析器 |

## 三、物理不可能（不要尝试用计算「补齐」）

- 1990 年代的 ETF / 现代指数成分：这些品种当时不存在；`benchmark` 表默认 CSI300 诞生于 2005-01-04，不能造诞生前指数点位。
- 从 5m OHLCV 反推真实 1m OHLCV，或从普通分笔反推 L2 委托编号。
- 把 FINVALUE 的合计项拆成 PTrade 明细字段；把最新 PCF/当前成分当作任意历史日期的数据。
- 把未知 QMT DAT 列「猜」成 PTrade 字段名；把零值当缺失或把缺失填零。
- 在 GitHub 托管 runner 上直接检查 `D:\君弘君智交易系统`（该探针是本机任务）。

## 四、验收顺序（拿到外部源之后）

1. 用授权源补齐缺口数据，保留 `derived_from`、单位、公告可见时点与健康状态。
2. 逐字段对照目标 PTrade 版本的真实返回：名称、类型、代码后缀、单位、频率、时区、日期边界、默认参数、排序、缺失值行为、pandas/字典形状。
3. 用 `data_health_report.py` / `check_integrity.py --strict` / `data_quality_gate.py` 复跑，并显式断言 `warnings == 0`（`--strict` 不会因 warning 失败）。
4. 只有全部通过且逐字段样本比对一致，才可以把口径从「对齐」升级为「等价」；任何一步都不能靠改文案或填造值跳过。
