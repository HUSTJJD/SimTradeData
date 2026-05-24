# scratch — 本地归档，不入库

`probes/` 是数据链路优化期间的一次性调查/探针脚本的冻结快照，`reports/` 是当时生成的健康度与质量门禁报告。它们**不是**运行管线的一部分，也不再由任何生产代码引用；`.gitignore` 已忽略本目录（只保留本 README）。

注意：这些脚本按当初所在的目录编写，`__file__` 相对路径、模块 import 和输出路径假设可能已失效，仅供查阅取证，不要直接运行。真正可复用的能力已沉淀在 `scripts/`、`simtradedata/` 和 `tests/`（例如 `scripts/qmt_finance_anchor.py` 是 QMT 财务导入的共享解码模块，`tests/test_fundamentals_bulk_validation.py` 是批量财务写入的正式回归测试）。

运行时生成的报告仍会写入 `ops/unified_data/*.md` / `*.json`，两者均被 `.gitignore` 忽略，不会污染仓库状态。
