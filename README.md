English | [Chinese](README_zh.md) | [German](README_de.md)

# SimTradeData

[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-red.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-1.2.0-brightgreen.svg)](CHANGELOG.md)
[![DuckDB](https://img.shields.io/badge/Storage-DuckDB-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Parquet](https://img.shields.io/badge/Export-Parquet-50ABF1)](https://parquet.apache.org/)
[![Code Style: Black](https://img.shields.io/badge/Code%20Style-Black-000000.svg)](https://github.com/psf/black)
[![uv](https://img.shields.io/badge/Packaging-uv-261230)](https://docs.astral.sh/uv/)

Prepare local backtesting data for [SimTradeLab](https://github.com/kay-ou/SimTradeLab) and SimTradeDesk: China stocks, ETFs and US stocks. Store data in DuckDB and export shared Parquet data packages.

## Start the web UI (recommended)

Install Python 3.10+, [uv](https://docs.astral.sh/uv/getting-started/installation/) and Node.js 20+ with npm. From the project root:
---

<div align="center">

### Recommended Combo: SimTradeData + SimTradeLab

**PTrade-compatible Data | A-Shares + US Stocks | 20x+ Backtesting Speedup**

[![SimTradeLab](https://img.shields.io/badge/SimTradeLab-Quant_Backtesting-blue?style=for-the-badge)](https://github.com/kay-ou/SimTradeLab)

**No PTrade Strategy Code Changes Needed** | **Ultra-Fast Local Backtesting** | **Zero-Cost Solution**

</div>

---

## Key Features

### Efficient Storage Architecture
- **DuckDB Intermediate Storage**: High-performance columnar database with SQL queries and incremental updates
- **Parquet Export Format**: Highly compressed, cross-platform compatible, ideal for large-scale data analysis
- **Automatic Incremental Updates**: Intelligently detects existing data, only downloads new records

### Comprehensive Data Coverage
- **Market Data**: OHLCV daily bars with limit-up/down prices and previous close
- **Valuation Metrics**: PE/PB/PS/PCF/Turnover Rate/Total Shares/Float Shares
- **Financial Data**: 23 quarterly financial indicators + automatic TTM calculation
- **Corporate Actions**: Dividends, bonus shares, rights offerings (with forward adjustment factors)
- **Metadata**: Stock info, trading calendar, index constituents, ST/suspension status
- **US Stock Support**: 6,000+ US common stocks, S&P 500 / NASDAQ-100 index constituents

### Data Quality Assurance
- **Auto-Validation**: Data integrity validation before writes
- **Export-Time Calculation**: Limit prices, TTM metrics computed at export for consistency
- **Detailed Logging**: Comprehensive error logs and warnings

## Unified Data Layer (Recommended Daily Entry Point)

Maintain **one authoritative dataset**: `data/cn.duckdb` is the only relational source of truth and `data/cn/` is the only PTrade/SimTradeLab-compatible serving view. QMT local DAT files, BaoStock/Mootdx/TDX/EastMoney and optional OSS adapters are sources for supplementation/cross-validation only; they never create a second dataset. Historical `_backup_*` copies and DB snapshots have been removed; the serving view is fully derivable from the canonical database.

The daily Parquet contract is the fixed PTrade 10-column schema:

```text
date, open, close, high, low, high_limit, low_limit, preclose, volume, money
```

One command performs a pre-publish data quality gate, metadata alignment, internet-first target discovery and active-symbol supplementation, optional QMT DAT cross-check/supplement, calendar/benchmark/valuation serving coverage repair, staged export with selective atomic promotion, strict integrity gate and PTrade storage smoke check. The default policy is **internet-first**: QMT local DAT is only an optional last fallback for validation/supplementation, and a stale QMT client never caps the target date; use `--no-qmt` to skip it entirely. It is dry-run by default; only `--apply` mutates data.

```bash
# Full dry-run audit (no data mutation)
uv run python scripts/unified_data_manager.py --json

# Extend to the latest real date available from internet sources and publish
uv run python scripts/unified_data_manager.py --apply --json --strict

# Explicit expected target date (only real source bars are written; no fabrication)
uv run python scripts/unified_data_manager.py --apply --json --strict --target-date 2026-11-26

# Disable optional OSS adapters only; first-party public/eastmoney/baostock stay enabled
uv run python scripts/unified_data_manager.py --apply --json --strict --no-oss

# Skip optional QMT local DAT entirely (recommended when QMT is not refreshed)
uv run python scripts/unified_data_manager.py --apply --json --strict --no-qmt

# Internet-first full-history rebuild near A-share inception (TDX + Mootdx + BaoStock)
uv run python scripts/unified_data_manager.py --apply --json --strict \
  --full-history --history-start 1990-12-19

# Disable all network SmartRouter supplementation (local validation/publish path)
uv run python scripts/unified_data_manager.py --apply --json --strict --no-router

# Validation only
uv run python scripts/unified_data_manager.py --validate-only --json --strict

# Generate a read-only data health report (coverage, date ranges, anomaly detection, repair boundaries)
uv run python scripts/data_health_report.py \
  --json-output ops/unified_data/data_health_latest.json \
  --markdown-output ops/unified_data/data_health_latest.md
```

The health report is a read-only audit: it inventories instrument classes (stocks / ETF-like / index-like), data domains (daily bars, valuation, fundamentals, exrights, calendar, status), active-universe coverage, date ranges, serving-view file counts, and detectable anomalies (duplicate keys, NULL OHLC, inconsistent OHLC, non-positive prices, negative volume/money, abnormal dates, metadata hygiene). It also classifies each issue as automatically repairable, source-refresh dependent, or unsafe to auto-repair. It does not fabricate values or silently rewrite the canonical database.

The QMT root defaults to `D:\君弘君智交易系统`; override it with `SIMTRADE_QMT_ROOT`. QMT freshness is not required: the manager probes a small set of index/liquid symbols through internet sources to discover the newest real bar date, supplements active symbols, and only then uses local QMT DAT as an optional cross-check/supplement. Optional maintained GitHub A-share tools can be installed with `uv pip install akshare adata efinance`; missing packages are skipped automatically. Route priority is `public → eastmoney → baostock → akshare → adata → efinance → mootdx` (the upstream TDX source stays as the last-resort local fallback), and every source writes to the same DuckDB. `--no-oss` removes only optional OSS adapters; `--no-router` disables all network supplementation.

Full-history mode calls `scripts/download.py` through `--full-history`: TDX `hsjday.zip` supplies long daily OHLCV, Mootdx supplies XDXR/financials, and BaoStock supplies valuation/status/calendar. The default start is `1990-12-19` (near A-share inception). `--history-start` sets `SIMTRADE_CN_HISTORY_START` and is propagated to TDX/Mootdx/BaoStock, the strict integrity gate and the health report; benchmark coverage follows the configured floor but is clamped to the default index's inception (CSI300: `2005-01-04`), since no source can supply index bars from before the index existed. Pre-floor exrights events are retained because adjusted-price factors for later bars need historical corporate actions. QMT local DAT is not used as a trusted 1990–2014 full-market OHLCV/financial/ETF source. Pre-2015 ETF, fundamental and valuation coverage is source-dependent and the reports do not claim parity with live PTrade platform entitlements.

Reports are written to `ops/unified_data/unified_*.json`; the latest published state is in `ops/unified_data/latest_status.json`. Apply mode CHECKPOINTs the WAL and runs the pre-publish data quality gate; it does **not** create DB or manifest byte-level snapshots (safety comes from the gate and idempotent upserts). Staged export replaces only DB-owned `stocks/`, `exrights/`, `fundamentals/`, `valuation/`, `metadata/` and `manifest.json`, preserving QMT-only `stocks_1m/`, `stocks_5m/` and `ptrade_adj_*.parquet`; the temporary promotion swap is deleted on success.

## Generated Data Structure

```
data/
├── cn.duckdb          # DuckDB database - A-shares (download source)
├── us.duckdb             # DuckDB database - US stocks (download source)
└── export/                      # Exported Parquet files (by market)
    ├── cn/                      # A-shares export
    │   ├── stocks/              # Daily bars (one file per stock)
    │   │   ├── 000001.SZ.parquet
    │   │   └── 600519.SS.parquet
    │   ├── exrights/            # Corporate action events
    │   ├── fundamentals/        # Quarterly financials (with TTM)
    │   ├── valuation/           # Valuation metrics (daily)
    │   ├── metadata/            # Metadata
    │   └── manifest.json
    └── us/                      # US stocks export
        ├── stocks/
        │   ├── AAPL.US.parquet
        │   └── MSFT.US.parquet
        ├── exrights/
        ├── fundamentals/
        ├── valuation/
        ├── metadata/
        └── manifest.json
```

## Prerequisites

- **Python**: 3.10 or higher
- **uv**: [Installation guide](https://docs.astral.sh/uv/getting-started/installation/)
- **Local CI only**: Node.js 20+ with npm; no `npm install` is needed
- **Network**: Required for downloading data from BaoStock/Mootdx/EastMoney/yfinance (China mainland network recommended for A-share data)

## Quick Start

### Option 1: Download Pre-Built Data (Recommended)

Download the latest data from [Releases](https://github.com/kay-ou/SimTradeData/releases):

- **A-shares**: `data-cn-v*` release → extract to `data/cn/`
- **US stocks**: `data-us-v*` release → extract to `data/us/`

```bash
npm run ui
```

Open the URL printed in the terminal (starting from **http://127.0.0.1:8765**, automatically skipping occupied ports). Optionally choose a fixed port:

```bash
npm run ui -- --port 8767
```

Click **Update backtesting data** to update stocks and ETFs together. Valid existing records are skipped; the UI shows live progress, cancellation and retries. Stock and ETF statistics are separate. Quarterly financials and ETF PCF appear under **Other data**.

The default database is `data/cn.duckdb`; use `--data-dir /path/to/data` for another directory. Completion is checked against stored data; download task history is not persisted. Startup installs frontend dependencies and builds only when needed; uv synchronizes Python dependencies. The first available port from 8765 is selected automatically; use the URL printed in the terminal. Switch Chinese, English or German in the header; the browser language is used initially and your manual choice is remembered.

After updating, use **Export backtest data** to generate and download CN or US Parquet packages for SimTradeLab / SimTradeDesk. Complete securities are skipped during smart updates; recent sessions can use small official daily packages instead of the historical ZIP. US downloads still use the CLI. See the [UI guide](docs/WEB_DATA_ADMIN.md) (Chinese).

## Use prebuilt data

Download `data-cn-v*` or `data-us-v*` from [Releases](https://github.com/kay-ou/SimTradeData/releases). Extract Parquet data into SimTradeLab’s `data/cn/` or `data/us/`. These packages are not editable DuckDB source databases.

## CLI download and export

For CLI-only use, install Python and uv, run `uv sync`, then:

```bash
uv run python scripts/download.py
uv run python scripts/export_parquet.py
```

Exports default to `data/export/cn/`. See the [CLI guide](docs/CLI_GUIDE.md) for US stocks, individual sources and export options. Avoid concurrent UI and CLI writes to the same database.

## Coverage and documentation

TDX supplies bulk daily history; Mootdx supports stock/ETF history queries, BaoStock supplies valuation/status/calendars, EastMoney supports selected fallback endpoints, and yfinance supplies US data. There are 223 mapped public financial fields; historical universes, publication-time semantics, L2 and some PCF data remain incomplete. Full PTrade equivalence is not guaranteed. Ordinary ETF price backtests do not require PCF.

| Need | Guide |
| --- | --- |
| Update, inspect and repair local data | [Web UI](docs/WEB_DATA_ADMIN.md) |
| US downloads, CLI and exports | [CLI guide](docs/CLI_GUIDE.md) |
| Available data and limitations | [Data coverage](docs/PTRADE_DATA_COVERAGE.md) |
| What is closed locally / needs an authorised source / is impossible | [Gap ledger](docs/PARITY_GAPS.md) |
| Reproducible performance baseline | [Performance](docs/benchmarks/PERFORMANCE.md) |
| CI, monitoring, publishing and recovery | [Operations](docs/OPERATIONS.md) |
| Schemas, financials and PCF | [Documentation index](docs/README.md) |

Detailed user and coverage guides are maintained in Chinese; operations documentation includes English. Provider health samples do not certify complete historical coverage.

See [CHANGELOG](CHANGELOG.md) for version history and [LICENSE](LICENSE) for AGPL-3.0 terms.

## Sponsor


If this project helps you, consider sponsoring!

| WeChat Pay | Alipay |
|:---:|:---:|
| <img src="docs/sponsor/WechatPay.png?raw=true" alt="WeChat Pay" width="200"> | <img src="docs/sponsor/AliPay.png?raw=true" alt="Alipay" width="200"> |

