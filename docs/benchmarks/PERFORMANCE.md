# Performance baseline

Reproducible, offline measurement of the financial import optimisation.

- Generated (UTC): `2026-11-27T14:31:01+00:00`
- Input: `gpcw20260930.zip` (5,493,940 bytes, sha256 `3a2a589608d46e21a0a886f3138725995a601125bc3668937e43536cc697d5f1`)
- Runtime: Python 3.11.18 / duckdb 1.5.6
- Companies: 100, rows written: 100

| Stage | Seconds |
|---|---:|
| parse_local | 1.6864 |
| _convert_to_ptrade_format | 0.0727 |
| write_fundamentals (individual) | 189.5745 |
| write_fundamentals_batch | 0.8987 |

- Speedup (individual / batch): **210.941×**
- Data equality asserted: `True`

Reproduce:

```bash
uv run --frozen python scripts/benchmark_financial_import.py \
  data/mootdx_financial/gpcw20260930.zip --companies 100 \
  --md-output docs/benchmarks/PERFORMANCE.md
```

## Historical observations (not a controlled A/B)

The publish pipeline logs below were captured on 2026-11-27 under
different workloads. The audit recorded ~507 s for a full export and
~27 s for an incremental export reusing 7,714 / 5,621 / 5,245 / 5,813
Parquet files. These are observations, not a reproducible speedup claim.

- full export: `path_full.log` (sha256 `da20dde7306ade04…`, 36769 bytes)
- incremental export: `path_incremental2.log` (sha256 `494063c54ce18457…`, 33199 bytes)

The reproducible measurement above is the individual-vs-bulk
financial import; re-run the documented command to refresh it.
