"""Read-only preflight facts before a full-history rebuild (no DB copies).

Earlier versions copied the 1.9 GB DuckDB and manifest before rebuilding. Those
byte-level snapshots were never consumed by any restore path, so they only
accumulated on disk. Safety now comes from the pre-publish data quality gate
plus idempotent upserts; this script records facts only.
"""
import json
import time
from pathlib import Path

import duckdb

root = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData")
ops = root / "ops" / "unified_data"
ops.mkdir(parents=True, exist_ok=True)
stamp = time.strftime("%Y%m%d_%H%M%S")
db = root / "data" / "cn.duckdb"
manifest = root / "data" / "cn" / "manifest.json"
out = {
    "stamp": stamp,
    "db": str(db),
    "manifest": str(manifest),
    "db_backup": None,
    "db_backup_note": "DB snapshots removed; no restore path consumed them",
    "db_size": db.stat().st_size,
}
con = duckdb.connect(str(db), read_only=True)
for table, col in [("stocks", "date"), ("valuation", "date"),
                   ("benchmark", "date"), ("trade_days", "date"),
                   ("exrights", "date")]:
    try:
        mn, mx, n = con.execute(
            f"SELECT MIN({col}), MAX({col}), COUNT(*) FROM {table}"
        ).fetchone()
        out[table] = {"min": str(mn), "max": str(mx), "rows": n}
    except Exception as exc:  # noqa: BLE001
        out[table] = {"error": str(exc)}
con.close()
(ops / "full_history_preflight.json").write_text(
    json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8"
)
print(json.dumps(out, ensure_ascii=False, indent=2))
