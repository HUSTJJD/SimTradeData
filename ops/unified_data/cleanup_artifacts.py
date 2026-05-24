"""One-shot removal of ALL derived backup copies (user-confirmed).

Deletes: staging dirs, every ``_backup_*`` dir under data/cn, promote swap
dirs, DB backup files, ``*.bak`` and old manifest copies. Source caches
(hsjday.zip, quarter ZIPs) are preserved — they are reused, not duplicated.
"""
import shutil
from pathlib import Path

DATA = Path(r"D:\learning\QuantificationResearch\PTrade\SimTradeData\data")
VIEW = DATA / "cn"


def size_of(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main() -> None:
    freed = 0
    removed = 0

    def drop_dir(path: Path) -> None:
        nonlocal freed, removed
        if path.is_dir():
            freed += size_of(path)
            shutil.rmtree(path, ignore_errors=True)
            removed += 1

    def drop_file(path: Path) -> None:
        nonlocal freed, removed
        if path.is_file():
            freed += size_of(path)
            try:
                path.unlink()
                removed += 1
            except OSError:
                return

    for p in DATA.glob("cn_staging_*"):
        drop_dir(p)
    for p in DATA.glob("_promote_swap_*"):
        drop_dir(p)
    for p in VIEW.glob("_backup_*"):
        drop_dir(p)
    for pattern in ("cn.duckdb.unified_backup_*", "cn.duckdb.pre_full_history_*"):
        for p in DATA.glob(pattern):
            drop_file(p)
    for pattern in ("*.bak", "*.promote.tmp", "manifest.pre_full_history_*.json",
                    "manifest.unified_backup_*.json"):
        for p in VIEW.glob(pattern):
            drop_file(p)

    print(f"removed_entries={removed} freed_MB={freed / 1024 / 1024:.1f}")
    print("remaining _backup_* dirs:", [p.name for p in VIEW.glob("_backup_*")])
    print("remaining staging/swaps:",
          [p.name for p in DATA.glob("cn_staging_*")] + [p.name for p in DATA.glob("_promote_swap_*")])
    print("remaining db backups:",
          [p.name for p in DATA.glob("cn.duckdb.*backup*")] + [p.name for p in DATA.glob("cn.duckdb.pre_full_history_*")])


if __name__ == "__main__":
    main()
