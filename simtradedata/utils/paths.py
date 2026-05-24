# -*- coding: utf-8 -*-
"""
Project path management

Provides unified path access for all project code.
"""

import gc
import shutil
import time
from pathlib import Path


_WINDOWS_SHARING_VIOLATION = 32
_RMTREE_RETRIES = 8
_RMTREE_RETRY_DELAY = 0.05


def get_project_root() -> Path:
    """Get project root directory

    Returns the project root directory from any location.

    Returns:
        Path object for project root
    """
    current = Path(__file__).resolve()

    # Find pyproject.toml going up
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent

    # Fallback: fixed level (current file in src/simtradelab/)
    return current.parent.parent.parent


def get_data_path() -> Path:
    """Get data directory path"""
    return get_project_root() / "data"


def get_strategies_path() -> Path:
    """Get strategies directory path"""
    return get_project_root() / "strategies"


# Convenient access
PROJECT_ROOT = get_project_root()
DATA_PATH = get_data_path()
STRATEGIES_PATH = get_strategies_path()

# DuckDB database paths
DUCKDB_PATH = DATA_PATH / "cn.duckdb"
US_DUCKDB_PATH = DATA_PATH / "us.duckdb"


def safe_rmtree(path: Path) -> None:
    """Remove a directory tree with basic safety guards.

    Refuses to delete top-level system paths to prevent catastrophic
    data loss from misconfigured output directories. Windows can briefly
    retain a handle on a just-written file (WinError 32); retry only that
    sharing violation a finite number of times.
    """
    resolved = path.resolve()
    dangerous = {Path("/"), Path.home()}
    if resolved in dangerous or len(resolved.parts) <= 2:
        raise ValueError(
            f"Refusing to delete unsafe path: {resolved}. "
            f"Check output_dir configuration."
        )
    for attempt in range(_RMTREE_RETRIES):
        try:
            shutil.rmtree(path)
            return
        except PermissionError as exc:
            if getattr(exc, "winerror", None) != _WINDOWS_SHARING_VIOLATION:
                raise
            if attempt == _RMTREE_RETRIES - 1:
                raise
            gc.collect()
            if not path.exists():
                return
            time.sleep(_RMTREE_RETRY_DELAY)

