"""Writers package. DuckDB 只在用到 DuckDBWriter 时导入。"""

import importlib

__all__ = ["DuckDBWriter"]

_LAZY = {
    "DuckDBWriter": "simtradedata.writers.duckdb_writer",
}


def __getattr__(name: str):
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError("module 'simtradedata.writers' has no attribute %r" % name)
    module = importlib.import_module(module_name)
    return getattr(module, name)
