# -*- coding: utf-8 -*-
"""Optional adapters for actively maintained open-source A-share data tools.

The adapters are intentionally lazy: SimTradeData already has first-party
``public`` / ``eastmoney`` / ``baostock`` fetchers.  These classes only become
active when the corresponding package is installed, so optional dependencies do
not break offline or minimal deployments.

Supported projects (used as cross-source supplements, not as a second database):

* AKShare — https://github.com/akfamily/akshare
* ADATA — https://github.com/1nchaos/adata
* efinance — https://github.com/Micro-sheep/efinance

All adapters normalize to SimTradeData's unadjusted daily-bar contract:
``date, open, high, low, close, volume, amount``.  Prices are never adjusted
here; PTrade/SimTradeLab applies its own adjustment logic during reads.
"""

from __future__ import annotations

import importlib
import logging
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from simtradedata.fetchers.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

_STANDARD_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]


def _plain_code(symbol: str) -> str:
    return str(symbol).split(".", 1)[0][-6:]


def _normalize_daily(df: pd.DataFrame | None, start_date: str, end_date: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=_STANDARD_COLUMNS)

    work = df.copy()
    work.columns = [str(c).strip().lower() for c in work.columns]
    aliases = {
        "日期": "date", "开盘": "open", "最高": "high", "最低": "low",
        "收盘": "close", "成交量": "volume", "成交额": "amount",
        "trade_date": "date", "day": "date", "vol": "volume",
        "turnover": "amount", "money": "amount", "close_price": "close",
        "open_price": "open", "high_price": "high", "low_price": "low",
    }
    work = work.rename(columns=aliases)
    if "date" not in work.columns:
        date_like = next((c for c in work.columns if "date" in c or "time" in c), None)
        if date_like:
            work = work.rename(columns={date_like: "date"})
    if "date" not in work.columns:
        return pd.DataFrame(columns=_STANDARD_COLUMNS)

    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"])
    start, end = pd.Timestamp(start_date), pd.Timestamp(end_date)
    work = work[(work["date"] >= start) & (work["date"] <= end)].copy()
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
        else:
            work[col] = pd.NA
    work = work.dropna(subset=["open", "high", "low", "close"])
    work = work[(work[["open", "high", "low", "close"]] > 0).all(axis=1)]
    work = work.sort_values("date").drop_duplicates("date", keep="last")
    return work[_STANDARD_COLUMNS].reset_index(drop=True)


def _date_args(start_date: str, end_date: str) -> tuple[str, str]:
    return pd.Timestamp(start_date).strftime("%Y%m%d"), pd.Timestamp(end_date).strftime("%Y%m%d")


class _OptionalDailyFetcher(BaseFetcher):
    package = ""
    source_name = ""

    def _do_login(self) -> None:
        return None

    def _do_logout(self) -> None:
        return None

    def _module(self):
        return importlib.import_module(self.package)

    def _fetch(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        @self._retry_with_backoff()
        def _call():
            return _normalize_daily(self._fetch(symbol, start_date, end_date), start_date, end_date)

        return _call()

    def fetch_reconciled_daily(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.fetch_daily_bars(symbol, start_date, end_date)

    def fetch_fund_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.fetch_daily_bars(symbol, start_date, end_date)


class AKShareFetcher(_OptionalDailyFetcher):
    source_name = "akshare"
    package = "akshare"

    def _fetch(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        ak = self._module()
        start, end = _date_args(start_date, end_date)
        func = getattr(ak, "stock_zh_a_hist_em", None) or getattr(ak, "stock_zh_a_hist")
        return func(
            symbol=_plain_code(symbol),
            period="daily",
            start_date=start,
            end_date=end,
            adjust="",
        )


class ADataFetcher(_OptionalDailyFetcher):
    source_name = "adata"
    package = "adata"

    def _fetch(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        adata = self._module()
        start, end = _date_args(start_date, end_date)
        candidates: list[tuple[Any, dict[str, Any]]] = []
        stock = getattr(adata, "stock", None)
        if stock is not None:
            market = getattr(stock, "market", None)
            if market is not None and hasattr(market, "get_market"):
                candidates.append((market.get_market, {
                    "stock_code": _plain_code(symbol),
                    "start_date": start,
                    "end_date": end,
                    "k_type": 1,
                }))
            info = getattr(stock, "info", None)
            if info is not None and hasattr(info, "get_history_kline"):
                candidates.append((info.get_history_kline, {
                    "stock_code": _plain_code(symbol),
                    "start_date": start,
                    "end_date": end,
                }))
        for func, kwargs in candidates:
            try:
                return func(**kwargs)
            except TypeError:
                try:
                    return func(_plain_code(symbol), start, end)
                except Exception:
                    continue
            except Exception as exc:  # noqa: BLE001
                logger.debug("ADATA candidate failed: %s", exc)
        raise RuntimeError("ADATA daily K-line interface is unavailable")


class EfinanceFetcher(_OptionalDailyFetcher):
    source_name = "efinance"
    package = "efinance"

    def _fetch(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        ef = self._module()
        start, end = _date_args(start_date, end_date)
        stock = getattr(ef, "stock", ef)
        func = getattr(stock, "get_quote_history", None)
        if func is None:
            raise RuntimeError("efinance.stock.get_quote_history is unavailable")
        kwargs = {
            "security_codes": [_plain_code(symbol)],
            "beg": start,
            "end": end,
            "adjust": "none",
        }
        try:
            result = func(**kwargs)
        except TypeError:
            result = func(_plain_code(symbol), beg=start, end=end)
        if isinstance(result, dict):
            result = result.get(_plain_code(symbol), pd.DataFrame())
        return result
