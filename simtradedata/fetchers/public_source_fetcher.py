# -*- coding: utf-8 -*-
"""腾讯行情与新浪报表。接口选择来自 source_registry.RANKED，请求是自己写的。"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request

import pandas as pd

from simtradedata.fetchers.base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

_UA = "Mozilla/5.0"
_HOSTS = (
    "https://web.ifzq.gtimg.cn",
    "https://proxy.finance.qq.com/ifzqgtimg",
    "https://ifzq.gtimg.cn",
)
_SINA = "https://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReport2022"
_SINA_KLINE = (
    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "CN_MarketData.getKLineData"
)
_STATEMENT_FIELDS = (
    "dividend_interest_payment",
    "net_operate_cash_flow",
    "oper_cash_ps_grow_rate",
    "np_parent_company_cut",
    "np_parent_company_cut_yoy",
    "net_operate_cash_flow_ttm",
    "net_operate_cash_flow_ps_ttm",
)
_FREQ = {0: "m5", 1: "m15", 2: "m30", 3: "m60", 7: "m1"}
_DIVIDEND_TITLES = (
    "分配股利、利润或偿付利息支付的现金",
    "分配股利、利润或偿付利息所支付的现金",
)
_OCF_TITLES = ("经营活动产生的现金流量净额",)
_CUT_TITLES = (
    "扣除非经常性损益后的净利润",
    "归属于母公司股东的扣除非经常性损益的净利润",
)


def paper_code(symbol: str) -> str:
    """600000.SS / 600000.XSHG / 600000.SH -> sh600000。"""
    raw = str(symbol).split(".")[0]
    suffix = str(symbol).rsplit(".", 1)[-1].upper() if "." in str(symbol) else ""
    if suffix in {"SS", "XSHG", "SH"} or raw.startswith(("5", "6", "9")):
        prefix = "sh"
    else:
        prefix = "sz"
    return prefix + raw[-6:]


def _get_json(url: str, params: dict) -> dict:
    payload = _load_json(url, params)
    return payload if isinstance(payload, dict) else {}


def _num(value):
    if value is None or value == "" or isinstance(value, (dict, list)):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _load_json(url: str, params: dict):
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=20) as resp:
        text = resp.read().decode("utf-8", "replace").strip()
    if text.startswith("null") or text == "":
        return None
    if not text.startswith("{") and not text.startswith("["):
        left = text.find("(")
        right = text.rfind(")")
        if left != -1 and right > left:
            text = text[left + 1:right]
    return json.loads(text)


def collect_paged_bars(fetch_page, start, end, max_pages: int = 40) -> list:
    """按结束日往前翻页。同一页重复出现就停，避免接口忽略截止日期时死循环。"""
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    cursor = end_ts.strftime("%Y-%m-%d")
    chosen = {}
    previous_oldest = None
    for _ in range(max_pages):
        bars = fetch_page(cursor) or []
        if not bars:
            break
        oldest = None
        for bar in bars:
            if not bar or len(bar) < 6:
                continue
            day = pd.Timestamp(str(bar[0])[:10])
            if oldest is None or day < oldest:
                oldest = day
            if start_ts <= day <= end_ts:
                chosen[day] = bar
        if oldest is None or oldest <= start_ts:
            break
        if previous_oldest is not None and oldest >= previous_oldest:
            break
        previous_oldest = oldest
        cursor = (oldest - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return [chosen[day] for day in sorted(chosen)]


def statement_on_date(statement: pd.DataFrame, quarter_end) -> dict:
    """取出某一报告期上有数的财务报表字段。没有这一期就返回空字典。"""
    if statement is None or statement.empty or "end_date" not in statement.columns:
        return {}
    end = pd.Timestamp(quarter_end)
    work = statement.copy()
    work["end_date"] = pd.to_datetime(work["end_date"])
    hit = work.loc[work["end_date"] == end]
    if hit.empty:
        return {}
    row = hit.iloc[-1]
    out = {}
    for name in _STATEMENT_FIELDS:
        if name not in hit.columns:
            continue
        value = row[name]
        if value is None or pd.isna(value):
            continue
        out[name] = float(value)
    return out


class PublicSourceFetcher(BaseFetcher):
    """不登录。腾讯负责行情和场内基金，新浪负责财务报表。"""

    source_name = "public"

    def _do_login(self):
        return None

    def _do_logout(self):
        return None

    def _tencent(self, path: str, param: str) -> dict:
        last = None
        symbol_error = None
        for host in _HOSTS:
            try:
                payload = _get_json(host + path, {"param": param})
            except Exception as exc:  # noqa: BLE001
                last = exc
                continue
            if payload.get("data"):
                return payload
            symbol_error = payload
        if last and symbol_error is None:
            raise RuntimeError("腾讯行情不可用: %s" % last)
        return symbol_error or {}

    def _day_page(self, code: str, cursor: str) -> list:
        payload = self._tencent(
            "/appstock/app/kline/kline",
            "%s,day,,%s,320," % (code, cursor),
        )
        node = (payload.get("data") or {}).get(code) or {}
        # 只要不复权的 day。qfqday 是前复权，不能写进价格序列。
        return node.get("day") or []

    def _bars_to_frame(self, code: str, bars: list) -> pd.DataFrame:
        share_volume = code[2:5] in {"688", "689"}
        rows = []
        for bar in bars:
            volume = float(bar[5])
            if not share_volume:
                volume *= 100.0
            rows.append({
                "date": pd.Timestamp(str(bar[0])[:10]),
                "open": float(bar[1]),
                "close": float(bar[2]),
                "high": float(bar[3]),
                "low": float(bar[4]),
                "volume": volume,
                "amount": _num(bar[6]) if len(bar) > 6 else None,
            })
        frame = pd.DataFrame(rows)
        if "amount" in frame.columns and frame["amount"].isna().all():
            frame = frame.drop(columns=["amount"])
        return frame

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """不复权日线。单次大约 320 根，按结束日往前翻，直到覆盖 start_date。成交量换成股。"""
        code = paper_code(symbol)
        bars = collect_paged_bars(
            lambda cursor: self._day_page(code, cursor),
            start_date,
            end_date,
        )
        if not bars:
            return pd.DataFrame()
        return self._bars_to_frame(code, bars)

    def fetch_sina_daily(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """新浪不复权日线，只用来对收盘价。窗口大约最近 1023 根。"""
        code = paper_code(symbol)
        span = max(10, (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days)
        datalen = min(1023, span + 10)
        payload = _load_json(_SINA_KLINE, {
            "symbol": code,
            "scale": "240",
            "ma": "no",
            "datalen": str(datalen),
        })
        if isinstance(payload, dict):
            payload = payload.get("data") or payload.get("result") or []
        if not isinstance(payload, list):
            return pd.DataFrame()
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        rows = []
        for item in payload:
            if not isinstance(item, dict) or "day" not in item or "close" not in item:
                continue
            day = pd.Timestamp(item["day"])
            if day < start or day > end:
                continue
            close = _num(item.get("close"))
            if close is None:
                continue
            rows.append({"date": day, "close": close})
        return pd.DataFrame(rows)

    def fetch_reconciled_daily(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """腾讯日线，再用新浪收盘价剔除对不上的日期。新浪没有的日期保留。"""
        from simtradedata.validators.crosscheck import keep_agreed_bars

        left = self.fetch_daily_bars(symbol, start_date, end_date)
        if left.empty:
            return left
        try:
            right = self.fetch_sina_daily(symbol, start_date, end_date)
        except Exception as exc:  # noqa: BLE001
            logger.warning("新浪日线对账不可用，保留腾讯: %s", exc)
            return left
        return keep_agreed_bars(left, right)

    def fetch_minute_bars(self, symbol: str, frequency: int = 1, offset: int = 320) -> pd.DataFrame:
        """分钟线。frequency 沿用路由器：0=5分 1=15分 2=30分 3=60分 7=1分。"""
        period = _FREQ.get(int(frequency))
        if period is None:
            raise ValueError("不支持的分钟周期: %s" % frequency)
        code = paper_code(symbol)
        count = max(1, min(int(offset or 320), 800))
        payload = self._tencent(
            "/appstock/app/kline/mkline",
            "%s,%s,,,%s" % (code, period, count),
        )
        node = (payload.get("data") or {}).get(code) or {}
        bars = node.get(period) or []
        share_volume = code[2:5] in {"688", "689"}
        rows = []
        for bar in bars:
            if len(bar) < 6:
                continue
            volume = float(bar[5])
            if not share_volume:
                volume *= 100.0
            rows.append({
                "datetime": bar[0],
                "open": float(bar[1]),
                "close": float(bar[2]),
                "high": float(bar[3]),
                "low": float(bar[4]),
                "volume": volume,
            })
        return pd.DataFrame(rows)

    def fetch_fund_bars(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """场内基金、ETF 与股票共用腾讯日线。"""
        return self.fetch_reconciled_daily(symbol, start_date, end_date)

    def _statement(self, symbol: str, report_type: str, num: int = 24) -> list[dict]:
        payload = _get_json(_SINA, {
            "paperCode": paper_code(symbol),
            "source": report_type,
            "type": "0",
            "page": "1",
            "num": str(num),
        })
        report_list = ((payload.get("result") or {}).get("data") or {}).get("report_list") or {}
        rows = []
        for period in sorted(report_list.keys(), reverse=True)[:num]:
            obj = report_list[period] or {}
            rec = {"end_date": "%s-%s-%s" % (period[:4], period[4:6], period[6:8])}
            for item in obj.get("data") or []:
                title = item.get("item_title") or ""
                if title and item.get("item_value") is not None:
                    rec[title] = item.get("item_value")
                    yoy = item.get("item_tongbi")
                    if yoy not in (None, ""):
                        rec[title + "_同比"] = yoy
            rows.append(rec)
        return rows

    @staticmethod
    def _pick(row: dict, titles: tuple[str, ...]):
        for title in titles:
            if title in row:
                return _num(row[title]), _num(row.get(title + "_同比"))
        return None, None

    def fetch_fundamentals(self, symbol: str, periods: int = 24) -> pd.DataFrame:
        """新浪报表收成 PTrade 字段。没有的列留空，不用别的科目顶替。

        经营现金流同比优先用报表自带的同比，没有再用四个季度前的数自己算。
        每股 TTM 需要总股本，这里只保留四个季度现金流之和，不除股本。
        """
        cash = self._statement(symbol, "llb", periods)
        income = {row["end_date"]: row for row in self._statement(symbol, "lrb", periods)}
        out = []
        for row in cash:
            end = row["end_date"]
            div, _ = self._pick(row, _DIVIDEND_TITLES)
            ocf, ocf_yoy = self._pick(row, _OCF_TITLES)
            cut, cut_yoy = self._pick(income.get(end) or {}, _CUT_TITLES)
            out.append({
                "end_date": end,
                "publ_date": None,
                "dividend_interest_payment": div,
                "net_operate_cash_flow": ocf,
                "oper_cash_ps_grow_rate": ocf_yoy,
                "np_parent_company_cut": cut,
                "np_parent_company_cut_yoy": cut_yoy,
            })
        df = pd.DataFrame(out)
        if df.empty:
            return df
        df["end_date"] = pd.to_datetime(df["end_date"])
        df = df.sort_values("end_date")
        prev = df["net_operate_cash_flow"].shift(4)
        derived = (df["net_operate_cash_flow"] / prev - 1.0) * 100.0
        derived = derived.where(prev.notna() & (prev != 0))
        df["oper_cash_ps_grow_rate"] = df["oper_cash_ps_grow_rate"].where(
            df["oper_cash_ps_grow_rate"].notna(), derived
        )
        df["net_operate_cash_flow_ttm"] = df["net_operate_cash_flow"].rolling(4).sum()
        df["net_operate_cash_flow_ps_ttm"] = None
        return df.reset_index(drop=True)

    def fetch_fundamentals_for_quarter(self, year: int, quarter: int) -> pd.DataFrame:
        """全市场按季度批量拉取不在这一层做。调用方按股票拉 fetch_fundamentals。"""
        raise NotImplementedError("新浪报表按股票拉取，不用全市场季度批量")
