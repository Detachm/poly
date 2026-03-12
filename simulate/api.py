"""Polymarket 公开 API 封装（Data API / CLOB / Gamma）"""
from __future__ import annotations

import json
import time
from typing import Optional

import requests

_DATA  = "https://data-api.polymarket.com"
_GAMMA = "https://gamma-api.polymarket.com"
_CLOB  = "https://clob.polymarket.com"


def _ts_int(v) -> int:
    """timestamp 字段可能是 int、float 或浮点字符串，统一转 int"""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _parse_json_field(val, default=None):
    """Gamma API 部分字段是 JSON 字符串，需要二次解析"""
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            pass
    return default if default is not None else []


_TIMEOUT_POLL    = (5, 15)   # 轮询成交用：快速失败，不阻塞主循环
_TIMEOUT_SETTLE  = (10, 30)  # 结算检测 / CLOB 查盘口：稍宽松


class PolyAPI:
    def __init__(self, proxy: Optional[str] = None):
        self.s = requests.Session()
        self.s.headers.update({"Accept": "application/json", "User-Agent": "PolyCopy/1.0"})
        if proxy:
            self.s.proxies = {"http": proxy, "https": proxy}

    def _get(self, url: str, params: dict | None = None,
             timeout: tuple = _TIMEOUT_SETTLE, retries: int = 3) -> list | dict:
        for attempt in range(retries):
            try:
                r = self.s.get(url, params=params, timeout=timeout)
                r.raise_for_status()
                return r.json() if r.text else []
            except Exception:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)
        return []

    # ── 用户成交 ──────────────────────────────────────────────────────

    def get_recent_trades(self, user: str, after_ts: int, limit: int = 100) -> list[dict]:
        """拉取 after_ts 之后的新成交，按时间升序返回"""
        try:
            trades = self._get(
                f"{_DATA}/trades",
                {"user": user, "limit": limit},
                timeout=_TIMEOUT_POLL,   # 轮询用短超时
            )
            if not isinstance(trades, list):
                return []
            new = [t for t in trades if _ts_int(t.get("timestamp")) > after_ts]
            return sorted(new, key=lambda t: _ts_int(t.get("timestamp")))
        except Exception:
            return []

    # ── CLOB 盘口 ─────────────────────────────────────────────────────

    def _get_book(self, token_id: str) -> dict:
        try:
            return self._get(f"{_CLOB}/book", {"token_id": token_id})
        except Exception:
            return {}

    def get_clob_best_ask(self, token_id: str) -> Optional[float]:
        """最优卖价（用于模拟买入执行）"""
        book = self._get_book(token_id)
        asks = book.get("asks", [])
        if asks:
            try:
                return float(asks[0]["price"])
            except (KeyError, ValueError, IndexError):
                pass
        return None

    def get_clob_best_bid(self, token_id: str) -> Optional[float]:
        """最优买价（用于模拟卖出执行）"""
        book = self._get_book(token_id)
        bids = book.get("bids", [])
        if bids:
            try:
                return float(bids[0]["price"])
            except (KeyError, ValueError, IndexError):
                pass
        return None

    # ── Gamma 市场结算状态 ────────────────────────────────────────────

    def get_market_by_condition(self, condition_id: str) -> Optional[dict]:
        """按 conditionId 查询市场信息，失败返回 None"""
        try:
            result = self._get(f"{_GAMMA}/markets", {"condition_id": condition_id})
            if isinstance(result, list) and result:
                return result[0]
            if isinstance(result, dict):
                return result
        except Exception:
            pass
        return None

    def parse_resolution(self, market: dict) -> tuple[bool, Optional[str]]:
        """
        解析市场是否已结算及胜方结局名称（如 "Yes"/"No"）。
        返回 (is_resolved, winning_outcome_str)
        只依赖 closed/resolved 字段，不用 active（active=False 仅表示停止下注，不代表已公布结果）
        """
        closed = market.get("closed") or market.get("resolved")
        if not closed:
            return False, None

        outcomes = _parse_json_field(market.get("outcomes"))
        prices   = _parse_json_field(market.get("outcomePrices"))

        if not outcomes or not prices or len(outcomes) != len(prices):
            return False, None

        for i, p in enumerate(prices):
            try:
                if float(p) >= 0.99:
                    return True, str(outcomes[i])
            except (ValueError, TypeError):
                pass

        return False, None
