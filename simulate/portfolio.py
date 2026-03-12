"""模拟盘持仓状态管理（JSON 持久化）"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _now() -> int:
    return int(time.time())


def _ts_dt(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(v, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


@dataclass
class PaperPosition:
    key: str            # f"{condition_id}-{outcome}"
    owner: str          # 触发开仓的目标地址
    condition_id: str
    outcome: str        # "YES" / "NO"
    token_id: str       # CLOB asset（用于后续查盘口）
    title: str
    shares: float
    entry_price: float  # 加权均价
    cost_usdc: float
    open_ts: int
    status: str = "open"   # open / closed / settled_win / settled_loss
    close_ts: Optional[int] = None
    close_price: Optional[float] = None
    pnl_usdc: Optional[float] = None


class Portfolio:
    def __init__(
        self,
        portfolio_path: str,
        conflicts_path: str,
        initial_capital: float = 10_000.0,
    ):
        self._ppath = Path(portfolio_path)
        self._cpath = Path(conflicts_path)
        self._cpath.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict = {}
        self._load(initial_capital)

    def _load(self, initial_capital: float):
        if self._ppath.exists():
            with open(self._ppath) as f:
                self._state = json.load(f)
        else:
            self._state = {
                "initial_capital": initial_capital,
                "cash": initial_capital,
                "realized_pnl": 0.0,
                "positions": {},    # key -> PaperPosition dict
                "trades": [],       # 所有成交记录
                "checkpoints": {},  # address -> last_seen_ts（轮询断点）
            }

    def save(self):
        self._ppath.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._ppath.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2, ensure_ascii=False)
        tmp.replace(self._ppath)  # POSIX 原子 rename，避免写到一半被 kill 导致 JSON 损坏

    # ── 只读查询 ──────────────────────────────────────────────────────

    @property
    def cash(self) -> float:
        return self._state["cash"]

    @property
    def initial_capital(self) -> float:
        return self._state["initial_capital"]

    @property
    def realized_pnl(self) -> float:
        return self._state["realized_pnl"]

    def nav(self) -> float:
        """NAV = 现金 + 所有开仓成本（保守估值，不实时查价）"""
        invested = sum(p.cost_usdc for p in self.open_positions())
        return self.cash + invested

    def open_positions(self) -> list[PaperPosition]:
        return [
            PaperPosition(**p)
            for p in self._state["positions"].values()
            if p["status"] == "open"
        ]

    def get_position(self, key: str) -> Optional[PaperPosition]:
        p = self._state["positions"].get(key)
        return PaperPosition(**p) if p else None

    def get_checkpoint(self, address: str) -> int:
        return self._state["checkpoints"].get(address, 0)

    def set_checkpoint(self, address: str, ts: int):
        self._state["checkpoints"][address] = ts
        self.save()

    # ── 开仓 / 加仓 ───────────────────────────────────────────────────

    def open_position(
        self,
        owner: str,
        condition_id: str,
        outcome: str,
        token_id: str,
        title: str,
        shares: float,
        entry_price: float,
        cost_usdc: float,
        signal_price: float,
    ) -> Optional[PaperPosition]:
        if cost_usdc > self.cash:
            return None

        key = f"{condition_id}-{outcome}"
        now_ts = _now()
        existing = self._state["positions"].get(key)

        if existing and existing["status"] == "open":
            # 加仓：更新加权均价
            old_cost   = existing["cost_usdc"]
            old_shares = existing["shares"]
            new_shares = old_shares + shares
            existing["entry_price"] = round((old_cost + cost_usdc) / new_shares, 6)
            existing["shares"]      = round(new_shares, 6)
            existing["cost_usdc"]   = round(old_cost + cost_usdc, 4)
        else:
            self._state["positions"][key] = asdict(PaperPosition(
                key=key,
                owner=owner,
                condition_id=condition_id,
                outcome=outcome,
                token_id=token_id,
                title=title,
                shares=round(shares, 6),
                entry_price=round(entry_price, 6),
                cost_usdc=round(cost_usdc, 4),
                open_ts=now_ts,
            ))

        self._state["cash"] = round(self._state["cash"] - cost_usdc, 4)
        self._append_trade("OPEN", key, shares, entry_price, cost_usdc, signal_price, None)
        self.save()
        return PaperPosition(**self._state["positions"][key])

    # ── 跟卖平仓 ──────────────────────────────────────────────────────

    def close_position(self, key: str, close_price: float) -> float:
        """按 CLOB bid 跟卖平仓，返回 PnL USDC"""
        p = self._state["positions"].get(key)
        if not p or p["status"] != "open":
            return 0.0

        pos      = PaperPosition(**p)
        proceeds = pos.shares * close_price
        pnl      = proceeds - pos.cost_usdc
        now_ts   = _now()

        p.update({
            "status":      "closed",
            "close_ts":    now_ts,
            "close_price": round(close_price, 6),
            "pnl_usdc":    round(pnl, 4),
        })
        self._state["cash"]         = round(self._state["cash"] + proceeds, 4)
        self._state["realized_pnl"] = round(self._state["realized_pnl"] + pnl, 4)
        self._append_trade("CLOSE", key, pos.shares, close_price, proceeds, close_price, round(pnl, 4))
        self.save()
        return pnl

    # ── Gamma 结算 ────────────────────────────────────────────────────

    def settle_position(self, key: str, won: bool) -> Optional[float]:
        """Gamma 结算：won=True 则兑付 1 USDC/份，返回 PnL；已非 open 则返回 None"""
        p = self._state["positions"].get(key)
        if not p or p["status"] != "open":
            return None

        pos         = PaperPosition(**p)
        close_price = 1.0 if won else 0.0
        proceeds    = pos.shares * close_price
        pnl         = proceeds - pos.cost_usdc
        now_ts      = _now()
        action      = "SETTLE_WIN" if won else "SETTLE_LOSS"

        p.update({
            "status":      "settled_win" if won else "settled_loss",
            "close_ts":    now_ts,
            "close_price": close_price,
            "pnl_usdc":    round(pnl, 4),
        })
        self._state["cash"]         = round(self._state["cash"] + proceeds, 4)
        self._state["realized_pnl"] = round(self._state["realized_pnl"] + pnl, 4)
        self._append_trade(action, key, pos.shares, close_price, proceeds, close_price, round(pnl, 4))
        self.save()
        return pnl

    # ── 冲突日志 ──────────────────────────────────────────────────────

    def log_conflict(self, data: dict):
        ts    = _now()
        entry = {"ts": ts, "dt": _ts_dt(ts), **data}
        with open(self._cpath, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # ── 内部工具 ──────────────────────────────────────────────────────

    def _append_trade(
        self,
        action: str,
        position_key: str,
        shares: float,
        price: float,
        usdc: float,
        signal_price: float,
        pnl: Optional[float],
    ):
        pos    = self._state["positions"].get(position_key, {})
        now_ts = _now()
        self._state["trades"].append({
            "ts":           now_ts,
            "dt":           _ts_dt(now_ts),
            "action":       action,
            "position_key": position_key,
            "owner":        pos.get("owner", ""),
            "condition_id": pos.get("condition_id", ""),
            "outcome":      pos.get("outcome", ""),
            "title":        pos.get("title", ""),
            "shares":       round(shares, 6),
            "price":        round(price, 6),
            "usdc":         round(usdc, 4),
            "signal_price": round(signal_price, 6),
            "pnl_usdc":     pnl,
        })

    # ── 统计汇总 ──────────────────────────────────────────────────────

    def stats(self) -> dict:
        all_closed = [
            p for p in self._state["positions"].values()
            if p["status"] in ("closed", "settled_win", "settled_loss")
        ]
        wins   = [p for p in all_closed if _safe_float(p.get("pnl_usdc")) > 0]
        losses = [p for p in all_closed if _safe_float(p.get("pnl_usdc")) <= 0]
        open_pos = self.open_positions()

        avg_win  = sum(_safe_float(p.get("pnl_usdc")) for p in wins)   / max(len(wins),   1)
        avg_loss = sum(_safe_float(p.get("pnl_usdc")) for p in losses) / max(len(losses), 1)

        return {
            "initial_capital":  self.initial_capital,
            "nav":              round(self.nav(), 2),
            "cash":             round(self.cash, 2),
            "realized_pnl":     round(self.realized_pnl, 2),
            "pnl_pct":          round(self.realized_pnl / self.initial_capital * 100, 2),
            "open_positions":   len(open_pos),
            "open_cost_usdc":   round(sum(p.cost_usdc for p in open_pos), 2),
            "resolved_count":   len(all_closed),
            "wins":             len(wins),
            "losses":           len(losses),
            "win_rate":         round(len(wins) / max(len(all_closed), 1), 4),
            "avg_win_usdc":     round(avg_win,  2),
            "avg_loss_usdc":    round(avg_loss, 2),
            "total_trades":     len(self._state["trades"]),
        }
