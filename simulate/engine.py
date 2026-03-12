"""跟单模拟盘主引擎：链上事件触发 + 轮询兜底 → 开/平仓 + 结算检测"""
from __future__ import annotations

import json
import queue
import signal
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from .api import PolyAPI
from .chain_listener import ChainListener
from .config import Config
from .portfolio import Portfolio


def _safe_float(v, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


class PaperEngine:
    def __init__(self, config: Config):
        self.cfg       = config
        self.api = PolyAPI(proxy=config.proxy)
        self.portfolio = Portfolio(
            portfolio_path=config.portfolio_file,
            conflicts_path=config.conflicts_log,
            initial_capital=config.initial_capital,
        )
        self.targets: list[dict] = []
        self.running = True
        signal.signal(signal.SIGINT,  self._stop)
        signal.signal(signal.SIGTERM, self._stop)

        # 链上事件队列（ChainListener → 主循环）
        self._chain_queue: queue.Queue = queue.Queue()
        # 待重试的用户地址 → {'target', 'deadline', 'last_retry'}
        self._pending_retries: dict[str, dict] = {}

    def _stop(self, *_):
        print("\n收到退出信号，停止中...")
        self.running = False

    def _log(self, msg: str):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def load_targets(self):
        with open(self.cfg.targets_file) as f:
            data = json.load(f)
        self.targets = data.get("targets", [])
        print(f"已加载 {len(self.targets)} 个跟单目标：")
        for t in self.targets:
            addr  = t["address"]
            label = t.get("label", addr[:10])
            print(f"  {label}  ({addr})")

    # ── 买入信号 ──────────────────────────────────────────────────────

    def _on_buy(self, trade: dict, target: dict):
        addr  = target["address"]
        label = target.get("label", addr[:10])

        condition_id = trade.get("conditionId", "")
        outcome      = (trade.get("outcome") or "").upper()
        token_id     = trade.get("asset", "")
        title        = (trade.get("title") or "")[:80]
        signal_price = _safe_float(trade.get("price"))
        target_usdc  = _safe_float(trade.get("size")) * signal_price

        if not condition_id or not outcome or not token_id:
            return

        key      = f"{condition_id}-{outcome}"
        existing = self.portfolio.get_position(key)

        # 该市场已有持仓（无论归属哪个目标）→ 只跟首次开仓，忽略加仓信号
        if existing and existing.status == "open":
            if existing.owner != addr:
                # 归属其他目标 → IGNORED_OPEN
                self.portfolio.log_conflict({
                    "type":                "IGNORED_OPEN",
                    "triggered_by":        addr,
                    "triggered_by_label":  label,
                    "owner":               existing.owner,
                    "condition_id":        condition_id,
                    "outcome":             outcome,
                    "title":               title,
                    "signal_price":        signal_price,
                    "signal_size_usdc":    round(target_usdc, 4),
                    "existing_entry_price": existing.entry_price,
                    "existing_cost_usdc":  existing.cost_usdc,
                    "existing_shares":     existing.shares,
                })
                self._log(
                    f"  ⚠️  IGNORED_OPEN [{label}] {title[:35]} "
                    f"(已由 {existing.owner[:10]}… 持仓 @ {existing.entry_price:.4f})"
                )
            # 同一目标的加仓信号 → 静默忽略（只跟首次）
            return

        # 查 CLOB best ask
        best_ask = self.api.get_clob_best_ask(token_id)
        if best_ask is None:
            self.portfolio.log_conflict({
                "type":             "SKIP_NO_CLOB",
                "triggered_by":     addr,
                "triggered_by_label": label,
                "side":             "BUY",
                "condition_id":     condition_id,
                "outcome":          outcome,
                "title":            title,
                "signal_price":     signal_price,
                "signal_size_usdc": round(target_usdc, 4),
            })
            self._log(f"  ⚠️  SKIP_NO_CLOB BUY [{label}] {title[:35]}")
            return

        # 计算仓位大小：min(目标本笔 × pct, NAV × pct)
        nav      = self.portfolio.nav()
        our_size = min(target_usdc * self.cfg.position_pct, nav * self.cfg.position_pct)
        if our_size <= 0:
            return

        shares = our_size / best_ask
        pos    = self.portfolio.open_position(
            owner=addr,
            condition_id=condition_id,
            outcome=outcome,
            token_id=token_id,
            title=title,
            shares=shares,
            entry_price=best_ask,
            cost_usdc=our_size,
            signal_price=signal_price,
        )

        if pos:
            self._log(
                f"  ✅ 开仓 [{outcome}] {title[:35]} "
                f"ask={best_ask:.4f} shares={shares:.2f} cost={our_size:.2f}U "
                f"(信号: {label} @ {signal_price:.4f}  目标本笔={target_usdc:.1f}U)"
            )
        else:
            self._log(f"  ❌ 开仓失败，资金不足 cash={self.portfolio.cash:.2f}U")

    # ── 卖出信号 ──────────────────────────────────────────────────────

    def _on_sell(self, trade: dict, target: dict):
        addr  = target["address"]
        label = target.get("label", addr[:10])

        condition_id = trade.get("conditionId", "")
        outcome      = (trade.get("outcome") or "").upper()
        token_id     = trade.get("asset", "")
        title        = (trade.get("title") or "")[:80]
        signal_price = _safe_float(trade.get("price"))

        if not condition_id or not outcome:
            return

        key = f"{condition_id}-{outcome}"
        pos = self.portfolio.get_position(key)

        # 无持仓
        if pos is None or pos.status != "open":
            status = pos.status if pos else "no_position"
            conflict_type = (
                "SELL_AFTER_SETTLE"
                if pos and "settled" in pos.status
                else "SELL_NO_POSITION"
            )
            self.portfolio.log_conflict({
                "type":               conflict_type,
                "triggered_by":       addr,
                "triggered_by_label": label,
                "condition_id":       condition_id,
                "outcome":            outcome,
                "title":              title,
                "signal_price":       signal_price,
                "position_status":    status,
            })
            self._log(f"  ⚠️  {conflict_type} [{label}] {title[:35]} (status: {status})")
            return

        # 持仓归属其他目标 → IGNORED_SELL（记录当时估值）
        if pos.owner != addr:
            best_bid   = self.api.get_clob_best_bid(token_id) if token_id else None
            mark_price = best_bid or signal_price
            unrealized = round(pos.shares * mark_price - pos.cost_usdc, 4)
            self.portfolio.log_conflict({
                "type":                    "IGNORED_SELL",
                "triggered_by":            addr,
                "triggered_by_label":      label,
                "owner":                   pos.owner,
                "condition_id":            condition_id,
                "outcome":                 outcome,
                "title":                   title,
                "signal_price":            signal_price,
                "clob_bid_at_signal":      best_bid,
                "position_cost_usdc":      pos.cost_usdc,
                "position_entry_price":    pos.entry_price,
                "position_shares":         pos.shares,
                "position_unrealized_pnl": unrealized,
            })
            self._log(
                f"  ⚠️  IGNORED_SELL [{label}] {title[:35]} "
                f"(持仓归属 {pos.owner[:10]}…  估值未实现={unrealized:+.2f}U)"
            )
            return

        # 查 CLOB best bid
        best_bid = self.api.get_clob_best_bid(token_id) if token_id else None
        if best_bid is None:
            self.portfolio.log_conflict({
                "type":               "SKIP_NO_CLOB",
                "triggered_by":       addr,
                "triggered_by_label": label,
                "side":               "SELL",
                "condition_id":       condition_id,
                "outcome":            outcome,
                "title":              title,
                "signal_price":       signal_price,
                "position_cost_usdc": pos.cost_usdc,
            })
            self._log(f"  ⚠️  SKIP_NO_CLOB SELL [{label}] {title[:35]}")
            return

        pnl   = self.portfolio.close_position(key, close_price=best_bid)
        emoji = "💰" if pnl > 0 else "💸"
        self._log(
            f"  {emoji} 跟卖平仓 [{outcome}] {title[:35]} "
            f"bid={best_bid:.4f} pnl={pnl:+.2f}U "
            f"(信号: {label} @ {signal_price:.4f})"
        )

    # ── 结算检测 ──────────────────────────────────────────────────────

    def _check_settlements(self):
        open_pos = self.portfolio.open_positions()
        if not open_pos:
            return

        # 按 condition_id 去重，同一市场只查一次 Gamma
        queried: dict[str, Optional[tuple[bool, Optional[str]]]] = {}

        for pos in open_pos:
            cid = pos.condition_id

            if cid not in queried:
                try:
                    market = self.api.get_market_by_condition(cid)
                    queried[cid] = self.api.parse_resolution(market) if market else (False, None)
                except Exception as e:
                    self._log(f"  [WARN] Gamma 查询失败 {cid[:12]}…: {e}")
                    queried[cid] = (False, None)

            is_resolved, winner = queried[cid]
            if not is_resolved:
                continue

            won = (winner.lower() == pos.outcome.lower()) if winner else False
            pnl = self.portfolio.settle_position(pos.key, won)
            if pnl is not None:
                emoji = "💰" if won else "💸"
                self._log(
                    f"  {emoji} 结算 [{pos.outcome}] {pos.title[:35]} "
                    f"胜方={winner}  {'WIN' if won else 'LOSS'}  pnl={pnl:+.2f}U"
                )

    # ── 主循环 ────────────────────────────────────────────────────────

    def _write_heartbeat(self):
        hb = Path(self.cfg.data_dir) / "heartbeat.json"
        hb.write_text(json.dumps({"ts": int(time.time()), "targets": len(self.targets)}))

    def _poll_once(self):
        self._write_heartbeat()
        for target in self.targets:
            if not self.running:
                break
            label = target.get("label", target["address"][:10])
            try:
                self._poll_target(target)
            except Exception as e:
                self._log(f"  [ERROR] 轮询 {label} 时异常: {e}")

    # ── 链上事件驱动处理 ─────────────────────────────────────────────────

    def _get_target_by_addr(self, addr: str) -> Optional[dict]:
        addr_lower = addr.lower()
        return next((t for t in self.targets if t["address"].lower() == addr_lower), None)

    def _process_chain_events(self):
        """
        消费链上事件队列，按用户去重后每用户只轮询一次。
        若 Data API 尚未索引（返回 0 笔），加入重试池。
        """
        now = time.time()

        # 1. 一次性排空队列，按用户去重（只保留最新的 block/recv_ts）
        triggered: dict[str, dict] = {}   # addr → {target, block, recv_ts, count}
        drained = 0
        while not self._chain_queue.empty():
            try:
                addr, tx, block, recv_ts = self._chain_queue.get_nowait()
            except queue.Empty:
                break
            drained += 1
            addr_lower = addr.lower()
            if addr_lower not in triggered:
                target = self._get_target_by_addr(addr)
                if not target:
                    continue
                triggered[addr_lower] = {
                    "target": target, "block": block,
                    "recv_ts": recv_ts, "count": 1,
                }
            else:
                entry = triggered[addr_lower]
                entry["count"] += 1
                if recv_ts > entry["recv_ts"]:
                    entry["block"]   = block
                    entry["recv_ts"] = recv_ts

        # 2. 每个触发用户只轮询一次
        for addr, info in triggered.items():
            target  = info["target"]
            label   = target.get("label", target["address"][:10])
            lag     = now - info["recv_ts"]
            n_evts  = info["count"]
            self._log(
                f"  ⚡ 链上触发 [{label}] block={info['block']} "
                f"(事件×{n_evts}, 延迟={lag*1000:.0f}ms)"
            )

            count = self._poll_target(target)
            if count == 0:
                # Data API 可能还没索引，加入重试池
                if addr not in self._pending_retries:
                    self._pending_retries[addr] = {
                        "target":     target,
                        "deadline":   now + self.cfg.chain_retry_window_sec,
                        "last_retry": now,
                    }

        if drained > len(triggered):
            self._log(
                f"  📦 队列排空: {drained} 事件 → {len(triggered)} 用户"
            )

        # 3. 处理重试池
        done = []
        for addr, info in self._pending_retries.items():
            if now > info["deadline"]:
                done.append(addr)
                continue
            if now - info["last_retry"] < self.cfg.chain_retry_interval_sec:
                continue

            info["last_retry"] = now
            target = info["target"]
            label  = target.get("label", addr[:10])
            count  = self._poll_target(target)
            if count > 0:
                self._log(f"  ✅ 重试成功 [{label}]，找到 {count} 笔新成交")
                done.append(addr)

        for addr in done:
            self._pending_retries.pop(addr, None)

    def _poll_target(self, target: dict) -> int:
        """轮询单个目标用户，返回本次处理的新成交笔数。"""
        addr    = target["address"]
        last_ts = self.portfolio.get_checkpoint(addr)

        new_trades = self.api.get_recent_trades(addr, after_ts=last_ts)
        if not new_trades:
            return 0

        now    = int(time.time())
        max_ts = last_ts
        count  = 0

        for trade in new_trades:
            ts = int(float(trade.get("timestamp") or 0))

            # ── 时间戳过滤：跳过超过 max_trade_age_sec 的旧单 ──────────
            age = now - ts
            if age > self.cfg.max_trade_age_sec:
                label = target.get("label", addr[:10])
                self._log(
                    f"  ⏭  SKIP_STALE [{label}] {(trade.get('title') or '')[:35]} "
                    f"(age={age}s > {self.cfg.max_trade_age_sec}s)"
                )
                max_ts = max(max_ts, ts)
                continue

            try:
                side = (trade.get("side") or "").upper()
                if side == "BUY":
                    self._on_buy(trade, target)
                elif side == "SELL":
                    self._on_sell(trade, target)
                count += 1
            except Exception as e:
                self._log(f"  [ERROR] 处理单笔成交异常: {e}  trade={trade}")
            finally:
                max_ts = max(max_ts, ts)

        if max_ts > last_ts:
            self.portfolio.set_checkpoint(addr, max_ts)

        return count

    def run(self):
        self.load_targets()
        if not self.targets:
            print("❌ 没有跟单目标，请先配置 copy_targets.json")
            return

        # ── 启动链上监听线程 ────────────────────────────────────────────
        chain_listener = ChainListener(
            target_addrs=[t["address"] for t in self.targets],
            event_queue=self._chain_queue,
        )
        chain_thread = chain_listener.run_in_thread()

        print(
            f"\n▶  模拟盘启动（链上实时 + 兜底轮询）\n"
            f"   初始资金   : {self.portfolio.initial_capital:,.2f} USDC\n"
            f"   仓位比例   : {self.cfg.position_pct:.1%}\n"
            f"   最大信号延迟: {self.cfg.max_trade_age_sec}s（超出则跳过）\n"
            f"   链上重试窗口: {self.cfg.chain_retry_window_sec}s\n"
            f"   兜底轮询间隔: {self.cfg.poll_interval_sec:.0f}s\n"
            f"   结算检查   : 每 {self.cfg.settlement_every_n} 轮\n"
        )

        poll_count   = 0
        last_poll_ts = 0.0

        while self.running:
            now = time.time()

            # 1. 处理链上事件（每 1s 检查一次）
            self._process_chain_events()

            # 2. 兜底轮询（每 poll_interval_sec 全量轮询一次）
            if now - last_poll_ts >= self.cfg.poll_interval_sec:
                self._poll_once()
                last_poll_ts = now
                poll_count  += 1

                if poll_count % self.cfg.settlement_every_n == 0:
                    self._check_settlements()
                    stats = self.portfolio.stats()
                    self._log(
                        f"📊 NAV={stats['nav']:,.2f}U  "
                        f"PnL={stats['realized_pnl']:+.2f}U({stats['pnl_pct']:+.1f}%)  "
                        f"开仓={stats['open_positions']}  "
                        f"W/L={stats['wins']}/{stats['losses']}  "
                        f"胜率={stats['win_rate']:.1%}"
                    )

            time.sleep(1.0)   # 主循环 1s tick

        # 退出前最后检查一次结算
        chain_listener.stop()
        self._check_settlements()
        stats = self.portfolio.stats()
        print(f"\n最终统计:\n{json.dumps(stats, indent=2, ensure_ascii=False)}")
