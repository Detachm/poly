#!/usr/bin/env python3
"""
0x8dxd Tracker：轮询 Polymarket Data API 获取指定地址的成交/活动/持仓，用于打标签与验证模型。
与其它录制器统一使用 local_receipt_ts_ms（本地接收时间戳）。

修复记录：
- Fix1: checkpoint 持久化 seen_hashes（transactionHash），重启后加载，避免内存无限增长和重复写入
- Fix2: 分页追溯，直到碰到已知 hash 或空页，防止高频期漏数据
- Fix3: 每次 poll 写 heartbeat.jsonl，解决 healthcheck 在无新成交时误报
- Fix4: 连续失败告警不再重置计数器
- Fix5: 代理默认值改为 127.0.0.1（Linux host 网络下 host.docker.internal 不可用）

增强记录：
- Enh1: 新增 Activity 轮询（REDEEM/TRADE/REWARD 等结算事件）
- Enh2: 新增 Positions 快照（仅在内容变化时写入）
- Enh3: 交易轮询频率 30s → 10s，三个端点独立轮询间隔
- Enh4: checkpoint 向后兼容旧格式
"""

import hashlib
import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import requests

DATA_API_TRADES = "https://data-api.polymarket.com/trades"
DATA_API_ACTIVITY = "https://data-api.polymarket.com/activity"
DATA_API_POSITIONS = "https://data-api.polymarket.com/positions"
TRACKER_ADDRESS_ENV = "TRACKER_USER_ADDRESS"
POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")
DEFAULT_ADDRESS = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"

# checkpoint 保留最近多少条 hash（内存 + 磁盘上限）
_SEEN_MAX = 200
# 每次 poll 最多翻多少页（防止首次启动拉取过多历史）
_MAX_PAGES = 10


def local_ts_ms() -> int:
    return int(time.time() * 1000)


class Tracker0x8dxd:
    def __init__(
        self,
        user_address: str = None,
        output_dir: str = None,
        poll_interval_sec: float = 10.0,
        activity_interval_sec: float = 30.0,
        positions_interval_sec: float = 60.0,
    ):
        self.user_address = (user_address or os.environ.get(TRACKER_ADDRESS_ENV) or DEFAULT_ADDRESS).strip()
        _out = output_dir or os.path.join(POLY_DATA_DIR, "tracker_0x8dxd")
        self.output_dir = Path(_out)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.poll_interval_sec = poll_interval_sec
        self.activity_interval_sec = activity_interval_sec
        self.positions_interval_sec = positions_interval_sec
        self.running = True

        # 去重状态：trades
        self._trade_seen: set = set()
        self._trade_seen_order: deque = deque(maxlen=_SEEN_MAX)
        # 去重状态：activity
        self._activity_seen: set = set()
        self._activity_seen_order: deque = deque(maxlen=_SEEN_MAX)
        # positions 变化检测
        self._positions_hash: str = ""

        self._consecutive_failures = 0
        self._checkpoint_path = self.output_dir / ".checkpoint.json"
        self._load_checkpoint()
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "Poly-Tracker/1.0"})
        # Fix5: 默认代理改为 127.0.0.1，host 网络模式下 host.docker.internal 在 Linux 上不可用
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "http://127.0.0.1:7897"
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    # ── checkpoint ────────────────────────────────────────────────────────────

    def _load_checkpoint(self):
        """启动时从磁盘恢复 seen_hashes，避免重启后重复写入。向后兼容旧格式。"""
        if not self._checkpoint_path.exists():
            return
        try:
            data = json.loads(self._checkpoint_path.read_text(encoding="utf-8"))
            # 向后兼容：优先读 trade_seen_hashes，fallback 到旧的 seen_hashes
            trade_hashes = data.get("trade_seen_hashes") or data.get("seen_hashes") or []
            for h in trade_hashes:
                if h and h not in self._trade_seen:
                    self._trade_seen.add(h)
                    self._trade_seen_order.append(h)
            # activity 去重
            for h in data.get("activity_seen_hashes", []):
                if h and h not in self._activity_seen:
                    self._activity_seen.add(h)
                    self._activity_seen_order.append(h)
            # positions hash
            self._positions_hash = data.get("positions_hash", "")
            print(f"[{datetime.now().isoformat()}] checkpoint 已加载，trade_seen={len(self._trade_seen)}, activity_seen={len(self._activity_seen)} hashes")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] checkpoint 加载失败（忽略）: {e}")

    def _save_checkpoint(self):
        """将最近 _SEEN_MAX 个 hash 原子写回磁盘。"""
        try:
            payload = {
                "trade_seen_hashes": list(self._trade_seen_order),
                "activity_seen_hashes": list(self._activity_seen_order),
                "positions_hash": self._positions_hash,
            }
            tmp = self._checkpoint_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self._checkpoint_path)
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] checkpoint 保存失败: {e}")

    def _mark_seen(self, tx_hash: str, seen_set: set, seen_order: deque):
        """记录一条 hash，维护 deque 上限，同时更新 set。"""
        if tx_hash in seen_set:
            return
        if len(seen_order) >= _SEEN_MAX:
            oldest = seen_order[0]  # deque maxlen 自动弹出，但 set 需要手动清理
            seen_set.discard(oldest)
        seen_set.add(tx_hash)
        seen_order.append(tx_hash)

    # ── API ──────────────────────────────────────────────────────────────────

    def _fetch_page(self, url: str, params: dict, timeout: int = 15):
        """成功返回 list，失败返回 None。"""
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json() if r.text else []
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] API 请求失败 ({url}, params={params}): {e}")
            return None

    def _fetch_new_trades(self):
        """
        Fix2: 分页追溯，收集所有新 trade。
        返回 (new_trades_list, api_ok)。
        api_ok=False 表示第一页请求失败。
        """
        new_trades = []
        for page in range(_MAX_PAGES):
            offset = page * 100
            trades = self._fetch_page(
                DATA_API_TRADES,
                params={"user": self.user_address, "limit": 100, "offset": offset},
            )
            if trades is None:
                if page == 0:
                    return [], False  # 第一页失败，记为 API 错误
                break  # 中间页失败，停止翻页，使用已收集的
            if not trades:
                break  # 无更多数据

            found_known = False
            for t in trades:
                tx = t.get("transactionHash") or ""
                if tx in self._trade_seen:
                    found_known = True
                else:
                    new_trades.append(t)

            if found_known:
                break  # 碰到已知 hash，说明已追上，停止翻页

        return new_trades, True

    def _fetch_new_activity(self):
        """
        与 _fetch_new_trades 同构，拉取 activity 端点。
        返回 (new_items, api_ok)。
        """
        new_items = []
        for page in range(_MAX_PAGES):
            offset = page * 500
            items = self._fetch_page(
                DATA_API_ACTIVITY,
                params={"user": self.user_address, "limit": 500, "offset": offset},
            )
            if items is None:
                if page == 0:
                    return [], False
                break
            if not items:
                break

            found_known = False
            for item in items:
                tx = item.get("transactionHash") or ""
                if tx in self._activity_seen:
                    found_known = True
                else:
                    new_items.append(item)

            if found_known:
                break

        return new_items, True

    def _fetch_all_positions(self):
        """
        分页拉取全部持仓。
        返回 (positions_list, api_ok)。
        """
        all_positions = []
        for page in range(50):  # 安全上限
            offset = page * 100
            positions = self._fetch_page(
                DATA_API_POSITIONS,
                params={"user": self.user_address, "limit": 100, "offset": offset},
            )
            if positions is None:
                if page == 0:
                    return [], False
                break
            if not positions:
                break
            all_positions.extend(positions)
            if len(positions) < 100:
                break  # 最后一页

        return all_positions, True

    def _positions_changed(self, positions: list) -> bool:
        """对排序后的 positions 做 SHA256，与上次比较。变化时更新 hash 并返回 True。"""
        content = json.dumps(sorted(positions, key=lambda p: json.dumps(p, sort_keys=True)), sort_keys=True, ensure_ascii=False)
        h = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if h == self._positions_hash:
            return False
        self._positions_hash = h
        return True

    # ── 输出 ──────────────────────────────────────────────────────────────────

    def _output_file(self, filename: str = "trades.jsonl") -> Path:
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        hour = datetime.utcnow().strftime("%H")
        part_dir = self.output_dir / f"dt={dt}" / f"hour={hour}"
        part_dir.mkdir(parents=True, exist_ok=True)
        return part_dir / filename

    def _write_heartbeat(self):
        """Fix3: 每次 poll 写入心跳，healthcheck 检查此文件即可避免无新成交时误报。"""
        try:
            hb_path = self.output_dir / "heartbeat.jsonl"
            with open(hb_path, "w", encoding="utf-8") as hf:
                hf.write(json.dumps({"type": "heartbeat", "local_receipt_ts_ms": local_ts_ms()}) + "\n")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] heartbeat 写入失败: {e}")

    # ── 主循环 ────────────────────────────────────────────────────────────────

    def _on_signal(self, signum, frame):
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self.running = False

    def run(self):
        print(f"[{datetime.now().isoformat()}] 0x8dxd Tracker 启动")
        print(f"  地址: {self.user_address}")
        print(f"  输出目录: {self.output_dir}")
        print(f"  轮询间隔: trades={self.poll_interval_sec}s, activity={self.activity_interval_sec}s, positions={self.positions_interval_sec}s\n")

        # 文件句柄管理：filename -> (file_handle, current_path)
        handles: dict[str, tuple] = {}
        last_trade_poll = 0.0
        last_activity_poll = 0.0
        last_positions_poll = 0.0

        def _get_handle(filename: str):
            """获取或创建文件句柄，小时切换时自动重开。"""
            out_file = self._output_file(filename)
            entry = handles.get(filename)
            if entry is not None and entry[1] == out_file:
                return entry[0]
            # 关闭旧句柄
            if entry is not None:
                try:
                    entry[0].flush()
                    entry[0].close()
                except Exception:
                    pass
            f = open(out_file, "a", encoding="utf-8")
            handles[filename] = (f, out_file)
            return f

        def _close_all():
            for fname, (fh, _) in handles.items():
                try:
                    fh.flush()
                    fh.close()
                except Exception:
                    pass
            handles.clear()

        try:
            while self.running:
                now = time.time()
                any_new_data = False

                # ── Trades ──
                if now - last_trade_poll >= self.poll_interval_sec:
                    last_trade_poll = now
                    local_ms = local_ts_ms()
                    new_trades, api_ok = self._fetch_new_trades()

                    if not api_ok:
                        self._consecutive_failures += 1
                        if self._consecutive_failures >= 5:
                            print(f"[{datetime.now().isoformat()}] ⚠️  Tracker API 连续 {self._consecutive_failures} 次失败")
                    else:
                        self._consecutive_failures = 0
                        if new_trades:
                            f = _get_handle("trades.jsonl")
                            for t in new_trades:
                                tx = t.get("transactionHash") or ""
                                self._mark_seen(tx, self._trade_seen, self._trade_seen_order)
                                record = {
                                    "local_receipt_ts_ms": local_ms,
                                    "unixtime": local_ms // 1000,
                                    "proxyWallet": t.get("proxyWallet"),
                                    "side": t.get("side"),
                                    "asset": t.get("asset"),
                                    "conditionId": t.get("conditionId"),
                                    "size": t.get("size"),
                                    "price": t.get("price"),
                                    "timestamp": t.get("timestamp"),
                                    "title": t.get("title"),
                                    "slug": t.get("slug"),
                                    "outcome": t.get("outcome"),
                                    "outcomeIndex": t.get("outcomeIndex"),
                                    "transactionHash": tx,
                                }
                                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                                f.flush()
                                print(f"[{datetime.now().isoformat()}] 成交 {t.get('side')} {t.get('outcome')} {t.get('size')} @ {t.get('price')}")
                            any_new_data = True

                # ── Activity ──
                if now - last_activity_poll >= self.activity_interval_sec:
                    last_activity_poll = now
                    local_ms = local_ts_ms()
                    new_activity, api_ok = self._fetch_new_activity()

                    if not api_ok:
                        print(f"[{datetime.now().isoformat()}] ⚠️  Activity API 请求失败")
                    elif new_activity:
                        f = _get_handle("activity.jsonl")
                        for item in new_activity:
                            tx = item.get("transactionHash") or ""
                            self._mark_seen(tx, self._activity_seen, self._activity_seen_order)
                            record = {
                                "local_receipt_ts_ms": local_ms,
                                "unixtime": local_ms // 1000,
                                "type": item.get("type"),
                                "conditionId": item.get("conditionId"),
                                "title": item.get("title"),
                                "slug": item.get("slug"),
                                "side": item.get("side"),
                                "outcome": item.get("outcome"),
                                "size": item.get("size"),
                                "price": item.get("price"),
                                "usdcSize": item.get("usdcSize"),
                                "transactionHash": tx,
                                "timestamp": item.get("timestamp"),
                            }
                            f.write(json.dumps(record, ensure_ascii=False) + "\n")
                            f.flush()
                            print(f"[{datetime.now().isoformat()}] 活动 {item.get('type')} {item.get('outcome')} {item.get('size')} @ {item.get('price')}")
                        any_new_data = True

                # ── Positions ──
                if now - last_positions_poll >= self.positions_interval_sec:
                    last_positions_poll = now
                    local_ms = local_ts_ms()
                    positions, api_ok = self._fetch_all_positions()

                    if not api_ok:
                        print(f"[{datetime.now().isoformat()}] ⚠️  Positions API 请求失败")
                    elif self._positions_changed(positions):
                        f = _get_handle("positions.jsonl")
                        record = {
                            "local_receipt_ts_ms": local_ms,
                            "unixtime": local_ms // 1000,
                            "type": "positions_snapshot",
                            "count": len(positions),
                            "positions": json.dumps(positions, ensure_ascii=False),
                        }
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                        f.flush()
                        print(f"[{datetime.now().isoformat()}] 持仓快照 count={len(positions)}")
                        any_new_data = True

                if any_new_data:
                    self._save_checkpoint()

                self._write_heartbeat()
                time.sleep(self.poll_interval_sec)
        finally:
            _close_all()

        print(f"[{datetime.now().isoformat()}] Tracker 已停止")


def main():
    import argparse
    p = argparse.ArgumentParser(description="轮询 Polymarket 指定用户成交/活动/持仓并录制（0x8dxd Tracker）")
    p.add_argument("--user", default=os.environ.get(TRACKER_ADDRESS_ENV, DEFAULT_ADDRESS), help="用户地址 (0x...)")
    p.add_argument("--output-dir", default=os.path.join(POLY_DATA_DIR, "tracker_0x8dxd"), help="输出目录")
    p.add_argument("--interval", type=float, default=10.0, help="Trade 轮询间隔（秒）")
    p.add_argument("--activity-interval", type=float, default=30.0, help="Activity 轮询间隔（秒）")
    p.add_argument("--positions-interval", type=float, default=60.0, help="Positions 轮询间隔（秒）")
    args = p.parse_args()
    tracker = Tracker0x8dxd(
        user_address=args.user,
        output_dir=args.output_dir,
        poll_interval_sec=args.interval,
        activity_interval_sec=args.activity_interval,
        positions_interval_sec=args.positions_interval,
    )
    tracker.run()


if __name__ == "__main__":
    main()
