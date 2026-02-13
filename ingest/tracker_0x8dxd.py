#!/usr/bin/env python3
"""
0x8dxd Tracker：轮询 Polymarket Data API 获取指定地址的成交，用于打标签与验证模型。
与其它录制器统一使用 local_receipt_ts_ms（本地接收时间戳）。
"""

import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

DATA_API_TRADES = "https://data-api.polymarket.com/trades"
TRACKER_ADDRESS_ENV = "TRACKER_USER_ADDRESS"
POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")
# 默认地址占位，请设置环境变量 TRACKER_USER_ADDRESS 为实际 0x 地址
DEFAULT_ADDRESS = "0x8dxd"


def local_ts_ms() -> int:
    return int(time.time() * 1000)


class Tracker0x8dxd:
    def __init__(
        self,
        user_address: str = None,
        output_dir: str = None,
        poll_interval_sec: float = 30.0,
    ):
        self.user_address = (user_address or os.environ.get(TRACKER_ADDRESS_ENV) or DEFAULT_ADDRESS).strip()
        _out = output_dir or os.path.join(POLY_DATA_DIR, "tracker_0x8dxd")
        self.output_dir = Path(_out)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.poll_interval_sec = poll_interval_sec
        self.running = True
        self._seen = set()  # (timestamp, asset, side, size, price) 去重
        self._consecutive_failures = 0
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "Poly-Tracker/1.0"})
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "http://host.docker.internal:7897"
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

    def _on_signal(self, signum, frame):
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self.running = False

    def _fetch_trades(self, limit: int = 100, offset: int = 0):
        """成功返回 list，失败返回 None。"""
        try:
            r = self.session.get(
                DATA_API_TRADES,
                params={"user": self.user_address, "limit": limit, "offset": offset},
                timeout=15,
            )
            r.raise_for_status()
            return r.json() if r.text else []
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] API 请求失败: {e}")
            return None

    def _output_file(self) -> Path:
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        hour = datetime.utcnow().strftime("%H")
        part_dir = self.output_dir / f"dt={dt}" / f"hour={hour}"
        part_dir.mkdir(parents=True, exist_ok=True)
        return part_dir / "trades.jsonl"

    def run(self):
        print(f"[{datetime.now().isoformat()}] 0x8dxd Tracker 启动")
        print(f"  地址: {self.user_address}")
        print(f"  输出目录: {self.output_dir}")
        print(f"  轮询间隔: {self.poll_interval_sec} s\n")

        f = None
        current_file = None
        try:
            while self.running:
                out_file = self._output_file()
                if current_file != out_file:
                    if f is not None:
                        f.flush()
                        f.close()
                    current_file = out_file
                    f = open(out_file, "a", encoding="utf-8")
                local_ms = local_ts_ms()
                unixtime = local_ms // 1000
                trades = self._fetch_trades(limit=100)
                if trades is None:
                    self._consecutive_failures += 1
                    if self._consecutive_failures >= 5:
                        print(f"[{datetime.now().isoformat()}] ⚠️  Tracker API 连续 {self._consecutive_failures} 次失败")
                        self._consecutive_failures = 0
                    time.sleep(self.poll_interval_sec)
                    continue
                else:
                    self._consecutive_failures = 0
                for t in trades:
                    key = (
                        t.get("timestamp"),
                        t.get("asset"),
                        t.get("side"),
                        t.get("size"),
                        t.get("price"),
                    )
                    if key in self._seen:
                        continue
                    self._seen.add(key)
                    record = {
                        "local_receipt_ts_ms": local_ms,
                        "unixtime": unixtime,
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
                        "transactionHash": t.get("transactionHash"),
                    }
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    f.flush()
                    print(f"[{datetime.now().isoformat()}] 成交 {t.get('side')} {t.get('outcome')} {t.get('size')} @ {t.get('price')}")
                time.sleep(self.poll_interval_sec)
        finally:
            if f is not None:
                try:
                    f.flush()
                    f.close()
                except Exception:
                    pass

        print(f"[{datetime.now().isoformat()}] Tracker 已停止")


def main():
    import argparse
    p = argparse.ArgumentParser(description="轮询 Polymarket 指定用户成交并录制（0x8dxd Tracker）")
    p.add_argument("--user", default=os.environ.get(TRACKER_ADDRESS_ENV, DEFAULT_ADDRESS), help="用户地址 (0x...)")
    p.add_argument("--output-dir", default=os.path.join(POLY_DATA_DIR, "tracker_0x8dxd"), help="输出目录")
    p.add_argument("--interval", type=float, default=30.0, help="轮询间隔（秒）")
    args = p.parse_args()
    tracker = Tracker0x8dxd(
        user_address=args.user,
        output_dir=args.output_dir,
        poll_interval_sec=args.interval,
    )
    tracker.run()


if __name__ == "__main__":
    main()
