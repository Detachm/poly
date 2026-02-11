#!/usr/bin/env python3
"""
Binance 现货录制器：AggTrades + BookTicker，记录含 local_receipt_ts_ms、unixtime。
默认代理 http://127.0.0.1:7897；禁用代理：BINANCE_NO_PROXY=1 python binance_recorder.py
"""

import asyncio
import json
import os
import signal
import time
from datetime import datetime
from pathlib import Path

import websockets

# 单文件超过此字节数时轮转新文件
ROTATE_BYTES = 100 * 1024 * 1024  # 100MB
# 超过此秒无数据则主动重连
WATCHDOG_IDLE_SEC = 60

# Binance 默认走代理（与 Polymarket 同：127.0.0.1:7897）；不需要代理时可设 BINANCE_NO_PROXY=1
DEFAULT_BINANCE_PROXY = "http://127.0.0.1:7897"

def _setup_binance_proxy():
    if os.environ.pop("BINANCE_NO_PROXY", "").lower() in ("1", "true", "yes"):
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.pop(k, None)
        return
    proxy = os.environ.pop("BINANCE_PROXY", None) or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or DEFAULT_BINANCE_PROXY
    os.environ["HTTP_PROXY"] = os.environ["HTTPS_PROXY"] = os.environ["http_proxy"] = os.environ["https_proxy"] = proxy

# 默认录制标的：BTC / ETH / SOL / XRP（与 Chainlink、Polymarket 一致）
DEFAULT_SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "xrpusdt"]
BINANCE_WS_BASE = "wss://stream.binance.com:9443"


def get_combined_stream_url(symbols: list, agg_trade: bool, book_ticker: bool) -> str:
    """构建组合流 URL：streams=btcusdt@aggTrade/ethusdt@aggTrade/btcusdt@bookTicker/..."""
    parts = []
    for s in symbols:
        s = s.lower()
        if agg_trade:
            parts.append(f"{s}@aggTrade")
        if book_ticker:
            parts.append(f"{s}@bookTicker")
    return f"{BINANCE_WS_BASE}/stream?streams={'/'.join(parts)}"


class BinanceRecorder:
    def __init__(
        self,
        output_dir: str = "data/binance",
        symbols: list = None,
        record_agg_trade: bool = True,
        record_book_ticker: bool = True,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.symbols = [s.lower() for s in (symbols or DEFAULT_SYMBOLS)]
        self.record_agg_trade = record_agg_trade
        self.record_book_ticker = record_book_ticker
        self.running = True
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, signum, frame):
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self.running = False

    def _local_ts_ms(self) -> int:
        return int(time.time() * 1000)

    def _rotate_path(self, base: str) -> Path:
        return self.output_dir / f"{base}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.jsonl"

    async def run(self):
        _setup_binance_proxy()
        url = get_combined_stream_url(
            self.symbols,
            agg_trade=self.record_agg_trade,
            book_ticker=self.record_book_ticker,
        )
        self.output_dir.mkdir(parents=True, exist_ok=True)
        agg_path = self.output_dir / "agg_trades.jsonl"
        book_path = self.output_dir / "book_ticker.jsonl"
        print(f"[{datetime.now().isoformat()}] Binance 录制器启动")
        print(f"  标的: {self.symbols}")
        print(f"  AggTrades: {agg_path} (>{ROTATE_BYTES // (1024*1024)}MB 轮转)")
        print(f"  BookTicker: {book_path}")
        print(f"  流: {url}\n")

        reconnect_delay = 1
        max_delay = 60

        while self.running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    reconnect_delay = 1
                    first_agg, first_book = True, True
                    last_received_ts = time.time()
                    f_agg = open(agg_path, "a", encoding="utf-8")
                    f_book = open(book_path, "a", encoding="utf-8")
                    try:
                        while self.running:
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            except asyncio.TimeoutError:
                                if time.time() - last_received_ts >= WATCHDOG_IDLE_SEC:
                                    print(f"[{datetime.now().isoformat()}] 看门狗: {WATCHDOG_IDLE_SEC}s 无数据，重连")
                                    break
                                continue
                            last_received_ts = time.time()
                            local_ms = self._local_ts_ms()
                            unixtime = local_ms // 1000
                            try:
                                msg = json.loads(raw)
                            except json.JSONDecodeError:
                                continue
                            stream = msg.get("stream", "")
                            data = msg.get("data") if isinstance(msg.get("data"), dict) else msg
                            if not isinstance(data, dict):
                                continue
                            event_type = data.get("e")
                            if event_type == "aggTrade" and self.record_agg_trade:
                                record = {
                                    "local_receipt_ts_ms": local_ms,
                                    "unixtime": unixtime,
                                    "stream": stream,
                                    "e": data.get("e"),
                                    "E": data.get("E"),
                                    "a": data.get("a"),
                                    "p": data.get("p"),
                                    "q": data.get("q"),
                                    "f": data.get("f"),
                                    "l": data.get("l"),
                                    "T": data.get("T"),
                                    "m": data.get("m"),
                                    "M": data.get("M"),
                                }
                                if first_agg:
                                    print(f"[{datetime.now().isoformat()}] Binance 已收到首条 AggTrade: {stream}")
                                    first_agg = False
                                f_agg.write(json.dumps(record, ensure_ascii=False) + "\n")
                                f_agg.flush()
                                if os.path.getsize(agg_path) >= ROTATE_BYTES:
                                    f_agg.close()
                                    agg_path = self._rotate_path("agg_trades")
                                    f_agg = open(agg_path, "a", encoding="utf-8")
                                    print(f"[{datetime.now().isoformat()}] AggTrades 轮转: {agg_path.name}")
                            elif (event_type == "bookTicker" or (stream.endswith("@bookTicker") and "u" in data)) and self.record_book_ticker:
                                record = {
                                    "local_receipt_ts_ms": local_ms,
                                    "unixtime": unixtime,
                                    "stream": stream,
                                    "u": data.get("u"),
                                    "s": data.get("s"),
                                    "b": data.get("b"),
                                    "B": data.get("B"),
                                    "a": data.get("a"),
                                    "A": data.get("A"),
                                }
                                if first_book:
                                    print(f"[{datetime.now().isoformat()}] Binance 已收到首条 BookTicker: {stream}")
                                    first_book = False
                                f_book.write(json.dumps(record, ensure_ascii=False) + "\n")
                                f_book.flush()
                                if os.path.getsize(book_path) >= ROTATE_BYTES:
                                    f_book.close()
                                    book_path = self._rotate_path("book_ticker")
                                    f_book = open(book_path, "a", encoding="utf-8")
                                    print(f"[{datetime.now().isoformat()}] BookTicker 轮转: {book_path.name}")
                    finally:
                        f_agg.close()
                        f_book.close()
            except websockets.exceptions.ConnectionClosed as e:
                if self.running:
                    print(f"[{datetime.now().isoformat()}] WebSocket 关闭，{reconnect_delay}s 后重连: {e}")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_delay)
                else:
                    break
            except Exception as e:
                if self.running:
                    print(f"[{datetime.now().isoformat()}] 错误: {e}")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_delay)
                else:
                    break

        print(f"[{datetime.now().isoformat()}] Binance 录制器已停止")


def main():
    import argparse
    p = argparse.ArgumentParser(description="录制 Binance 现货 AggTrades + BookTicker")
    p.add_argument("--output-dir", default="data/binance", help="输出目录")
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="交易对，如 btcusdt ethusdt")
    p.add_argument("--no-agg", action="store_true", help="不录制 AggTrades")
    p.add_argument("--no-book", action="store_true", help="不录制 BookTicker")
    args = p.parse_args()
    r = BinanceRecorder(
        output_dir=args.output_dir,
        symbols=args.symbols,
        record_agg_trade=not args.no_agg,
        record_book_ticker=not args.no_book,
    )
    asyncio.run(r.run())


if __name__ == "__main__":
    main()
