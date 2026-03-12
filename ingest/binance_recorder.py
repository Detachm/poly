#!/usr/bin/env python3
"""
Binance 现货录制器：AggTrades + BookTicker，双连接 + 生产者-消费者 + 批量写入。
- 拆分为两个独立 WebSocket（aggTrade / bookTicker），互不抢占带宽与 CPU。
- WS 循环只收包、打时间戳、入队；Writer 循环批量写盘与轮转，避免写盘阻塞收包。
默认代理 http://127.0.0.1:7897；禁用代理：BINANCE_NO_PROXY=1 python binance_recorder.py
"""

import asyncio
import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import websockets

# 单文件超过此字节数时轮转新文件
ROTATION_SIZE_BYTES = 100 * 1024 * 1024  # 100MB
# 批量写入：积攒条数或超时即写盘
BATCH_SIZE = 1000
FLUSH_INTERVAL = 1.0  # 秒
# 内存队列上限，防止写盘过慢导致内存暴涨
QUEUE_MAX_SIZE = 100_000
# 超过此秒无数据则主动重连
WATCHDOG_IDLE_SEC = 60

# Binance 默认走代理（与 Polymarket 同：127.0.0.1:7897）；不需要代理时可设 BINANCE_NO_PROXY=1
DEFAULT_BINANCE_PROXY = "http://host.docker.internal:7897"
BINANCE_WS_BASE = "wss://stream.binance.com:9443"
DEFAULT_SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "xrpusdt"]
# 数据根目录，可通过环境变量 POLY_DATA_DIR 覆盖
POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")


def _setup_binance_proxy():
    if os.environ.pop("BINANCE_NO_PROXY", "").lower() in ("1", "true", "yes"):
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.pop(k, None)
        return
    proxy = (
        os.environ.pop("BINANCE_PROXY", None)
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
        or DEFAULT_BINANCE_PROXY
    )
    os.environ["HTTP_PROXY"] = os.environ["HTTPS_PROXY"] = os.environ["http_proxy"] = os.environ["https_proxy"] = proxy


def _stream_url(symbols: List[str], stream_type: str) -> str:
    """构建单类型组合流 URL。stream_type: aggTrade / bookTicker / depth20@100ms"""
    parts = []
    for s in symbols:
        s = s.lower()
        if stream_type == "aggTrade":
            parts.append(f"{s}@aggTrade")
        elif stream_type == "bookTicker":
            parts.append(f"{s}@bookTicker")
        elif stream_type == "depth20":
            parts.append(f"{s}@depth20@100ms")
    return f"{BINANCE_WS_BASE}/stream?streams={'/'.join(parts)}"


class StreamRecorder:
    """
    单流录制器：一个 WS 连接 + 一个内存队列 + 一个后台 Writer。
    只负责一种类型：仅 aggTrade 或 仅 bookTicker。
    """

    def __init__(self, stream_type: str, symbols: List[str], output_dir: Path):
        assert stream_type in ("aggTrade", "bookTicker", "depth20")
        self.stream_type = stream_type
        self.symbols = [s.lower() for s in symbols]
        self.output_dir = Path(output_dir)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self.running = True
        self.ws_task: Optional[asyncio.Task] = None
        self.writer_task: Optional[asyncio.Task] = None

        self.current_file = None
        self.current_path: Optional[Path] = None
        self._first_log = True
        self._current_partition = ""
        self._file_seq = 0

        self.url = _stream_url(self.symbols, stream_type)
        if stream_type == "aggTrade":
            self.file_base = "agg_trades"
        elif stream_type == "bookTicker":
            self.file_base = "book_ticker"
        else:
            self.file_base = "depth20"

    async def start(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.writer_task = asyncio.create_task(self._writer_loop())
        self.ws_task = asyncio.create_task(self._ws_loop())
        print(f"[{datetime.now().isoformat()}] Binance {self.stream_type} 录制已启动: {len(self.symbols)} 个标的")

    async def stop(self):
        self.running = False
        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
        await self.queue.put(None)
        if self.writer_task:
            await self.writer_task
        self._close_file()
        print(f"[{datetime.now().isoformat()}] Binance {self.stream_type} 录制已停止")

    def _local_ts_ms(self) -> int:
        return int(time.time() * 1000)

    def _record_agg(self, msg: dict, stream: str, local_ms: int) -> dict:
        data = msg.get("data") if isinstance(msg.get("data"), dict) else msg
        if not isinstance(data, dict) or data.get("e") != "aggTrade":
            return None
        return {
            "local_receipt_ts_ms": local_ms,
            "unixtime": local_ms // 1000,
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

    def _record_book(self, msg: dict, stream: str, local_ms: int) -> dict:
        data = msg.get("data") if isinstance(msg.get("data"), dict) else msg
        if not isinstance(data, dict):
            return None
        if data.get("e") != "bookTicker" and not (stream.endswith("@bookTicker") and "u" in data):
            return None
        return {
            "local_receipt_ts_ms": local_ms,
            "unixtime": local_ms // 1000,
            "stream": stream,
            "u": data.get("u"),
            "s": data.get("s"),
            "b": data.get("b"),
            "B": data.get("B"),
            "a": data.get("a"),
            "A": data.get("A"),
        }

    def _record_depth(self, msg: dict, stream: str, local_ms: int) -> dict:
        """解析 depth20@100ms 流，记录 20 档 bid/ask。"""
        data = msg.get("data") if isinstance(msg.get("data"), dict) else msg
        if not isinstance(data, dict):
            return None
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids and not asks:
            return None
        # 提取 symbol: "btcusdt@depth20@100ms" -> "BTCUSDT"
        s = stream.split("@")[0].upper() if stream else data.get("s", "")
        return {
            "local_receipt_ts_ms": local_ms,
            "unixtime": local_ms // 1000,
            "stream": stream,
            "s": s,
            "lastUpdateId": data.get("lastUpdateId"),
            "bids": json.dumps(bids),
            "asks": json.dumps(asks),
        }

    async def _ws_loop(self):
        """生产者：只收包、打时间戳、入队，不写盘。"""
        reconnect_delay = 1
        max_delay = 60
        while self.running:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    reconnect_delay = 1
                    last_received_ts = time.time()
                    while self.running:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            if time.time() - last_received_ts >= WATCHDOG_IDLE_SEC:
                                print(f"[{datetime.now().isoformat()}] {self.stream_type} 看门狗: {WATCHDOG_IDLE_SEC}s 无数据，重连")
                                break
                            continue
                        last_received_ts = time.time()
                        local_ms = self._local_ts_ms()
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        stream = msg.get("stream", "")
                        if self.stream_type == "aggTrade":
                            record = self._record_agg(msg, stream, local_ms)
                        elif self.stream_type == "bookTicker":
                            record = self._record_book(msg, stream, local_ms)
                        else:
                            record = self._record_depth(msg, stream, local_ms)
                        if record is None:
                            continue
                        if self._first_log:
                            print(f"[{datetime.now().isoformat()}] Binance 已收到首条 {self.stream_type}: {stream}")
                            self._first_log = False
                        try:
                            self.queue.put_nowait(record)
                        except asyncio.QueueFull:
                            print(f"[{datetime.now().isoformat()}] {self.stream_type} 队列已满，丢包（写盘过慢）")
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self.running:
                    print(f"[{datetime.now().isoformat()}] {self.stream_type} WS 错误: {e}，{reconnect_delay}s 后重连")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_delay)

    async def _writer_loop(self):
        """消费者：从队列取数据，批量写盘与轮转。"""
        buffer: List[str] = []
        last_flush = time.time()
        while True:
            try:
                try:
                    record = await asyncio.wait_for(self.queue.get(), timeout=FLUSH_INTERVAL)
                except asyncio.TimeoutError:
                    record = None
                if record is None:
                    if not self.running:
                        if buffer:
                            self._write_batch(buffer)
                            buffer = []
                        break
                    if buffer:
                        self._write_batch(buffer)
                        buffer = []
                        last_flush = time.time()
                    continue
                buffer.append(json.dumps(record, ensure_ascii=False))
                now = time.time()
                if len(buffer) >= BATCH_SIZE or (buffer and now - last_flush >= FLUSH_INTERVAL):
                    self._write_batch(buffer)
                    buffer = []
                    last_flush = now
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] {self.stream_type} Writer 错误: {e}")
                await asyncio.sleep(1)

    def _write_batch(self, lines: List[str]):
        if not lines:
            return
        if self.current_file and self.current_path and self.current_path.stat().st_size >= ROTATION_SIZE_BYTES:
            self._close_file()
        if not self.current_file:
            self._open_new_file()
        try:
            self.current_file.write("\n".join(lines) + "\n")
            self.current_file.flush()
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] {self.stream_type} 写文件错误: {e}")

    def _open_new_file(self):
        now = datetime.now(timezone.utc)
        dt = now.strftime("%Y-%m-%d")
        hour = now.strftime("%H")
        partition = f"dt={dt}/hour={hour}"
        if partition != self._current_partition:
            self._current_partition = partition
            self._file_seq = 0
        part_dir = self.output_dir / f"dt={dt}" / f"hour={hour}"
        part_dir.mkdir(parents=True, exist_ok=True)
        suffix = "" if self._file_seq == 0 else f"_{self._file_seq:03d}"
        self.current_path = part_dir / f"{self.file_base}{suffix}.jsonl"
        self._file_seq += 1
        print(f"[{datetime.now().isoformat()}] {self.stream_type} 打开文件: {self.current_path.name}")
        self.current_file = open(self.current_path, "a", encoding="utf-8")

    def _close_file(self):
        if self.current_file:
            try:
                self.current_file.flush()
                self.current_file.close()
                print(f"[{datetime.now().isoformat()}] {self.stream_type} 关闭文件: {self.current_path.name}")
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] {self.stream_type} 关闭文件错误: {e}")
            finally:
                self.current_file = None
                self.current_path = None


async def _run(
    output_dir: str,
    symbols: List[str],
    record_agg: bool,
    record_book: bool,
    record_depth: bool,
    stop_event: asyncio.Event,
):
    _setup_binance_proxy()
    out = Path(output_dir)
    recorders: List[StreamRecorder] = []
    if record_agg:
        recorders.append(StreamRecorder("aggTrade", symbols, out))
    if record_book:
        recorders.append(StreamRecorder("bookTicker", symbols, out))
    if record_depth:
        recorders.append(StreamRecorder("depth20", symbols, out))
    if not recorders:
        print("未启用任何流，退出")
        return
    for r in recorders:
        await r.start()
    n = len(recorders)
    print(f"[{datetime.now().isoformat()}] Binance 录制器已启动（{n} 个连接 + 批量写入）。Ctrl+C 停止。\n")
    await stop_event.wait()
    print(f"[{datetime.now().isoformat()}] 正在停止...")
    for r in recorders:
        await r.stop()
    print(f"[{datetime.now().isoformat()}] Binance 录制器已停止")


def main():
    import argparse
    p = argparse.ArgumentParser(description="录制 Binance 现货 AggTrades + BookTicker + Depth20")
    p.add_argument("--output-dir", default=os.path.join(POLY_DATA_DIR, "binance"), help="输出目录")
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="交易对，如 btcusdt ethusdt")
    p.add_argument("--no-agg", action="store_true", help="不录制 AggTrades")
    p.add_argument("--no-book", action="store_true", help="不录制 BookTicker")
    p.add_argument("--no-depth", action="store_true", help="不录制 Depth20")
    args = p.parse_args()

    stop_event = asyncio.Event()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def on_signal():
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, on_signal)
        loop.add_signal_handler(signal.SIGTERM, on_signal)
    except NotImplementedError:
        pass

    try:
        loop.run_until_complete(
            _run(
                output_dir=args.output_dir,
                symbols=args.symbols,
                record_agg=not args.no_agg,
                record_book=not args.no_book,
                record_depth=not args.no_depth,
                stop_event=stop_event,
            )
        )
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        loop.close()


if __name__ == "__main__":
    main()
