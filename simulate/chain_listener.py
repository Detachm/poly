"""
Polygon WebSocket 监听 CTF Exchange OrderFilled 事件
当目标用户出现在 maker / taker 位置时，立即通知主引擎处理

延迟链路：
  Polymarket 成交 → Polygon 出块(~2s) → WS 推送(<100ms) → 主引擎轮询 Data API
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
import threading
import time
from typing import Optional

import websockets

logger = logging.getLogger(__name__)

# CTF Exchange 合约地址（小写，Polygon 主网）
CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

# OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
ORDER_FILLED_SIG = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# 公开可用的 Polygon WSS 节点（轮换备用）
POLYGON_WSS_NODES = [
    "wss://polygon.drpc.org",          # 主节点，延迟低
    "wss://polygon.drpc.org",          # 重试仍用主节点
    "wss://polygon-bor-rpc.publicnode.com",  # 备用节点
]


def _addr_from_topic(topic: str) -> str:
    """从 32 字节 topic 中提取 20 字节地址（小写）"""
    return "0x" + topic[-40:].lower()


class ChainListener:
    """
    后台线程：订阅 Polygon CTF Exchange 的 OrderFilled 事件。
    当目标用户出现在 maker 或 taker 位置时，把 (addr, tx_hash, block_number)
    放入 event_queue，供主引擎消费。
    """

    def __init__(
        self,
        target_addrs: list[str],
        event_queue: queue.Queue,
        wss_nodes: Optional[list[str]] = None,
    ):
        self.targets: set[str] = {a.lower() for a in target_addrs}
        self.event_queue = event_queue
        self.wss_nodes = wss_nodes or POLYGON_WSS_NODES
        self._stop = False

        # 去重：同一 tx × 同一用户只入队一次（一笔大单会拆成多个 fill）
        self._seen: dict[tuple[str, str], float] = {}   # (tx, addr) -> ts

    def stop(self):
        self._stop = True

    # ── 事件解析 ────────────────────────────────────────────────────────

    def _check_log(self, log: dict) -> Optional[tuple[str, str, int]]:
        """
        解析 OrderFilled log，返回 (target_addr, tx_hash, block) 或 None。
        topics 布局:
          [0] = event sig
          [1] = orderHash (bytes32, indexed)
          [2] = maker    (address, indexed)
          [3] = taker    (address, indexed)
        """
        topics = log.get("topics", [])
        if len(topics) < 4 or topics[0] != ORDER_FILLED_SIG:
            return None

        maker = _addr_from_topic(topics[2])
        taker = _addr_from_topic(topics[3])

        if maker in self.targets:
            addr = maker
        elif taker in self.targets:
            addr = taker
        else:
            return None

        tx    = log.get("transactionHash", "")
        block = int(log.get("blockNumber", "0x0"), 16)
        return addr, tx, block

    def _handle_log(self, log: dict):
        result = self._check_log(log)
        if result is None:
            return

        addr, tx, block = result
        key = (tx, addr)
        now = time.time()

        if key in self._seen:
            return                          # 同 tx 同用户已入队，跳过

        self._seen[key] = now
        # 只保留最近 5 分钟的去重记录
        cutoff = now - 300
        self._seen = {k: v for k, v in self._seen.items() if v > cutoff}

        logger.info(f"[链上] ⚡ block={block} addr={addr[:10]}… tx={tx[:16]}…")
        self.event_queue.put((addr, tx, block, now))

    # ── WebSocket 连接 ──────────────────────────────────────────────────

    async def _connect_and_listen(self, url: str):
        logger.info(f"[链上] 连接 {url}")
        async with websockets.connect(
            url,
            ping_interval=20,
            open_timeout=10,
        ) as ws:
            # 订阅 OrderFilled 事件（全量，客户端侧过滤用户）
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_subscribe",
                "params": ["logs", {
                    "address": CTF_EXCHANGE,
                    "topics": [ORDER_FILLED_SIG],
                }],
            }))

            confirm = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            sub_id = confirm.get("result")
            if not sub_id:
                raise RuntimeError(f"订阅失败: {confirm}")

            logger.info(f"[链上] ✅ 订阅成功 sub_id={sub_id}")

            while not self._stop:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                except asyncio.TimeoutError:
                    # 心跳超时但连接仍在，继续
                    continue

                msg  = json.loads(raw)
                log  = msg.get("params", {}).get("result", {})
                if log:
                    self._handle_log(log)

    async def _run_async(self):
        url_idx = 0
        backoff  = 1
        while not self._stop:
            url = self.wss_nodes[url_idx % len(self.wss_nodes)]
            try:
                await self._connect_and_listen(url)
                backoff = 1
            except Exception as e:
                logger.warning(
                    f"[链上] {url} 断开: {type(e).__name__}: {e}，"
                    f"{backoff}s 后重连（切换节点）"
                )
                url_idx += 1
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def run_in_thread(self) -> threading.Thread:
        """启动后台线程，内含独立 asyncio 事件循环。返回 Thread 对象。"""
        def _thread_main():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._run_async())
            finally:
                loop.close()

        t = threading.Thread(target=_thread_main, name="chain-listener", daemon=True)
        t.start()
        return t
