#!/usr/bin/env python3
"""
0x8dxd 链上事件录制器：订阅 Polygon CTF Exchange 的 OrderFilled/OrderCancelled 事件，
当目标用户出现在 maker/taker 位置时写入 JSONL。

基于 simulate/chain_listener.py 的逻辑改造为独立录制器。

数据流：
  Polygon 出块 → WSS 推送 → 过滤 0x8dxd 地址 → 写 events.jsonl

输出目录结构：
  raw/chain_0x8dxd/
    dt=YYYY-MM-DD/
      hour=HH/
        events.jsonl
    heartbeat.jsonl
"""

import asyncio
import json
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import websockets

# CTF Exchange 合约地址（小写，Polygon 主网）
CTF_EXCHANGE = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"

# OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)
ORDER_FILLED_SIG = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# OrderCancelled(bytes32 orderHash)
ORDER_CANCELLED_SIG = "0x5152abf959f6564662358c2e52b702259b78bac5ee7842571a7391f09d9af715"

TRACKER_ADDRESS_ENV = "TRACKER_USER_ADDRESS"
DEFAULT_ADDRESS = "0x63ce342161250d705dc0b16df89036c8e5f9ba9a"
POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")

# 公开可用的 Polygon WSS 节点（轮换备用）
DEFAULT_WSS_NODES = [
    "wss://polygon.drpc.org",
    "wss://polygon.drpc.org",
    "wss://polygon-bor-rpc.publicnode.com",
]

# 去重保留上限
_SEEN_MAX = 500


def local_ts_ms() -> int:
    return int(time.time() * 1000)


def _addr_from_topic(topic: str) -> str:
    """从 32 字节 topic 中提取 20 字节地址（小写）"""
    return "0x" + topic[-40:].lower()


def _decode_order_filled_data(data_hex: str) -> dict:
    """
    解码 OrderFilled event data（非 indexed 参数）。
    data = makerAssetId(32B) + takerAssetId(32B) + makerAmountFilled(32B) + takerAmountFilled(32B) + fee(32B)
    """
    # 去掉 0x 前缀
    d = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if len(d) < 320:  # 5 * 64
        return {}
    return {
        "maker_asset_id": str(int(d[0:64], 16)),
        "taker_asset_id": str(int(d[64:128], 16)),
        "maker_amount_filled": str(int(d[128:192], 16)),
        "taker_amount_filled": str(int(d[192:256], 16)),
        "fee": str(int(d[256:320], 16)),
    }


class ChainRecorder0x8dxd:
    def __init__(
        self,
        target_address: str = None,
        output_dir: str = None,
        wss_nodes: list = None,
    ):
        self.target = (target_address or os.environ.get(TRACKER_ADDRESS_ENV) or DEFAULT_ADDRESS).strip().lower()
        _out = output_dir or os.path.join(POLY_DATA_DIR, "chain_0x8dxd")
        self.output_dir = Path(_out)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.wss_nodes = wss_nodes or DEFAULT_WSS_NODES
        self._stop = False

        # 去重：(transactionHash, log_index)
        self._seen: set = set()
        self._seen_order: deque = deque(maxlen=_SEEN_MAX)

        # 文件句柄
        self._file = None
        self._current_path = None

        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, signum, frame):
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self._stop = True

    def _mark_seen(self, key: tuple) -> bool:
        """返回 True 如果是新 key，False 如果已见过。"""
        if key in self._seen:
            return False
        if len(self._seen_order) >= _SEEN_MAX:
            oldest = self._seen_order[0]
            self._seen.discard(oldest)
        self._seen.add(key)
        self._seen_order.append(key)
        return True

    def _output_file(self) -> Path:
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        hour = datetime.utcnow().strftime("%H")
        part_dir = self.output_dir / f"dt={dt}" / f"hour={hour}"
        part_dir.mkdir(parents=True, exist_ok=True)
        return part_dir / "events.jsonl"

    def _get_handle(self):
        out_file = self._output_file()
        if self._current_path == out_file and self._file is not None:
            return self._file
        if self._file is not None:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass
        self._file = open(out_file, "a", encoding="utf-8")
        self._current_path = out_file
        return self._file

    def _write_heartbeat(self):
        try:
            hb_path = self.output_dir / "heartbeat.jsonl"
            with open(hb_path, "w", encoding="utf-8") as hf:
                hf.write(json.dumps({"type": "heartbeat", "local_receipt_ts_ms": local_ts_ms()}) + "\n")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] heartbeat 写入失败: {e}")

    def _process_log(self, log: dict):
        """解析一条 event log，如果与目标地址相关则写入 JSONL。"""
        topics = log.get("topics", [])
        if not topics:
            return

        tx_hash = log.get("transactionHash", "")
        log_index = int(log.get("logIndex", "0x0"), 16)
        block_number = int(log.get("blockNumber", "0x0"), 16)

        # 去重
        key = (tx_hash, log_index)
        if not self._mark_seen(key):
            return

        event_sig = topics[0]
        local_ms = local_ts_ms()

        if event_sig == ORDER_FILLED_SIG:
            if len(topics) < 4:
                return
            maker = _addr_from_topic(topics[2])
            taker = _addr_from_topic(topics[3])

            if maker == self.target:
                role = "maker"
            elif taker == self.target:
                role = "taker"
            else:
                return  # 目标用户不在此事件中

            data_fields = _decode_order_filled_data(log.get("data", ""))
            record = {
                "local_receipt_ts_ms": local_ms,
                "unixtime": local_ms // 1000,
                "event_type": "OrderFilled",
                "block_number": block_number,
                "transactionHash": tx_hash,
                "log_index": log_index,
                "maker": maker,
                "taker": taker,
                "role": role,
                **data_fields,
            }

            f = self._get_handle()
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            print(f"[{datetime.now().isoformat()}] OrderFilled block={block_number} role={role} tx={tx_hash[:16]}...")

        elif event_sig == ORDER_CANCELLED_SIG:
            # OrderCancelled 只有 orderHash 在 topics[1]，无 maker/taker
            # 需要通过 transaction 的 from 地址判断是否为目标用户
            # 但 log 中没有 from，所以记录所有 cancelled 事件
            # 后续分析层可通过 transactionHash 关联
            order_hash = topics[1] if len(topics) > 1 else ""
            record = {
                "local_receipt_ts_ms": local_ms,
                "unixtime": local_ms // 1000,
                "event_type": "OrderCancelled",
                "block_number": block_number,
                "transactionHash": tx_hash,
                "log_index": log_index,
                "order_hash": order_hash,
            }

            f = self._get_handle()
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            print(f"[{datetime.now().isoformat()}] OrderCancelled block={block_number} tx={tx_hash[:16]}...")

    async def _connect_and_record(self, url: str):
        print(f"[{datetime.now().isoformat()}] 连接 {url}")
        async with websockets.connect(
            url,
            ping_interval=20,
            open_timeout=10,
        ) as ws:
            # 订阅 OrderFilled + OrderCancelled（OR 语义）
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_subscribe",
                "params": ["logs", {
                    "address": CTF_EXCHANGE,
                    "topics": [[ORDER_FILLED_SIG, ORDER_CANCELLED_SIG]],
                }],
            }))

            confirm = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
            sub_id = confirm.get("result")
            if not sub_id:
                raise RuntimeError(f"订阅失败: {confirm}")

            print(f"[{datetime.now().isoformat()}] 订阅成功 sub_id={sub_id}")

            last_heartbeat = time.time()
            while not self._stop:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                except asyncio.TimeoutError:
                    # 写心跳保持 healthcheck
                    self._write_heartbeat()
                    continue

                msg = json.loads(raw)
                log = msg.get("params", {}).get("result", {})
                if log:
                    self._process_log(log)

                # 定期写心跳
                now = time.time()
                if now - last_heartbeat >= 30:
                    self._write_heartbeat()
                    last_heartbeat = now

    async def run(self):
        print(f"[{datetime.now().isoformat()}] 0x8dxd Chain Recorder 启动")
        print(f"  目标地址: {self.target}")
        print(f"  输出目录: {self.output_dir}")
        print(f"  WSS 节点: {self.wss_nodes}\n")

        url_idx = 0
        backoff = 1
        while not self._stop:
            url = self.wss_nodes[url_idx % len(self.wss_nodes)]
            try:
                await self._connect_and_record(url)
                backoff = 1
            except Exception as e:
                print(
                    f"[{datetime.now().isoformat()}] {url} 断开: {type(e).__name__}: {e}，"
                    f"{backoff}s 后重连（切换节点）"
                )
                url_idx += 1
                self._write_heartbeat()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

        # 清理
        if self._file is not None:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                pass
        print(f"[{datetime.now().isoformat()}] Chain Recorder 已停止")


def main():
    import argparse
    p = argparse.ArgumentParser(description="链上事件录制器：CTF Exchange OrderFilled/OrderCancelled（0x8dxd）")
    p.add_argument("--user", default=os.environ.get(TRACKER_ADDRESS_ENV, DEFAULT_ADDRESS), help="目标用户地址 (0x...)")
    p.add_argument("--output-dir", default=os.path.join(POLY_DATA_DIR, "chain_0x8dxd"), help="输出目录")
    p.add_argument("--wss-nodes", nargs="+", default=None, help="Polygon WSS 节点列表")
    args = p.parse_args()
    recorder = ChainRecorder0x8dxd(
        target_address=args.user,
        output_dir=args.output_dir,
        wss_nodes=args.wss_nodes,
    )
    asyncio.run(recorder.run())


if __name__ == "__main__":
    main()
