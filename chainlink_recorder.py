#!/usr/bin/env python3
"""
Chainlink 预言机监控器（Polygon）
轮询 latestRoundData()，round 更新时写入；记录含 local_receipt_ts_ms、unixtime。
建议轮询间隔 0.2s；若有 wss:// RPC 可后续改为事件订阅以进一步降延迟。
"""

import json
import os
import signal
import time
from datetime import datetime
from pathlib import Path

try:
    from web3 import Web3
except ImportError:
    Web3 = None

# 连续失败超过此次数时打一条 warning
CONSECUTIVE_FAILURE_WARN_THRESHOLD = 5

# Polygon 上 Chainlink 价格 Feed 代理地址（见 https://data.chain.link/feeds，BTC/ETH/SOL/XRP）
POLYGON_FEEDS = {
    "BTC/USD": "0xc907E116054Ad103354f2D4Fd4e30EC22C8797A8",
    "ETH/USD": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
    "SOL/USD": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
    "XRP/USD": "0x785ba89291f676b5386652eB12b30cF361020694",
}

# latestRoundData 返回 (roundId, answer, startedAt, updatedAt, answeredInRound)
# 使用简单的 contract call 即可，无需 ABI 完整定义，仅需函数选择器
FEED_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]


class ChainlinkRecorder:
    def __init__(
        self,
        output_dir: str = "data/chainlink",
        rpc_url: str = None,
        poll_interval_sec: float = 0.2,
        feeds: dict = None,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rpc_url = rpc_url or os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com")
        self.poll_interval_sec = poll_interval_sec
        self.feeds = feeds or POLYGON_FEEDS
        self.running = True
        self._last = {}  # feed_name -> (roundId, updatedAt)
        self._consecutive_failures = {}  # feed_name -> int
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        if Web3 is None:
            raise RuntimeError("请安装 web3: pip install web3")

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        if not self.w3.is_connected():
            raise RuntimeError(f"无法连接 Polygon RPC: {self.rpc_url}")

    def _on_signal(self, signum, frame):
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self.running = False

    def _local_ts_ms(self) -> int:
        return int(time.time() * 1000)

    def _fetch_round(self, feed_name: str, address: str) -> dict:
        contract = self.w3.eth.contract(address=Web3.to_checksum_address(address), abi=FEED_ABI)
        try:
            round_id, answer, started_at, updated_at, answered_in_round = contract.functions.latestRoundData().call()
            return {
                "roundId": round_id,
                "answer": answer,
                "startedAt": started_at,
                "updatedAt": updated_at,
                "answeredInRound": answered_in_round,
            }
        except Exception as e:
            return {"error": str(e)}

    def run_sync(self):
        out_file = self.output_dir / "updates.jsonl"
        print(f"[{datetime.now().isoformat()}] Chainlink 监控器启动 (Polygon)")
        print(f"  RPC: {self.rpc_url}")
        print(f"  Feeds: {list(self.feeds.keys())}")
        print(f"  输出: {out_file}")
        print(f"  轮询间隔: {self.poll_interval_sec} s\n")

        with open(out_file, "a", encoding="utf-8") as f:
            while self.running:
                local_ms = self._local_ts_ms()
                unixtime = local_ms // 1000
                for feed_name, address in self.feeds.items():
                    try:
                        data = self._fetch_round(feed_name, address)
                        if "error" in data:
                            self._consecutive_failures[feed_name] = self._consecutive_failures.get(feed_name, 0) + 1
                            if self._consecutive_failures[feed_name] >= CONSECUTIVE_FAILURE_WARN_THRESHOLD:
                                print(f"[{datetime.now().isoformat()}] ⚠️  Chainlink {feed_name} 连续 {self._consecutive_failures[feed_name]} 次失败")
                                self._consecutive_failures[feed_name] = 0
                            continue
                        self._consecutive_failures[feed_name] = 0
                        last_key = self._last.get(feed_name)
                        if last_key != (data["roundId"], data["updatedAt"]):
                            self._last[feed_name] = (data["roundId"], data["updatedAt"])
                            record = {
                                "local_receipt_ts_ms": local_ms,
                                "unixtime": unixtime,
                                "feed": feed_name,
                                "roundId": data["roundId"],
                                "answer": data["answer"],
                                "updatedAt": data["updatedAt"],
                                "startedAt": data["startedAt"],
                                "answeredInRound": data["answeredInRound"],
                            }
                            f.write(json.dumps(record, ensure_ascii=False) + "\n")
                            f.flush()
                            print(f"[{datetime.now().isoformat()}] Chainlink 更新 {feed_name} round={data['roundId']} answer={data['answer']}")
                    except Exception:
                        self._consecutive_failures[feed_name] = self._consecutive_failures.get(feed_name, 0) + 1
                        if self._consecutive_failures[feed_name] >= CONSECUTIVE_FAILURE_WARN_THRESHOLD:
                            print(f"[{datetime.now().isoformat()}] ⚠️  Chainlink {feed_name} 连续 {self._consecutive_failures[feed_name]} 次异常")
                            self._consecutive_failures[feed_name] = 0
                time.sleep(self.poll_interval_sec)

        print(f"[{datetime.now().isoformat()}] Chainlink 监控器已停止")


def main():
    import argparse
    p = argparse.ArgumentParser(description="监控 Polygon 上 Chainlink 价格 feed 更新")
    p.add_argument("--output-dir", default="data/chainlink", help="输出目录")
    p.add_argument("--rpc", default=os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com"), help="Polygon RPC URL")
    p.add_argument("--poll-interval", type=float, default=0.2, help="轮询间隔（秒），建议 0.2 以降低延迟")
    args = p.parse_args()
    r = ChainlinkRecorder(
        output_dir=args.output_dir,
        rpc_url=args.rpc,
        poll_interval_sec=args.poll_interval,
    )
    r.run_sync()


if __name__ == "__main__":
    main()
