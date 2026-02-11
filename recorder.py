#!/usr/bin/env python3
"""
Market Data Recorder for Polymarket CLOB WebSocket.
Streams Level 2 order book (snapshot + price_change). All records include
local_receipt_ts_ms and unixtime for multi-source alignment.
"""

import asyncio
import io
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiofiles
import websockets

try:
    import psutil
except ImportError:
    psutil = None

if sys.stdout and hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)
if sys.stderr and hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, line_buffering=True)

# 连接无数据超过此秒数则主动重连
WATCHDOG_IDLE_SEC = 60
# 心跳写入间隔（秒）
HEARTBEAT_INTERVAL_SEC = 60
# 按小时轮转文件
ROTATE_HOURLY = True


class OrderBookRecorder:
    """Records Level 2 order book data from Polymarket CLOB WebSocket."""
    
    WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    def __init__(self, targets_file: str = "targets.json", output_dir: str = "data"):
        """
        Initialize the Order Book Recorder.
        
        Args:
            targets_file: Path to targets.json file
            output_dir: Directory to save recorded data
        """
        self.targets_file = targets_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.running = True
        
        proxy = "http://127.0.0.1:7897"
        os.environ["HTTP_PROXY"] = proxy
        os.environ["HTTPS_PROXY"] = proxy
        os.environ["http_proxy"] = proxy
        os.environ["https_proxy"] = proxy
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """处理退出信号"""
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self.running = False
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                current_task = asyncio.current_task(loop)
                for task in asyncio.all_tasks(loop):
                    if task != current_task and not task.done():
                        task.cancel()
        except:
            pass
    
    @staticmethod
    def _event_type_and_snapshot(data: Any) -> Tuple[str, bool]:
        """从 raw_data 解析 event_type 与是否为快照。返回 (event_type, is_snapshot)。"""
        if isinstance(data, list):
            first = data[0] if data else {}
            et = first.get("event_type", "book") if isinstance(first, dict) else "unknown"
            return (et, True)
        if isinstance(data, dict):
            et = data.get("event_type", "unknown")
            is_snap = et == "book"
            return (et, is_snap)
        return ("unknown", False)

    def load_targets(self) -> List[Dict[str, Any]]:
        """加载目标市场列表，过滤 is_active=False 的项。"""
        try:
            with open(self.targets_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            targets = [t for t in (raw if isinstance(raw, list) else []) if t.get("is_active", True)]
            print(f"[{datetime.now().isoformat()}] 加载了 {len(targets)} 个目标市场", flush=True)
            return targets
        except FileNotFoundError:
            print(f"错误: 找不到文件 {self.targets_file}")
            print("请先运行 discovery.py 生成目标市场列表")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"错误: 无法解析 {self.targets_file}: {e}")
            sys.exit(1)
    
    def create_subscription_message(self, asset_ids: List[str]) -> Dict[str, Any]:
        """
        创建订阅消息（根据Polymarket官方格式）
        
        Args:
            asset_ids: CLOB asset IDs列表
            
        Returns:
            订阅消息字典
        """
        return {
            "assets_ids": asset_ids,
            "type": "market"
        }
    
    def _market_file_path(self, market_id: str) -> Path:
        """当前小时对应的市场文件路径（用于按小时轮转）。"""
        if not ROTATE_HOURLY:
            return self.output_dir / f"market_{market_id}.jsonl"
        hour_suffix = datetime.utcnow().strftime("%Y%m%d%H")
        return self.output_dir / f"market_{market_id}_{hour_suffix}.jsonl"

    async def record_market(self, target: Dict[str, Any], _output_file_unused: Path):
        """记录单个市场的订单簿数据；含快照标记、unixtime、看门狗与按小时轮转。"""
        market_id = target["market_id"]
        token_ids = target["token_ids"]

        print(f"[{datetime.now().isoformat()}] 开始记录市场: {market_id}")
        print(f"  问题: {target.get('question', '')}")
        print(f"  Asset IDs: {len(token_ids)}")

        reconnect_delay = 1
        max_reconnect_delay = 60

        while self.running:
            try:
                connect_kwargs = {
                    "additional_headers": {"User-Agent": "Polymarket-Market-Recorder/2.0"},
                    "open_timeout": 30,
                    "ping_interval": None,
                    "ping_timeout": 10,
                    "close_timeout": 10,
                    "max_size": 2 * 1024 * 1024,
                    "max_queue": 8,
                    "write_limit": 64 * 1024,
                    "proxy": True,
                }

                async with websockets.connect(self.WS_MARKET_URL, **connect_kwargs) as websocket:
                    print(f"  [{datetime.now().isoformat()}] WebSocket连接已建立")

                    subscribe_msg = self.create_subscription_message(token_ids)
                    await websocket.send(json.dumps(subscribe_msg))
                    print(f"  [{datetime.now().isoformat()}] 已订阅 {len(token_ids)} 个assets")

                    async def ping_task():
                        while self.running:
                            try:
                                await asyncio.sleep(10)
                                await websocket.send("PING")
                            except Exception:
                                break

                    ping_task_handle = asyncio.create_task(ping_task())
                    reconnect_delay = 1

                    current_hour = datetime.utcnow().strftime("%Y%m%d%H")
                    path = self._market_file_path(market_id)
                    f = await aiofiles.open(path, "a", encoding="utf-8").__aenter__()
                    try:
                        local_ts_ms = int(time.time() * 1000)
                        connection_info = {
                            "timestamp": datetime.now().isoformat(),
                            "local_receipt_ts_ms": local_ts_ms,
                            "unixtime": local_ts_ms // 1000,
                            "event": "connection_established",
                            "market_id": market_id,
                            "token_ids": token_ids,
                        }
                        await f.write(json.dumps(connection_info, ensure_ascii=False) + "\n")

                        write_count = 0
                        last_flush_time = asyncio.get_event_loop().time()
                        last_received_ts = time.time()

                        while self.running:
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                                now = time.time()
                                last_received_ts = now
                                local_receipt_ts_ms = int(now * 1000)
                                unixtime = local_receipt_ts_ms // 1000

                                if message in ("PONG", "PING"):
                                    continue

                                try:
                                    data = json.loads(message)
                                    event_type, is_snapshot = self._event_type_and_snapshot(data)
                                    record = {
                                        "timestamp": datetime.now().isoformat(),
                                        "local_receipt_ts_ms": local_receipt_ts_ms,
                                        "unixtime": unixtime,
                                        "event_type": event_type,
                                        "is_snapshot": is_snapshot,
                                        "market_id": market_id,
                                        "raw_data": data,
                                    }
                                    # 按小时轮转：若已跨小时则切换文件
                                    if ROTATE_HOURLY:
                                        new_hour = datetime.utcnow().strftime("%Y%m%d%H")
                                        if new_hour != current_hour:
                                            await f.flush()
                                            await f.close()
                                            current_hour = new_hour
                                            path = self._market_file_path(market_id)
                                            f = await aiofiles.open(path, "a", encoding="utf-8").__aenter__()

                                    await f.write(json.dumps(record, ensure_ascii=False) + "\n")
                                    write_count += 1

                                    cur = asyncio.get_event_loop().time()
                                    if write_count >= 10 or (cur - last_flush_time) >= 1.0:
                                        await f.flush()
                                        write_count = 0
                                        last_flush_time = cur

                                except json.JSONDecodeError as e:
                                    print(f"  [{datetime.now().isoformat()}] JSON解析错误: {e}")
                                    print(f"  消息内容: {message[:200]}")
                                except Exception as e:
                                    print(f"  [{datetime.now().isoformat()}] 处理消息时出错: {e}")

                            except asyncio.TimeoutError:
                                # 看门狗：超过 WATCHDOG_IDLE_SEC 无数据则重连
                                if time.time() - last_received_ts >= WATCHDOG_IDLE_SEC:
                                    print(f"  [{datetime.now().isoformat()}] 看门狗: {WATCHDOG_IDLE_SEC}s 无数据，主动重连")
                                    break
                                continue
                            except websockets.exceptions.ConnectionClosed:
                                break
                    finally:
                        try:
                            await f.flush()
                        except Exception:
                            pass
                        try:
                            await f.close()
                        except Exception:
                            pass
                        ping_task_handle.cancel()
                        try:
                            await ping_task_handle
                        except (asyncio.CancelledError, Exception):
                            pass
                        try:
                            await websocket.close()
                        except Exception:
                            pass
                    
            except websockets.exceptions.ConnectionClosed:
                if self.running:
                    print(f"  [{datetime.now().isoformat()}] WebSocket连接关闭，{reconnect_delay}秒后重连...")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                else:
                    break
                    
            except websockets.exceptions.InvalidStatus as e:
                if not self.running:
                    break
                status_code = getattr(e, 'status_code', None)
                if status_code == 429:
                    wait_time = min(reconnect_delay * 10, 600)
                    print(f"  [{datetime.now().isoformat()}] ⚠️  请求过于频繁 (HTTP 429)，等待 {wait_time}秒后重连...", flush=True)
                    await asyncio.sleep(wait_time)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                else:
                    print(f"  [{datetime.now().isoformat()}] WebSocket连接被拒绝: {e}", flush=True)
                    if status_code:
                        print(f"  HTTP状态码: {status_code}", flush=True)
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except websockets.exceptions.InvalidURI as e:
                print(f"  [{datetime.now().isoformat()}] WebSocket URL无效: {e}")
                break
            except asyncio.TimeoutError as e:
                if not self.running:
                    break
                print(f"  [{datetime.now().isoformat()}] 连接超时: {e}")
                print(f"  提示: 可能需要代理或VPN才能连接")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except Exception as e:
                if not self.running:
                    break
                error_msg = str(e)
                print(f"  [{datetime.now().isoformat()}] 连接错误: {e}")
                
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    wait_time = min(reconnect_delay * 10, 600)
                    print(f"  [{datetime.now().isoformat()}] ⚠️  检测到速率限制，等待 {wait_time}秒...", flush=True)
                    await asyncio.sleep(wait_time)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                elif "timeout" in error_msg.lower():
                    print(f"  提示: 可能需要代理或VPN", flush=True)
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                else:
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def monitor_resources(self, interval: int = 60):
        """资源监控 + 心跳写入（便于可观测性）。"""
        heartbeat_path = self.output_dir / "heartbeat.jsonl"
        while self.running:
            try:
                now_ms = int(time.time() * 1000)
                heartbeat_line = json.dumps({
                    "event": "heartbeat",
                    "source": "polymarket",
                    "local_receipt_ts_ms": now_ms,
                    "unixtime": now_ms // 1000,
                }, ensure_ascii=False) + "\n"
                async with aiofiles.open(heartbeat_path, "a", encoding="utf-8") as hb:
                    await hb.write(heartbeat_line)
                    await hb.flush()
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] 心跳写入错误: {e}")

            if psutil:
                try:
                    mem = psutil.virtual_memory()
                    cpu = psutil.cpu_percent(interval=1)
                    if mem.percent > 85 or cpu > 80:
                        print(f"[{datetime.now().isoformat()}] ⚠️  资源警告 - 内存: {mem.percent:.1f}%, CPU: {cpu:.1f}%")
                    elif mem.percent > 70:
                        print(f"[{datetime.now().isoformat()}] 📊 资源 - 内存: {mem.percent:.1f}%, CPU: {cpu:.1f}%")
                except Exception as e:
                    print(f"[{datetime.now().isoformat()}] 资源监控错误: {e}")

            await asyncio.sleep(interval)
    
    async def record_all_markets(self, max_concurrent: int = 0):
        """记录所有目标市场"""
        targets = self.load_targets()
        
        if not targets:
            print("没有找到目标市场")
            return
        
        total_count = len(targets)
        
        print(f"[{datetime.now().isoformat()}] 🚀 开始监控所有 {total_count} 个市场", flush=True)
        print(f"  数据保存目录: {self.output_dir.absolute()}", flush=True)
        
        connection_delay = 0.1
        connected_count = 0
        failed_count = 0
        
        async def establish_and_record(target, index):
            nonlocal connected_count, failed_count
            market_id = target["market_id"]
            output_file = self.output_dir / f"market_{market_id}.jsonl"
            
            await asyncio.sleep(index * connection_delay)
            
            try:
                connected_count += 1
                if connected_count % 50 == 0 or connected_count == total_count:
                    print(f"[{datetime.now().isoformat()}] 📊 进度: {connected_count}/{total_count} 个市场已启动", flush=True)
                await self.record_market(target, output_file)
            except Exception as e:
                failed_count += 1
                print(f"  [{datetime.now().isoformat()}] ⚠️  市场 {market_id} 记录失败: {e}")
        
        tasks = [asyncio.create_task(establish_and_record(target, i)) for i, target in enumerate(targets)]
        monitor_task = asyncio.create_task(self.monitor_resources())
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            
            for task in tasks:
                if not task.done():
                    task.cancel()
            monitor_task.cancel()
            
            try:
                await asyncio.wait_for(
                    asyncio.gather(*[t for t in tasks if not t.done()], monitor_task, return_exceptions=True),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                pass
            
            print(f"\n[{datetime.now().isoformat()}] 所有记录任务已停止")
            print(f"  ✅ 成功连接: {connected_count}/{total_count} 个市场")
            if failed_count > 0:
                print(f"  ❌ 连接失败: {failed_count} 个市场")


def main():
    """Main entry point for the recorder script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="记录Polymarket Level 2订单簿数据")
    parser.add_argument(
        "--targets",
        default="targets.json",
        help="目标市场文件路径 (默认: targets.json)"
    )
    parser.add_argument(
        "--output-dir",
        default="data",
        help="数据输出目录 (默认: data)"
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=0,
        help="同时启动的任务数限制 (默认: 0表示不限制，同时监控所有市场。设置为正数可分批启动)"
    )
    
    args = parser.parse_args()
    
    print(f"[{datetime.now().isoformat()}] 正在初始化...", flush=True)
    recorder = OrderBookRecorder(
        targets_file=args.targets,
        output_dir=args.output_dir
    )
    print(f"[{datetime.now().isoformat()}] 初始化完成，开始记录...", flush=True)
    
    try:
        asyncio.run(recorder.record_all_markets(max_concurrent=args.concurrent))
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().isoformat()}] 程序已停止", flush=True)
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] 程序异常: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
