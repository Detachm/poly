#!/home/hliu/miniconda3/envs/poly_new/bin/python
"""
Market Data Recorder for Polymarket
同时录制交易流（trades）和订单簿快照（orderbooks），保存为 Parquet 格式。

注意：订单簿快照默认通过 REST 轮询（约 1 秒一次），对毫秒级套利策略延迟较大。
如需低延迟，请使用 recorder.py（纯 WebSocket，含 ticks 与 book 推送），
或设置 USE_REST_ORDERBOOK=False 仅录制 WebSocket 交易流。
"""

import asyncio
import json
import websockets
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import aiohttp
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import os
import signal
import sys
import time
from collections import defaultdict

# 禁用输出缓冲
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, line_buffering=True)


# ============================================================================
# 配置区域
# ============================================================================
class Config:
    """配置参数"""
    # 数据源配置
    WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    CLOB_API_URL = "https://clob.polymarket.com"
    CHAIN_ID = 137  # Polygon
    
    # 批量刷新配置
    BATCH_SIZE = 1000  # 每 N 条记录刷新一次
    FLUSH_INTERVAL_SEC = 10.0  # 每 M 秒刷新一次
    
    # REST API 轮询配置（对高频策略建议关闭，用 recorder.py 的 WebSocket 代替）
    USE_REST_ORDERBOOK = False  # True=同时轮询订单簿；False=仅 WebSocket 交易流
    POLL_INTERVAL_SEC = 1.0  # 订单簿快照轮询间隔（秒）
    
    # 订单簿深度
    ORDERBOOK_DEPTH = 20  # 记录前 N 档
    
    # 速率限制配置（严格遵守 Polymarket API 限制）
    # CLOB /book: 1500 requests / 10s = 150 requests/s
    # 为了安全，我们使用更保守的限制：120 requests/s (1200 requests / 10s)
    RATE_LIMIT_REQUESTS_PER_10S = 1200  # 每10秒最大请求数（保守值，实际限制是1500）
    RATE_LIMIT_WINDOW_SEC = 10.0  # 速率限制时间窗口（秒）
    
    # 代理配置
    PROXY = "http://127.0.0.1:7897"
    
    # 输出目录
    OUTPUT_DIR = Path("data")
    TRADES_DIR = OUTPUT_DIR / "trades"
    ORDERBOOKS_DIR = OUTPUT_DIR / "orderbooks"


# ============================================================================
# 数据记录器类
# ============================================================================
class MarketRecorder:
    """市场数据记录器 - 双流并发架构"""
    
    def __init__(self, targets_file: str = "targets.json"):
        """
        初始化记录器
        
        Args:
            targets_file: 目标市场 JSON 文件路径
        """
        self.targets_file = targets_file
        self.running = True
        
        # 创建输出目录
        Config.TRADES_DIR.mkdir(parents=True, exist_ok=True)
        Config.ORDERBOOKS_DIR.mkdir(parents=True, exist_ok=True)
        
        # 设置代理
        os.environ["HTTP_PROXY"] = Config.PROXY
        os.environ["HTTPS_PROXY"] = Config.PROXY
        os.environ["http_proxy"] = Config.PROXY
        os.environ["https_proxy"] = Config.PROXY
        
            # 初始化 HTTP 会话（用于 REST API）
        self.http_session = None
        
        # 数据缓冲区：{token_id: list}
        self.trades_buffers = defaultdict(list)
        self.orderbooks_buffers = defaultdict(list)
        
        # 最后刷新时间：{token_id: timestamp}
        self.last_flush_time = defaultdict(float)
        
        # 文件序列号：{token_id: seq}
        self.file_sequences = defaultdict(int)
        
        # 速率限制控制（滑动窗口）
        self.rate_limit_timestamps = []  # 记录最近10秒内的请求时间戳
        self._rate_limit_lock = None  # 将在异步上下文中初始化
        
        # 统计信息
        self.stats = {
            'trades_recorded': defaultdict(int),
            'orderbooks_recorded': defaultdict(int),
            'trades_files_written': defaultdict(int),
            'orderbooks_files_written': defaultdict(int),
            'ws_reconnects': defaultdict(int),
            'rest_errors': defaultdict(int),
            'rate_limit_delays': 0,  # 速率限制导致的延迟次数
            'rate_limit_violations': 0  # 速率限制违规次数（警告）
        }
        
        # 信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """处理退出信号"""
        print(f"\n[{datetime.now().isoformat()}] 收到退出信号，正在关闭...")
        self.running = False
        # 刷新所有缓冲区
        asyncio.create_task(self._flush_all_buffers())
    
    def load_targets(self) -> List[Dict[str, Any]]:
        """加载目标市场列表"""
        try:
            with open(self.targets_file, "r", encoding="utf-8") as f:
                targets = json.load(f)
            print(f"[{datetime.now().isoformat()}] 加载了 {len(targets)} 个目标市场")
            return targets
        except FileNotFoundError:
            print(f"错误: 找不到文件 {self.targets_file}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"错误: 无法解析 {self.targets_file}: {e}")
            sys.exit(1)
    
    def _get_timestamp_ms(self) -> int:
        """获取当前时间戳（毫秒）"""
        return int(time.time() * 1000)
    
    def _parse_timestamp(self, ts: Any) -> int:
        """解析时间戳为毫秒"""
        if isinstance(ts, (int, float)):
            # 如果是秒级时间戳，转换为毫秒
            if ts < 1e12:
                return int(ts * 1000)
            return int(ts)
        elif isinstance(ts, str):
            try:
                # 尝试解析 ISO 格式或数字字符串
                if ts.isdigit():
                    ts_val = int(ts)
                    if ts_val < 1e12:
                        return ts_val * 1000
                    return ts_val
                # ISO 格式
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            except:
                return self._get_timestamp_ms()
        return self._get_timestamp_ms()
    
    def _create_subscription_message(self, token_ids: List[str]) -> Dict[str, Any]:
        """创建 WebSocket 订阅消息（使用旧代码格式）"""
        return {
            "assets_ids": token_ids,
            "type": "market"
        }
    
    def _get_output_filename(self, token_id: str, data_type: str) -> Path:
        """生成输出文件名"""
        seq = self.file_sequences[token_id]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = Config.TRADES_DIR if data_type == "trades" else Config.ORDERBOOKS_DIR
        filename = f"{data_type}_{token_id[:16]}_{timestamp}_{seq:04d}.parquet"
        return base_dir / filename
    
    async def _flush_trades_buffer(self, token_id: str, force: bool = False):
        """刷新交易数据缓冲区到 Parquet 文件"""
        buffer = self.trades_buffers[token_id]
        if not buffer:
            return
        
        current_time = time.time()
        should_flush = (
            force or
            len(buffer) >= Config.BATCH_SIZE or
            (current_time - self.last_flush_time[token_id]) >= Config.FLUSH_INTERVAL_SEC
        )
        
        if not should_flush:
            return
        
        try:
            # 创建 DataFrame
            df = pd.DataFrame(buffer)
            
            # 确保数据类型正确
            df['timestamp'] = df['timestamp'].astype('int64')
            df['price'] = df['price'].astype('float64')
            df['size'] = df['size'].astype('float64')
            
            # 写入 Parquet
            output_file = self._get_output_filename(token_id, "trades")
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # 更新统计
            self.stats['trades_files_written'][token_id] += 1
            self.stats['trades_recorded'][token_id] += len(buffer)
            
            # 每10次写入或每100条记录打印一次
            if (self.stats['trades_files_written'][token_id] % 10 == 0 or 
                len(buffer) >= 100):
                print(f"[{datetime.now().isoformat()}] ✓ 交易数据: {token_id[:16]}... "
                      f"{len(buffer)} 条 -> {output_file.name}")
            
            # 清空缓冲区
            buffer.clear()
            self.last_flush_time[token_id] = current_time
            
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ✗ 写入交易数据失败 ({token_id[:16]}...): {e}")
    
    async def _flush_orderbooks_buffer(self, token_id: str, force: bool = False):
        """刷新订单簿数据缓冲区到 Parquet 文件"""
        buffer = self.orderbooks_buffers[token_id]
        if not buffer:
            return
        
        current_time = time.time()
        should_flush = (
            force or
            len(buffer) >= Config.BATCH_SIZE or
            (current_time - self.last_flush_time[token_id]) >= Config.FLUSH_INTERVAL_SEC
        )
        
        if not should_flush:
            return
        
        try:
            # 创建 DataFrame
            df = pd.DataFrame(buffer)
            
            # 确保数据类型正确
            df['timestamp'] = df['timestamp'].astype('int64')
            
            # 写入 Parquet
            output_file = self._get_output_filename(token_id, "orderbooks")
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # 更新统计
            self.stats['orderbooks_files_written'][token_id] += 1
            self.stats['orderbooks_recorded'][token_id] += len(buffer)
            
            # 每20次写入或每100条记录打印一次
            if (self.stats['orderbooks_files_written'][token_id] % 20 == 0 or 
                len(buffer) >= 100):
                print(f"[{datetime.now().isoformat()}] ✓ 订单簿: {token_id[:16]}... "
                      f"{len(buffer)} 条 -> {output_file.name}")
            
            # 清空缓冲区
            buffer.clear()
            self.last_flush_time[token_id] = current_time
            self.file_sequences[token_id] += 1
            
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] ✗ 写入订单簿失败 ({token_id[:16]}...): {e}")
    
    async def _flush_all_buffers(self):
        """刷新所有缓冲区"""
        print(f"[{datetime.now().isoformat()}] 刷新所有缓冲区...")
        for token_id in list(self.trades_buffers.keys()):
            await self._flush_trades_buffer(token_id, force=True)
        for token_id in list(self.orderbooks_buffers.keys()):
            await self._flush_orderbooks_buffer(token_id, force=True)
    
    async def _record_trades_stream_for_market(self, market_id: str, token_ids: List[str], market_info: Dict[str, Any]):
        """录制市场的交易流（WebSocket）- 按市场连接，订阅该市场的所有token"""
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while self.running:
            try:
                connect_kwargs = {
                    "additional_headers": {"User-Agent": "Polymarket-Market-Recorder/2.0"},
                    "open_timeout": 30,
                    "ping_interval": None,  # 使用手动ping
                    "ping_timeout": 10,
                    "close_timeout": 10,
                    "max_size": 2 * 1024 * 1024,
                    "max_queue": 8,
                    "write_limit": 64 * 1024,
                    "proxy": True
                }
                
                async with websockets.connect(Config.WS_MARKET_URL, **connect_kwargs) as websocket:
                    # 移除首次连接打印，避免重复打印
                    
                    # 订阅该市场的所有token（使用旧代码格式）
                    subscribe_msg = self._create_subscription_message(token_ids)
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    reconnect_delay = 1  # 重置重连延迟
                    
                    # 添加ping保活机制（参考旧代码）
                    async def ping_task():
                        while self.running:
                            try:
                                await asyncio.sleep(10)  # 每10秒发送一次PING
                                await websocket.send("PING")
                            except Exception:
                                break
                    
                    ping_task_handle = asyncio.create_task(ping_task())
                    
                    try:
                        while self.running:
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                                
                                if message in ("PONG", "PING"):
                                    continue
                                
                                try:
                                    data = json.loads(message)
                                    
                                    # 处理交易事件
                                    # 根据文档，交易事件格式可能是数组或单个对象
                                    events = data if isinstance(data, list) else [data]
                                    
                                    for event in events:
                                        event_type = event.get("event_type", "")
                                        
                                        # 只处理交易相关事件
                                        if "trade" in event_type.lower() or "last_trade" in event_type.lower():
                                            asset_id = event.get("asset_id", "")
                                            
                                            # 检查是否是该市场的token
                                            if asset_id not in token_ids:
                                                continue
                                            
                                            # 提取交易数据
                                            price_str = event.get("price", "0")
                                            size_str = event.get("size", "0")
                                            side = event.get("side", "")
                                            timestamp_str = event.get("timestamp", "")
                                            
                                            try:
                                                price = float(price_str)
                                                size = float(size_str)
                                                timestamp = self._parse_timestamp(timestamp_str)
                                                local_receipt_ts_ms = int(time.time() * 1000)
                                                
                                                # 验证数据
                                                if price > 0 and size > 0:
                                                    trade_record = {
                                                        'timestamp': timestamp,
                                                        'local_receipt_ts_ms': local_receipt_ts_ms,
                                                        'token_id': asset_id,
                                                        'price': price,
                                                        'size': size,
                                                        'side': side,
                                                        'maker_address': event.get("maker_address", "")
                                                    }
                                                    
                                                    self.trades_buffers[asset_id].append(trade_record)
                                                    
                                                    # 检查是否需要刷新
                                                    await self._flush_trades_buffer(asset_id)
                                            
                                            except (ValueError, TypeError) as e:
                                                # 数据格式错误，跳过
                                                continue
                                
                                except json.JSONDecodeError:
                                    continue
                            
                            except asyncio.TimeoutError:
                                # 定期刷新所有token的缓冲区
                                for token_id in token_ids:
                                    await self._flush_trades_buffer(token_id)
                                continue
                            
                            except websockets.exceptions.ConnectionClosed:
                                break
                    finally:
                        # 清理ping任务
                        ping_task_handle.cancel()
                        try:
                            await ping_task_handle
                        except (asyncio.CancelledError, Exception):
                            pass
            
            except websockets.exceptions.ConnectionClosed:
                if self.running:
                    # 使用market_id作为key来统计重连
                    reconnect_key = market_id
                    if reconnect_key not in self.stats['ws_reconnects']:
                        self.stats['ws_reconnects'][reconnect_key] = 0
                    self.stats['ws_reconnects'][reconnect_key] += 1
                    
                    # 只在重连时打印，且每50次重连打印一次详细信息
                    if self.stats['ws_reconnects'][reconnect_key] % 50 == 1:
                        print(f"[{datetime.now().isoformat()}] WebSocket 重连 (市场: {market_id[:16]}...), "
                              f"第 {self.stats['ws_reconnects'][reconnect_key]} 次")
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                else:
                    break
            
            except Exception as e:
                if not self.running:
                    break
                reconnect_key = market_id
                if reconnect_key not in self.stats['ws_reconnects']:
                    self.stats['ws_reconnects'][reconnect_key] = 0
                self.stats['ws_reconnects'][reconnect_key] += 1
                
                # 每100次错误打印一次
                if self.stats['ws_reconnects'][reconnect_key] % 100 == 0:
                    print(f"[{datetime.now().isoformat()}] WebSocket 错误 (市场: {market_id[:16]}...): {e}")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
        
        # 退出前刷新所有token的缓冲区
        for token_id in token_ids:
            await self._flush_trades_buffer(token_id, force=True)
    
    async def _wait_for_rate_limit(self):
        """等待直到可以发送请求（速率限制控制）"""
        # 延迟初始化锁（在异步上下文中）
        if self._rate_limit_lock is None:
            self._rate_limit_lock = asyncio.Lock()
        
        async with self._rate_limit_lock:
            current_time = time.time()
            
            # 清理10秒前的请求记录
            cutoff_time = current_time - Config.RATE_LIMIT_WINDOW_SEC
            self.rate_limit_timestamps = [
                ts for ts in self.rate_limit_timestamps if ts > cutoff_time
            ]
            
            # 检查是否超过速率限制
            if len(self.rate_limit_timestamps) >= Config.RATE_LIMIT_REQUESTS_PER_10S:
                # 计算需要等待的时间（等待最旧的请求过期）
                oldest_timestamp = min(self.rate_limit_timestamps)
                wait_time = Config.RATE_LIMIT_WINDOW_SEC - (current_time - oldest_timestamp) + 0.1
                
                if wait_time > 0:
                    self.stats['rate_limit_delays'] += 1
                    # 每50次延迟打印一次
                    if self.stats['rate_limit_delays'] % 50 == 0:
                        print(f"[{datetime.now().isoformat()}] ⚠️  速率限制：等待 {wait_time:.2f} 秒 "
                              f"(当前: {len(self.rate_limit_timestamps)}/{Config.RATE_LIMIT_REQUESTS_PER_10S} requests/10s)")
                    await asyncio.sleep(wait_time)
                    
                    # 重新清理
                    current_time = time.time()
                    cutoff_time = current_time - Config.RATE_LIMIT_WINDOW_SEC
                    self.rate_limit_timestamps = [
                        ts for ts in self.rate_limit_timestamps if ts > cutoff_time
                    ]
            
            # 记录本次请求
            self.rate_limit_timestamps.append(time.time())
    
    async def _get_order_book(self, token_id: str) -> Optional[Dict[str, Any]]:
        """通过 REST API 获取订单簿（带速率限制）"""
        # 等待速率限制
        await self._wait_for_rate_limit()
        
        if self.http_session is None:
            connector = aiohttp.TCPConnector(limit=100)
            timeout = aiohttp.ClientTimeout(total=10)
            self.http_session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={"User-Agent": "Polymarket-Market-Recorder/2.0"}
            )
        
        # Polymarket CLOB API endpoint
        url = f"{Config.CLOB_API_URL}/book"
        params = {"token_id": token_id}
        
        try:
            async with self.http_session.get(url, params=params, proxy=Config.PROXY) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                elif response.status == 404:
                    # Token 不存在或已下架
                    return None
                elif response.status == 429:
                    # 速率限制被触发（虽然我们已经控制，但以防万一）
                    self.stats['rate_limit_violations'] += 1
                    retry_after = response.headers.get('Retry-After', '5')
                    wait_time = float(retry_after) if retry_after.isdigit() else 5.0
                    print(f"[{datetime.now().isoformat()}] ⚠️  收到 429 响应，等待 {wait_time} 秒")
                    await asyncio.sleep(wait_time)
                    return None
                else:
                    raise Exception(f"HTTP {response.status}")
        except aiohttp.ClientError as e:
            raise Exception(f"网络错误: {e}")
        except Exception as e:
            raise Exception(f"API 错误: {e}")
    
    async def _record_orderbook_snapshots(self, token_id: str):
        """录制订单簿快照（REST API 轮询）"""
        while self.running:
            try:
                # 获取订单簿
                order_book_data = await self._get_order_book(token_id)
                
                if order_book_data is None:
                    raise Exception("订单簿数据为空")
                
                timestamp = self._get_timestamp_ms()
                local_receipt_ts_ms = int(time.time() * 1000)
                
                # 提取 bids 和 asks（前 N 档）
                # 支持多种格式：对象格式 {"price": "...", "size": "..."} 或数组格式 ["price", "size"]
                bids_raw = order_book_data.get("bids", [])[:Config.ORDERBOOK_DEPTH]
                asks_raw = order_book_data.get("asks", [])[:Config.ORDERBOOK_DEPTH]
                
                # 解析 bids
                bids_prices = []
                bids_sizes = []
                for bid in bids_raw:
                    if isinstance(bid, dict):
                        bids_prices.append(float(bid.get("price", 0)))
                        bids_sizes.append(float(bid.get("size", 0)))
                    elif isinstance(bid, (list, tuple)) and len(bid) >= 2:
                        bids_prices.append(float(bid[0]))
                        bids_sizes.append(float(bid[1]))
                
                # 解析 asks
                asks_prices = []
                asks_sizes = []
                for ask in asks_raw:
                    if isinstance(ask, dict):
                        asks_prices.append(float(ask.get("price", 0)))
                        asks_sizes.append(float(ask.get("size", 0)))
                    elif isinstance(ask, (list, tuple)) and len(ask) >= 2:
                        asks_prices.append(float(ask[0]))
                        asks_sizes.append(float(ask[1]))
                
                # 构建记录（含本地接收时间戳，便于多源对齐）
                orderbook_record = {
                    'timestamp': timestamp,
                    'local_receipt_ts_ms': local_receipt_ts_ms,
                    'token_id': token_id,
                    'bids_price_array': bids_prices,
                    'bids_size_array': bids_sizes,
                    'asks_price_array': asks_prices,
                    'asks_size_array': asks_sizes
                }
                
                self.orderbooks_buffers[token_id].append(orderbook_record)
                
                # 检查是否需要刷新
                await self._flush_orderbooks_buffer(token_id)
                
            except Exception as e:
                self.stats['rest_errors'][token_id] += 1
                # REST API 错误时跳过本次，继续下一次轮询（每50次打印一次）
                if self.stats['rest_errors'][token_id] % 50 == 0:
                    print(f"[{datetime.now().isoformat()}] REST API 错误 ({token_id[:16]}...): {e}")
            
            # 等待下一次轮询
            await asyncio.sleep(Config.POLL_INTERVAL_SEC)
        
        # 退出前刷新缓冲区
        await self._flush_orderbooks_buffer(token_id, force=True)
    
    async def _record_market(self, market_id: str, token_ids: List[str], market_info: Dict[str, Any]):
        """为单个市场启动录制：WebSocket 交易流 + 可选 REST 订单簿轮询"""
        async def trades_task():
            await self._record_trades_stream_for_market(market_id, token_ids, market_info)
        
        async def orderbooks_tasks():
            tasks = [self._record_orderbook_snapshots(tid) for tid in token_ids]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        if Config.USE_REST_ORDERBOOK:
            await asyncio.gather(trades_task(), orderbooks_tasks(), return_exceptions=True)
        else:
            await trades_task()
    
    async def record_all_tokens(self):
        """录制所有目标市场（按市场连接，而不是按token连接）"""
        targets = self.load_targets()
        
        if not targets:
            print("没有找到目标市场")
            return
        
        # 统计信息
        total_tokens = sum(len(target.get("token_ids", [])) for target in targets)
        total_markets = len(targets)
        
        print(f"[{datetime.now().isoformat()}] 🚀 开始监控 {total_markets} 个市场 ({total_tokens} 个 Token)")
        print(f"  交易数据目录: {Config.TRADES_DIR.absolute()}")
        print(f"  订单簿数据目录: {Config.ORDERBOOKS_DIR.absolute()}")
        print(f"  批量刷新: {Config.BATCH_SIZE} 条或 {Config.FLUSH_INTERVAL_SEC} 秒")
        print(f"  订单簿: {'REST 轮询 ' + str(Config.POLL_INTERVAL_SEC) + 's' if Config.USE_REST_ORDERBOOK else '已关闭（仅 WebSocket 交易流）'}")
        print(f"  速率限制: {Config.RATE_LIMIT_REQUESTS_PER_10S} requests/10s")
        print(f"  连接方式: 按市场连接 ({total_markets} 个 WebSocket 连接)")
        print(f"\n  提示: 详细信息每60秒打印一次，文件写入每10-20次打印一次\n")
        
        # 为每个市场创建录制任务（按市场连接）
        tasks = []
        connected_count = 0
        for target in targets:
            market_id = target.get("market_id", "")
            question = target.get("question", "")
            token_ids = target.get("token_ids", [])
            
            if not token_ids:
                continue
            
            market_info = {
                'market_id': market_id,
                'question': question,
                'token_ids': token_ids
            }
            
            task = asyncio.create_task(self._record_market(market_id, token_ids, market_info))
            tasks.append(task)
            connected_count += 1
            
            # 每50个市场打印一次进度
            if connected_count % 50 == 0 or connected_count == total_markets:
                print(f"[{datetime.now().isoformat()}] 已启动 {connected_count}/{total_markets} 个市场连接...")
            
            # 稍微延迟启动，避免同时连接过多
            await asyncio.sleep(0.1)
        
        print(f"[{datetime.now().isoformat()}] ✓ 所有市场连接已启动，开始录制...\n")
        
        # 定期打印统计信息
        async def print_stats():
            while self.running:
                await asyncio.sleep(60)  # 每60秒打印一次
                if self.running:
                    if self._rate_limit_lock:
                        async with self._rate_limit_lock:
                            current_rate = len(self.rate_limit_timestamps)
                    else:
                        current_rate = len(self.rate_limit_timestamps)
                    
                    total_trades = sum(self.stats['trades_recorded'].values())
                    total_orderbooks = sum(self.stats['orderbooks_recorded'].values())
                    total_trade_files = sum(self.stats['trades_files_written'].values())
                    total_orderbook_files = sum(self.stats['orderbooks_files_written'].values())
                    total_ws_reconnects = sum(self.stats['ws_reconnects'].values())
                    total_rest_errors = sum(self.stats['rest_errors'].values())
                    
                    print(f"\n[{datetime.now().isoformat()}] 📊 统计:")
                    print(f"  交易: {total_trades:,} 条 ({total_trade_files} 文件) | "
                          f"订单簿: {total_orderbooks:,} 条 ({total_orderbook_files} 文件)")
                    print(f"  速率: {current_rate}/{Config.RATE_LIMIT_REQUESTS_PER_10S} req/10s | "
                          f"延迟: {self.stats['rate_limit_delays']} 次 | "
                          f"重连: {total_ws_reconnects} 次 | "
                          f"错误: {total_rest_errors} 次")
        
        stats_task = asyncio.create_task(print_stats())
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            pass
        finally:
            self.running = False
            stats_task.cancel()
            
            # 等待所有任务完成
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            # 关闭 HTTP 会话
            if self.http_session:
                await self.http_session.close()
            
            # 刷新所有缓冲区
            await self._flush_all_buffers()
            
            # 打印最终统计
            print(f"\n[{datetime.now().isoformat()}] 📊 最终统计:")
            print(f"  交易记录: {sum(self.stats['trades_recorded'].values()):,} 条")
            print(f"  订单簿快照: {sum(self.stats['orderbooks_recorded'].values()):,} 条")
            print(f"  交易文件: {sum(self.stats['trades_files_written'].values())} 个")
            print(f"  订单簿文件: {sum(self.stats['orderbooks_files_written'].values())} 个")
            print(f"  WebSocket 重连: {sum(self.stats['ws_reconnects'].values())} 次")
            print(f"  REST API 错误: {sum(self.stats['rest_errors'].values())} 次")
            print(f"  速率限制延迟: {self.stats['rate_limit_delays']} 次")
            if self.stats['rate_limit_violations'] > 0:
                print(f"  ⚠️  速率限制违规: {self.stats['rate_limit_violations']} 次")


# ============================================================================
# 主函数
# ============================================================================
def main():
    """主入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="录制 Polymarket 市场数据（交易流 + 订单簿快照）")
    parser.add_argument(
        "--targets",
        default="targets.json",
        help="目标市场文件路径 (默认: targets.json)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=Config.BATCH_SIZE,
        help=f"批量刷新记录数 (默认: {Config.BATCH_SIZE})"
    )
    parser.add_argument(
        "--flush-interval",
        type=float,
        default=Config.FLUSH_INTERVAL_SEC,
        help=f"批量刷新时间间隔（秒）(默认: {Config.FLUSH_INTERVAL_SEC})"
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=Config.POLL_INTERVAL_SEC,
        help=f"订单簿轮询间隔（秒）(默认: {Config.POLL_INTERVAL_SEC})"
    )
    parser.add_argument(
        "--rest-orderbook",
        action="store_true",
        help="启用 REST 订单簿轮询（默认关闭，低延迟场景请用 recorder.py）"
    )
    
    args = parser.parse_args()
    
    # 更新配置
    Config.BATCH_SIZE = args.batch_size
    Config.FLUSH_INTERVAL_SEC = args.flush_interval
    Config.POLL_INTERVAL_SEC = args.poll_interval
    Config.USE_REST_ORDERBOOK = args.rest_orderbook
    
    print(f"[{datetime.now().isoformat()}] 正在初始化...")
    recorder = MarketRecorder(targets_file=args.targets)
    print(f"[{datetime.now().isoformat()}] 初始化完成，开始录制...")
    
    try:
        asyncio.run(recorder.record_all_tokens())
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().isoformat()}] 程序已停止")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] 程序异常: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
