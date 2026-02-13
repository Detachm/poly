#!/home/hliu/miniconda3/envs/poly_new/bin/python
"""
Market Discovery Module for Polymarket (Hardened)
Scans the Gamma API with robust error handling for proxy connections.
"""

import json
import re
import requests
import time
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 定向狙击：只保留 Crypto 相关市场（BTC / ETH / SOL / XRP，与 BN、Chainlink 一致）
DEFAULT_CRYPTO_TAGS = ["Bitcoin", "Ethereum", "Solana", "Crypto", "BTC", "ETH", "SOL", "XRP", "Ripple"]


class MarketDiscovery:
    """Discovers and filters Polymarket markets for arbitrage opportunities."""
    
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    
    def __init__(
        self,
        min_volume_usdc: float = 1000.0,
        min_outcomes: int = 2,
        proxy: Optional[str] = None,
        verify_ssl: bool = True,
        filter_crypto_only: bool = True,
        crypto_tags: Optional[List[str]] = None,
    ):
        self.min_volume_usdc = min_volume_usdc
        self.min_outcomes = min_outcomes
        self.proxy = proxy or self._detect_proxy()
        self.verify_ssl = verify_ssl  # SSL 验证开关
        self.filter_crypto_only = filter_crypto_only
        self.crypto_tags = crypto_tags or DEFAULT_CRYPTO_TAGS
        
        # 如果指定了代理，也设置环境变量（某些库可能使用环境变量）
        if self.proxy:
            os.environ['HTTP_PROXY'] = self.proxy
            os.environ['HTTPS_PROXY'] = self.proxy
            os.environ['http_proxy'] = self.proxy
            os.environ['https_proxy'] = self.proxy
        
        # 初始化 Session
        self._init_session()
    
    def _test_proxy_connection(self) -> bool:
        """测试代理连接是否正常"""
        if not self.proxy:
            return True  # 没有代理，直接返回 True
        
        # 先测试 HTTP 连接（检查代理是否基本可用）
        try:
            print(f"  [诊断] 测试代理 HTTP 连接...")
            http_test = requests.get(
                "http://httpbin.org/ip",
                proxies={"http": self.proxy, "https": self.proxy},
                timeout=5
            )
            if http_test.status_code == 200:
                print(f"  [诊断] ✓ HTTP 连接正常")
            else:
                print(f"  [诊断] ✗ HTTP 连接异常: 状态码 {http_test.status_code}")
        except Exception as e:
            print(f"  [诊断] ✗ HTTP 连接失败: {type(e).__name__}")
        
        # 测试 HTTPS 连接（使用多个测试 URL，避免被代理规则阻止）
        test_urls = [
            "https://httpbin.org/ip",  # 优先使用 httpbin，更可靠
            "https://www.google.com",
            "https://gamma-api.polymarket.com/events?limit=1",  # 直接测试目标 API
        ]
        
        for test_url in test_urls:
            try:
                print(f"  [诊断] 测试代理 HTTPS 连接 ({test_url[:30]}...)...")
                response = requests.get(
                    test_url, 
                    proxies={"http": self.proxy, "https": self.proxy},
                    timeout=10,
                    verify=self.verify_ssl
                )
                if response.status_code == 200:
                    print(f"  [诊断] ✓ HTTPS 连接正常")
                    if "ip" in test_url:
                        try:
                            print(f"  [诊断] → 返回 IP: {response.json().get('origin', 'N/A')}")
                        except:
                            pass
                    return True
            except requests.exceptions.SSLError as e:
                error_msg = str(e)
                if "UNEXPECTED_EOF" in error_msg or "unexpected eof" in error_msg.lower():
                    # 如果是最后一个测试 URL 也失败，才报告错误
                    if test_url == test_urls[-1]:
                        print(f"  [诊断] ✗ HTTPS SSL 错误: {error_msg[:200]}")
                        print(f"  [诊断] ⚠️  代理服务器不支持 HTTPS 连接！")
                        print(f"  [诊断] ⚠️  这通常意味着代理配置有问题或代理类型不匹配")
                        print(f"  [诊断] 💡 建议：")
                        print(f"     1. 检查代理服务器配置（可能需要支持 CONNECT 方法）")
                        print(f"     2. 确认代理类型（HTTP 代理 vs SOCKS 代理）")
                        print(f"     3. 尝试使用 --no-proxy 参数直接连接（如果网络允许）")
                    continue  # 尝试下一个 URL
                else:
                    # 其他 SSL 错误，继续尝试下一个 URL
                    continue
            except Exception as e:
                # 其他错误（如连接超时、代理错误等），继续尝试下一个 URL
                if test_url == test_urls[-1]:
                    print(f"  [诊断] ✗ HTTPS 连接失败: {type(e).__name__}: {str(e)[:100]}")
                continue
        
        return False
    
    def _init_session(self):
        """初始化或重置 Session，用于解决 SSL 错误"""
        if hasattr(self, 'session'):
            self.session.close()
            
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Polymarket-Market-Recorder/2.1",
            "Accept": "application/json",
            "Connection": "keep-alive" # 尝试保持连接
        })
        
        # 禁用 SSL 验证（如果指定）
        if not self.verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.session.verify = False
        
        # 尝试使用更宽松的 SSL 配置（解决某些代理的 SSL 握手问题）
        import ssl
        import urllib3.util.ssl_
        
        # 创建更宽松的 SSL 上下文
        try:
            # 尝试使用默认的 SSL 上下文，但允许更多协议版本
            ssl_context = ssl.create_default_context()
            # 允许更多 TLS 版本（某些代理可能需要）
            ssl_context.minimum_version = ssl.TLSVersion.MINIMUM_SUPPORTED
            ssl_context.check_hostname = False if not self.verify_ssl else True
            ssl_context.verify_mode = ssl.CERT_NONE if not self.verify_ssl else ssl.CERT_REQUIRED
            
            # 设置 urllib3 使用这个上下文
            urllib3.util.ssl_.create_urllib3_context = lambda *args, **kwargs: ssl_context
        except Exception:
            # 如果设置失败，继续使用默认配置
            pass
        
        if self.proxy:
            self.session.proxies = {
                "http": self.proxy,
                "https": self.proxy
            }
            proxy_status = f"，代理: {self.proxy}"
            if not self.verify_ssl:
                proxy_status += " (SSL 验证已禁用)"
            print(f"  [网络] Session 已(重)建立{proxy_status}")
            # 首次建立时测试代理（非阻塞，仅用于诊断）
            if not hasattr(self, '_proxy_tested'):
                proxy_ok = self._test_proxy_connection()
                if not proxy_ok:
                    print(f"  [警告] 代理连接测试失败，但将继续尝试实际请求...")
                self._proxy_tested = True
        
        # 激进的重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=2, # 失败后等待 2s, 4s, 8s...
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        # 使用更少的连接池大小，避免连接复用问题
        adapter = HTTPAdapter(
            max_retries=retry_strategy, 
            pool_connections=1,  # 减少连接池大小
            pool_maxsize=1,      # 避免连接复用可能导致的问题
            pool_block=False
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _detect_proxy(self) -> Optional[str]:
        for env_var in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY"]:
            proxy = os.environ.get(env_var)
            if proxy: return proxy
        return "http://host.docker.internal:7897"
    
    def fetch_active_events(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        url = f"{self.GAMMA_API_BASE}/events"
        api_batch_size = 100
        
        params = {
            "active": "true",
            "closed": "false",
            "limit": api_batch_size,
            "offset": 0,
            "order": "volume24hr",
            "ascending": "false"
        }
        
        all_events = []
        seen_ids = set()
        offset = 0
        consecutive_empty_pages = 0
        max_retries_per_page = 5 # 每一页最多重试5次
        
        print(f"  [API] 开始稳健抓取 (按 Volume24hr 排序)...")
        
        while True:
            if limit and len(all_events) >= limit:
                print(f"  [API] 达到上限 ({limit})，停止。")
                break

            # === 单页重试循环 ===
            page_success = False
            for attempt in range(max_retries_per_page):
                params["offset"] = offset
                # 动态进度条
                sys.stdout.write(f"\r  [API] Offset: {offset} | 已收集: {len(all_events)} | 尝试: {attempt+1}/{max_retries_per_page}")
                sys.stdout.flush()
                
                # 超时时间加长到 30秒
                # 使用 session 的 verify 设置（已在 _init_session 中设置）
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                events = data if isinstance(data, list) else data.get("data", [])
                
                # 1. 判空逻辑
                if not events:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 2:
                        print(f"\n  [API] 连续空页，抓取结束。")
                        return all_events[:limit] if limit else all_events
                else:
                    consecutive_empty_pages = 0
                
                # 2. 数据处理
                for event in events:
                    e_id = event.get("id")
                    if e_id not in seen_ids:
                        all_events.append(event)
                        seen_ids.add(e_id)
                
                # 3. 翻页逻辑
                if len(events) < api_batch_size:
                    print(f"\n  [API] 最后一页数据不足 {api_batch_size} 条，抓取结束。")
                    return all_events[:limit] if limit else all_events
                    
                offset += api_batch_size
                page_success = True
                time.sleep(0.5) # 给一点喘息时间
                break # 跳出重试循环，进入下一页

            # 如果尝试了5次还是失败，只能跳过这页或者退出
            if not page_success:
                print(f"\n  [Error] Offset {offset} 连续失败 {max_retries_per_page} 次，跳过此页...")
                offset += api_batch_size # 迫不得已跳过
        
        print(f"\n  [API] 抓取完成，共获取 {len(all_events)} 个事件。")
        return all_events[:limit] if limit else all_events
    
    def extract_market_data(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        # ... (这部分逻辑保持不变) ...
        markets = event.get("markets", [])
        filtered_markets = []
        for market in markets:
            outcomes_raw = market.get("outcomes", [])
            outcomes = self._safe_parse_json(outcomes_raw)
            if len(outcomes) < self.min_outcomes: continue
            
            volume = self._parse_volume(market.get("volume"))
            if volume < self.min_volume_usdc: continue
            
            token_ids_raw = market.get("clobTokenIds", [])
            token_ids = self._safe_parse_json(token_ids_raw)
            if len(token_ids) != len(outcomes) or len(token_ids) == 0: continue
            
            market_data = {
                "market_id": market.get("id") or market.get("marketId", ""),
                "question": market.get("question") or event.get("question", ""),
                "token_ids": token_ids,
                "outcomes": outcomes,
                "volume_usdc": volume,
                "is_active": True,
            }
            filtered_markets.append(market_data)
        return filtered_markets

    def _safe_parse_json(self, raw_data: Any) -> list:
        if isinstance(raw_data, list): return raw_data
        if isinstance(raw_data, str):
            try: return json.loads(raw_data)
            except: return []
        return []

    def _parse_volume(self, vol_raw: Any) -> float:
        if vol_raw is None: return 0.0
        try: return float(vol_raw)
        except: return 0.0

    def _event_matches_crypto_tags(self, event: Dict[str, Any]) -> bool:
        """判断 event 是否属于 Crypto 相关（tags / groupItemTitle / title / slug 包含关键词）。"""
        text_parts: List[str] = []
        tags = event.get("tags")
        if isinstance(tags, list):
            text_parts.extend(str(t) for t in tags)
        elif isinstance(tags, str):
            text_parts.append(tags)
        for key in ("groupItemTitle", "title", "slug", "question", "description"):
            val = event.get(key)
            if isinstance(val, str) and val:
                text_parts.append(val)
        combined = " ".join(text_parts).lower()
        for keyword in self.crypto_tags:
            if keyword.lower() in combined:
                return True
        return False

    def _market_question_matches_crypto(self, market: Dict[str, Any]) -> bool:
        """按市场 question 判断是否 Crypto 相关；短词 ETH/SOL/BTC/XRP 按词边界匹配，避免误伤。"""
        q = (market.get("question") or "").lower()
        if not q:
            return False
        for keyword in self.crypto_tags:
            kw = keyword.lower()
            if kw in ("eth", "sol", "btc", "xrp"):
                if kw == "eth" and ("ethereum" in q or bool(re.search(r"\beth\b", q))):
                    return True
                if kw == "sol" and ("solana" in q or bool(re.search(r"\bsol\b", q))):
                    return True
                if kw == "btc" and ("bitcoin" in q or bool(re.search(r"\bbtc\b", q))):
                    return True
                if kw == "xrp" and ("ripple" in q or "xrp" in q or bool(re.search(r"\bxrp\b", q))):
                    return True
            elif kw in q:
                return True
        return False

    def discover_targets(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始扫描市场...")
        if self.filter_crypto_only:
            print(f"  [筛选] 仅保留 Crypto 相关市场 (关键词: {self.crypto_tags})")
        events = self.fetch_active_events(limit=limit)
        all_targets = []
        skipped_events = 0
        for event in events:
            markets = self.extract_market_data(event)
            if self.filter_crypto_only:
                # 只保留 question 含 Crypto 关键词的市场（不依赖 API 是否返回 event tags）
                markets = [m for m in markets if self._market_question_matches_crypto(m)]
                if not markets:
                    skipped_events += 1
            all_targets.extend(markets)
        all_targets.sort(key=lambda x: x['volume_usdc'], reverse=True)
        if self.filter_crypto_only and skipped_events:
            print(f"  [筛选] 已跳过 {skipped_events} 个无 Crypto 市场的事件。")
        print(f"  最终筛选出 {len(all_targets)} 个有效市场。")
        return all_targets
    
    def save_targets(self, targets: List[Dict[str, Any]], output_file: str = "targets.json"):
        path = Path(output_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(targets, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 结果已保存至: {output_file}")

def main():
    proxy = None
    verify_ssl = True
    filter_crypto_only = True
    
    # 检查是否明确禁用代理
    if "--no-proxy" in sys.argv:
        proxy = None
        print("  [信息] 已禁用代理，将直接连接")
    elif "--proxy" in sys.argv:
        try: 
            proxy = sys.argv[sys.argv.index("--proxy") + 1]
        except: 
            print("  [错误] --proxy 参数后需要指定代理地址")
            return
    
    if "--no-verify-ssl" in sys.argv or "--insecure" in sys.argv:
        verify_ssl = False
        print("  [警告] SSL 验证已禁用，连接可能不安全！")
    
    if "--all-markets" in sys.argv:
        filter_crypto_only = False
        print("  [信息] 已启用全部市场（不限于 Crypto）")
    
    # Crypto 定向狙击：成交量门槛可放低，便于捕捉套利机会
    min_volume = 1000.0 if filter_crypto_only else 100000.0
    discovery = MarketDiscovery(
        min_volume_usdc=min_volume,
        min_outcomes=2,
        proxy=proxy,
        verify_ssl=verify_ssl,
        filter_crypto_only=filter_crypto_only,
    )
    try:
        targets = discovery.discover_targets(limit=None)
        discovery.save_targets(targets, "targets.json")
    except KeyboardInterrupt:
        print("\n用户手动停止。")
    except Exception as e:
        print(f"\n严重错误: {e}")

if __name__ == "__main__":
    main()