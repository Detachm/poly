#!/usr/bin/env python3
"""
Discovery 定时合并：每 10 分钟运行一次 discovery，将新出现的 Crypto 市场合并进 targets.json，
供 Polymarket 录制使用。可与 recorder 等进程同时运行。
"""

import json
import os
import sys
import time
from pathlib import Path

# 使用项目内 discovery
from discovery import MarketDiscovery

TARGETS_FILE = "targets.json"
INTERVAL_SEC = 600  # 10 分钟


def load_existing(targets_path: str) -> dict:
    """加载已有 targets，返回 market_id -> target 的映射"""
    path = Path(targets_path)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            list_ = json.load(f)
        return {t["market_id"]: t for t in list_} if isinstance(list_, list) else {}
    except Exception:
        return {}


def merge_targets(existing: dict, new_list: list) -> list:
    """合并：保留已有，加入新 market_id，按 volume 排序"""
    for t in new_list:
        mid = t.get("market_id")
        if mid:
            existing[mid] = t
    out = list(existing.values())
    out.sort(key=lambda x: float(x.get("volume_usdc", 0)), reverse=True)
    return out


def run_once(discovery: MarketDiscovery, targets_path: str) -> int:
    """执行一次 discovery 并保存。Crypto 模式下用本次结果完全替换；非 Crypto 模式下与现有合并。若 API 失败则不覆盖。"""
    existing = load_existing(targets_path)
    try:
        new_list = discovery.discover_targets(limit=None)
    except Exception as e:
        print(f"  [错误] Discovery API 失败: {e}，保留现有 targets 共 {len(existing)} 个")
        return len(existing)
    # Crypto 定向模式：只保留本次 API 返回的 Crypto 市场，不合并历史（避免混入非 Crypto）
    if getattr(discovery, "filter_crypto_only", True):
        merged = new_list
        print(f"  [Crypto 模式] 已用本次 {len(merged)} 个 Crypto 市场覆盖 targets")
    else:
        merged = merge_targets(existing, new_list)
        added = len(merged) - len(existing)
        if added > 0:
            print(f"  [合并] 新增 {added} 个市场，当前共 {len(merged)} 个")
    with open(targets_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    return len(merged)


def main():
    import argparse
    p = argparse.ArgumentParser(description="每 10 分钟运行 discovery 并合并新 Crypto 市场到 targets.json")
    p.add_argument("--targets", default=TARGETS_FILE, help="targets.json 路径")
    p.add_argument("--interval", type=int, default=INTERVAL_SEC, help="间隔秒数")
    p.add_argument("--once", action="store_true", help="只运行一次不循环")
    args = p.parse_args()

    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "http://127.0.0.1:7897"
    discovery = MarketDiscovery(
        min_volume_usdc=1000.0,
        min_outcomes=2,
        proxy=proxy,
        verify_ssl=True,
        filter_crypto_only=True,
    )
    print(f"[Discovery 循环] 每 {args.interval} 秒刷新 targets: {args.targets}\n")

    while True:
        try:
            n = run_once(discovery, args.targets)
            print(f"  [{time.strftime('%H:%M:%S')}] 已保存 {n} 个市场")
        except KeyboardInterrupt:
            print("\n用户停止")
            break
        except Exception as e:
            print(f"  [错误] {e}")
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
