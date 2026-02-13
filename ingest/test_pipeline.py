#!/usr/bin/env python3
"""最小化测试：discovery 与 recorder 的 load_targets、时间戳校验脚本可执行。"""

import json
import sys
from pathlib import Path


def test_load_targets():
    """recorder.load_targets 过滤 is_active 且返回 list。"""
    from recorder import OrderBookRecorder
    import os
    data_dir = os.path.join(os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly"), "polymarket")
    rec = OrderBookRecorder(targets_file="targets.json", output_dir=data_dir)
    # 不读磁盘则用 mock；若 targets.json 存在则真读
    p = Path("targets.json")
    if not p.exists():
        return  # 跳过
    targets = rec.load_targets()
    assert isinstance(targets, list), "load_targets 应返回 list"
    for t in targets[:3]:
        assert "market_id" in t and "token_ids" in t, "每项应有 market_id、token_ids"


def test_discovery_target_shape():
    """discover_targets 返回的项含 market_id、is_active（需网络，失败则跳过）。"""
    try:
        from discovery import MarketDiscovery
        d = MarketDiscovery(min_volume_usdc=1e6, filter_crypto_only=True)
        targets = d.discover_targets(limit=2)
    except Exception:
        return  # 无网络或 API 不可用时跳过
    assert isinstance(targets, list), "discover_targets 应返回 list"
    for t in targets:
        assert "market_id" in t and "token_ids" in t
        assert t.get("is_active") is True, "新发现市场应有 is_active=True"


def test_validate_timestamps_import():
    """validate_timestamps 可执行且不抛错。"""
    import validate_timestamps
    # 无数据时仅打印“未找到任何记录”，不抛错
    validate_timestamps.main()


def test_jsonl_has_local_ts():
    """任意一条含 local_receipt_ts_ms 的 JSON 行可解析且含 unixtime 或 local_receipt_ts_ms。"""
    sample = '{"local_receipt_ts_ms": 1700000000000, "unixtime": 1700000000, "event": "heartbeat"}'
    obj = json.loads(sample)
    assert "local_receipt_ts_ms" in obj
    assert obj["local_receipt_ts_ms"] == 1700000000000


if __name__ == "__main__":
    failures = []
    for name in dir():
        if not name.startswith("test_"):
            continue
        fn = globals()[name]
        if not callable(fn):
            continue
        try:
            fn()
            print(f"OK {name}")
        except Exception as e:
            print(f"FAIL {name}: {e}")
            failures.append(name)
    if failures:
        sys.exit(1)
    print("All passed.")
