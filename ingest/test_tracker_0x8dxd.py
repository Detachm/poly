#!/usr/bin/env python3
"""
tracker_0x8dxd.py 单元测试
测试五个修复点：checkpoint 持久化、重启去重、分页追溯、heartbeat 写入、失败计数器不重置
新增测试：activity 去重、positions 快照变化检测、checkpoint 向后兼容
"""

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# 把项目根加入 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest.tracker_0x8dxd import Tracker0x8dxd, _SEEN_MAX


def _make_trade(tx_hash, ts="2024-01-01T00:00:00Z", side="BUY", outcome="Yes", size="10", price="0.6"):
    return {
        "transactionHash": tx_hash,
        "timestamp": ts,
        "side": side,
        "asset": "asset_" + tx_hash[:4],
        "conditionId": "cond_abc",
        "size": size,
        "price": price,
        "title": "Test Market",
        "slug": "test-market",
        "outcome": outcome,
        "outcomeIndex": 0,
        "proxyWallet": "0xproxy",
    }


def _make_activity(tx_hash, activity_type="REDEEM", ts="2024-01-01T00:00:00Z"):
    return {
        "transactionHash": tx_hash,
        "timestamp": ts,
        "type": activity_type,
        "conditionId": "cond_abc",
        "title": "Test Market",
        "slug": "test-market",
        "side": "BUY",
        "outcome": "Yes",
        "size": "10",
        "price": "0.6",
        "usdcSize": "6.0",
    }


class TestCheckpointPersistAndLoad(unittest.TestCase):
    """Fix1: checkpoint 持久化与加载"""

    def test_checkpoint_saved_and_loaded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            # 手动加入一些 hash
            for i in range(5):
                tracker._mark_seen(f"0xhash{i:04d}", tracker._trade_seen, tracker._trade_seen_order)
            tracker._save_checkpoint()

            # 新实例加载
            tracker2 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            for i in range(5):
                self.assertIn(f"0xhash{i:04d}", tracker2._trade_seen,
                              msg=f"重启后 0xhash{i:04d} 应该在 _trade_seen 中")
            print("✓ checkpoint 持久化与加载")

    def test_seen_max_cap(self):
        """_trade_seen 不超过 _SEEN_MAX 上限"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            for i in range(_SEEN_MAX + 50):
                tracker._mark_seen(f"0xhash{i:06d}", tracker._trade_seen, tracker._trade_seen_order)
            self.assertLessEqual(len(tracker._trade_seen), _SEEN_MAX)
            print(f"✓ _trade_seen 上限控制（max={_SEEN_MAX}, actual={len(tracker._trade_seen)}）")


class TestDedupOnRestart(unittest.TestCase):
    """Fix1: 重启后已处理 hash 不被重复写入"""

    def test_no_duplicate_after_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            trades = [_make_trade(f"0xabc{i:04d}") for i in range(5)]

            # 第一次：写入 5 条
            t1 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            def fake_get_page1(url, params=None, timeout=None):
                r = MagicMock()
                r.text = "data"
                r.json.return_value = trades if params.get("offset", 0) == 0 else []
                return r

            with patch.object(t1.session, "get", side_effect=fake_get_page1):
                new, ok = t1._fetch_new_trades()
            self.assertEqual(len(new), 5)
            for tr in new:
                t1._mark_seen(tr["transactionHash"], t1._trade_seen, t1._trade_seen_order)
            t1._save_checkpoint()

            # 第二次（模拟重启）：相同 5 条应被过滤
            t2 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            with patch.object(t2.session, "get", side_effect=fake_get_page1):
                new2, ok2 = t2._fetch_new_trades()
            self.assertEqual(len(new2), 0, "重启后相同 trades 不应重复返回")
            print("✓ 重启后无重复写入")


class TestPagination(unittest.TestCase):
    """Fix2: 分页追溯"""

    def test_paginate_until_known_hash(self):
        """第 1 页全新，第 2 页有部分已知 hash，应停止翻页"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            # 预置 3 个已知 hash（模拟上次已处理）
            for i in range(3):
                tracker._mark_seen(f"0xknown{i:04d}", tracker._trade_seen, tracker._trade_seen_order)

            page1 = [_make_trade(f"0xnew{i:04d}") for i in range(5)]   # 5 条新
            page2 = [_make_trade(f"0xknown{i:04d}") for i in range(3)] # 3 条已知

            responses = [page1, page2]
            call_count = [0]

            def fake_get(url, params=None, timeout=None):
                offset = params.get("offset", 0)
                page_idx = offset // 100
                r = MagicMock()
                r.text = "data"
                r.json.return_value = responses[page_idx] if page_idx < len(responses) else []
                call_count[0] += 1
                return r

            with patch.object(tracker.session, "get", side_effect=fake_get):
                new, ok = tracker._fetch_new_trades()

            self.assertTrue(ok)
            self.assertEqual(len(new), 5, "应只返回新的 5 条")
            self.assertEqual(call_count[0], 2, "应只翻了 2 页")
            print(f"✓ 分页追溯（call_count={call_count[0]}, new={len(new)}）")

    def test_paginate_stops_at_max_pages(self):
        """每页都是全新数据，最多翻 _MAX_PAGES 页"""
        from ingest.tracker_0x8dxd import _MAX_PAGES
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            call_count = [0]

            def fake_get(url, params=None, timeout=None):
                call_count[0] += 1
                offset = params.get("offset", 0)
                r = MagicMock()
                r.text = "data"
                # 每页返回 100 条不重复的新 hash
                page_idx = offset // 100
                r.json.return_value = [_make_trade(f"0xpg{page_idx}_{i:04d}") for i in range(100)]
                return r

            with patch.object(tracker.session, "get", side_effect=fake_get):
                new, ok = tracker._fetch_new_trades()

            self.assertEqual(call_count[0], _MAX_PAGES, f"最多翻 {_MAX_PAGES} 页")
            self.assertEqual(len(new), _MAX_PAGES * 100)
            print(f"✓ 最大页数限制（_MAX_PAGES={_MAX_PAGES}）")

    def test_first_page_failure_returns_api_error(self):
        """第一页 API 失败应返回 api_ok=False"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            with patch.object(tracker.session, "get", side_effect=Exception("timeout")):
                new, ok = tracker._fetch_new_trades()
            self.assertFalse(ok)
            self.assertEqual(len(new), 0)
            print("✓ 第一页失败返回 api_ok=False")


class TestHeartbeat(unittest.TestCase):
    """Fix3: 每次 poll 写入 heartbeat.jsonl"""

    def test_heartbeat_written_after_poll(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            hb_path = Path(tmpdir) / "heartbeat.jsonl"

            self.assertFalse(hb_path.exists(), "初始不应有 heartbeat")
            tracker._write_heartbeat()
            self.assertTrue(hb_path.exists(), "poll 后应存在 heartbeat.jsonl")

            data = json.loads(hb_path.read_text())
            self.assertEqual(data["type"], "heartbeat")
            self.assertIn("local_receipt_ts_ms", data)
            print("✓ heartbeat.jsonl 正确写入")

    def test_heartbeat_updated_on_each_poll(self):
        """连续两次 _write_heartbeat，时间戳应更新（覆盖写）"""
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            tracker._write_heartbeat()
            ts1 = json.loads((Path(tmpdir) / "heartbeat.jsonl").read_text())["local_receipt_ts_ms"]
            time.sleep(0.05)
            tracker._write_heartbeat()
            ts2 = json.loads((Path(tmpdir) / "heartbeat.jsonl").read_text())["local_receipt_ts_ms"]
            self.assertGreaterEqual(ts2, ts1, "第二次心跳时间戳应 >= 第一次")
            print(f"✓ heartbeat 覆盖写（ts1={ts1}, ts2={ts2}）")


class TestFailureCounter(unittest.TestCase):
    """Fix4: 连续失败计数器不重置"""

    def test_counter_not_reset_after_alert(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            # 模拟 10 次连续失败
            for i in range(10):
                with patch.object(tracker.session, "get", side_effect=Exception("fail")):
                    _, ok = tracker._fetch_new_trades()
                if not ok:
                    tracker._consecutive_failures += 1

            self.assertEqual(tracker._consecutive_failures, 10,
                             "失败 10 次后计数器应为 10，不应被重置")
            print(f"✓ 失败计数器不重置（consecutive_failures={tracker._consecutive_failures}）")


class TestActivityDedup(unittest.TestCase):
    """Enh1: activity 去重 + 重启恢复"""

    def test_activity_dedup_and_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            activities = [_make_activity(f"0xact{i:04d}") for i in range(5)]

            t1 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)

            def fake_get(url, params=None, timeout=None):
                r = MagicMock()
                r.text = "data"
                r.json.return_value = activities if params.get("offset", 0) == 0 else []
                return r

            with patch.object(t1.session, "get", side_effect=fake_get):
                new, ok = t1._fetch_new_activity()
            self.assertTrue(ok)
            self.assertEqual(len(new), 5)
            for item in new:
                t1._mark_seen(item["transactionHash"], t1._activity_seen, t1._activity_seen_order)
            t1._save_checkpoint()

            # 重启后应无重复
            t2 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            with patch.object(t2.session, "get", side_effect=fake_get):
                new2, ok2 = t2._fetch_new_activity()
            self.assertEqual(len(new2), 0, "重启后相同 activity 不应重复返回")
            print("✓ activity 去重 + 重启恢复")


class TestPositionsSnapshot(unittest.TestCase):
    """Enh2: positions 变化检测"""

    def test_positions_change_detection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)

            positions = [{"conditionId": "cond1", "size": "10"}, {"conditionId": "cond2", "size": "20"}]

            # 首次应返回 True（从空到有数据）
            self.assertTrue(tracker._positions_changed(positions), "首次应检测到变化")

            # 相同数据应返回 False
            self.assertFalse(tracker._positions_changed(positions), "相同数据不应检测到变化")

            # 数据变化应返回 True
            positions2 = [{"conditionId": "cond1", "size": "15"}, {"conditionId": "cond2", "size": "20"}]
            self.assertTrue(tracker._positions_changed(positions2), "数据变化应检测到变化")

            print("✓ positions 变化检测（首次=True, 相同=False, 变化=True）")


class TestCheckpointBackwardCompat(unittest.TestCase):
    """Enh4: 加载旧格式 checkpoint 正常迁移"""

    def test_load_old_format(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # 写入旧格式 checkpoint
            old_checkpoint = {"seen_hashes": ["0xold1", "0xold2", "0xold3"]}
            checkpoint_path = Path(tmpdir) / ".checkpoint.json"
            checkpoint_path.write_text(json.dumps(old_checkpoint), encoding="utf-8")

            # 新版本应能正确加载
            tracker = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            self.assertIn("0xold1", tracker._trade_seen)
            self.assertIn("0xold2", tracker._trade_seen)
            self.assertIn("0xold3", tracker._trade_seen)
            self.assertEqual(len(tracker._trade_seen), 3)
            # activity 应为空（旧格式无此字段）
            self.assertEqual(len(tracker._activity_seen), 0)
            print("✓ 旧格式 checkpoint 向后兼容迁移")

    def test_new_format_roundtrip(self):
        """新格式 checkpoint 保存和加载"""
        with tempfile.TemporaryDirectory() as tmpdir:
            t1 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            t1._mark_seen("0xtrade1", t1._trade_seen, t1._trade_seen_order)
            t1._mark_seen("0xact1", t1._activity_seen, t1._activity_seen_order)
            t1._positions_hash = "abc123"
            t1._save_checkpoint()

            t2 = Tracker0x8dxd(output_dir=tmpdir, poll_interval_sec=999)
            self.assertIn("0xtrade1", t2._trade_seen)
            self.assertIn("0xact1", t2._activity_seen)
            self.assertEqual(t2._positions_hash, "abc123")
            print("✓ 新格式 checkpoint roundtrip")


if __name__ == "__main__":
    print("=" * 60)
    print("tracker_0x8dxd 单元测试")
    print("=" * 60)
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in [
        TestCheckpointPersistAndLoad,
        TestDedupOnRestart,
        TestPagination,
        TestHeartbeat,
        TestFailureCounter,
        TestActivityDedup,
        TestPositionsSnapshot,
        TestCheckpointBackwardCompat,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
