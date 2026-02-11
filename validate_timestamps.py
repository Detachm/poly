#!/usr/bin/env python3
"""
多源录制完整性校验：时间对齐 + 断连/丢包/漏 round/心跳间隔。
输出完整分析到 --output 文件；控制台为摘要。无 --output 时仅打印摘要。
用法: python validate_timestamps.py [--data-dir data] [--output report.txt] ...
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

DEFAULT_GAP_SEC = 120
DEFAULT_HEARTBEAT_MAX_SEC = 90
# 控制台摘要每类最多显示条数
CONSOLE_TOP_N = 10


def _read_last_lines(path: Path, n: int) -> list:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with open(path, "r", encoding="utf-8") as f:
        buf_size = min(256 * 1024, path.stat().st_size)
        f.seek(max(0, path.stat().st_size - buf_size))
        tail = f.read()
    lines = [s.strip() for s in tail.split("\n") if s.strip()][-n:]
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return out


def _latest_ts(records: list) -> int | None:
    for r in reversed(records):
        ts = r.get("local_receipt_ts_ms")
        if ts is not None:
            return int(ts)
    return None


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                pass


def run_timestamp_alignment(root: Path, n: int) -> tuple[list[str], list[str]]:
    """返回 (摘要行, 完整行)，内容相同。"""
    lines = []
    sources = []
    agg = _read_last_lines(root / "binance" / "agg_trades.jsonl", n)
    book = _read_last_lines(root / "binance" / "book_ticker.jsonl", n)
    ts_binance = max(_latest_ts(agg) or 0, _latest_ts(book) or 0) or None
    if ts_binance is not None:
        sources.append(("binance", ts_binance))
    chain = _read_last_lines(root / "chainlink" / "updates.jsonl", n)
    if _latest_ts(chain) is not None:
        sources.append(("chainlink", _latest_ts(chain)))
    track = _read_last_lines(root / "tracker_0x8dxd" / "trades.jsonl", n)
    if _latest_ts(track) is not None:
        sources.append(("tracker_0x8dxd", _latest_ts(track)))
    pm_heart = _read_last_lines(root / "polymarket" / "heartbeat.jsonl", n)
    ts_pm = _latest_ts(pm_heart)
    if ts_pm is not None:
        sources.append(("polymarket_heartbeat", ts_pm))
    else:
        pm_dir = root / "polymarket"
        if pm_dir.exists():
            for f in list(pm_dir.glob("market_*.jsonl"))[:3]:
                t = _latest_ts(_read_last_lines(f, n))
                if t is not None:
                    sources.append((f"polymarket_{f.stem}", t))
                    break
    if not sources:
        lines.append("  [时间对齐] 未找到任何带 local_receipt_ts_ms 的记录")
        return (lines, lines)
    sources.sort(key=lambda x: x[1])
    base = sources[0][1]
    lines.append("  [时间对齐] 各源最新 local_receipt_ts_ms（升序）:")
    for name, ts in sources:
        lines.append("    {}: {}  (+{} ms)".format(name, ts, ts - base))
    if len(sources) >= 2:
        lines.append("    首尾时间差: {} ms".format(sources[-1][1] - sources[0][1]))
    return (lines, lines)


def run_polymarket_integrity(root: Path, gap_sec: int, max_files: int) -> tuple[list[str], list[str]]:
    summary, full = [], []
    pm_dir = root / "polymarket"
    if not pm_dir.exists():
        summary.append("  [Polymarket] 目录不存在")
        return (summary, summary)
    gap_ms = gap_sec * 1000
    all_pm = sorted(pm_dir.glob("market_*.jsonl"))
    total_files = len(all_pm)
    files = all_pm[:max_files]
    if not files:
        summary.append("  [Polymarket] 无 market_*.jsonl")
        return (summary, summary)

    total_connects = 0
    files_with_reconnects = []
    all_gaps = []

    for path in files:
        conn_count = 0
        prev_ts = None
        for rec in _iter_jsonl(path):
            if rec.get("event") == "connection_established":
                conn_count += 1
            ts = rec.get("local_receipt_ts_ms")
            if ts is not None:
                ts = int(ts)
                if prev_ts is not None and (ts - prev_ts) > gap_ms:
                    all_gaps.append((path.name, prev_ts, ts, ts - prev_ts))
                prev_ts = ts
        total_connects += conn_count
        if conn_count > 1:
            files_with_reconnects.append((path.name, conn_count))

    def fmt_poly():
        out = []
        out.append("  [Polymarket] 已检查 {} / {} 个 market 文件".format(len(files), total_files))
        out.append("  [Polymarket] connection_established 总次数（抽样）: {}".format(total_connects))
        if files_with_reconnects:
            out.append("    存在重连的文件（>1 次）: {} 个，完整列表:".format(len(files_with_reconnects)))
            for name, c in sorted(files_with_reconnects, key=lambda x: -x[1]):
                out.append("      {}: {} 次".format(name, c))
        else:
            out.append("    存在重连的文件: 0")
        if all_gaps:
            out.append("  [Polymarket] local_receipt_ts_ms 空洞（>{}s）: {} 处，完整列表:".format(gap_sec, len(all_gaps)))
            for name, prev_ts, next_ts, gap in sorted(all_gaps, key=lambda x: -x[3]):
                out.append("      {}: {} -> {}  空洞 {}s".format(name, prev_ts, next_ts, gap // 1000))
        else:
            out.append("  [Polymarket] 未发现 >{}s 的时间空洞".format(gap_sec))
        return out

    full = fmt_poly()
    summary = [full[0], full[1], full[2]]
    n_reconn = len(files_with_reconnects)
    if n_reconn:
        summary.extend(full[3:3 + min(CONSOLE_TOP_N, n_reconn)])
        if n_reconn > CONSOLE_TOP_N:
            summary.append("      ... 共 {} 个文件".format(n_reconn))
    idx = 3 + n_reconn
    if all_gaps:
        summary.append(full[idx])
        summary.extend(full[idx + 1:idx + 1 + min(CONSOLE_TOP_N, len(all_gaps))])
        if len(all_gaps) > CONSOLE_TOP_N:
            summary.append("      ... 共 {} 处".format(len(all_gaps)))
    else:
        summary.append(full[-1])
    return (summary, full)


def _binance_files_sorted(dir_path: Path, base_name: str, max_files: int) -> list[Path]:
    base = dir_path / f"{base_name}.jsonl"
    rest = sorted(dir_path.glob(f"{base_name}_*.jsonl"), key=lambda p: p.name)
    combined = (rest + [base]) if base.exists() else rest
    return combined[-max_files:] if max_files > 0 else combined


def run_binance_integrity(root: Path, max_files_per_stream: int) -> tuple[list[str], list[str]]:
    summary, full = [], []
    bn_dir = root / "binance"
    if not bn_dir.exists():
        summary.append("  [Binance] 目录不存在")
        return (summary, summary)

    last_a: dict[str, int] = {}
    gaps_agg: list[tuple[str, int, int, int]] = []
    agg_paths = _binance_files_sorted(bn_dir, "agg_trades", max_files_per_stream)
    for path in agg_paths:
        for rec in _iter_jsonl(path):
            stream = rec.get("stream") or ""
            a = rec.get("a")
            if stream and a is not None:
                a = int(a)
                prev = last_a.get(stream)
                if prev is not None and a > prev + 1:
                    gaps_agg.append((stream, prev + 1, a, a - prev - 1))
                last_a[stream] = a

    last_u: dict[str, int] = {}
    gaps_book: list[tuple[str, int, int, int]] = []
    book_paths = _binance_files_sorted(bn_dir, "book_ticker", max_files_per_stream)
    for path in book_paths:
        for rec in _iter_jsonl(path):
            stream = rec.get("stream") or ""
            u = rec.get("u")
            if stream and u is not None:
                u = int(u)
                prev = last_u.get(stream)
                if prev is not None and u > prev + 1:
                    gaps_book.append((stream, prev + 1, u, u - prev - 1))
                last_u[stream] = u

    full.append("  [Binance] 已检查 agg_trades {} 个文件, book_ticker {} 个文件".format(len(agg_paths), len(book_paths)))
    if gaps_agg:
        full.append("  [Binance agg_trades] 序列 a 空洞: {} 处，完整列表:".format(len(gaps_agg)))
        for t in sorted(gaps_agg, key=lambda x: -x[3]):
            full.append("      {}: 缺失 {} 个 id ({}..{})".format(t[0], t[3], t[1], t[2] - 1))
    else:
        full.append("  [Binance agg_trades] 未发现序列 a 空洞")
    if gaps_book:
        full.append("  [Binance book_ticker] 序列 u 空洞: {} 处，完整列表:".format(len(gaps_book)))
        for t in sorted(gaps_book, key=lambda x: -x[3]):
            full.append("      {}: 缺失 {} 个 id ({}..{})".format(t[0], t[3], t[1], t[2] - 1))
    else:
        full.append("  [Binance book_ticker] 未发现序列 u 空洞")

    summary.append(full[0])
    if gaps_agg:
        summary.append(full[1])
        summary.extend(full[2:2 + CONSOLE_TOP_N])
        if len(gaps_agg) > CONSOLE_TOP_N:
            summary.append("      ... 共 {} 处".format(len(gaps_agg)))
    else:
        summary.append(full[1])
    idx = 2 + len(gaps_agg) if gaps_agg else 2
    if gaps_book:
        summary.append(full[idx])
        summary.extend(full[idx + 1:idx + 1 + CONSOLE_TOP_N])
        if len(gaps_book) > CONSOLE_TOP_N:
            summary.append("      ... 共 {} 处".format(len(gaps_book)))
    else:
        summary.append(full[idx])
    return (summary, full)


def run_chainlink_integrity(root: Path) -> tuple[list[str], list[str]]:
    summary, full = [], []
    path = root / "chainlink" / "updates.jsonl"
    if not path.exists():
        summary.append("  [Chainlink] updates.jsonl 不存在")
        return (summary, summary)
    last_round: dict[str, int] = {}
    gaps: list[tuple[str, int, int, int]] = []
    for rec in _iter_jsonl(path):
        feed = rec.get("feed") or ""
        rid = rec.get("roundId")
        if feed and rid is not None:
            rid = int(rid)
            prev = last_round.get(feed)
            if prev is not None and rid > prev + 1:
                gaps.append((feed, prev + 1, rid, rid - prev - 1))
            last_round[feed] = rid

    if gaps:
        full.append("  [Chainlink] roundId 空洞: {} 处，完整列表:".format(len(gaps)))
        for feed, exp, got, count in sorted(gaps, key=lambda x: -x[3]):
            full.append("      {}: 缺失 {} 个 round ({}..{})".format(feed, count, exp, got - 1))
    else:
        full.append("  [Chainlink] 未发现 roundId 空洞")

    summary = full[:1 + CONSOLE_TOP_N] if gaps else full
    if gaps and len(gaps) > CONSOLE_TOP_N:
        summary.append("      ... 共 {} 处".format(len(gaps)))
    return (summary, full)


def run_heartbeat_integrity(root: Path, heartbeat_max_sec: int) -> tuple[list[str], list[str]]:
    summary, full = [], []
    path = root / "polymarket" / "heartbeat.jsonl"
    if not path.exists():
        summary.append("  [Heartbeat] heartbeat.jsonl 不存在")
        return (summary, summary)
    max_ms = heartbeat_max_sec * 1000
    prev_ts = None
    gaps: list[tuple[int, int, int]] = []
    for rec in _iter_jsonl(path):
        ts = rec.get("local_receipt_ts_ms")
        if ts is not None:
            ts = int(ts)
            if prev_ts is not None and (ts - prev_ts) > max_ms:
                gaps.append((prev_ts, ts, ts - prev_ts))
            prev_ts = ts

    if gaps:
        full.append("  [Heartbeat] 间隔 >{}s: {} 处（可能进程卡住），完整列表:".format(heartbeat_max_sec, len(gaps)))
        for prev_ts, next_ts, gap in sorted(gaps, key=lambda x: -x[2]):
            full.append("      {} -> {}  间隔 {}s".format(prev_ts, next_ts, gap // 1000))
    else:
        full.append("  [Heartbeat] 未发现 >{}s 的间隔".format(heartbeat_max_sec))

    summary = full[:1 + CONSOLE_TOP_N] if gaps else full
    if gaps and len(gaps) > CONSOLE_TOP_N:
        summary.append("      ... 共 {} 处".format(len(gaps)))
    return (summary, full)


def main():
    p = argparse.ArgumentParser(description="多源录制完整性校验，可输出完整分析文件")
    p.add_argument("--data-dir", default="data", help="数据根目录")
    p.add_argument("--output", "-o", default="", help="完整分析报告输出路径（含全部明细）")
    p.add_argument("--lines", type=int, default=20, help="时间对齐时每源读取最后 N 行")
    p.add_argument("--gap-sec", type=int, default=DEFAULT_GAP_SEC, help="Polymarket 时间空洞阈值（秒）")
    p.add_argument("--heartbeat-max-sec", type=int, default=DEFAULT_HEARTBEAT_MAX_SEC, help="Heartbeat 间隔告警阈值（秒）")
    p.add_argument("--no-integrity", action="store_true", help="仅做时间对齐，不做 2-5 完整性检查")
    p.add_argument("--max-polymarket-files", type=int, default=100, help="Polymarket 最多检查的 market 文件数")
    p.add_argument("--max-binance-files", type=int, default=2, help="Binance 每种流最多检查文件数，0=不限制")
    args = p.parse_args()
    root = Path(args.data_dir)

    report_full = []
    report_full.append("多源录制完整性校验报告")
    report_full.append("生成时间: {}".format(datetime.now().isoformat()))
    report_full.append("数据目录: {}".format(root.absolute()))
    report_full.append("")

    # 1
    report_full.append("========== 1. 多源时间对齐 ==========")
    sum1, full1 = run_timestamp_alignment(root, args.lines)
    report_full.extend(full1)
    report_full.append("")
    for line in sum1:
        print(line)

    if args.no_integrity:
        if args.output:
            Path(args.output).write_text("\n".join(report_full), encoding="utf-8")
            print("\n报告已写入: {}".format(args.output))
        return

    # 2
    report_full.append("========== 2. Polymarket 断连与时间空洞 ==========")
    sum2, full2 = run_polymarket_integrity(root, args.gap_sec, args.max_polymarket_files)
    report_full.extend(full2)
    report_full.append("")
    print("\n========== 2. Polymarket 断连与时间空洞 ==========")
    for line in sum2:
        print(line)

    # 3
    report_full.append("========== 3. Binance 序列丢包 ==========")
    sum3, full3 = run_binance_integrity(root, args.max_binance_files)
    report_full.extend(full3)
    report_full.append("")
    print("\n========== 3. Binance 序列丢包 ==========")
    for line in sum3:
        print(line)

    # 4
    report_full.append("========== 4. Chainlink roundId 漏 round ==========")
    sum4, full4 = run_chainlink_integrity(root)
    report_full.extend(full4)
    report_full.append("")
    print("\n========== 4. Chainlink roundId 漏 round ==========")
    for line in sum4:
        print(line)

    # 5
    report_full.append("========== 5. Heartbeat 间隔（进程是否卡住） ==========")
    sum5, full5 = run_heartbeat_integrity(root, args.heartbeat_max_sec)
    report_full.extend(full5)
    print("\n========== 5. Heartbeat 间隔（进程是否卡住） ==========")
    for line in sum5:
        print(line)

    if args.output:
        Path(args.output).write_text("\n".join(report_full), encoding="utf-8")
        print("\n完整分析报告已写入: {}".format(args.output))


if __name__ == "__main__":
    main()
