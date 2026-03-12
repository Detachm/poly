#!/usr/bin/env python3
"""
下载 Binance 历史 aggTrades + klines 数据（用于 XGBoost 训练）。

数据源: https://data.binance.vision
- 月度 aggTrades: data/spot/monthly/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY-MM}.zip
- 月度 5m klines: data/spot/monthly/klines/{SYMBOL}/5m/{SYMBOL}-5m-{YYYY-MM}.zip
- 日级 aggTrades（补最近月）: data/spot/daily/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{YYYY-MM-DD}.zip
- 日级 5m klines（补最近月）: data/spot/daily/klines/{SYMBOL}/5m/{SYMBOL}-5m-{YYYY-MM-DD}.zip

用法:
    python scripts/download_binance_history.py --symbols BTCUSDT --years 3
    python scripts/download_binance_history.py --symbols BTCUSDT ETHUSDT SOLUSDT --years 3 --workers 4
"""

import argparse
import hashlib
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from urllib.request import urlretrieve, urlopen
from urllib.error import HTTPError

BASE_URL_SPOT = "https://data.binance.vision/data/spot"
BASE_URL_FUTURES = "https://data.binance.vision/data/futures/um"
DEFAULT_OUTPUT = "/vault/core/data/poly/historical/binance"


def _months_range(start: date, end: date):
    """生成 [start, end) 之间的所有 (year, month)。"""
    cur = start.replace(day=1)
    while cur < end:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)


def _days_range(start: date, end: date):
    """生成 [start, end) 之间的所有日期。"""
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)


def _download_file(url: str, dest: Path, verify_checksum: bool = True) -> bool:
    """下载单个文件，支持跳过已存在文件和校验。返回 True=成功/已跳过。"""
    if dest.exists() and dest.stat().st_size > 0:
        return True  # 已下载

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    try:
        urlretrieve(url, str(tmp))
    except HTTPError as e:
        if e.code == 404:
            # 文件不存在（未来的日期或停牌的交易对）
            tmp.unlink(missing_ok=True)
            return False
        raise

    # 校验
    if verify_checksum:
        checksum_url = url + ".CHECKSUM"
        try:
            resp = urlopen(checksum_url)
            expected = resp.read().decode().strip().split()[0].lower()
            actual = hashlib.sha256(tmp.read_bytes()).hexdigest().lower()
            if expected != actual:
                print(f"  CHECKSUM 不匹配: {dest.name}")
                tmp.unlink(missing_ok=True)
                return False
        except HTTPError:
            pass  # 没有 CHECKSUM 文件，跳过校验

    tmp.rename(dest)
    return True


def _extract_zip(zip_path: Path, extract_dir: Path) -> Path:
    """解压 zip，返回解压后的文件路径。"""
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        zf.extractall(extract_dir)
    return extract_dir / names[0] if names else None


def build_download_tasks(symbols: list, years: int, output_dir: Path,
                         market: str = "spot") -> list:
    """构建下载任务列表。market: spot / futures。"""
    today = date.today()
    start = date(today.year - years, today.month, 1)

    if today.day <= 5:
        monthly_end = date(today.year, today.month, 1) - timedelta(days=32)
        monthly_end = monthly_end.replace(day=1)
    else:
        monthly_end = date(today.year, today.month, 1)

    base_url = BASE_URL_FUTURES if market == "futures" else BASE_URL_SPOT
    tasks = []

    for symbol in symbols:
        sym = symbol.upper()
        prefix = "futures_" if market == "futures" else ""
        sym_dir = output_dir / sym.lower()

        # 1. 月度 aggTrades
        for y, m in _months_range(start, monthly_end):
            tag = f"{y}-{m:02d}"
            fname = f"{sym}-aggTrades-{tag}.zip"
            url = f"{base_url}/monthly/aggTrades/{sym}/{fname}"
            dest = sym_dir / f"{prefix}monthly_aggTrades" / fname
            extract = sym_dir / f"{prefix}aggTrades"
            tasks.append((url, dest, extract, f"{sym} {prefix}aggTrades {tag}"))

        # 2. 月度 5m klines
        for y, m in _months_range(start, monthly_end):
            tag = f"{y}-{m:02d}"
            fname = f"{sym}-5m-{tag}.zip"
            url = f"{base_url}/monthly/klines/{sym}/5m/{fname}"
            dest = sym_dir / f"{prefix}monthly_klines_5m" / fname
            extract = sym_dir / f"{prefix}klines_5m"
            tasks.append((url, dest, extract, f"{sym} {prefix}klines5m {tag}"))

        # 3. 月度 fundingRate (仅 futures)
        if market == "futures":
            for y, m in _months_range(start, monthly_end):
                tag = f"{y}-{m:02d}"
                fname = f"{sym}-fundingRate-{tag}.zip"
                url = f"{base_url}/monthly/fundingRate/{sym}/{fname}"
                dest = sym_dir / "futures_monthly_fundingRate" / fname
                extract = sym_dir / "futures_fundingRate"
                tasks.append((url, dest, extract, f"{sym} fundingRate {tag}"))

        # 4. 日级 aggTrades 补最近月
        for d in _days_range(monthly_end, today):
            tag = d.isoformat()
            fname = f"{sym}-aggTrades-{tag}.zip"
            url = f"{base_url}/daily/aggTrades/{sym}/{fname}"
            dest = sym_dir / f"{prefix}daily_aggTrades" / fname
            extract = sym_dir / f"{prefix}aggTrades"
            tasks.append((url, dest, extract, f"{sym} {prefix}aggTrades {tag}"))

        # 5. 日级 5m klines 补最近月
        for d in _days_range(monthly_end, today):
            tag = d.isoformat()
            fname = f"{sym}-5m-{tag}.zip"
            url = f"{base_url}/daily/klines/{sym}/5m/{fname}"
            dest = sym_dir / f"{prefix}daily_klines_5m" / fname
            extract = sym_dir / f"{prefix}klines_5m"
            tasks.append((url, dest, extract, f"{sym} {prefix}klines5m {tag}"))

        # 6. 日级 fundingRate 补最近月 (仅 futures)
        if market == "futures":
            for d in _days_range(monthly_end, today):
                tag = d.isoformat()
                fname = f"{sym}-fundingRate-{tag}.zip"
                url = f"{base_url}/daily/fundingRate/{sym}/{fname}"
                dest = sym_dir / "futures_daily_fundingRate" / fname
                extract = sym_dir / "futures_fundingRate"
                tasks.append((url, dest, extract, f"{sym} fundingRate {tag}"))

    return tasks


def run_task(task: tuple) -> tuple:
    """执行单个下载+解压任务。返回 (label, status)。"""
    url, dest_zip, extract_dir, label = task

    # 检查是否已解压（csv 已存在）
    csv_name = dest_zip.stem  # 去掉 .zip
    csv_path = extract_dir / csv_name
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return label, "skip"

    ok = _download_file(url, dest_zip)
    if not ok:
        return label, "404"

    try:
        _extract_zip(dest_zip, extract_dir)
        return label, "ok"
    except Exception as e:
        return label, f"extract_err: {e}"


def main():
    parser = argparse.ArgumentParser(description="下载 Binance 历史 aggTrades + klines")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT"], help="交易对列表")
    parser.add_argument("--years", type=int, default=3, help="下载最近几年数据")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT, help="输出目录")
    parser.add_argument("--workers", type=int, default=4, help="并行下载线程数")
    parser.add_argument("--no-checksum", action="store_true", help="跳过 CHECKSUM 校验")
    parser.add_argument("--agg-only", action="store_true", help="只下载 aggTrades")
    parser.add_argument("--klines-only", action="store_true", help="只下载 klines")
    parser.add_argument("--market", default="spot", choices=["spot", "futures"], help="市场类型")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tasks = build_download_tasks(args.symbols, args.years, output_dir, market=args.market)

    # 按类型过滤
    if args.agg_only:
        tasks = [t for t in tasks if "aggTrades" in t[3]]
    elif args.klines_only:
        tasks = [t for t in tasks if "klines" in t[3]]

    print(f"共 {len(tasks)} 个下载任务，{args.workers} 个线程")
    print(f"输出目录: {output_dir}")
    print()

    ok_count = 0
    skip_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_task, t): t for t in tasks}
        for i, future in enumerate(as_completed(futures), 1):
            label, status = future.result()
            if status == "ok":
                ok_count += 1
                print(f"[{i}/{len(tasks)}] {label} ... ok")
            elif status == "skip":
                skip_count += 1
            elif status == "404":
                fail_count += 1
            else:
                fail_count += 1
                print(f"[{i}/{len(tasks)}] {label} ... {status}")

            if i % 50 == 0:
                print(f"  进度: {i}/{len(tasks)} (下载 {ok_count}, 跳过 {skip_count}, 失败 {fail_count})")

    print(f"\n完成: 下载 {ok_count}, 跳过 {skip_count}, 失败/404 {fail_count}")


if __name__ == "__main__":
    main()
