#!/usr/bin/env python3
"""
根据 audit_result.json 执行保留策略：删除非保留市场/时间的数据，重写 0x8dxd raw。
支持 --dry-run 仅打印将要删除的路径；--phase polymarket|tracker|binance|chainlink|all
--silver-only：只删 silver 层，不删 raw/bronze、不改 checkpoint（原数据保留，下游 ETL 只读 silver 故等效保留集）；tracker 在 silver-only 下跳过。
从项目根运行: python eda/retention/apply_retention.py --audit audit_result.json [--phase all] [--dry-run] [--silver-only]
"""

import json
import shutil
import sys
from pathlib import Path

_DIR = Path(__file__).resolve().parent
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))
import lib


def _load_audit(audit_path: Path) -> dict:
    with open(audit_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _checkpoint_path(data_dir: Path, name: str) -> Path:
    return lib.get_checkpoint_root(data_dir) / name


# ---------- Phase: Polymarket ----------
def _list_polymarket_raw_files(data_dir: Path) -> tuple[list[Path], Path]:
    """返回 (文件列表, raw_root)。raw_root 为 raw/polymarket 或 legacy polymarket。"""
    raw_root = data_dir / "raw" / "polymarket"
    if not raw_root.exists():
        raw_root = data_dir / "polymarket"
    if not raw_root.exists():
        return [], raw_root
    return sorted(raw_root.rglob("market_*.jsonl")), raw_root


def _market_id_from_polymarket_path(path: Path, raw_root: Path) -> str | None:
    """从路径得到 market_id。如 dt=.../hour=.../market_123.jsonl 或 market_123.jsonl -> 123。"""
    # 用 stem 避免 .jsonl 被当成 _ 后一段
    stem = path.stem
    if stem.startswith("market_"):
        mid = stem.replace("market_", "").split("_")[0]
        return mid if mid.isdigit() else None
    for part in path.parts:
        if part.startswith("market_"):
            mid = Path(part).stem.replace("market_", "").split("_")[0]
            if mid.isdigit():
                return mid
    return None


def phase_polymarket(data_dir: Path, kept_market_ids: set[str], dry_run: bool, silver_only: bool = False) -> None:
    pm_files, raw_root = _list_polymarket_raw_files(data_dir)
    bronze_root = data_dir / "lake" / "bronze" / "polymarket"
    silver_root = data_dir / "lake" / "silver" / "polymarket"
    compact_path = _checkpoint_path(data_dir, "compact_polymarket.json")
    silver_ckpt_path = _checkpoint_path(data_dir, "silver_polymarket.json")

    to_remove_raw: list[Path] = []
    for path in pm_files:
        mid = _market_id_from_polymarket_path(path, raw_root)
        if mid and mid not in kept_market_ids:
            to_remove_raw.append(path)

    # Bronze/Silver: 按目录删 market_XXX
    to_remove_bronze: list[Path] = []
    to_remove_silver: list[Path] = []
    if bronze_root.exists():
        for d in bronze_root.rglob("market_*"):
            if d.is_dir():
                mid = d.name.replace("market_", "").strip()
                if mid.isdigit() and mid not in kept_market_ids:
                    to_remove_bronze.append(d)
    if silver_root.exists():
        for d in silver_root.rglob("market_*"):
            if d.is_dir():
                mid = d.name.replace("market_", "").strip()
                if mid.isdigit() and mid not in kept_market_ids:
                    to_remove_silver.append(d)

    if dry_run:
        if silver_only:
            for p in to_remove_silver:
                print(f"[dry-run] 将删除 silver（仅）: {p}")
            print(f"[dry-run] 不删 raw/bronze，不改 checkpoint")
        else:
            for p in to_remove_raw:
                print(f"[dry-run] 将删除 raw: {p}")
            for p in to_remove_bronze:
                print(f"[dry-run] 将删除 bronze: {p}")
            for p in to_remove_silver:
                print(f"[dry-run] 将删除 silver: {p}")
            print(f"[dry-run] 将从 compact_polymarket.json 移除 {len(to_remove_raw)} 个 key")
            print(f"[dry-run] 将从 silver_polymarket.json 移除对应 bronze 路径")
        return

    if silver_only:
        for p in to_remove_silver:
            if p.exists():
                shutil.rmtree(p)
                print(f"已删除 silver: {p}")
        return

    for p in to_remove_raw:
        p.unlink()
        print(f"已删除 raw: {p}")
    for p in to_remove_bronze:
        shutil.rmtree(p)
        print(f"已删除 bronze: {p}")
    for p in to_remove_silver:
        shutil.rmtree(p)
        print(f"已删除 silver: {p}")

    # Checkpoint: 移除已删 raw 的 key（rel 相对于 raw_root）
    compact = lib.load_checkpoint(compact_path)
    keys_to_drop = []
    for path in to_remove_raw:
        try:
            rel = path.relative_to(raw_root)
            keys_to_drop.append(str(rel))
        except ValueError:
            pass
    for k in keys_to_drop:
        compact.pop(k, None)
    if keys_to_drop:
        lib.save_checkpoint(compact_path, compact)
        print(f"已从 compact_polymarket.json 移除 {len(keys_to_drop)} 个 key")

    # Silver checkpoint: 移除已删 bronze 路径的 key（key 为 bronze parquet 相对路径，含 market_XXX）
    silver_ckpt = lib.load_checkpoint(silver_ckpt_path)
    to_drop_silver = []
    for d in to_remove_bronze:
        mid = d.name.replace("market_", "").strip()
        for key in silver_ckpt:
            if f"market_{mid}/" in key or key.startswith(f"market_{mid}"):
                to_drop_silver.append(key)
    for k in to_drop_silver:
        silver_ckpt.pop(k, None)
    if to_drop_silver:
        lib.save_checkpoint(silver_ckpt_path, silver_ckpt)
        print(f"已从 silver_polymarket.json 移除 {len(to_drop_silver)} 个 key")


# ---------- Phase: Tracker 0x8dxd ----------
def phase_tracker(data_dir: Path, kept_condition_ids: set[str], dry_run: bool, silver_only: bool = False) -> None:
    if silver_only:
        if dry_run:
            print("[dry-run] --silver-only 下跳过 tracker（tracker 无按市场 silver，不单独删）")
        return
    raw_root = data_dir / "raw" / "tracker_0x8dxd"
    bronze_root = data_dir / "lake" / "bronze" / "tracker_0x8dxd"
    silver_root = data_dir / "lake" / "silver" / "tracker_0x8dxd"
    compact_path = _checkpoint_path(data_dir, "compact_tracker_0x8dxd.json")
    silver_ckpt_path = _checkpoint_path(data_dir, "silver_tracker_0x8dxd.json")

    if dry_run:
        print("[dry-run] 将重写 raw/tracker_0x8dxd 下各 trades.jsonl，仅保留 kept_condition_ids 行")
        print("[dry-run] 将清空 bronze/silver tracker_0x8dxd 并清空相关 checkpoint，随后需手动重跑 compact + silver")
        return

    # 重写每个 trades.jsonl：只保留 conditionId in kept_condition_ids
    rewritten = 0
    for path in sorted(raw_root.rglob("trades.jsonl")):
        rows = []
        for rec in lib._iter_jsonl(path):
            cid = rec.get("conditionId") or rec.get("condition_id")
            if cid and str(cid).strip() in kept_condition_ids:
                rows.append(rec)
        if len(rows) < 100000:  # 小文件直接重写
            with open(path, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            rewritten += 1
        else:
            tmp = path.with_suffix(".jsonl.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
            tmp.replace(path)
            rewritten += 1
    print(f"已重写 {rewritten} 个 trades.jsonl")

    # 删除 bronze/silver tracker_0x8dxd 全部
    if bronze_root.exists():
        shutil.rmtree(bronze_root)
        bronze_root.mkdir(parents=True, exist_ok=True)
        print("已清空 lake/bronze/tracker_0x8dxd")
    if silver_root.exists():
        shutil.rmtree(silver_root)
        silver_root.mkdir(parents=True, exist_ok=True)
        print("已清空 lake/silver/tracker_0x8dxd")

    # 清空 compact 与 silver checkpoint（让下次全量重读）
    lib.save_checkpoint(compact_path, {})
    lib.save_checkpoint(silver_ckpt_path, {})
    print("已清空 compact_tracker_0x8dxd.json 与 silver_tracker_0x8dxd.json，请重跑: compact tracker_0x8dxd + build_silver tracker_0x8dxd")


# ---------- Phase: Binance & Chainlink ----------
def _list_partition_dirs(root: Path) -> list[tuple[str, str]]:
    """返回 (dt, hour) 列表。"""
    out = set()
    if not root.exists():
        return []
    for dt_dir in root.iterdir():
        if not dt_dir.is_dir() or not dt_dir.name.startswith("dt="):
            continue
        dt = dt_dir.name[3:]
        for hour_dir in dt_dir.iterdir():
            if not hour_dir.is_dir() or not hour_dir.name.startswith("hour="):
                continue
            hour = hour_dir.name[5:]
            out.add((dt, hour))
    return sorted(out)


def phase_binance(data_dir: Path, keep_hours: set[tuple[str, str]], dry_run: bool, silver_only: bool = False) -> None:
    raw_root = data_dir / "raw" / "binance"
    bronze_root = data_dir / "lake" / "bronze" / "binance"
    silver_root = data_dir / "lake" / "silver" / "binance"
    keep_set = set(keep_hours)
    to_del_raw = [raw_root / f"dt={dt}" / f"hour={hour}" for (dt, hour) in _list_partition_dirs(raw_root) if (dt, hour) not in keep_set]
    to_del_bronze = [bronze_root / f"dt={dt}" / f"hour={hour}" for (dt, hour) in _list_partition_dirs(bronze_root) if (dt, hour) not in keep_set]
    to_del_silver = [silver_root / f"dt={dt}" / f"hour={hour}" for (dt, hour) in _list_partition_dirs(raw_root) if (dt, hour) not in keep_set]

    if dry_run:
        if silver_only:
            for p in to_del_silver:
                print(f"[dry-run] 将删除 silver（仅）: {p}")
            print(f"[dry-run] 不删 raw/bronze，不改 checkpoint")
        else:
            for p in to_del_raw:
                print(f"[dry-run] 将删除 raw: {p}")
            for p in to_del_bronze:
                print(f"[dry-run] 将删除 bronze: {p}")
            print(f"[dry-run] 将更新 compact_binance.json / silver_binance.json 移除对应 key")
        return

    if silver_only:
        for p in to_del_silver:
            if p.exists():
                shutil.rmtree(p)
                print(f"已删除 silver: {p}")
        return

    deleted_partitions = [(dt, hour) for (dt, hour) in _list_partition_dirs(raw_root) if (dt, hour) not in keep_set]
    for p in to_del_raw:
        if p.exists():
            shutil.rmtree(p)
            print(f"已删除 raw: {p}")
    for p in to_del_bronze:
        if p.exists():
            shutil.rmtree(p)
            print(f"已删除 bronze: {p}")

    compact_path = _checkpoint_path(data_dir, "compact_binance.json")
    silver_ckpt_path = _checkpoint_path(data_dir, "silver_binance.json")
    compact = lib.load_checkpoint(compact_path)
    silver_ckpt = lib.load_checkpoint(silver_ckpt_path)
    for (dt, hour) in deleted_partitions:
        prefix = f"dt={dt}/hour={hour}/"
        for k in list(compact.keys()):
            if k.startswith(prefix):
                compact.pop(k, None)
        for k in list(silver_ckpt.keys()):
            if prefix in k or k.startswith(prefix):
                silver_ckpt.pop(k, None)
    lib.save_checkpoint(compact_path, compact)
    lib.save_checkpoint(silver_ckpt_path, silver_ckpt)
    print("已更新 binance checkpoint")


def phase_chainlink(data_dir: Path, keep_hours: set[tuple[str, str]], dry_run: bool, silver_only: bool = False) -> None:
    raw_root = data_dir / "raw" / "chainlink"
    bronze_root = data_dir / "lake" / "bronze" / "chainlink"
    silver_root = data_dir / "lake" / "silver" / "chainlink"
    keep_set = set(keep_hours)
    to_del_raw = [raw_root / f"dt={dt}" / f"hour={hour}" for (dt, hour) in _list_partition_dirs(raw_root) if (dt, hour) not in keep_set]
    to_del_bronze = [bronze_root / f"dt={dt}" / f"hour={hour}" for (dt, hour) in _list_partition_dirs(bronze_root) if (dt, hour) not in keep_set]
    to_del_silver = [silver_root / f"dt={dt}" / f"hour={hour}" for (dt, hour) in _list_partition_dirs(raw_root) if (dt, hour) not in keep_set]

    if dry_run:
        if silver_only:
            for p in to_del_silver:
                print(f"[dry-run] 将删除 silver（仅）: {p}")
            print(f"[dry-run] 不删 raw/bronze，不改 checkpoint")
        else:
            for p in to_del_raw:
                print(f"[dry-run] 将删除 raw: {p}")
            for p in to_del_bronze:
                print(f"[dry-run] 将删除 bronze: {p}")
            print(f"[dry-run] 将更新 compact_chainlink.json / silver_chainlink.json")
        return

    if silver_only:
        for p in to_del_silver:
            if p.exists():
                shutil.rmtree(p)
                print(f"已删除 silver: {p}")
        return

    deleted_partitions = [(dt, hour) for (dt, hour) in _list_partition_dirs(raw_root) if (dt, hour) not in keep_set]
    for p in to_del_raw:
        if p.exists():
            shutil.rmtree(p)
            print(f"已删除 raw: {p}")
    for p in to_del_bronze:
        if p.exists():
            shutil.rmtree(p)
            print(f"已删除 bronze: {p}")

    compact_path = _checkpoint_path(data_dir, "compact_chainlink.json")
    silver_ckpt_path = _checkpoint_path(data_dir, "silver_chainlink.json")
    compact = lib.load_checkpoint(compact_path)
    silver_ckpt = lib.load_checkpoint(silver_ckpt_path)
    for (dt, hour) in deleted_partitions:
        prefix = f"dt={dt}/hour={hour}/"
        for k in list(compact.keys()):
            if k.startswith(prefix):
                compact.pop(k, None)
        for k in list(silver_ckpt.keys()):
            if prefix in k or k.startswith(prefix):
                silver_ckpt.pop(k, None)
    lib.save_checkpoint(compact_path, compact)
    lib.save_checkpoint(silver_ckpt_path, silver_ckpt)
    print("已更新 chainlink checkpoint")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="按 audit 结果执行数据保留（删除/重写）")
    parser.add_argument("--audit", "-a", default="", help="audit_result.json 路径，默认 eda/retention/audit_result.json")
    parser.add_argument("--data-dir", default="", help="数据根目录，默认从 audit 中读")
    parser.add_argument("--phase", choices=["polymarket", "tracker", "binance", "chainlink", "all"], default="all")
    parser.add_argument("--dry-run", action="store_true", help="仅打印将要执行的操作")
    parser.add_argument("--silver-only", action="store_true", help="只删 silver 层，不删 raw/bronze、不改 checkpoint；tracker 阶段跳过")
    args = parser.parse_args()

    audit_path = Path(args.audit) if args.audit else _DIR / "audit_result.json"
    if not audit_path.exists():
        print(f"审计结果不存在: {audit_path}，请先运行 audit.py", file=sys.stderr)
        return 1
    audit = _load_audit(audit_path)
    data_dir = Path(args.data_dir or audit.get("data_dir", lib.POLY_DATA_DIR))
    if not data_dir.exists():
        print(f"数据目录不存在: {data_dir}", file=sys.stderr)
        return 1

    kept_market_ids = set(audit.get("kept_market_ids", []))
    kept_condition_ids = set(audit.get("kept_condition_ids", []))
    keep_hours = set(tuple(x) for x in audit.get("keep_hours_binance_chainlink", []))

    if args.dry_run:
        print("--- dry-run 模式，不执行实际删除 ---")

    silver_only = args.silver_only
    phase = args.phase
    if phase in ("polymarket", "all"):
        phase_polymarket(data_dir, kept_market_ids, args.dry_run, silver_only)
    if phase in ("tracker", "all"):
        phase_tracker(data_dir, kept_condition_ids, args.dry_run, silver_only)
    if phase in ("binance", "all"):
        phase_binance(data_dir, keep_hours, args.dry_run, silver_only)
    if phase in ("chainlink", "all"):
        phase_chainlink(data_dir, keep_hours, args.dry_run, silver_only)

    if args.dry_run:
        print("--- dry-run 结束 ---")
    return 0


if __name__ == "__main__":
    sys.exit(main())
