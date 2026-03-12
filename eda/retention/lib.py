#!/usr/bin/env python3
"""Retention 公共：conditionId 映射、crypto 判断、checkpoint、路径约定。"""

import json
import os
import re
from pathlib import Path
from typing import Iterator

POLY_DATA_DIR = os.environ.get("POLY_DATA_DIR", "/vault/core/data/poly")
CRYPTO_KEYWORDS = ["Bitcoin", "Ethereum", "Solana", "Crypto", "BTC", "ETH", "SOL", "XRP", "Ripple"]


def _iter_jsonl(path: Path) -> Iterator[dict]:
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


def partition_hour_to_start_ms(dt: str, hour: str) -> int | None:
    """dt=YYYY-MM-DD, hour=H -> 该小时起始的毫秒时间戳（UTC）。"""
    try:
        import datetime as _dt
        d = _dt.datetime.strptime(f"{dt} {hour}", "%Y-%m-%d %H")
        return int(d.timestamp() * 1000)
    except Exception:
        return None


def partition_from_path(path: Path) -> tuple[str, str] | None:
    """从路径解析 dt 和 hour，如 dt=2026-02-13, hour=01 -> ('2026-02-13', '01')。"""
    dt, hour = None, None
    for p in path.parts:
        if p.startswith("dt="):
            dt = p[3:]
        if p.startswith("hour="):
            hour = p[5:]
    if dt and hour is not None:
        return (dt, hour)
    return None


def is_crypto_trade(rec: dict) -> bool:
    """根据 title/slug 判断是否为 Crypto 相关（与 select_top2 一致）。"""
    text = " ".join(
        str(x).lower() for x in (rec.get("title") or "", rec.get("slug") or "") if x
    )
    if not text:
        return False
    for kw in CRYPTO_KEYWORDS:
        k = kw.lower()
        if k in ("eth", "sol", "btc", "xrp"):
            if k == "eth" and ("ethereum" in text or re.search(r"\beth\b", text)):
                return True
            if k == "sol" and ("solana" in text or re.search(r"\bsol\b", text)):
                return True
            if k == "btc" and ("bitcoin" in text or re.search(r"\bbtc\b", text)):
                return True
            if k == "xrp" and ("ripple" in text or re.search(r"\bxrp\b", text)):
                return True
        elif k in text:
            return True
    return False


def coin_from_market(entry: dict) -> str:
    """从 title/slug 推断币种，用于 Binance symbol（如 ETH -> ethusdt）。"""
    text = " ".join(
        str(x).lower() for x in (entry.get("title") or "", entry.get("slug") or "") if x
    )
    if not text:
        return ""
    if "ethereum" in text or re.search(r"\beth\b", text):
        return "ETH"
    if "bitcoin" in text or re.search(r"\bbtc\b", text):
        return "BTC"
    if "solana" in text or re.search(r"\bsol\b", text):
        return "SOL"
    if "ripple" in text or re.search(r"\bxrp\b", text):
        return "XRP"
    return ""


def build_condition_id_to_market_id_from_silver(data_dir: Path) -> dict[str, str]:
    """从 lake/silver/polymarket 的 parquet 建立 conditionId -> market_id。"""
    cid_to_mid = {}
    silver_root = data_dir / "lake" / "silver" / "polymarket"
    if not silver_root.exists():
        return cid_to_mid
    market_to_one_file: dict[str, Path] = {}
    for d in silver_root.iterdir():
        if not d.is_dir():
            continue
        for hour_dir in (d.iterdir() if d.name.startswith("dt=") else []):
            if not hour_dir.is_dir() or not hour_dir.name.startswith("hour="):
                continue
            for mid_dir in hour_dir.iterdir():
                if not mid_dir.is_dir() or not mid_dir.name.startswith("market_"):
                    continue
                mid = mid_dir.name.replace("market_", "").strip()
                if not mid.isdigit() or mid in market_to_one_file:
                    continue
                one = next(mid_dir.rglob("*.parquet"), None)
                if one is not None:
                    market_to_one_file[mid] = one
    try:
        import pandas as pd
    except ImportError:
        return cid_to_mid
    for market_id, path in market_to_one_file.items():
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        if "raw_data" not in df.columns or df.empty:
            continue
        row = df.iloc[0]
        raw = row.get("raw_data")
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
        if not isinstance(raw, dict):
            continue
        cid = raw.get("market")
        if cid:
            cid_to_mid[str(cid).strip()] = market_id
    return cid_to_mid


def build_condition_id_to_market_id_from_raw(polymarket_dir: Path) -> dict[str, str]:
    """从 raw polymarket 的 market_*.jsonl 首条含 raw_data.market 建立 conditionId -> market_id。"""
    cid_to_mid = {}
    if not polymarket_dir.exists():
        return cid_to_mid
    for path in sorted(polymarket_dir.rglob("market_*.jsonl")):
        name = path.stem
        parts = name.replace("market_", "").split("_")
        market_id = parts[0] if parts else None
        if not market_id or not market_id.isdigit():
            continue
        for rec in _iter_jsonl(path):
            raw = rec.get("raw_data")
            if not isinstance(raw, dict):
                continue
            cid = raw.get("market")
            if cid:
                cid_to_mid[str(cid).strip()] = market_id
                break
    return cid_to_mid


def get_checkpoint_root(data_dir: Path) -> Path:
    v = os.environ.get("POLY_CHECKPOINT_DIR", "").strip()
    return Path(v).resolve() if v else data_dir / "checkpoint"


def load_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_checkpoint(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def coin_to_binance_symbol(coin: str) -> str:
    return (coin.strip().lower() + "usdt") if coin else ""
