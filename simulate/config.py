from __future__ import annotations

import dataclasses
import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    # 资金与仓位
    initial_capital: float = 10_000.0
    position_pct: float = 0.02          # 目标本笔 USDC 和 NAV 各取 2%，取小值

    # 兜底轮询（链上实时触发后，全量轮询仅作保底）
    poll_interval_sec: float = 60.0     # 全量轮询间隔（秒）
    settlement_every_n: int = 10        # 每 N 轮检查一次市场结算

    # 链上实时
    max_trade_age_sec: int = 120        # 超过此时间的信号视为旧单直接跳过
    chain_retry_window_sec: int = 30    # 链上触发后 Data API 未索引时的重试窗口（秒）
    chain_retry_interval_sec: float = 2.0  # 重试间隔（秒）

    # 路径
    data_dir: str = "data/simulate"
    targets_file: str = "data/simulate/copy_targets.json"
    portfolio_file: str = "data/simulate/portfolio.json"
    conflicts_log: str = "data/simulate/conflicts_log.jsonl"

    # 代理
    proxy: str | None = None

    def __post_init__(self):
        if self.proxy is None:
            self.proxy = (
                os.environ.get("HTTPS_PROXY")
                or os.environ.get("HTTP_PROXY")
                or None
            )
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_file(cls, path: str) -> Config:
        with open(path) as f:
            d = json.load(f)
        valid_keys = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in valid_keys})

    def save(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(dataclasses.asdict(self), f, indent=2, ensure_ascii=False)
