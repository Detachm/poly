# BTC 5min 方向预测因子测试报告

日期：2026-03-11

## 1. 测试目标

验证常见量价因子对 BTC 5min 涨跌的预测力，确定是否能达到复刻 0x8dxd 策略所需的 63% 准确率。

## 2. 回测系统设计

### 2.1 数据源

| 数据 | 来源 | 字段 | 分辨率 |
|------|------|------|--------|
| book_ticker | Binance WS → silver parquet | bid/ask price + qty (best L1) | tick 级，重采样至 1s |
| agg_trades | Binance WS → silver parquet | price, qty, is_buyer_maker | tick 级，重采样至 1s |

- 标的：BTCUSDT
- 时间范围：2026-02-12 至 2026-03-11（19 天可用）
- 数据路径：`/vault/core/data/poly/lake/silver/binance/`

### 2.2 数据处理流程

```
原始 tick 数据
    │
    ▼
1s 重采样（每秒取最后一条 tick / 聚合成交）
    │
    ├── book_ticker: mid, spread, OBI(bid_qty-ask_qty)/(bid_qty+ask_qty)
    └── agg_trades:  buy_vol, sell_vol, total_vol, vwap, n_trades
    │
    ▼
合并 + 滚动窗口计算因子（10s/30s/60s/300s 多尺度）
    │
    ▼
对齐到 5min 窗口：
    - 特征：窗口起始时刻的因子值（前 5s 平均减噪）
    - 目标：窗口末 mid 相对窗口初 mid 的涨跌 (binary: Up=1, Down=0)
    - 每天 ~288 个窗口，总计 4,796 个样本
```

### 2.3 测试的 5 类因子

#### Factor 1: OBI — Order Book Imbalance（盘口失衡）

```python
OBI = (bid_qty - ask_qty) / (bid_qty + ask_qty)
```

- 逻辑：买单挂单多于卖单 → 买压更大 → 预测涨
- 变体：obi_10s / obi_30s / obi_60s（滚动均值）

#### Factor 2: TFI — Trade Flow Imbalance（成交流失衡）

```python
TFI = (buy_vol - sell_vol) / (buy_vol + sell_vol)
# buy_vol: is_buyer_maker=False 的成交额（买方主动吃单）
# sell_vol: is_buyer_maker=True 的成交额（卖方主动吃单）
```

- 逻辑：主动买入量 > 主动卖出量 → 买方更激进 → 预测涨
- 变体：tfi_10s / tfi_30s / tfi_60s / tfi_300s

#### Factor 3: MOM — Mid Price Momentum（价格动量）

```python
MOM_Ns = (mid_t - mid_{t-N}) / mid_{t-N}
```

- 逻辑：短期价格上涨 → 趋势延续 / 长期上涨 → 均值回归
- 变体：mom_10s / mom_30s / mom_60s / mom_300s

#### Factor 4: VSUR — Volume Surge Ratio（成交量突变）

```python
VSUR = vol_short_window / vol_5min_baseline
```

- 逻辑：放量 → 有方向性事件发生
- 变体：vsur_10s / vsur_30s / vsur_60s

#### Factor 5: SPRD — Spread 变化率

```python
SPRD_ratio = spread_now / spread_5min_ma
SPRD_chg = spread_pct_change(N seconds)
```

- 逻辑：spread 突然放大 → 不确定性增加，可能预示方向性变动
- 变体：sprd_ratio / sprd_chg_10s / sprd_chg_30s

### 2.4 评估方法

**单因子评估：**
- IC (Information Coefficient)：因子值与未来 5min 收益的 Spearman 秩相关
- 方向准确率：因子正/负与实际涨/跌的一致率
- Q5-Q1 Spread：因子五等分后，最高分位与最低分位的平均收益差 (bps)

**组合评估：**
- Logistic Regression，L2 正则化
- 时间序列 5-fold 交叉验证（前 N 期训练，下一期测试，无未来信息泄露）
- 特征标准化（训练集统计量，独立应用到测试集）

## 3. 测试结果

### 3.1 数据概况

- 总样本：4,796 个 5min 窗口
- Up/Down 分布：Up 2,468 (51.5%) / Down 2,328 (48.5%)
- 平均 5min 收益：+0.14 bps（微弱正偏）

### 3.2 单因子预测力排名

| 排名 | 因子 | IC | |IC| | 准确率 | 方向 | Q5-Q1 (bps) |
|------|------|-----|------|--------|------|------------|
| 1 | tfi_300s | -0.057 | 0.057 | 52.4% | 反向 | -1.97 |
| 2 | mom_10s | 0.053 | 0.053 | 52.2% | 正向 | 2.49 |
| 3 | mom_300s | -0.049 | 0.049 | 51.7% | 反向 | -1.51 |
| 4 | tfi_10s | 0.046 | 0.046 | 51.0% | 正向 | 1.74 |
| 5 | obi_10s | 0.036 | 0.036 | 51.4% | 正向 | 1.48 |
| 6 | tfi_60s | -0.027 | 0.027 | 52.2% | 反向 | -0.74 |
| 7 | tfi_30s | -0.017 | 0.017 | 50.5% | 反向 | -0.62 |
| 8 | mom_60s | -0.015 | 0.015 | 50.3% | 反向 | 0.18 |
| 9-17 | 其余 | <0.01 | <0.01 | ~50% | - | ~0 |

### 3.3 因子组合预测力

| 组合 | 因子数 | CV 准确率 (均值±标准差) | 最低 fold | 最高 fold |
|------|--------|----------------------|----------|----------|
| **TFI 全家桶** | 4 | **53.26% ± 0.88** | 52.41% | 54.47% |
| **全部因子** | 17 | **52.55% ± 0.60** | 51.45% | 53.10% |
| MOM | 4 | 52.16% ± 1.51 | 50.06% | 54.10% |
| OBI | 3 | 51.61% ± 0.90 | 50.69% | 53.07% |
| VSUR | 3 | 50.85% ± 0.75 | 49.72% | 51.93% |
| SPRD | 3 | 50.75% ± 1.43 | 48.99% | 52.51% |

## 4. 关键发现

### 4.1 微观结构特征确认

- **短期趋势延续**：10s 动量和 10s 成交流方向预测继续涨/跌（IC 正）
- **长期均值回归**：5min 动量和 5min 成交流方向预测**反转**（IC 负）
- 转折点在 30-60s 附近，短于此趋势延续，长于此均值回归

### 4.2 因子有效性层级

```
成交流 (TFI) > 价格动量 (MOM) > 盘口失衡 (OBI) >> 成交量 (VSUR) ≈ Spread
```

- TFI 是最有信息量的因子类别，agg_trades 中的主动买卖方向信息 > book_ticker 中的挂单量信息
- Volume surge 和 spread 变化几乎无独立预测力

### 4.3 与 63% 目标的差距

- 最优组合准确率：**53.3%**
- 目标准确率：**63%**
- 差距：**~10 个百分点**

这些因子即使全部组合也远不够。纯 Binance 量价数据无法解释 0x8dxd 的方向预判能力。

## 5. 可能的方向

0x8dxd 的 edge 来源猜测（按可能性排序）：

1. **Polymarket 自身盘口信息** — 他可能在看 Polymarket 的 orderbook 深度/flow，这些我们目前未采集
2. **更高频/非线性特征** — tick 级 orderbook 事件序列、LOB 深度变化等，可能用神经网络捕捉非线性模式
3. **多源信号融合** — Binance + Chainlink + Polymarket + 链上数据联合建模
4. **外部信息源** — 新闻/社交媒体情绪实时分析

## 6. 文件索引

| 文件 | 说明 |
|------|------|
| `factor_test_report.md` | 本报告 |
| `factor_samples.csv` | 4,796 个窗口的全部因子值和目标 |
| `factor_ic_ranking.csv` | 单因子 IC 排名 |
| `factor_combination_accuracy.csv` | 因子组合 CV 准确率 |
| `../scripts/factor_test.py` | 完整代码 |
