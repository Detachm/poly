#!/bin/bash
# 一键跑通流程：创建目录 → 可选 discovery 一次 → 后台启动各录制器（或提示用 supervisord）
# 用法: ./run_all.sh [--skip-discovery]

set -e
cd "$(dirname "$0")"
mkdir -p data/polymarket data/binance data/chainlink data/tracker_0x8dxd logs

echo "========== 0. 依赖 =========="
pip install -q -r requirements.txt 2>/dev/null || true

echo "========== 1. Discovery（合并新 Crypto 市场到 targets.json）=========="
if [[ "$1" != "--skip-discovery" ]]; then
  echo "运行 discovery 一次（需可访问 Gamma API，国内请设 HTTP_PROXY/HTTPS_PROXY）..."
  if python discovery_loop.py --once 2>&1; then
    echo "Discovery 完成"
  else
    echo "Discovery 失败或超时，将使用现有 targets.json（若存在）"
  fi
else
  echo "跳过 discovery（--skip-discovery）"
fi

if [[ ! -f targets.json ]] || [[ $(python -c "import json; print(len(json.load(open('targets.json'))))" 2>/dev/null) -eq 0 ]]; then
  echo "警告: targets.json 不存在或为空，Polymarket 录制将无市场可录。请先运行: python discovery.py"
fi

echo ""
echo "========== 2. 启动多源录制（后台）=========="
echo "Polymarket (recorder) ..."
python -u recorder.py --targets targets.json --output-dir data/polymarket &
PID_PM=$!
echo "Binance ..."
python -u binance_recorder.py --output-dir data/binance &
PID_BN=$!
echo "Chainlink ..."
python -u chainlink_recorder.py --output-dir data/chainlink &
PID_CL=$!
echo "0x8dxd Tracker ..."
python -u tracker_0x8dxd.py --output-dir data/tracker_0x8dxd &
PID_TR=$!
echo "Discovery 循环（每 10 分钟）..."
python -u discovery_loop.py --targets targets.json --interval 600 &
PID_DL=$!

echo ""
echo "已启动: Polymarket=$PID_PM Binance=$PID_BN Chainlink=$PID_CL Tracker=$PID_TR DiscoveryLoop=$PID_DL"
echo "数据目录: data/{polymarket,binance,chainlink,tracker_0x8dxd}"
echo "停止: kill $PID_PM $PID_BN $PID_CL $PID_TR $PID_DL"
echo "或使用 supervisord: mkdir -p logs && supervisord -c supervisord.conf"
