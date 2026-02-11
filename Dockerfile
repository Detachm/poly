FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY discovery.py discovery_loop.py recorder.py binance_recorder.py chainlink_recorder.py tracker_0x8dxd.py ./
# targets.json 由宿主机挂载；TRACKER_USER_ADDRESS 可选
CMD ["python", "-u", "recorder.py"]
