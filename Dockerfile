FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ingest/ ./ingest/
# targets.json 由宿主机挂载；TRACKER_USER_ADDRESS 可选
CMD ["python", "-u", "ingest/recorder.py"]
