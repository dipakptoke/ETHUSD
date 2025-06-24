FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

HEALTHCHECK --interval=60s --timeout=5s --start-period=30s --retries=3 \
  CMD test -f /app/heartbeat && find /app/heartbeat -mmin -2 | grep -q .

CMD ["python", "live_eth_bot.py"]