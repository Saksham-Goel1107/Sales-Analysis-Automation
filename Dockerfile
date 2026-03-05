FROM python:3.12-slim

# 1. System dependencies & Supercronic (lightweight cron)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSLo /usr/local/bin/supercronic "https://github.com/aptible/supercronic/releases/download/v0.2.29/supercronic-linux-amd64" \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get purge -y curl && rm -rf /var/lib/apt/lists/* \
    && useradd -m appuser

WORKDIR /app

# 2. Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Source code & Environment setup
COPY analyze.py crontab ./
RUN mkdir logs /secrets && chown -R appuser:appuser /app /secrets

USER appuser

# Healthcheck: Verify supercronic is running
HEALTHCHECK --interval=1m --timeout=10s --start-period=30s --retries=3 \
    CMD ps -ef | grep [s]upercronic || exit 1

CMD ["supercronic", "crontab"]
