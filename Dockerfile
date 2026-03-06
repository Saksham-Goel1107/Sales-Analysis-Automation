FROM python:3.11-slim

# Set timezone dynamically if requested, otherwise default to UTC
ENV TZ=UTC

# Update OS and install Cron
RUN apt-get update \
    && apt-get install -y --no-install-recommends cron ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy application dependencies and install
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source repository
COPY . /app/

# Ensure logs directory exists (useful when not mounting from host)
RUN mkdir -p /app/logs

# Normalize cron file line endings (in case host is Windows)
RUN if [ -f /app/crontab ]; then sed -i 's/\r$//' /app/crontab; fi

# Setup Cronjob File into the container's cron.d
# We'll copy the repo `crontab` file into /etc/cron.d and install it via crontab
COPY crontab /etc/cron.d/stock-analyzer-cron

# Give execution rights and apply cron job
RUN chmod 0644 /etc/cron.d/stock-analyzer-cron \
    && crontab /etc/cron.d/stock-analyzer-cron

# Ensure cron log exists so `tail -f` has a file to follow
RUN touch /var/log/cron.log

# Command starts the cron daemon in the background and streams the log
CMD cron && echo "Cron daemon started. Application running in background." && tail -f /var/log/cron.log
