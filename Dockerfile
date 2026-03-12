FROM python:3.11-slim

# Set timezone dynamically if requested, otherwise default to UTC
ENV TZ=UTC

# Update OS and install Cron
RUN apt-get update \
    && apt-get install -y --no-install-recommends cron ca-certificates curl \
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

# Some base Python images provide `python3` but not `python` symlink.
# Ensure `/usr/bin/python` exists so wrapper scripts calling `python` work.
RUN if [ -x "/usr/local/bin/python3" ] && [ ! -e "/usr/bin/python" ]; then \
            ln -sf /usr/local/bin/python3 /usr/bin/python; \
        fi

# Normalize cron file line endings (in case host is Windows)
RUN sed -i 's/\r$//' /app/crontab

# Copy cron job file into /etc/cron.d
COPY crontab /etc/cron.d/stock-analyzer-cron

# Give correct permissions so cron will read the file
RUN chmod 0644 /etc/cron.d/stock-analyzer-cron

# Ensure cron log exists so `tail -f` has a file to follow
RUN touch /var/log/cron.log

# Command starts the cron daemon in the background and streams the log
CMD cron && echo "Cron daemon started. Application running in background." && tail -f /var/log/cron.log
