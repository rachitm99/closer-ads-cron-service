FROM python:3.11-slim

WORKDIR /usr/src/app

# Install system dependencies for Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxcb1 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libexpat1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

COPY . .

ENV PORT=8080

# Gunicorn tuning (overridable at container runtime)
ENV GUNICORN_WORKERS=2
ENV GUNICORN_THREADS=8
ENV GUNICORN_TIMEOUT=300
ENV GUNICORN_WORKER_CLASS=gthread

EXPOSE 8080

# Start Gunicorn using env-configurable args (threads recommended because the app performs blocking I/O)
CMD ["sh", "-c", "exec gunicorn -b 0.0.0.0:${PORT} \
  --workers ${GUNICORN_WORKERS} \
  --threads ${GUNICORN_THREADS} \
  --timeout ${GUNICORN_TIMEOUT} \
  --worker-class ${GUNICORN_WORKER_CLASS} \
  --log-level info app:app"]

# Container healthcheck (uses unauthenticated /_healthz endpoint added to app)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -O /dev/null http://localhost:${PORT}/_healthz || exit 1
