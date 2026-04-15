# Dockerfile for FEC Monitor
# Supports both web server and job modes

FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8080

# Default: web server. Set MODE=job for the light F3X/IE/PTR ingest job,
# or MODE=sa_sb_worker for the SA/SB worker (which also reads
# SA_SB_WORKER_MODE=small|big to choose the size tier).
CMD ["sh", "-c", "case \"$MODE\" in \
  job) python -m scripts.ingest_job ;; \
  sa_sb_worker) python -m scripts.sa_sb_worker ;; \
  *) gunicorn app.main:app --bind 0.0.0.0:8080 --workers 1 \
     --worker-class uvicorn.workers.UvicornWorker --timeout 300 ;; \
esac"]
