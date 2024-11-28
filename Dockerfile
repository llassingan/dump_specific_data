FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backup scripts
COPY backup_database.py /app/backup_database.py
COPY config /config

WORKDIR /app

ENTRYPOINT ["python", "backup_database.py"]