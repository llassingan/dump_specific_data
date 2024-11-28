FROM python:3.9-slim

# Install system dependencies with retry logic and alternative mirrors
RUN apt-get update --allow-releaseinfo-change && \
    for i in $(seq 1 3); do \
    apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/* && \
    break || \
    if [ $i -lt 3 ]; then sleep 5; fi; \
    done

# Rest of your Dockerfile remains the same
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /app/main.py
COPY config /config

WORKDIR /app

ENTRYPOINT ["python", "main.py"]
