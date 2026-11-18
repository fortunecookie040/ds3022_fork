# Wikipedia recent-changes producer image
# Build from the REPO ROOT (requirements.txt lives there):
#   docker build -f 06_kafka/wikipedia/Dockerfile -t wikipedia-producer .
FROM python:3.13-slim

# Unbuffered output so print() shows up immediately in `docker logs`; no .pyc files
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

# Install dependencies first so this layer is cached when only the script changes
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY 06_kafka/wikipedia/wikipedia-producer.py .

# Default to the brokers' internal addresses on the compose network (stream_network).
# Override with `-e KAFKA_BROKER=...` at runtime if needed.
ENV KAFKA_BROKER=redpanda-0:9092,redpanda-1:9092,redpanda-2:9092

# Run as a non-root user
RUN useradd --create-home appuser
USER appuser

ENTRYPOINT ["python", "wikipedia-producer.py"]
