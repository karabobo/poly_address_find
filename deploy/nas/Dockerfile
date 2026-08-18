FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY pyproject.toml /app/

RUN apt-get update && \
    apt-get install --no-install-recommends -y openssh-client && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir "duckdb>=1.5,<2"

COPY src /app/src
COPY config /app/config
COPY deploy /app/deploy
COPY scripts /app/scripts

EXPOSE 8787

CMD ["python", "-m", "pm_robot.cli", "--env", "/app/.env", "web", "--host", "0.0.0.0", "--port", "8787"]
