FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    PORT=8080

WORKDIR /srv

COPY app ./app
COPY tests ./tests
COPY scripts ./scripts
COPY docker/healthcheck.py /usr/local/bin/healthcheck.py

RUN mkdir -p /data && chmod +x /usr/local/bin/healthcheck.py

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 \
    CMD ["python3", "/usr/local/bin/healthcheck.py"]

CMD ["python3", "app/server.py"]
