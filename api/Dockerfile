FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	libpq-dev \
	gcc \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY ./ /app
COPY scripts/container-entrypoint.sh /app/scripts/container-entrypoint.sh
RUN chmod +x /app/scripts/container-entrypoint.sh
ENTRYPOINT ["/app/scripts/container-entrypoint.sh"]
CMD ["gunicorn", "api.app:app", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "--log-level", "info"]
