FROM python:3.11-slim
WORKDIR /app
# Install build deps and tzdata so the container can adopt the TZ setting.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
	build-essential \
	libpq-dev \
	gcc \
	tzdata \
 && rm -rf /var/lib/apt/lists/*
ENV DEBIAN_FRONTEND=
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY ./ /app
COPY scripts/container-entrypoint.sh /app/scripts/container-entrypoint.sh
RUN chmod +x /app/scripts/container-entrypoint.sh
ENTRYPOINT ["/app/scripts/container-entrypoint.sh"]
CMD ["python", "scanner/main.py"]
