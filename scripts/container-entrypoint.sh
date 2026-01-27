#!/bin/sh
set -e
# Entrypoint that runs migrations (if API service) and starts the main process
SERVICE_NAME=${SERVICE_NAME:-container}
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
echo "$(ts) [${SERVICE_NAME}] starting"

# If this is the API container, run migrations first
if [ "$SERVICE_NAME" = "api" ]; then
    echo "$(ts) [${SERVICE_NAME}] running database migrations..."
    python /app/scripts/run_migrations.py || {
        echo "$(ts) [${SERVICE_NAME}] migration failed"
        exit 1
    }
  # Optionally initialize scan configuration for new databases.
  # Controlled by INIT_SCAN_CONFIG (default: true). This is idempotent.
  INIT_SCAN_CONFIG=${INIT_SCAN_CONFIG:-true}
  if [ "${INIT_SCAN_CONFIG}" = "true" ]; then
    echo "$(ts) [${SERVICE_NAME}] initializing scan configuration (if needed)..."
    python /app/initialize_scan_config.py || {
      echo "$(ts) [${SERVICE_NAME}] initialize_scan_config.py failed"
      exit 1
    }
  fi
fi

# Start the main process
"$@" &
CHILD_PID=$!

shutdown() {
  echo "$(ts) [${SERVICE_NAME}] stopping"
  if kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -TERM "$CHILD_PID" 2>/dev/null
    wait "$CHILD_PID"
  fi
}

trap shutdown TERM INT

wait "$CHILD_PID"
EXIT_CODE=$?
echo "$(ts) [${SERVICE_NAME}] exited with ${EXIT_CODE}"
exit ${EXIT_CODE}
