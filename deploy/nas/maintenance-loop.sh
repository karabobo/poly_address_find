#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_MAINTENANCE_INTERVAL:-3600}"
WAL_CHECKPOINT="${PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT:-none}"
STALE_INGEST_RUN_SECONDS="${PM_ROBOT_MAINTENANCE_STALE_INGEST_RUN_SECONDS:-21600}"

runtime_heartbeat() {
  name="$1"
  status="${2:-ok}"
  error="${3:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$name" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

while true; do
  echo "$(date -Iseconds) maintenance loop: start"
  if python -m pm_robot.cli --env /app/.env maintenance \
      --skip-cleanup \
      --reset-stale-jobs \
      --reset-stale-ingest-runs \
      --stale-ingest-run-seconds "$STALE_INGEST_RUN_SECONDS" \
      --wal-checkpoint "$WAL_CHECKPOINT"; then
    echo "$(date -Iseconds) maintenance loop: ok"
    runtime_heartbeat loop_maintenance ok
  else
    echo "$(date -Iseconds) maintenance loop: failed" >&2
    runtime_heartbeat loop_maintenance failed "maintenance failed"
  fi
  sleep "$INTERVAL"
done
