#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_MAINTENANCE_INTERVAL:-3600}"
START_DELAY="${PM_ROBOT_MAINTENANCE_START_DELAY:-300}"
WAL_CHECKPOINT="${PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT:-none}"
STALE_INGEST_RUN_SECONDS="${PM_ROBOT_MAINTENANCE_STALE_INGEST_RUN_SECONDS:-21600}"
FAILED_JOB_COOLDOWN_SECONDS="${PM_ROBOT_MAINTENANCE_FAILED_JOB_COOLDOWN_SECONDS:-21600}"
KEEP_BACKUPS="${PM_ROBOT_MAINTENANCE_KEEP_BACKUPS:-0}"
RUNTIME_HEARTBEAT_DAYS="${PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS:-30}"
PRUNE_ENABLED="${PM_ROBOT_RETENTION_PRUNE_ENABLED:-1}"
PRUNE_BATCHES="${PM_ROBOT_RETENTION_PRUNE_BATCHES:-1}"
PRUNE_LIMIT="${PM_ROBOT_RETENTION_PRUNE_LIMIT:-5}"
PRUNE_KEEP_RECENT_ACTIVITY="${PM_ROBOT_RETENTION_KEEP_RECENT_ACTIVITY:-0}"

runtime_heartbeat() {
  name="$1"
  status="${2:-ok}"
  error="${3:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$name" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

if [ "$START_DELAY" -gt 0 ]; then
  echo "$(date -Iseconds) maintenance loop: initial delay ${START_DELAY}s"
  sleep "$START_DELAY"
fi

while true; do
  echo "$(date -Iseconds) maintenance loop: start"
  maintenance_ok=1
  if ! python -m pm_robot.cli --env /app/.env maintenance \
      --skip-cleanup \
      --reset-stale-jobs \
      --failed-job-cooldown-seconds "$FAILED_JOB_COOLDOWN_SECONDS" \
      --reset-stale-ingest-runs \
      --stale-ingest-run-seconds "$STALE_INGEST_RUN_SECONDS" \
      --runtime-heartbeat-days "$RUNTIME_HEARTBEAT_DAYS" \
      --keep-backups "$KEEP_BACKUPS" \
      --wal-checkpoint "$WAL_CHECKPOINT"; then
    maintenance_ok=0
  fi
  if [ "$maintenance_ok" -eq 1 ] && [ "$PRUNE_ENABLED" = "1" ]; then
    batch=0
    while [ "$batch" -lt "$PRUNE_BATCHES" ]; do
      if ! python -m pm_robot.cli --env /app/.env prune-evidence \
          --execute \
          --limit "$PRUNE_LIMIT" \
          --keep-recent-activity "$PRUNE_KEEP_RECENT_ACTIVITY"; then
        maintenance_ok=0
        break
      fi
      batch=$((batch + 1))
    done
  fi
  if [ "$maintenance_ok" -eq 1 ]; then
    echo "$(date -Iseconds) maintenance loop: ok"
    runtime_heartbeat loop_maintenance ok
  else
    echo "$(date -Iseconds) maintenance loop: failed" >&2
    runtime_heartbeat loop_maintenance failed "maintenance failed"
  fi
  sleep "$INTERVAL"
done
