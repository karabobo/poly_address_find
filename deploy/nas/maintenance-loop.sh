#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_MAINTENANCE_INTERVAL:-900}"
START_DELAY="${PM_ROBOT_MAINTENANCE_START_DELAY:-300}"
WAL_CHECKPOINT="${PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT:-passive}"
REPORT_PATH="${PM_ROBOT_MAINTENANCE_REPORT_PATH:-/app/reports/maintenance_status.json}"
STALE_INGEST_RUN_SECONDS="${PM_ROBOT_MAINTENANCE_STALE_INGEST_RUN_SECONDS:-21600}"
FAILED_JOB_COOLDOWN_SECONDS="${PM_ROBOT_MAINTENANCE_FAILED_JOB_COOLDOWN_SECONDS:-21600}"
KEEP_BACKUPS="${PM_ROBOT_MAINTENANCE_KEEP_BACKUPS:-0}"
RUNTIME_HEARTBEAT_DAYS="${PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS:-30}"
CLEANUP_BATCH_LIMIT="${PM_ROBOT_MAINTENANCE_CLEANUP_BATCH_LIMIT:-500}"
PRUNE_ENABLED="${PM_ROBOT_RETENTION_PRUNE_ENABLED:-1}"
PRUNE_BATCHES="${PM_ROBOT_RETENTION_PRUNE_BATCHES:-2}"
PRUNE_LIMIT="${PM_ROBOT_RETENTION_PRUNE_LIMIT:-20}"
PRUNE_MAX_ACTIVITY_ROWS="${PM_ROBOT_RETENTION_PRUNE_MAX_ACTIVITY_ROWS:-5000}"
PRUNE_BATCH_DELAY="${PM_ROBOT_RETENTION_PRUNE_BATCH_DELAY:-10}"
PRUNE_KEEP_RECENT_ACTIVITY="${PM_ROBOT_RETENTION_KEEP_RECENT_ACTIVITY:-0}"
PRUNE_ARCHIVE_ENABLED="${PM_ROBOT_RETENTION_ARCHIVE_ENABLED:-0}"
PRUNE_REPORT_PATH="${PM_ROBOT_RETENTION_PRUNE_REPORT_PATH:-/app/reports/retention_prune_status.json}"

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
  REPORT_TMP="${REPORT_PATH}.tmp.$$"
  report_dir_ready=1
  if ! mkdir -p "$(dirname "$REPORT_PATH")"; then
    echo "$(date -Iseconds) maintenance loop: report directory unavailable" >&2
    maintenance_ok=0
    report_dir_ready=0
  fi
  if [ "$report_dir_ready" -eq 1 ] && ! python -m pm_robot.cli --env /app/.env maintenance \
      --cleanup-batch-limit "$CLEANUP_BATCH_LIMIT" \
      --reset-stale-jobs \
      --failed-job-cooldown-seconds "$FAILED_JOB_COOLDOWN_SECONDS" \
      --reset-stale-ingest-runs \
      --stale-ingest-run-seconds "$STALE_INGEST_RUN_SECONDS" \
      --runtime-heartbeat-days "$RUNTIME_HEARTBEAT_DAYS" \
      --keep-backups "$KEEP_BACKUPS" \
      --wal-checkpoint "$WAL_CHECKPOINT" >"$REPORT_TMP"; then
    maintenance_ok=0
    rm -f "$REPORT_TMP" || true
  elif [ "$report_dir_ready" -eq 1 ]; then
    if ! cat "$REPORT_TMP"; then
      echo "$(date -Iseconds) maintenance loop: could not read checkpoint report" >&2
      maintenance_ok=0
      rm -f "$REPORT_TMP" || true
    elif ! mv "$REPORT_TMP" "$REPORT_PATH"; then
      echo "$(date -Iseconds) maintenance loop: could not publish checkpoint report" >&2
      maintenance_ok=0
      rm -f "$REPORT_TMP" || true
    fi
  fi
  if [ "$maintenance_ok" -eq 1 ] && [ "$PRUNE_ENABLED" = "1" ]; then
    batch=0
    while [ "$batch" -lt "$PRUNE_BATCHES" ]; do
      archive_args="--no-archive"
      if [ "$PRUNE_ARCHIVE_ENABLED" = "1" ]; then
        archive_args="--archive"
      fi
      if ! prune_output="$(python -m pm_robot.cli --env /app/.env prune-evidence \
          --execute \
          "$archive_args" \
          --limit "$PRUNE_LIMIT" \
          --max-activity-rows "$PRUNE_MAX_ACTIVITY_ROWS" \
          --keep-recent-activity "$PRUNE_KEEP_RECENT_ACTIVITY")"; then
        maintenance_ok=0
        break
      fi
      printf '%s\n' "$prune_output"
      PRUNE_REPORT_TMP="${PRUNE_REPORT_PATH}.tmp.$$"
      if mkdir -p "$(dirname "$PRUNE_REPORT_PATH")" \
          && printf '%s\n' "$prune_output" >"$PRUNE_REPORT_TMP" \
          && mv "$PRUNE_REPORT_TMP" "$PRUNE_REPORT_PATH"; then
        :
      else
        echo "$(date -Iseconds) maintenance loop: could not publish prune report" >&2
        rm -f "$PRUNE_REPORT_TMP" || true
        maintenance_ok=0
        break
      fi
      batch=$((batch + 1))
      if [ "$batch" -lt "$PRUNE_BATCHES" ] && [ "$PRUNE_BATCH_DELAY" -gt 0 ]; then
        sleep "$PRUNE_BATCH_DELAY"
      fi
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
