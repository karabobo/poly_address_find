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
PRUNE_BATCHES="${PM_ROBOT_RETENTION_PRUNE_BATCHES:-6}"
PRUNE_LIMIT="${PM_ROBOT_RETENTION_PRUNE_LIMIT:-20}"
PRUNE_MAX_ACTIVITY_ROWS="${PM_ROBOT_RETENTION_PRUNE_MAX_ACTIVITY_ROWS:-5000}"
PRUNE_BATCH_DELAY="${PM_ROBOT_RETENTION_PRUNE_BATCH_DELAY:-10}"
PRUNE_KEEP_RECENT_ACTIVITY="${PM_ROBOT_RETENTION_KEEP_RECENT_ACTIVITY:-0}"
PRUNE_ARCHIVE_ENABLED="${PM_ROBOT_RETENTION_ARCHIVE_ENABLED:-0}"
PRUNE_REPORT_PATH="${PM_ROBOT_RETENTION_PRUNE_REPORT_PATH:-/app/reports/retention_prune_status.json}"
PRUNE_CATCHUP_PASSES="${PM_ROBOT_RETENTION_CATCHUP_PASSES:-4}"
PRUNE_CATCHUP_DELAY="${PM_ROBOT_RETENTION_CATCHUP_DELAY:-60}"
PRUNE_CATCHUP_BACKLOG_ROWS="${PM_ROBOT_RETENTION_CATCHUP_BACKLOG_ROWS:-1000000}"
PRUNE_HIGH_BACKLOG_INTERVAL="${PM_ROBOT_RETENTION_HIGH_BACKLOG_INTERVAL:-60}"
PRUNE_CONTROL_LOCK_TIMEOUT="${PM_ROBOT_RETENTION_CONTROL_LOCK_TIMEOUT:-60}"
PRUNE_SQLITE_CACHE_MIB="${PM_ROBOT_RETENTION_SQLITE_CACHE_MIB:-128}"
PRUNE_SQLITE_MMAP_MIB="${PM_ROBOT_RETENTION_SQLITE_MMAP_MIB:-256}"

case "$PRUNE_CATCHUP_BACKLOG_ROWS" in
  ''|*[!0-9]*) PRUNE_CATCHUP_BACKLOG_ROWS=1000000 ;;
esac
case "$PRUNE_HIGH_BACKLOG_INTERVAL" in
  ''|*[!0-9]*) PRUNE_HIGH_BACKLOG_INTERVAL=60 ;;
esac
case "$PRUNE_SQLITE_CACHE_MIB" in
  ''|*[!0-9]*) PRUNE_SQLITE_CACHE_MIB=128 ;;
esac
case "$PRUNE_SQLITE_MMAP_MIB" in
  ''|*[!0-9]*) PRUNE_SQLITE_MMAP_MIB=256 ;;
esac
if [ "$PRUNE_HIGH_BACKLOG_INTERVAL" -lt 30 ]; then
  PRUNE_HIGH_BACKLOG_INTERVAL=30
fi

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
  prune_state=""
  prune_backlog_rows=0
  if [ "$maintenance_ok" -eq 1 ] && [ "$PRUNE_ENABLED" = "1" ]; then
    archive_args="--no-archive"
    if [ "$PRUNE_ARCHIVE_ENABLED" = "1" ]; then
      archive_args="--archive"
    fi
    prune_pass=1
    while [ "$prune_pass" -le "$PRUNE_CATCHUP_PASSES" ]; do
      if prune_output="$(python -m pm_robot.cli --env /app/.env retention-cycle \
          --execute \
          "$archive_args" \
          --batches "$PRUNE_BATCHES" \
          --limit "$PRUNE_LIMIT" \
          --max-activity-rows "$PRUNE_MAX_ACTIVITY_ROWS" \
          --batch-delay-seconds "$PRUNE_BATCH_DELAY" \
          --cycle-interval-seconds "$INTERVAL" \
          --control-lock-timeout-seconds "$PRUNE_CONTROL_LOCK_TIMEOUT" \
          --sqlite-cache-mib "$PRUNE_SQLITE_CACHE_MIB" \
          --sqlite-mmap-mib "$PRUNE_SQLITE_MMAP_MIB" \
          --previous-report "$PRUNE_REPORT_PATH" \
          --report-path "$PRUNE_REPORT_PATH" \
          --keep-recent-activity "$PRUNE_KEEP_RECENT_ACTIVITY")"; then
        prune_command_ok=1
      else
        prune_command_ok=0
        maintenance_ok=0
      fi
      if [ -n "$prune_output" ]; then
        printf '%s\n' "$prune_output"
      elif [ "$prune_command_ok" -eq 0 ]; then
        echo "$(date -Iseconds) maintenance loop: prune command failed without report" >&2
      fi
      if [ "$prune_command_ok" -eq 0 ] || [ -z "$prune_output" ]; then
        break
      fi
      prune_state="$(python -c 'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8")).get("state", ""))' "$PRUNE_REPORT_PATH" 2>/dev/null || true)"
      prune_backlog_rows="$(python -c 'import json, sys; payload=json.load(open(sys.argv[1], encoding="utf-8")); print(max(0, int((payload.get("backlog_after") or {}).get("total_activity_rows") or 0)))' "$PRUNE_REPORT_PATH" 2>/dev/null || printf '0')"
      case "$prune_state" in
        inflow_outpacing_cleanup|yielded_to_research|retention_starved)
          if [ "$prune_pass" -lt "$PRUNE_CATCHUP_PASSES" ]; then
            echo "$(date -Iseconds) maintenance loop: retention ${prune_state}; retry in ${PRUNE_CATCHUP_DELAY}s"
            sleep "$PRUNE_CATCHUP_DELAY"
          fi
          ;;
        draining)
          if [ "$prune_backlog_rows" -gt "$PRUNE_CATCHUP_BACKLOG_ROWS" ] && [ "$prune_pass" -lt "$PRUNE_CATCHUP_PASSES" ]; then
            echo "$(date -Iseconds) maintenance loop: retention backlog ${prune_backlog_rows} remains above ${PRUNE_CATCHUP_BACKLOG_ROWS}; retry in ${PRUNE_CATCHUP_DELAY}s"
            sleep "$PRUNE_CATCHUP_DELAY"
          else
            break
          fi
          ;;
        *)
          break
          ;;
      esac
      prune_pass=$((prune_pass + 1))
    done
    if [ "$prune_state" = "retention_starved" ]; then
      echo "$(date -Iseconds) maintenance loop: retention remained starved after ${PRUNE_CATCHUP_PASSES} passes" >&2
      maintenance_ok=0
    fi
  fi
  if [ "$maintenance_ok" -eq 1 ]; then
    echo "$(date -Iseconds) maintenance loop: ok"
    runtime_heartbeat loop_maintenance ok
  else
    echo "$(date -Iseconds) maintenance loop: failed" >&2
    runtime_heartbeat loop_maintenance failed "maintenance failed"
  fi
  next_interval="$INTERVAL"
  high_backlog_state=0
  case "$prune_state" in
    draining|inflow_outpacing_cleanup|yielded_to_research)
      high_backlog_state=1
      ;;
  esac
  if [ "$maintenance_ok" -eq 1 ] && [ "$PRUNE_ENABLED" = "1" ] && [ "$high_backlog_state" -eq 1 ] && [ "$prune_backlog_rows" -gt "$PRUNE_CATCHUP_BACKLOG_ROWS" ]; then
    next_interval="$PRUNE_HIGH_BACKLOG_INTERVAL"
    echo "$(date -Iseconds) maintenance loop: retention backlog ${prune_backlog_rows} remains high; next cycle in ${next_interval}s"
  fi
  sleep "$next_interval"
done
