#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_MAINTENANCE_INTERVAL:-900}"
START_DELAY="${PM_ROBOT_MAINTENANCE_START_DELAY:-300}"
RUN_ONCE="${PM_ROBOT_MAINTENANCE_RUN_ONCE:-0}"
WAL_CHECKPOINT="${PM_ROBOT_MAINTENANCE_WAL_CHECKPOINT:-passive}"
REPORT_PATH="${PM_ROBOT_MAINTENANCE_REPORT_PATH:-/app/reports/maintenance_status.json}"
FAILED_JOB_COOLDOWN_SECONDS="${PM_ROBOT_MAINTENANCE_FAILED_JOB_COOLDOWN_SECONDS:-21600}"
KEEP_BACKUPS="${PM_ROBOT_MAINTENANCE_KEEP_BACKUPS:-0}"
RUNTIME_HEARTBEAT_DAYS="${PM_ROBOT_MAINTENANCE_RUNTIME_HEARTBEAT_DAYS:-30}"
CLEANUP_BATCH_LIMIT="${PM_ROBOT_MAINTENANCE_CLEANUP_BATCH_LIMIT:-500}"
HISTORY_GC_ENABLED="${PM_ROBOT_WALLET_HISTORY_GC_ENABLED:-1}"
HISTORY_GC_MIN_AGE_SECONDS="${PM_ROBOT_WALLET_HISTORY_GC_MIN_AGE_SECONDS:-2592000}"
HISTORY_GC_KEEP_PER_WALLET="${PM_ROBOT_WALLET_HISTORY_GC_KEEP_PER_WALLET:-1}"
HISTORY_GC_LIMIT="${PM_ROBOT_WALLET_HISTORY_GC_LIMIT:-500}"
HISTORY_AUDIT_ENABLED="${PM_ROBOT_WALLET_HISTORY_AUDIT_ENABLED:-1}"
HISTORY_AUDIT_VERIFY_CHECKSUMS="${PM_ROBOT_WALLET_HISTORY_AUDIT_VERIFY_CHECKSUMS:-0}"
HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS="${PM_ROBOT_WALLET_HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS:-604800}"
HISTORY_AUDIT_ORPHAN_LIMIT="${PM_ROBOT_WALLET_HISTORY_AUDIT_ORPHAN_LIMIT:-500}"
HISTORY_AUDIT_DELETE_ORPHANS="${PM_ROBOT_WALLET_HISTORY_AUDIT_DELETE_ORPHANS:-1}"

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name loop_maintenance \
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
  report_tmp="${REPORT_PATH}.tmp.$$"

  if ! mkdir -p "$(dirname "$REPORT_PATH")"; then
    echo "$(date -Iseconds) maintenance loop: report directory unavailable" >&2
    maintenance_ok=0
  elif ! python -m pm_robot.cli --env /app/.env maintenance \
      --cleanup-batch-limit "$CLEANUP_BATCH_LIMIT" \
      --reset-stale-jobs \
      --failed-job-cooldown-seconds "$FAILED_JOB_COOLDOWN_SECONDS" \
      --heartbeat-days "$RUNTIME_HEARTBEAT_DAYS" \
      --keep-backups "$KEEP_BACKUPS" \
      --wal-checkpoint "$WAL_CHECKPOINT" >"$report_tmp"; then
    maintenance_ok=0
    rm -f "$report_tmp" || true
  elif ! cat "$report_tmp" || ! mv "$report_tmp" "$REPORT_PATH"; then
    echo "$(date -Iseconds) maintenance loop: could not write report" >&2
    maintenance_ok=0
    rm -f "$report_tmp" || true
  fi

  if [ "$maintenance_ok" -eq 1 ] && [ "$HISTORY_AUDIT_ENABLED" = "1" ]; then
    echo "$(date -Iseconds) wallet history audit: start"
    set -- \
      --orphan-min-age-seconds "$HISTORY_AUDIT_ORPHAN_MIN_AGE_SECONDS" \
      --orphan-limit "$HISTORY_AUDIT_ORPHAN_LIMIT"
    if [ "$HISTORY_AUDIT_VERIFY_CHECKSUMS" = "1" ]; then
      set -- "$@" --verify-checksums
    fi
    if [ "$HISTORY_AUDIT_DELETE_ORPHANS" = "1" ]; then
      set -- "$@" --delete-orphans
    fi
    if audit_output="$(python -m pm_robot.cli --env /app/.env wallet-history-audit "$@")"; then
      printf '%s\n' "$audit_output"
    else
      if [ -n "${audit_output:-}" ]; then
        printf '%s\n' "$audit_output"
      fi
      echo "$(date -Iseconds) wallet history audit: failed" >&2
      maintenance_ok=0
    fi
  fi

  # GC runs only after the catalog/filesystem audit succeeds.
  if [ "$maintenance_ok" -eq 1 ] && [ "$HISTORY_GC_ENABLED" = "1" ]; then
    echo "$(date -Iseconds) wallet history GC: start"
    if gc_output="$(python -m pm_robot.cli --env /app/.env wallet-history-gc \
        --min-age-seconds "$HISTORY_GC_MIN_AGE_SECONDS" \
        --keep-per-wallet "$HISTORY_GC_KEEP_PER_WALLET" \
        --limit "$HISTORY_GC_LIMIT" \
        --execute)"; then
      printf '%s\n' "$gc_output"
    else
      if [ -n "${gc_output:-}" ]; then
        printf '%s\n' "$gc_output"
      fi
      echo "$(date -Iseconds) wallet history GC: failed" >&2
      maintenance_ok=0
    fi
  fi

  if [ "$maintenance_ok" -eq 1 ]; then
    echo "$(date -Iseconds) maintenance loop: ok"
    runtime_heartbeat ok
  else
    echo "$(date -Iseconds) maintenance loop: failed" >&2
    runtime_heartbeat failed "maintenance, wallet history audit, or GC failed"
  fi

  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$INTERVAL"
done
