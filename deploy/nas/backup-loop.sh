#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_BACKUP_INTERVAL:-86400}"
START_DELAY="${PM_ROBOT_BACKUP_START_DELAY:-600}"

runtime_heartbeat() {
  status="$1"
  rows_written="${2:-0}"
  error="${3:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name loop_backup \
    --status "$status" \
    --rows-written "$rows_written" \
    --error "$error" >/dev/null 2>&1 || true
}

if [ "$START_DELAY" -gt 0 ]; then
  sleep "$START_DELAY"
fi

while true; do
  echo "$(date -Iseconds) backup loop: start"
  if output="$(python -m pm_robot.cli --env /app/.env backup 2>&1)"; then
    echo "$(date -Iseconds) backup loop: $output"
    runtime_heartbeat ok 1
  else
    error="$(printf '%s' "$output" | tail -c 800)"
    echo "$(date -Iseconds) backup loop: failed: $error" >&2
    runtime_heartbeat failed 0 "$error"
  fi
  sleep "$INTERVAL"
done
