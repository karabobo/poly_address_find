#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_BACKUP_INTERVAL:-86400}"
START_DELAY="${PM_ROBOT_BACKUP_START_DELAY:-600}"
BACKUP_DIR="${PM_ROBOT_BACKUP_DIR:-/app/backups}"

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

initial_wait="$START_DELAY"
if scheduled_wait="$(python - "$BACKUP_DIR" "$INTERVAL" "$START_DELAY" <<'PY'
from pathlib import Path
import sys

from pm_robot.ops import next_backup_delay_seconds

print(next_backup_delay_seconds(
    Path(sys.argv[1]),
    interval_seconds=int(sys.argv[2]),
    start_delay_seconds=int(sys.argv[3]),
))
PY
)"; then
  initial_wait="$scheduled_wait"
else
  initial_wait=0
  echo "$(date -Iseconds) backup loop: schedule calculation failed; backing up now" >&2
  runtime_heartbeat failed 0 "backup schedule calculation failed"
fi

if [ "$initial_wait" -gt 0 ]; then
  echo "$(date -Iseconds) backup loop: next backup in ${initial_wait}s"
  runtime_heartbeat ok 0
  sleep "$initial_wait"
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
