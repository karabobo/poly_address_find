#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PUBLISH_LOOP_INTERVAL:-900}"
TTL_SECONDS="${PM_ROBOT_PUBLISH_TTL_SECONDS:-86400}"
OUTPUT_PATH="${PM_ROBOT_PUBLISHED_LEADERS_PATH:-/app/reports/published_leaders.json}"
LOCK_WAIT="${PM_ROBOT_PUBLISH_LOCK_WAIT:-120}"
LOCK_PATH="${PM_ROBOT_WRITE_LOCK_PATH:-/app/data/pm_robot.write.lock}"

runtime_heartbeat() {
  name="$1"
  status="${2:-ok}"
  error="${3:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$name" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

run_locked() {
  PM_ROBOT_LOCK="$LOCK_PATH" \
    PM_ROBOT_LOCK_WAIT="$LOCK_WAIT" \
    PM_ROBOT_TASK_NAME=publish \
    /app/deploy/scripts/run_locked.sh "$@"
}

while true; do
  echo "$(date -Iseconds) publish loop: start"
  if run_locked python -m pm_robot.cli --env /app/.env publish-leaders \
      --ttl-seconds "$TTL_SECONDS" \
      --out "$OUTPUT_PATH"; then
    echo "$(date -Iseconds) publish loop: ok"
    runtime_heartbeat loop_publish ok
  else
    echo "$(date -Iseconds) publish loop: failed" >&2
    runtime_heartbeat loop_publish failed "publish-leaders failed from execution profile"
  fi
  sleep "$INTERVAL"
done
