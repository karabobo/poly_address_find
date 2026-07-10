#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PAPER_SETTLE_LOOP_INTERVAL:-900}"
LOCK_WAIT="${PM_ROBOT_PAPER_SETTLE_LOCK_WAIT:-120}"
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
    PM_ROBOT_TASK_NAME=paper-settle \
    /app/deploy/scripts/run_locked.sh "$@"
}

while true; do
  echo "$(date -Iseconds) paper settle loop: start"
  if run_locked python -m pm_robot.cli --env /app/.env paper-settle; then
    echo "$(date -Iseconds) paper settle loop: ok"
    runtime_heartbeat loop_paper_settle ok
  else
    echo "$(date -Iseconds) paper settle loop: failed" >&2
    runtime_heartbeat loop_paper_settle failed "paper-settle failed from execution profile"
  fi
  sleep "$INTERVAL"
done
