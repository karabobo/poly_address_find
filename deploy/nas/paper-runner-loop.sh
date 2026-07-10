#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PAPER_RUN_LOOP_INTERVAL:-60}"
LIMIT="${PM_ROBOT_PAPER_RUN_LIMIT:-50}"
MAX_STAKE_USD="${PM_ROBOT_PAPER_RUN_MAX_STAKE_USD:-40}"
MAX_SIGNAL_AGE_SEC="${PM_ROBOT_PAPER_RUN_MAX_SIGNAL_AGE_SEC:-300}"
LOCK_WAIT="${PM_ROBOT_PAPER_RUN_LOCK_WAIT:-120}"
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
    PM_ROBOT_TASK_NAME=paper-run \
    /app/deploy/scripts/run_locked.sh "$@"
}

while true; do
  echo "$(date -Iseconds) paper runner loop: start"
  if run_locked python -m pm_robot.cli --env /app/.env paper-run \
      --limit "$LIMIT" \
      --max-stake-usd "$MAX_STAKE_USD" \
      --max-signal-age-sec "$MAX_SIGNAL_AGE_SEC"; then
    echo "$(date -Iseconds) paper runner loop: ok"
    runtime_heartbeat loop_paper_runner ok
  else
    echo "$(date -Iseconds) paper runner loop: failed" >&2
    runtime_heartbeat loop_paper_runner failed "paper-run failed from execution profile"
  fi
  sleep "$INTERVAL"
done
