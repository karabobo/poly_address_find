#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_COPYABILITY_PLANNER_INTERVAL:-600}"
LIMIT="${PM_ROBOT_COPYABILITY_PLANNER_LIMIT:-50}"
MAX_ACTIVE_JOBS="${PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS:-50}"
MIN_SCORE="${PM_ROBOT_COPYABILITY_MIN_SCORE:-40}"
MIN_ACTIVITY_EVENTS="${PM_ROBOT_COPYABILITY_MIN_ACTIVITY_EVENTS:-25}"
SHARD_COUNT="${PM_ROBOT_COPYABILITY_SHARD_COUNT:-1}"
RESCAN_SECONDS="${PM_ROBOT_COPYABILITY_RESCAN_SECONDS:-21600}"

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
  echo "$(date -Iseconds) copyability planner: start"
  if python -m pm_robot.cli --env /app/.env copyability-plan \
      --limit "$LIMIT" \
      --max-active-jobs "$MAX_ACTIVE_JOBS" \
      --min-score "$MIN_SCORE" \
      --min-activity-events "$MIN_ACTIVITY_EVENTS" \
      --shard-count "$SHARD_COUNT" \
      --rescan-seconds "$RESCAN_SECONDS"; then
    echo "$(date -Iseconds) copyability planner: ok"
    runtime_heartbeat loop_copyability_planner ok
  else
    echo "$(date -Iseconds) copyability planner: failed" >&2
    runtime_heartbeat loop_copyability_planner failed "copyability-plan failed"
  fi
  sleep "$INTERVAL"
done
