#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PIPELINE_PLANNER_INTERVAL:-900}"
LIGHT_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_LIGHT_LIMIT:-30}"
MEDIUM_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_MEDIUM_LIMIT:-20}"
DEEP_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_DEEP_LIMIT:-5}"
SHARD_COUNT="${PM_ROBOT_PIPELINE_SHARD_COUNT:-3}"
MAX_ACTIVE_JOBS="${PM_ROBOT_PIPELINE_PLANNER_MAX_ACTIVE_JOBS:-240}"

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
  echo "$(date -Iseconds) wallet pipeline planner: start"
  if python -m pm_robot.cli --env /app/.env wallet-pipeline-plan \
      --light-limit "$LIGHT_LIMIT" \
      --medium-limit "$MEDIUM_LIMIT" \
      --deep-limit "$DEEP_LIMIT" \
      --shard-count "$SHARD_COUNT" \
      --max-active-jobs "$MAX_ACTIVE_JOBS"; then
    echo "$(date -Iseconds) wallet pipeline planner: ok"
    runtime_heartbeat loop_wallet_pipeline_planner ok
  else
    echo "$(date -Iseconds) wallet pipeline planner: failed" >&2
    runtime_heartbeat loop_wallet_pipeline_planner failed "wallet-pipeline-plan failed"
  fi
  sleep "$INTERVAL"
done
