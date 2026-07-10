#!/usr/bin/env bash
set -euo pipefail

LOCK="${PM_ROBOT_LOCK:-/opt/pm-robot/data/pm_robot.write.lock}"
WAIT="${PM_ROBOT_LOCK_WAIT:-30}"
TASK="${PM_ROBOT_TASK_NAME:-$(basename "${1:-unknown}")}"
TIMEOUT_EXIT="${PM_ROBOT_LOCK_TIMEOUT_EXIT:-0}"

exec 9>"$LOCK"
started_at="$(date +%s)"
if ! flock -w "$WAIT" 9; then
  waited="$(( $(date +%s) - started_at ))"
  echo "lock_skipped: task=${TASK} lock=${LOCK} waited_seconds=${waited}" >&2
  exit "$TIMEOUT_EXIT"
fi

waited="$(( $(date +%s) - started_at ))"
echo "lock_acquired: task=${TASK} waited_seconds=${waited}"
exec "$@"
