#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_COPYABILITY_WORKER_INTERVAL:-30}"
SHARD_INDEX="${PM_ROBOT_COPYABILITY_SHARD_INDEX:?PM_ROBOT_COPYABILITY_SHARD_INDEX is required}"
SHARD_COUNT="${PM_ROBOT_COPYABILITY_SHARD_COUNT:-1}"
LIMIT="${PM_ROBOT_COPYABILITY_WORKER_LIMIT:-1}"
LEASE_SECONDS="${PM_ROBOT_COPYABILITY_WORKER_LEASE_SECONDS:-7200}"
MAX_LEADER_EVENTS="${PM_ROBOT_COPYABILITY_MAX_LEADER_EVENTS:-3000}"
MAX_FOLLOWERS_PER_EVENT="${PM_ROBOT_COPYABILITY_MAX_FOLLOWERS_PER_EVENT:-200}"
PREFER_SCAN_MODE="${PM_ROBOT_COPYABILITY_PREFER_SCAN_MODE:-}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_COPYABILITY_WORKER_ID:-nas-copyability-${SHARD_INDEX}-${HOSTNAME_VALUE}}"

while true; do
  echo "$(date -Iseconds) copyability worker ${SHARD_INDEX}/${SHARD_COUNT}: start"
  if python -m pm_robot.cli --env /app/.env copyability-worker \
      --shard-index "$SHARD_INDEX" \
      --shard-count "$SHARD_COUNT" \
      --limit "$LIMIT" \
      --lease-seconds "$LEASE_SECONDS" \
      --worker-id "$WORKER_ID" \
      --max-leader-events "$MAX_LEADER_EVENTS" \
      --max-followers-per-event "$MAX_FOLLOWERS_PER_EVENT" \
      --prefer-scan-mode "$PREFER_SCAN_MODE"; then
    echo "$(date -Iseconds) copyability worker ${SHARD_INDEX}/${SHARD_COUNT}: ok"
  else
    echo "$(date -Iseconds) copyability worker ${SHARD_INDEX}/${SHARD_COUNT}: failed" >&2
  fi
  sleep "$INTERVAL"
done
