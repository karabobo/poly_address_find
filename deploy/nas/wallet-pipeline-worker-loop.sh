#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PIPELINE_WORKER_INTERVAL:-60}"
SHARD_INDEX="${PM_ROBOT_PIPELINE_SHARD_INDEX:?PM_ROBOT_PIPELINE_SHARD_INDEX is required}"
SHARD_COUNT="${PM_ROBOT_PIPELINE_SHARD_COUNT:-3}"
LIMIT="${PM_ROBOT_PIPELINE_WORKER_LIMIT:-6}"
PAGE_LIMIT="${PM_ROBOT_PIPELINE_WORKER_PAGE_LIMIT:-120}"
SLEEP_SECONDS="${PM_ROBOT_PIPELINE_WORKER_SLEEP:-0.05}"
LEASE_SECONDS="${PM_ROBOT_PIPELINE_WORKER_LEASE_SECONDS:-900}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_PIPELINE_WORKER_ID:-nas-wallet-pipeline-${SHARD_INDEX}-${HOSTNAME_VALUE}}"

while true; do
  echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: start"
  if python -m pm_robot.cli --env /app/.env wallet-pipeline-worker \
      --shard-index "$SHARD_INDEX" \
      --shard-count "$SHARD_COUNT" \
      --limit "$LIMIT" \
      --page-limit "$PAGE_LIMIT" \
      --sleep "$SLEEP_SECONDS" \
      --lease-seconds "$LEASE_SECONDS" \
      --worker-id "$WORKER_ID"; then
    echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: ok"
  else
    echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: failed" >&2
  fi
  sleep "$INTERVAL"
done
