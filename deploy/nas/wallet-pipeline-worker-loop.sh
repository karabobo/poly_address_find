#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_PIPELINE_WORKER_INTERVAL:-60}"
ACTIVE_INTERVAL="${PM_ROBOT_PIPELINE_WORKER_ACTIVE_INTERVAL:-5}"
RUN_ONCE="${PM_ROBOT_PIPELINE_WORKER_RUN_ONCE:-0}"
SHARD_INDEX="${PM_ROBOT_PIPELINE_SHARD_INDEX:?PM_ROBOT_PIPELINE_SHARD_INDEX is required}"
SHARD_COUNT="${PM_ROBOT_PIPELINE_SHARD_COUNT:-3}"
LIMIT="${PM_ROBOT_PIPELINE_WORKER_LIMIT:-6}"
PAGE_LIMIT="${PM_ROBOT_PIPELINE_WORKER_PAGE_LIMIT:-500}"
SLEEP_SECONDS="${PM_ROBOT_PIPELINE_WORKER_SLEEP:-0.05}"
LEASE_SECONDS="${PM_ROBOT_PIPELINE_WORKER_LEASE_SECONDS:-900}"
PRIORITY_AGING_SECONDS="${PM_ROBOT_PIPELINE_PRIORITY_AGING_SECONDS:-1800}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_PIPELINE_WORKER_ID:-nas-wallet-pipeline-${SHARD_INDEX}-${HOSTNAME_VALUE}}"

while true; do
  echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: start"
  sleep_interval="$INTERVAL"
  worker_output=""
  if worker_output="$(python -m pm_robot.cli --env /app/.env wallet-pipeline-worker \
      --shard-index "$SHARD_INDEX" \
      --shard-count "$SHARD_COUNT" \
      --limit "$LIMIT" \
      --page-limit "$PAGE_LIMIT" \
      --sleep "$SLEEP_SECONDS" \
      --lease-seconds "$LEASE_SECONDS" \
      --priority-aging-seconds "$PRIORITY_AGING_SECONDS" \
      --worker-id "$WORKER_ID")"; then
    printf '%s\n' "$worker_output"
    worker_state=""
    if worker_state="$(printf '%s' "$worker_output" | python -c '
import json
import sys

payload = json.load(sys.stdin)
status = str(payload.get("status", ""))
jobs_attempted = int(payload.get("jobs_attempted", 0))
if status not in {"ok", "partial"} or jobs_attempted < 0:
    raise ValueError("unsupported wallet worker summary")
print(status, jobs_attempted)
' 2>/dev/null)"; then
      worker_status="${worker_state%% *}"
      jobs_attempted="${worker_state#* }"
    else
      worker_status="invalid"
      jobs_attempted=0
      summary_preview="$(printf '%.160s' "$worker_output" | tr '\n\r' '  ')"
      echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: invalid JSON summary; using idle interval; output=${summary_preview}" >&2
    fi
    if [ "$worker_status" = "ok" ] && [ "$jobs_attempted" -gt 0 ]; then
      # Drain non-empty shards promptly; empty shards retain the conservative poll interval.
      sleep_interval="$ACTIVE_INTERVAL"
    fi
    echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: ok"
  else
    if [ -n "$worker_output" ]; then
      printf '%s\n' "$worker_output"
    fi
    echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: failed" >&2
    worker_status="failed"
    jobs_attempted=0
  fi
  echo "$(date -Iseconds) wallet pipeline worker ${SHARD_INDEX}/${SHARD_COUNT}: next poll in ${sleep_interval}s (status=${worker_status}, jobs_attempted=${jobs_attempted})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
