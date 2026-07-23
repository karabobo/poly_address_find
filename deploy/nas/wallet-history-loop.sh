#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_WALLET_HISTORY_WORKER_INTERVAL:-60}"
ACTIVE_INTERVAL="${PM_ROBOT_WALLET_HISTORY_ACTIVE_INTERVAL:-30}"
RUN_ONCE="${PM_ROBOT_WALLET_HISTORY_RUN_ONCE:-0}"
SHARD_COUNT="${PM_ROBOT_WALLET_HISTORY_SHARD_COUNT:-3}"
SHARD_INDEX="${PM_ROBOT_WALLET_HISTORY_SHARD_INDEX:-}"
WORKER_LIMIT="${PM_ROBOT_WALLET_HISTORY_WORKER_LIMIT:-1}"
LEASE_SECONDS="${PM_ROBOT_WALLET_HISTORY_LEASE_SECONDS:-1800}"
SLEEP_SECONDS="${PM_ROBOT_WALLET_HISTORY_REQUEST_SLEEP:-0.05}"
ARCHIVE_DIR="${PM_ROBOT_ARCHIVE_DIR:-/app/data/parquet}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_WALLET_HISTORY_WORKER_ID:-nas-wallet-history-${SHARD_INDEX}-${HOSTNAME_VALUE}}"
HEARTBEAT_NAME="${PM_ROBOT_WALLET_HISTORY_HEARTBEAT_NAME:-}"

if [ -z "$SHARD_INDEX" ]; then
  echo "PM_ROBOT_WALLET_HISTORY_SHARD_INDEX is required" >&2
  exit 2
fi
if [ -z "$HEARTBEAT_NAME" ]; then
  HEARTBEAT_NAME="loop_wallet_history_worker_${SHARD_INDEX}"
fi

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name "$HEARTBEAT_NAME" \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

while true; do
  sleep_interval="$INTERVAL"
  command_status="failed"
  jobs_attempted=0
  command_output=""

  echo "$(date -Iseconds) wallet history worker ${SHARD_INDEX}/${SHARD_COUNT}: start"
  if command_output="$(python -m pm_robot.cli --env /app/.env wallet-history-worker \
      --archive-dir "$ARCHIVE_DIR" \
      --shard-index "$SHARD_INDEX" \
      --shard-count "$SHARD_COUNT" \
      --limit "$WORKER_LIMIT" \
      --lease-seconds "$LEASE_SECONDS" \
      --sleep "$SLEEP_SECONDS" \
      --worker-id "$WORKER_ID")"; then
    printf '%s\n' "$command_output"
    worker_state=""
    if worker_state="$(printf '%s' "$command_output" | python -c '
import json
import sys

payload = json.load(sys.stdin)
status = str(payload.get("status", ""))
jobs_attempted = payload.get("jobs_attempted")
if status not in {"ok", "partial"}:
    raise ValueError("unsupported wallet history worker status")
if isinstance(jobs_attempted, bool) or not isinstance(jobs_attempted, int) or jobs_attempted < 0:
    raise ValueError("jobs_attempted must be a nonnegative integer")
print(status, jobs_attempted)
' 2>/dev/null)"; then
      command_status="${worker_state%% *}"
      jobs_attempted="${worker_state#* }"
      if [ "$jobs_attempted" -gt 0 ]; then
        sleep_interval="$ACTIVE_INTERVAL"
      fi
    else
      command_status="invalid"
      echo "$(date -Iseconds) wallet history worker returned invalid JSON" >&2
    fi
  else
    if [ -n "$command_output" ]; then
      printf '%s\n' "$command_output"
    fi
    echo "$(date -Iseconds) wallet history worker failed" >&2
  fi

  if [ "$command_status" = "ok" ]; then
    runtime_heartbeat ok
  elif [ "$command_status" = "partial" ]; then
    runtime_heartbeat partial
  else
    runtime_heartbeat failed "wallet history worker failed or returned invalid summary"
  fi

  echo "$(date -Iseconds) wallet history worker: next poll in ${sleep_interval}s (status=${command_status}, jobs=${jobs_attempted})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
