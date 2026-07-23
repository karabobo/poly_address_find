#!/usr/bin/env sh
set -eu

MODE="${PM_ROBOT_WALLET_SCREEN_MODE:-worker}"
PLANNER_INTERVAL="${PM_ROBOT_WALLET_SCREEN_PLANNER_INTERVAL:-180}"
WORKER_INTERVAL="${PM_ROBOT_WALLET_SCREEN_WORKER_INTERVAL:-60}"
ACTIVE_INTERVAL="${PM_ROBOT_WALLET_SCREEN_ACTIVE_INTERVAL:-30}"
RUN_ONCE="${PM_ROBOT_WALLET_SCREEN_RUN_ONCE:-0}"
PLANNER_LIMIT="${PM_ROBOT_WALLET_SCREEN_PLANNER_LIMIT:-24}"
MAX_ACTIVE_JOBS="${PM_ROBOT_WALLET_SCREEN_MAX_ACTIVE_JOBS:-72}"
RESCREEN_AFTER_SECONDS="${PM_ROBOT_WALLET_SCREEN_RESCREEN_AFTER_SECONDS:-604800}"
SHARD_COUNT="${PM_ROBOT_WALLET_SCREEN_SHARD_COUNT:-3}"
SHARD_INDEX="${PM_ROBOT_WALLET_SCREEN_SHARD_INDEX:-}"
WORKER_LIMIT="${PM_ROBOT_WALLET_SCREEN_WORKER_LIMIT:-2}"
LEASE_SECONDS="${PM_ROBOT_WALLET_SCREEN_LEASE_SECONDS:-600}"
HOSTNAME_VALUE="$(hostname 2>/dev/null || echo nas)"
WORKER_ID="${PM_ROBOT_WALLET_SCREEN_WORKER_ID:-nas-wallet-screen-${SHARD_INDEX:-planner}-${HOSTNAME_VALUE}}"
HEARTBEAT_NAME="${PM_ROBOT_WALLET_SCREEN_HEARTBEAT_NAME:-}"

case "$MODE" in
  planner)
    INTERVAL="$PLANNER_INTERVAL"
    if [ -z "$HEARTBEAT_NAME" ]; then
      HEARTBEAT_NAME="loop_wallet_screen_planner"
    fi
    ;;
  worker)
    INTERVAL="$WORKER_INTERVAL"
    if [ -z "$SHARD_INDEX" ]; then
      echo "PM_ROBOT_WALLET_SCREEN_SHARD_INDEX is required in worker mode" >&2
      exit 2
    fi
    if [ -z "$HEARTBEAT_NAME" ]; then
      HEARTBEAT_NAME="loop_wallet_screen_worker_${SHARD_INDEX}"
    fi
    ;;
  *)
    echo "PM_ROBOT_WALLET_SCREEN_MODE must be planner or worker" >&2
    exit 2
    ;;
esac

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
  work_count=0
  command_output=""

  if [ "$MODE" = "planner" ]; then
    echo "$(date -Iseconds) wallet screen planner: start"
    if command_output="$(python -m pm_robot.cli --env /app/.env wallet-screen-plan \
        --limit "$PLANNER_LIMIT" \
        --max-active-jobs "$MAX_ACTIVE_JOBS" \
        --rescreen-after-seconds "$RESCREEN_AFTER_SECONDS" \
        --shard-count "$SHARD_COUNT")"; then
      printf '%s\n' "$command_output"
      planner_state=""
      if planner_state="$(printf '%s' "$command_output" | python -c '
import json
import sys

payload = json.load(sys.stdin)
status = str(payload.get("status", ""))
jobs_enqueued = payload.get("jobs_enqueued")
active_jobs = payload.get("active_jobs")
throttled = payload.get("throttled")
if status != "ok":
    raise ValueError("unsupported wallet screen planner status")
for value in (jobs_enqueued, active_jobs):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("wallet screen planner counters must be nonnegative integers")
if not isinstance(throttled, bool):
    raise ValueError("wallet screen planner throttled must be boolean")
print(status, jobs_enqueued, active_jobs, int(throttled))
' 2>/dev/null)"; then
        command_status="${planner_state%% *}"
        remaining_state="${planner_state#* }"
        work_count="${remaining_state%% *}"
        if [ "$work_count" -gt 0 ]; then
          sleep_interval="$ACTIVE_INTERVAL"
        fi
        echo "$(date -Iseconds) wallet screen planner: ok"
      else
        command_status="invalid"
        summary_preview="$(printf '%.160s' "$command_output" | tr '\n\r' '  ')"
        echo "$(date -Iseconds) wallet screen planner: invalid JSON summary; output=${summary_preview}" >&2
      fi
    else
      if [ -n "$command_output" ]; then
        printf '%s\n' "$command_output"
      fi
      echo "$(date -Iseconds) wallet screen planner: failed" >&2
    fi
  else
    echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: start"
    if command_output="$(python -m pm_robot.cli --env /app/.env wallet-screen-worker \
        --shard-index "$SHARD_INDEX" \
        --shard-count "$SHARD_COUNT" \
        --limit "$WORKER_LIMIT" \
        --lease-seconds "$LEASE_SECONDS" \
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
    raise ValueError("unsupported wallet screen worker status")
if isinstance(jobs_attempted, bool) or not isinstance(jobs_attempted, int) or jobs_attempted < 0:
    raise ValueError("wallet screen worker jobs_attempted must be a nonnegative integer")
print(status, jobs_attempted)
' 2>/dev/null)"; then
        command_status="${worker_state%% *}"
        work_count="${worker_state#* }"
        if [ "$work_count" -gt 0 ]; then
          sleep_interval="$ACTIVE_INTERVAL"
        fi
        echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: ok"
      else
        command_status="invalid"
        summary_preview="$(printf '%.160s' "$command_output" | tr '\n\r' '  ')"
        echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: invalid JSON summary; output=${summary_preview}" >&2
      fi
    else
      if [ -n "$command_output" ]; then
        printf '%s\n' "$command_output"
      fi
      echo "$(date -Iseconds) wallet screen worker ${SHARD_INDEX}/${SHARD_COUNT}: failed" >&2
    fi
  fi

  if [ "$command_status" = "ok" ]; then
    runtime_heartbeat ok
  elif [ "$command_status" = "partial" ]; then
    runtime_heartbeat partial
  else
    runtime_heartbeat failed "wallet screen ${MODE} failed or returned invalid summary"
  fi

  echo "$(date -Iseconds) wallet screen ${MODE}: next poll in ${sleep_interval}s (status=${command_status}, work=${work_count})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
