#!/usr/bin/env sh
set -eu

# This loop owns only local control-plane decisions. Network history reads are
# executed by sharded wallet-history workers.
INTERVAL="${PM_ROBOT_RESEARCH_CONTROL_INTERVAL:-180}"
ACTIVE_INTERVAL="${PM_ROBOT_RESEARCH_CONTROL_ACTIVE_INTERVAL:-30}"
RUN_ONCE="${PM_ROBOT_RESEARCH_CONTROL_RUN_ONCE:-0}"
SHARD_COUNT="${PM_ROBOT_WALLET_HISTORY_SHARD_COUNT:-3}"
HISTORY_LIMIT="${PM_ROBOT_WALLET_HISTORY_PLANNER_LIMIT:-12}"
HISTORY_MAX_ACTIVE_JOBS="${PM_ROBOT_WALLET_HISTORY_MAX_ACTIVE_JOBS:-36}"
LIGHT_REFRESH_SECONDS="${PM_ROBOT_WALLET_HISTORY_LIGHT_REFRESH_SECONDS:-2592000}"
DEEP_REFRESH_SECONDS="${PM_ROBOT_WALLET_HISTORY_DEEP_REFRESH_SECONDS:-604800}"
MIN_COHORT_SIZE="${PM_ROBOT_WALLET_LEVEL_MIN_COHORT_SIZE:-20}"
TIMEOUT_MIN_COHORT_SIZE="${PM_ROBOT_WALLET_LEVEL_TIMEOUT_MIN_COHORT_SIZE:-5}"
MAX_WAIT_SECONDS="${PM_ROBOT_WALLET_LEVEL_MAX_WAIT_SECONDS:-3600}"
L3_FRACTION="${PM_ROBOT_WALLET_LEVEL_L3_FRACTION:-0.25}"
L4_FRACTION="${PM_ROBOT_WALLET_LEVEL_L4_FRACTION:-0.20}"
L5_FRACTION="${PM_ROBOT_WALLET_LEVEL_L5_FRACTION:-0.10}"
L3_MAX_PROMOTIONS="${PM_ROBOT_WALLET_LEVEL_L3_MAX_PROMOTIONS:-12}"
L4_MAX_PROMOTIONS="${PM_ROBOT_WALLET_LEVEL_L4_MAX_PROMOTIONS:-6}"
L5_MAX_PROMOTIONS="${PM_ROBOT_WALLET_LEVEL_L5_MAX_PROMOTIONS:-2}"
L6_LIMIT="${PM_ROBOT_WALLET_L6_PLANNER_LIMIT:-5}"
L6_MAX_ACTIVE_JOBS="${PM_ROBOT_WALLET_L6_MAX_ACTIVE_JOBS:-10}"
L6_SHARD_COUNT="${PM_ROBOT_WALLET_L6_SHARD_COUNT:-1}"
L6_REFRESH_SECONDS="${PM_ROBOT_WALLET_L6_REFRESH_SECONDS:-1209600}"

runtime_heartbeat() {
  status="$1"
  error="${2:-}"
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name loop_wallet_history_planner \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
  python -m pm_robot.cli --env /app/.env runtime-heartbeat \
    --name loop_wallet_level_control \
    --status "$status" \
    --error "$error" >/dev/null 2>&1 || true
}

json_counter_sum() {
  python -c '
import json
import sys

selection = json.loads(sys.argv[1])
history = json.loads(sys.argv[2])
l6 = json.loads(sys.argv[3])
if selection.get("status") != "ok" or history.get("status") != "ok" or l6.get("status") != "ok":
    raise ValueError("unsupported control summary status")
keys = ("promoted_l3", "promoted_l4", "promoted_l5")
values = [selection.get(key) for key in keys]
values.append(history.get("jobs_enqueued"))
values.append(l6.get("jobs_enqueued"))
if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values):
    raise ValueError("control counters must be nonnegative integers")
print(sum(values))
' "$1" "$2" "$3"
}

while true; do
  sleep_interval="$INTERVAL"
  cycle_status="failed"
  work_count=0
  selection_output=""
  history_output=""
  l6_output=""

  echo "$(date -Iseconds) wallet level control: start"
  selection_ok=0
  if selection_output="$(python -m pm_robot.cli --env /app/.env wallet-level-select \
      --min-cohort-size "$MIN_COHORT_SIZE" \
      --timeout-min-cohort-size "$TIMEOUT_MIN_COHORT_SIZE" \
      --max-wait-seconds "$MAX_WAIT_SECONDS" \
      --l3-fraction "$L3_FRACTION" \
      --l4-fraction "$L4_FRACTION" \
      --l5-fraction "$L5_FRACTION" \
      --l3-max-promotions "$L3_MAX_PROMOTIONS" \
      --l4-max-promotions "$L4_MAX_PROMOTIONS" \
      --l5-max-promotions "$L5_MAX_PROMOTIONS")"; then
    selection_ok=1
    printf '%s\n' "$selection_output"
  else
    echo "$(date -Iseconds) wallet level selection failed" >&2
  fi

  history_ok=0
  if history_output="$(python -m pm_robot.cli --env /app/.env wallet-history-plan \
      --limit "$HISTORY_LIMIT" \
      --max-active-jobs "$HISTORY_MAX_ACTIVE_JOBS" \
      --light-refresh-seconds "$LIGHT_REFRESH_SECONDS" \
      --deep-refresh-seconds "$DEEP_REFRESH_SECONDS" \
      --shard-count "$SHARD_COUNT")"; then
    history_ok=1
    printf '%s\n' "$history_output"
  else
    echo "$(date -Iseconds) wallet history planning failed" >&2
  fi

  l6_ok=0
  if l6_output="$(python -m pm_robot.cli --env /app/.env wallet-l6-plan \
      --limit "$L6_LIMIT" \
      --max-active-jobs "$L6_MAX_ACTIVE_JOBS" \
      --shard-count "$L6_SHARD_COUNT" \
      --refresh-seconds "$L6_REFRESH_SECONDS")"; then
    l6_ok=1
    printf '%s\n' "$l6_output"
  else
    echo "$(date -Iseconds) L6 validation planning failed" >&2
  fi

  if [ "$selection_ok" -eq 1 ] && [ "$history_ok" -eq 1 ] && [ "$l6_ok" -eq 1 ]; then
    if work_count="$(json_counter_sum "$selection_output" "$history_output" "$l6_output" 2>/dev/null)"; then
      cycle_status="ok"
      if [ "$work_count" -gt 0 ]; then
        sleep_interval="$ACTIVE_INTERVAL"
      fi
      runtime_heartbeat ok
    else
      cycle_status="invalid"
      runtime_heartbeat partial "wallet level control returned invalid summaries"
    fi
  else
    runtime_heartbeat partial "wallet level selection, history planning, or L6 planning failed"
  fi

  echo "$(date -Iseconds) wallet level control: next cycle in ${sleep_interval}s (status=${cycle_status}, work=${work_count})"
  if [ "$RUN_ONCE" = "1" ]; then
    break
  fi
  sleep "$sleep_interval"
done
