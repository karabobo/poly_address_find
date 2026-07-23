#!/usr/bin/env sh
set -eu

ENDPOINT="${PM_ROBOT_RTDS_ENDPOINT:-wss://ws-live-data.polymarket.com}"
MIN_TRADE_USDC="${PM_ROBOT_RTDS_MIN_TRADE_USDC:-1}"
BATCH_SIZE="${PM_ROBOT_RTDS_BATCH_SIZE:-100}"
FLUSH_INTERVAL="${PM_ROBOT_RTDS_FLUSH_INTERVAL:-10}"
PING_INTERVAL="${PM_ROBOT_RTDS_PING_INTERVAL:-5}"
RECEIVE_TIMEOUT="${PM_ROBOT_RTDS_RECEIVE_TIMEOUT:-1}"
MAX_IDLE_SECONDS="${PM_ROBOT_RTDS_MAX_IDLE_SECONDS:-300}"
RECONNECT_SLEEP="${PM_ROBOT_RTDS_RECONNECT_SLEEP:-5}"
MAX_RUNTIME_SECONDS="${PM_ROBOT_RTDS_MAX_RUNTIME_SECONDS:-0}"
MAX_RECONNECTS="${PM_ROBOT_RTDS_MAX_RECONNECTS:-0}"

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
  echo "$(date -Iseconds) rtds discovery: start"
  if python -m pm_robot.cli --env /app/.env discover-rtds \
      --endpoint "$ENDPOINT" \
      --min-trade-usdc "$MIN_TRADE_USDC" \
      --batch-size "$BATCH_SIZE" \
      --flush-interval "$FLUSH_INTERVAL" \
      --ping-interval "$PING_INTERVAL" \
      --receive-timeout "$RECEIVE_TIMEOUT" \
      --max-idle-seconds "$MAX_IDLE_SECONDS" \
      --reconnect-sleep "$RECONNECT_SLEEP" \
      --max-runtime-seconds "$MAX_RUNTIME_SECONDS" \
      --max-reconnects "$MAX_RECONNECTS"; then
    echo "$(date -Iseconds) rtds discovery: exited normally"
    runtime_heartbeat loop_rtds_discovery ok
  else
    echo "$(date -Iseconds) rtds discovery: failed" >&2
    runtime_heartbeat loop_rtds_discovery failed "discover-rtds failed"
  fi
  sleep "$RECONNECT_SLEEP"
done
