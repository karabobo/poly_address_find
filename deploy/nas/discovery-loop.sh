#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_DISCOVERY_LOOP_INTERVAL:-3600}"

LEADERBOARD_METRICS="${PM_ROBOT_DISCOVERY_LEADERBOARD_METRICS:-}"
LEADERBOARD_WINDOWS="${PM_ROBOT_DISCOVERY_LEADERBOARD_WINDOWS:-}"
LEADERBOARD_CATEGORIES="${PM_ROBOT_DISCOVERY_LEADERBOARD_CATEGORIES:-OVERALL,POLITICS,SPORTS,CRYPTO}"
LEADERBOARD_TIME_PERIODS="${PM_ROBOT_DISCOVERY_LEADERBOARD_TIME_PERIODS:-WEEK,MONTH}"
LEADERBOARD_ORDER_BYS="${PM_ROBOT_DISCOVERY_LEADERBOARD_ORDER_BYS:-PNL,VOL}"
LEADERBOARD_V1_LIMIT="${PM_ROBOT_DISCOVERY_LEADERBOARD_V1_LIMIT:-50}"
LEADERBOARD_V1_PAGES="${PM_ROBOT_DISCOVERY_LEADERBOARD_V1_PAGES:-1}"

ACTIVITY_PAGES="${PM_ROBOT_DISCOVERY_ACTIVITY_PAGES:-3}"
ACTIVITY_PAGE_LIMIT="${PM_ROBOT_DISCOVERY_ACTIVITY_PAGE_LIMIT:-100}"
ACTIVITY_MIN_TRADE_FILTER_USDC="${PM_ROBOT_DISCOVERY_ACTIVITY_MIN_TRADE_FILTER_USDC:-500}"
ACTIVITY_MAX_CANDIDATES="${PM_ROBOT_DISCOVERY_ACTIVITY_MAX_CANDIDATES:-150}"
ACTIVITY_SLEEP="${PM_ROBOT_DISCOVERY_ACTIVITY_SLEEP:-0.05}"

STATE_LIMIT="${PM_ROBOT_DISCOVERY_STATE_LIMIT:-250}"
STATE_COMMIT_EVERY="${PM_ROBOT_DISCOVERY_STATE_COMMIT_EVERY:-50}"
PIPELINE_SHARD_COUNT="${PM_ROBOT_PIPELINE_SHARD_COUNT:-3}"
PIPELINE_LIGHT_LIMIT="${PM_ROBOT_DISCOVERY_PIPELINE_LIGHT_LIMIT:-30}"
PIPELINE_MEDIUM_LIMIT="${PM_ROBOT_DISCOVERY_PIPELINE_MEDIUM_LIMIT:-10}"
PIPELINE_DEEP_LIMIT="${PM_ROBOT_DISCOVERY_PIPELINE_DEEP_LIMIT:-0}"
PIPELINE_MAX_ACTIVE_JOBS="${PM_ROBOT_PIPELINE_PLANNER_MAX_ACTIVE_JOBS:-240}"

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
  echo "$(date -Iseconds) discovery loop: leaderboard discovery start"
  if python -m pm_robot.cli --env /app/.env discover-leaderboard \
      --metrics "$LEADERBOARD_METRICS" \
      --windows "$LEADERBOARD_WINDOWS" \
      --categories "$LEADERBOARD_CATEGORIES" \
      --time-periods "$LEADERBOARD_TIME_PERIODS" \
      --order-bys "$LEADERBOARD_ORDER_BYS" \
      --v1-limit "$LEADERBOARD_V1_LIMIT" \
      --v1-pages "$LEADERBOARD_V1_PAGES"; then
    echo "$(date -Iseconds) discovery loop: leaderboard discovery ok"
    runtime_heartbeat loop_discovery_leaderboard ok
  else
    echo "$(date -Iseconds) discovery loop: leaderboard discovery failed" >&2
    runtime_heartbeat loop_discovery_leaderboard failed "discover-leaderboard failed"
  fi

  echo "$(date -Iseconds) discovery loop: whale activity discovery start"
  if python -m pm_robot.cli --env /app/.env discover-activity \
      --pages "$ACTIVITY_PAGES" \
      --page-limit "$ACTIVITY_PAGE_LIMIT" \
      --min-trades 1 \
      --min-usdc-volume 0 \
      --min-trade-filter-usdc "$ACTIVITY_MIN_TRADE_FILTER_USDC" \
      --max-candidates "$ACTIVITY_MAX_CANDIDATES" \
      --sleep "$ACTIVITY_SLEEP"; then
    echo "$(date -Iseconds) discovery loop: whale activity discovery ok"
    runtime_heartbeat loop_discovery_activity ok
  else
    echo "$(date -Iseconds) discovery loop: whale activity discovery failed" >&2
    runtime_heartbeat loop_discovery_activity failed "discover-activity failed"
  fi

  echo "$(date -Iseconds) discovery loop: materialize new wallet state start"
  if python -m pm_robot.cli --env /app/.env wallet-pipeline-state \
      --materialize \
      --limit "$STATE_LIMIT" \
      --commit-every "$STATE_COMMIT_EVERY"; then
    echo "$(date -Iseconds) discovery loop: materialize new wallet state ok"
    runtime_heartbeat loop_discovery_state ok
  else
    echo "$(date -Iseconds) discovery loop: materialize new wallet state failed" >&2
    runtime_heartbeat loop_discovery_state failed "wallet-pipeline-state failed"
  fi

  echo "$(date -Iseconds) discovery loop: plan new wallet jobs start"
  if python -m pm_robot.cli --env /app/.env wallet-pipeline-plan \
      --light-limit "$PIPELINE_LIGHT_LIMIT" \
      --medium-limit "$PIPELINE_MEDIUM_LIMIT" \
      --deep-limit "$PIPELINE_DEEP_LIMIT" \
      --shard-count "$PIPELINE_SHARD_COUNT" \
      --max-active-jobs "$PIPELINE_MAX_ACTIVE_JOBS"; then
    echo "$(date -Iseconds) discovery loop: plan new wallet jobs ok"
    runtime_heartbeat loop_wallet_pipeline_planner ok
  else
    echo "$(date -Iseconds) discovery loop: plan new wallet jobs failed" >&2
    runtime_heartbeat loop_wallet_pipeline_planner failed "wallet-pipeline-plan failed from discovery loop"
  fi

  sleep "$INTERVAL"
done
