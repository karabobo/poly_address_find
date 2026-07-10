#!/usr/bin/env sh
set -eu

INTERVAL="${PM_ROBOT_SCORE_LOOP_INTERVAL:-300}"
FULL_SCORE_INTERVAL="${PM_ROBOT_SCORE_FULL_INTERVAL:-300}"
FEATURE_LIMIT="${PM_ROBOT_SCORE_FEATURE_LIMIT:-80}"
SCORE_LIMIT="${PM_ROBOT_SCORE_LIMIT:-300}"
MIN_ACTIVITY_EVENTS="${PM_ROBOT_SCORE_MIN_ACTIVITY_EVENTS:-25}"
STATE_LIMIT="${PM_ROBOT_SCORE_STATE_LIMIT:-${PM_ROBOT_SCORE_PRIORITY_STATE_LIMIT:-120}}"
STATE_COMMIT_EVERY="${PM_ROBOT_SCORE_STATE_COMMIT_EVERY:-${PM_ROBOT_SCORE_PRIORITY_STATE_COMMIT_EVERY:-40}}"
PIPELINE_SHARD_COUNT="${PM_ROBOT_PIPELINE_SHARD_COUNT:-3}"
PIPELINE_LIGHT_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_LIGHT_LIMIT:-30}"
PIPELINE_MEDIUM_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_MEDIUM_LIMIT:-20}"
PIPELINE_DEEP_LIMIT="${PM_ROBOT_PIPELINE_PLANNER_DEEP_LIMIT:-5}"
PIPELINE_MAX_ACTIVE_JOBS="${PM_ROBOT_PIPELINE_PLANNER_MAX_ACTIVE_JOBS:-240}"
LAST_FULL_SCORE=0

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
  NOW="$(date +%s)"
  echo "$(date -Iseconds) score loop: materialize features start"
  if python -m pm_robot.cli --env /app/.env materialize-features \
      --limit "$FEATURE_LIMIT" \
      --min-activity-events "$MIN_ACTIVITY_EVENTS"; then
    echo "$(date -Iseconds) score loop: materialize features ok"
    runtime_heartbeat loop_score_features ok
  else
    echo "$(date -Iseconds) score loop: materialize features failed" >&2
    runtime_heartbeat loop_score_features failed "materialize-features failed"
  fi

  if [ $((NOW - LAST_FULL_SCORE)) -ge "$FULL_SCORE_INTERVAL" ]; then
    echo "$(date -Iseconds) score loop: incremental build review start"
    if python -m pm_robot.cli --env /app/.env build-review \
        --incremental \
        --limit "$SCORE_LIMIT" \
        --no-import-csv; then
      LAST_FULL_SCORE="$(date +%s)"
      echo "$(date -Iseconds) score loop: incremental build review ok"
      runtime_heartbeat loop_score_review ok

      echo "$(date -Iseconds) score loop: sync wallet pipeline state start"
      if python -m pm_robot.cli --env /app/.env wallet-pipeline-state \
          --materialize \
          --limit "$STATE_LIMIT" \
          --commit-every "$STATE_COMMIT_EVERY"; then
        echo "$(date -Iseconds) score loop: sync wallet pipeline state ok"
        echo "$(date -Iseconds) score loop: plan wallet pipeline jobs start"
        if python -m pm_robot.cli --env /app/.env wallet-pipeline-plan \
            --light-limit "$PIPELINE_LIGHT_LIMIT" \
            --medium-limit "$PIPELINE_MEDIUM_LIMIT" \
            --deep-limit "$PIPELINE_DEEP_LIMIT" \
            --shard-count "$PIPELINE_SHARD_COUNT" \
            --max-active-jobs "$PIPELINE_MAX_ACTIVE_JOBS"; then
          echo "$(date -Iseconds) score loop: plan wallet pipeline jobs ok"
          runtime_heartbeat loop_score_state_plan ok
        else
          echo "$(date -Iseconds) score loop: plan wallet pipeline jobs failed" >&2
          runtime_heartbeat loop_score_state_plan failed "wallet-pipeline-plan failed from score loop"
        fi
      else
        echo "$(date -Iseconds) score loop: sync wallet pipeline state failed" >&2
        runtime_heartbeat loop_score_state_plan failed "wallet-pipeline-state failed from score loop"
      fi

      echo "$(date -Iseconds) score loop: export paper handoff start"
      if python -m pm_robot.cli --env /app/.env paper-handoff-export \
          --out /app/reports/paper_handoff.json \
          --csv-out /app/reports/paper_handoff.csv \
          --limit 250; then
        echo "$(date -Iseconds) score loop: export paper handoff ok"
        runtime_heartbeat loop_score_paper_handoff ok
      else
        echo "$(date -Iseconds) score loop: export paper handoff failed" >&2
        runtime_heartbeat loop_score_paper_handoff failed "paper-handoff-export failed from score loop"
      fi

    else
      echo "$(date -Iseconds) score loop: build review failed" >&2
      runtime_heartbeat loop_score_review failed "build-review failed"
    fi
  fi

  sleep "$INTERVAL"
done
