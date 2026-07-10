#!/usr/bin/env bash
set -euo pipefail

REMOTE_USER="${PM_ROBOT_VPS_USER:-root}"
REMOTE_HOST="${PM_ROBOT_VPS_HOST:-}"
LOCAL_PROXY_HOST="${PM_ROBOT_PROXY_LOCAL_HOST:-0.0.0.0}"
LOCAL_PROXY_PORT="${PM_ROBOT_PROXY_LOCAL_PORT:-18082}"
REMOTE_PROXY_HOST="${PM_ROBOT_PROXY_REMOTE_HOST:-127.0.0.1}"
REMOTE_PROXY_PORT="${PM_ROBOT_PROXY_REMOTE_PORT:-18081}"
KEY_PATH="${PM_ROBOT_VPS_KEY_PATH:-/ssh/id_ed25519_pmrobot_vps}"
LOG_PATH="${PM_ROBOT_PROXY_TUNNEL_LOG_PATH:-/logs/vps-http-proxy-tunnel.log}"

: "${REMOTE_HOST:?Set PM_ROBOT_VPS_HOST}"

mkdir -p "$(dirname "$LOG_PATH")"

while true; do
  printf '%s starting VPS HTTP proxy tunnel local=%s:%s remote=%s:%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$LOCAL_PROXY_HOST" "$LOCAL_PROXY_PORT" "$REMOTE_PROXY_HOST" "$REMOTE_PROXY_PORT" >>"$LOG_PATH"
  ssh \
    -i "$KEY_PATH" \
    -g \
    -N \
    -T \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o StrictHostKeyChecking=accept-new \
    -L "${LOCAL_PROXY_HOST}:${LOCAL_PROXY_PORT}:${REMOTE_PROXY_HOST}:${REMOTE_PROXY_PORT}" \
    "${REMOTE_USER}@${REMOTE_HOST}" >>"$LOG_PATH" 2>&1 || true
  printf '%s VPS HTTP proxy tunnel stopped; restarting shortly\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$LOG_PATH"
  sleep 5
done
