#!/usr/bin/env bash
set -euo pipefail

REMOTE_USER="${PM_ROBOT_TUNNEL_REMOTE_USER:-root}"
REMOTE_HOST="${PM_ROBOT_TUNNEL_REMOTE_HOST:-${PM_ROBOT_VPS_HOST:-}}"
REMOTE_PORT="${PM_ROBOT_TUNNEL_REMOTE_PORT:-18787}"
LOCAL_HOST="${PM_ROBOT_TUNNEL_LOCAL_HOST:-127.0.0.1}"
LOCAL_PORT="${PM_ROBOT_TUNNEL_LOCAL_PORT:-8787}"
KEY_PATH="${PM_ROBOT_TUNNEL_KEY_PATH:-/volume1/docker/pm-robot/ssh/id_ed25519_pmrobot_vps}"
LOG_PATH="${PM_ROBOT_TUNNEL_LOG_PATH:-/volume1/docker/pm-robot/logs/reverse-tunnel.log}"

: "${REMOTE_HOST:?Set PM_ROBOT_TUNNEL_REMOTE_HOST or PM_ROBOT_VPS_HOST}"

mkdir -p "$(dirname "$LOG_PATH")"

while true; do
  printf '%s starting reverse tunnel remote=%s:%s local=%s:%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$REMOTE_HOST" "$REMOTE_PORT" "$LOCAL_HOST" "$LOCAL_PORT" >>"$LOG_PATH"
  ssh \
    -i "$KEY_PATH" \
    -N \
    -T \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o StrictHostKeyChecking=accept-new \
    -R "127.0.0.1:${REMOTE_PORT}:${LOCAL_HOST}:${LOCAL_PORT}" \
    "${REMOTE_USER}@${REMOTE_HOST}" >>"$LOG_PATH" 2>&1 || true
  printf '%s reverse tunnel stopped; restarting shortly\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$LOG_PATH"
  sleep 5
done
