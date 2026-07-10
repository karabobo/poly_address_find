#!/usr/bin/env bash
set -euo pipefail

ROOT="${PM_ROBOT_HOME:-/opt/pm-robot}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo for /opt install, or set PM_ROBOT_HOME to a writable path." >&2
  exit 1
fi

mkdir -p "$ROOT"/{data,logs,backups,reports,config}
rsync -a --delete \
  --exclude '.git' \
  --exclude '.venv' \
  --exclude '.env' \
  --exclude '.env.*' \
  --exclude 'data/pm_robot.sqlite' \
  --exclude 'data/pm_robot.sqlite-wal' \
  --exclude 'data/pm_robot.sqlite-shm' \
  --exclude 'logs' \
  --exclude 'backups' \
  "$SRC"/ "$ROOT"/

if [[ ! -f "$ROOT/.env" ]]; then
  cp "$ROOT/deploy/env.example" "$ROOT/.env"
fi

python3 -m venv "$ROOT/.venv"
"$ROOT/.venv/bin/pip" install --upgrade pip
"$ROOT/.venv/bin/pip" install -e "$ROOT"

chmod +x "$ROOT/deploy/scripts/"*.sh

cp "$ROOT/deploy/systemd/"*.service /etc/systemd/system/
cp "$ROOT/deploy/systemd/"*.timer /etc/systemd/system/

systemctl daemon-reload
systemctl enable --now pm-robot-health.timer
systemctl enable --now pm-robot-discover.timer
systemctl enable --now pm-robot-discover-activity.timer
systemctl disable --now pm-robot-evidence-backfill.timer >/dev/null 2>&1 || true
systemctl enable --now pm-robot-evidence-planner.timer
systemctl enable --now pm-robot-evidence-worker@0.timer
systemctl enable --now pm-robot-evidence-worker@1.timer
systemctl enable --now pm-robot-evidence-worker@2.timer
systemctl enable --now pm-robot-materialize-features.timer
systemctl enable --now pm-robot-trade-role.timer
systemctl enable --now pm-robot-ingest.timer
systemctl enable --now pm-robot-activity.timer
systemctl enable --now pm-robot-gamma-paper.timer
systemctl enable --now pm-robot-gamma.timer
systemctl enable --now pm-robot-copy-backtest.timer
systemctl enable --now pm-robot-score.timer
systemctl enable --now pm-robot-paper.timer
systemctl enable --now pm-robot-paper-settle.timer
systemctl enable --now pm-robot-publish.timer
if grep -q '^PM_ROBOT_GDRIVE_REMOTE=.' "$ROOT/.env" && command -v rclone >/dev/null 2>&1; then
  systemctl enable --now pm-robot-gdrive-backup.timer
  systemctl disable --now pm-robot-backup.timer >/dev/null 2>&1 || true
else
  systemctl enable --now pm-robot-backup.timer
  systemctl disable --now pm-robot-gdrive-backup.timer >/dev/null 2>&1 || true
fi
systemctl enable --now pm-robot-maintenance.timer
if grep -q '^PM_ROBOT_UI_TOKEN=.' "$ROOT/.env"; then
  systemctl enable --now pm-robot-web.service
else
  systemctl disable --now pm-robot-web.service >/dev/null 2>&1 || true
fi

echo "Installed pm-robot to $ROOT"
echo "Edit $ROOT/.env for research ingestion and paper-evaluation settings."
echo "pm-robot-copy-graph.timer is installed but not enabled by default; enable it after the copy graph job is optimized for your dataset size."
echo "pm-robot-gdrive-backup.timer starts after rclone is installed and PM_ROBOT_GDRIVE_REMOTE is set."
echo "pm-robot-web.service starts after PM_ROBOT_UI_TOKEN is set in $ROOT/.env."
