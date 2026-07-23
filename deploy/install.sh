#!/usr/bin/env bash
set -euo pipefail

ROOT="${PM_ROBOT_HOME:-/opt/pm-robot}"
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo for /opt install, or set PM_ROBOT_HOME to a writable path." >&2
  exit 1
fi

mkdir -p "$ROOT"/{data/parquet,logs,backups,reports,config}
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
"$ROOT/.venv/bin/python" -m pm_robot.cli --env "$ROOT/.env" migrate

# Remove any previously installed pm-robot unit that is absent from the current
# research-only manifest. This keeps upgrades self-cleaning without naming old products.
shopt -s nullglob
for installed_path in /etc/systemd/system/pm-robot-*.service /etc/systemd/system/pm-robot-*.timer; do
  installed_unit="$(basename "$installed_path")"
  if [[ ! -f "$ROOT/deploy/systemd/$installed_unit" ]]; then
    systemctl disable --now "$installed_unit" >/dev/null 2>&1 || true
    rm -f "$installed_path"
    rm -f "/etc/systemd/system/timers.target.wants/$installed_unit"
    rm -f "/etc/systemd/system/multi-user.target.wants/$installed_unit"
  fi
done
shopt -u nullglob

cp "$ROOT/deploy/systemd/"*.service /etc/systemd/system/
cp "$ROOT/deploy/systemd/"*.timer /etc/systemd/system/

systemctl daemon-reload
systemctl enable --now pm-robot-health.timer
systemctl enable --now pm-robot-discover.timer
systemctl enable --now pm-robot-discover-activity.timer
systemctl enable --now pm-robot-wallet-screen-planner.timer
systemctl enable --now pm-robot-wallet-screen-worker@0.timer
systemctl enable --now pm-robot-wallet-screen-worker@1.timer
systemctl enable --now pm-robot-wallet-screen-worker@2.timer
systemctl enable --now pm-robot-wallet-level-select.timer
systemctl enable --now pm-robot-wallet-history-planner.timer
systemctl enable --now pm-robot-wallet-history-worker@0.timer
systemctl enable --now pm-robot-wallet-history-worker@1.timer
systemctl enable --now pm-robot-wallet-history-worker@2.timer
systemctl enable --now pm-robot-wallet-l6-planner.timer
systemctl enable --now pm-robot-wallet-l6-worker.timer
systemctl enable --now pm-robot-maintenance.timer
if grep -q '^PM_ROBOT_UI_TOKEN=.' "$ROOT/.env"; then
  systemctl enable --now pm-robot-web.service
else
  systemctl disable --now pm-robot-web.service >/dev/null 2>&1 || true
fi

echo "Installed pm-robot to $ROOT"
echo "Edit $ROOT/.env for L0-L6 wallet discovery settings."
echo "Units absent from the current research manifest were stopped and removed."
echo "Database backups are explicit CLI operations; no backup timer is installed."
echo "pm-robot-web.service starts after PM_ROBOT_UI_TOKEN is set in $ROOT/.env."
