#!/usr/bin/env bash
set -euo pipefail

ROOT="${PM_ROBOT_HOME:-/opt/pm-robot}"
cd "$ROOT"
"$ROOT/.venv/bin/python" -m pm_robot.cli --env "$ROOT/.env" backup
