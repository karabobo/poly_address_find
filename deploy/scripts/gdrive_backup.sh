#!/usr/bin/env bash
set -euo pipefail

ROOT="${PM_ROBOT_HOME:-/opt/pm-robot}"
ENV_FILE="${PM_ROBOT_ENV_FILE:-$ROOT/.env}"
REMOTE="${PM_ROBOT_GDRIVE_REMOTE:-}"
RETENTION_DAYS="${PM_ROBOT_GDRIVE_RETENTION_DAYS:-30}"
ZSTD_LEVEL="${PM_ROBOT_GDRIVE_ZSTD_LEVEL:-10}"

if [[ -z "$REMOTE" ]]; then
  echo "PM_ROBOT_GDRIVE_REMOTE is required, for example pmrobot-gdrive:" >&2
  exit 2
fi
if ! command -v rclone >/dev/null 2>&1; then
  echo "rclone is required before Google Drive backups can run" >&2
  exit 127
fi
if ! command -v zstd >/dev/null 2>&1; then
  echo "zstd is required before Google Drive backups can run" >&2
  exit 127
fi

cd "$ROOT"

if [[ "$REMOTE" == *: ]]; then
  REMOTE_DIR="${REMOTE}backups"
else
  REMOTE_DIR="${REMOTE%/}/backups"
fi

TS="$(date -u +%Y%m%d-%H%M%S)"
DUMP_NAME="pm_robot-${TS}.sql.zst"
META_NAME="pm_robot-${TS}.json"
TMP_DIR="${PM_ROBOT_BACKUP_DIR:-$ROOT/backups}/.gdrive-tmp"
mkdir -p "$TMP_DIR"

SHA_FILE="$(mktemp "$TMP_DIR/sha.XXXXXX")"
SIZE_FILE="$(mktemp "$TMP_DIR/size.XXXXXX")"
META_FILE="$TMP_DIR/$META_NAME"
cleanup() {
  rm -f "$SHA_FILE" "$SIZE_FILE" "$META_FILE"
}
trap cleanup EXIT

rclone mkdir "$REMOTE_DIR"

"$ROOT/.venv/bin/python" -m pm_robot.cli --env "$ENV_FILE" backup-sql-dump \
  | zstd -T0 "-$ZSTD_LEVEL" \
  | tee >(sha256sum | awk '{print $1}' > "$SHA_FILE") \
  | tee >(wc -c | awk '{print $1}' > "$SIZE_FILE") \
  | rclone rcat "$REMOTE_DIR/$DUMP_NAME"

SHA256="$(cat "$SHA_FILE")"
SIZE_BYTES="$(cat "$SIZE_FILE")"
cat > "$META_FILE" <<JSON
{
  "created_at_utc": "$TS",
  "format": "sqlite-sql-dump+zstd",
  "file": "$DUMP_NAME",
  "sha256": "$SHA256",
  "compressed_size_bytes": $SIZE_BYTES,
  "restore_hint": "zstd -dc $DUMP_NAME | sqlite3 restored.sqlite"
}
JSON

rclone copyto "$META_FILE" "$REMOTE_DIR/$META_NAME"
rclone copyto "$REMOTE_DIR/$DUMP_NAME" "$REMOTE_DIR/pm_robot-latest.sql.zst"
rclone copyto "$META_FILE" "$REMOTE_DIR/pm_robot-latest.json"

if [[ "$RETENTION_DAYS" != "0" ]]; then
  rclone delete "$REMOTE_DIR" \
    --min-age "${RETENTION_DAYS}d" \
    --filter "- pm_robot-latest.*" \
    --filter "+ pm_robot-*.sql.zst" \
    --filter "+ pm_robot-*.json" \
    --filter "- *"
fi

echo "gdrive backup uploaded: $REMOTE_DIR/$DUMP_NAME sha256=$SHA256 bytes=$SIZE_BYTES"
