#!/usr/bin/env sh
set -eu

# Pushes one immutable research manifest through a restricted SSH key. The
# receiver controls the destination path and this script never enables trading.
if [ "${PM_ROBOT_L6_HANDOFF_ENABLED:-0}" != "1" ]; then
  exit 0
fi

MANIFEST_PATH="${PM_ROBOT_HIGH_CONFIDENCE_L6_EXPORT_PATH:-/app/data/exports/current_high_confidence_l6.json}"
STATE_PATH="${PM_ROBOT_L6_HANDOFF_STATE_PATH:-/app/data/state/l6-handoff.sha256}"
REFRESH_SECONDS="${PM_ROBOT_L6_HANDOFF_REFRESH_SECONDS:-21600}"
TARGET_HOST="${PM_ROBOT_L6_HANDOFF_HOST:?PM_ROBOT_L6_HANDOFF_HOST is required}"
TARGET_PORT="${PM_ROBOT_L6_HANDOFF_PORT:-22}"
TARGET_USER="${PM_ROBOT_L6_HANDOFF_USER:-ubuntu}"
IDENTITY_FILE="${PM_ROBOT_L6_HANDOFF_IDENTITY_FILE:-/app/ssh/polyhermes-l6-handoff-ed25519}"
KNOWN_HOSTS_FILE="${PM_ROBOT_L6_HANDOFF_KNOWN_HOSTS_FILE:-/app/ssh/polyhermes-l6-known_hosts}"

[ -s "$MANIFEST_PATH" ] || {
  echo "$(date -Iseconds) L6 handoff skipped: manifest is missing or empty" >&2
  exit 1
}
[ -r "$IDENTITY_FILE" ] || {
  echo "$(date -Iseconds) L6 handoff key is missing or unreadable: $IDENTITY_FILE" >&2
  exit 1
}
[ -r "$KNOWN_HOSTS_FILE" ] || {
  echo "$(date -Iseconds) L6 handoff known_hosts is missing or unreadable: $KNOWN_HOSTS_FILE" >&2
  exit 1
}

manifest_sha="$(python3 - "$MANIFEST_PATH" <<'PY'
import hashlib
import json
import math
import sys
from decimal import Decimal


def canonical_manifest_json(value):
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SystemExit("schema-v3 canonical JSON requires finite numbers")
        decimal_value = Decimal(repr(value))
        if decimal_value.is_zero():
            return "0.0"
        return format(decimal_value, "f")
    if isinstance(value, dict):
        return "{" + ",".join(
            canonical_manifest_json(str(key)) + ":" + canonical_manifest_json(value[key])
            for key in sorted(value)
        ) + "}"
    if isinstance(value, list):
        return "[" + ",".join(canonical_manifest_json(item) for item in value) + "]"
    raise SystemExit(f"unsupported schema-v3 canonical JSON value: {type(value)!r}")


def manifest_checksum(manifest):
    payload = dict(manifest)
    payload.pop("manifest_checksum", None)
    return hashlib.sha256(canonical_manifest_json(payload).encode("utf-8")).hexdigest()


with open(sys.argv[1], "r", encoding="utf-8") as handle:
    manifest = json.load(handle)
if manifest.get("schema_version") != 3:
    raise SystemExit("L6 handoff requires schema_version 3")
if manifest.get("source") != "pm_robot.current_high_confidence_l6":
    raise SystemExit("L6 handoff source is invalid")
handoff_status = manifest.get("handoff_status")
if handoff_status not in {"ready", "warming", "degraded"}:
    raise SystemExit("L6 handoff status is invalid")
if not isinstance(manifest.get("replace_active_set_allowed"), bool):
    raise SystemExit("L6 handoff replace_active_set_allowed must be boolean")
if manifest.get("automatic_trading_activation") is not False:
    raise SystemExit("L6 handoff automatic_trading_activation must be false")
if manifest.get("research_only") is not True or manifest.get("not_for_trading") is not True:
    raise SystemExit("L6 handoff must remain research-only")
candidates = manifest.get("candidates")
if not isinstance(candidates, list):
    raise SystemExit("L6 handoff candidates must be a list")
if handoff_status == "ready" and (
    manifest.get("replace_active_set_allowed") is not True or not candidates
):
    raise SystemExit("ready L6 handoff must contain a replaceable candidate set")
expected_checksum = manifest.get("manifest_checksum")
if not isinstance(expected_checksum, str) or len(expected_checksum) != 64:
    raise SystemExit("L6 handoff manifest_checksum is missing or invalid")
actual_checksum = manifest_checksum(manifest)
if actual_checksum != expected_checksum:
    raise SystemExit("L6 handoff manifest_checksum mismatch")
print(actual_checksum)
PY
)"
if [ -r "$STATE_PATH" ] && [ "$(cat "$STATE_PATH")" = "$manifest_sha" ]; then
  state_mtime="$(stat -c %Y "$STATE_PATH" 2>/dev/null || stat -f %m "$STATE_PATH" 2>/dev/null || printf '0')"
  now_epoch="$(date +%s)"
  case "$REFRESH_SECONDS:$state_mtime" in
    *[!0-9:]*|:*|*:)
      echo "$(date -Iseconds) L6 handoff refresh settings are invalid" >&2
      exit 1
      ;;
  esac
  if [ "$REFRESH_SECONDS" -gt 0 ] && [ $((now_epoch - state_mtime)) -lt "$REFRESH_SECONDS" ]; then
    exit 0
  fi
fi

ssh \
  -T \
  -p "$TARGET_PORT" \
  -i "$IDENTITY_FILE" \
  -o BatchMode=yes \
  -o ConnectTimeout=15 \
  -o HostKeyAlgorithms=ssh-ed25519 \
  -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes \
  -o UserKnownHostsFile="$KNOWN_HOSTS_FILE" \
  "$TARGET_USER@$TARGET_HOST" \
  pmrobot-l6-upload < "$MANIFEST_PATH"

state_dir="$(dirname "$STATE_PATH")"
mkdir -p "$state_dir"
state_tmp="${STATE_PATH}.tmp.$$"
printf '%s\n' "$manifest_sha" > "$state_tmp"
mv "$state_tmp" "$STATE_PATH"
echo "$(date -Iseconds) L6 handoff pushed: manifest_sha256=$manifest_sha"
