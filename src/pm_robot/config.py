"""Filesystem configuration for the wallet research service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RobotSettings:
    db_path: Path = Path("data/pm_robot.sqlite")
    rate_limit_db_path: Path | None = None
    candidate_addresses_path: Path = Path("data/candidate_addresses.csv")
    log_dir: Path = Path("logs")
    backup_dir: Path = Path("backups")
    archive_dir: Path = Path("data/parquet")
    required_runtime_heartbeats: tuple[str, ...] = ()
    runtime_heartbeat_max_age_seconds: int = 0
    runtime_heartbeat_max_age_overrides: tuple[tuple[str, int], ...] = ()

    @classmethod
    def load(cls, env_path: Path | None = None) -> "RobotSettings":
        load_dotenv(env_path or Path(".env"))
        rate_limit_db_path = os.environ.get("PM_ROBOT_RATE_LIMIT_DB_PATH", "").strip()
        required_heartbeats = tuple(
            value.strip()
            for value in os.environ.get("PM_ROBOT_REQUIRED_RUNTIME_HEARTBEATS", "").split(",")
            if value.strip()
        )
        return cls(
            db_path=Path(os.environ.get("PM_ROBOT_DB_PATH", "data/pm_robot.sqlite")),
            rate_limit_db_path=Path(rate_limit_db_path) if rate_limit_db_path else None,
            candidate_addresses_path=Path(
                os.environ.get("PM_ROBOT_CANDIDATE_ADDRESSES", "data/candidate_addresses.csv")
            ),
            log_dir=Path(os.environ.get("PM_ROBOT_LOG_DIR", "logs")),
            backup_dir=Path(os.environ.get("PM_ROBOT_BACKUP_DIR", "backups")),
            archive_dir=Path(os.environ.get("PM_ROBOT_ARCHIVE_DIR", "data/parquet")),
            required_runtime_heartbeats=required_heartbeats,
            runtime_heartbeat_max_age_seconds=max(
                0,
                int(os.environ.get("PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_SECONDS", "0")),
            ),
            runtime_heartbeat_max_age_overrides=_heartbeat_max_age_overrides(
                os.environ.get("PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_OVERRIDES", "")
            ),
        )


def _heartbeat_max_age_overrides(raw: str) -> tuple[tuple[str, int], ...]:
    """Parse loop-specific health windows from ``name:seconds`` pairs."""

    values: dict[str, int] = {}
    for item in str(raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        name, separator, seconds = item.partition(":")
        if not separator or not name.strip() or not seconds.strip():
            raise ValueError(
                "PM_ROBOT_RUNTIME_HEARTBEAT_MAX_AGE_OVERRIDES must use name:seconds pairs"
            )
        values[name.strip()] = max(60, int(seconds.strip()))
    return tuple(values.items())


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
