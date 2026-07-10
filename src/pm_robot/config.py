"""Configuration and policy loading."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_POLICY_PATH = Path("config/leader_scoring_policy.json")


@dataclass(frozen=True)
class RobotSettings:
    db_path: Path = Path("data/pm_robot.sqlite")
    policy_path: Path = DEFAULT_POLICY_PATH
    candidate_addresses_path: Path = Path("data/candidate_addresses.csv")
    candidate_review_path: Path = Path("reports/candidate_review_queue.csv")
    paper_ledger_path: Path = Path("data/paper_ledger.jsonl")
    execution_mode: str = "paper"
    log_dir: Path = Path("logs")
    backup_dir: Path = Path("backups")
    paper_bankroll_usd: float = 2000.0

    @classmethod
    def load(cls, env_path: Path | None = None) -> "RobotSettings":
        load_dotenv(env_path or Path(".env"))
        return cls(
            db_path=Path(os.environ.get("PM_ROBOT_DB_PATH", "data/pm_robot.sqlite")),
            policy_path=Path(os.environ.get("PM_ROBOT_POLICY_PATH", str(DEFAULT_POLICY_PATH))),
            candidate_addresses_path=Path(
                os.environ.get("PM_ROBOT_CANDIDATE_ADDRESSES", "data/candidate_addresses.csv")
            ),
            candidate_review_path=Path(
                os.environ.get("PM_ROBOT_REVIEW_PATH", "reports/pm_robot_review_queue.csv")
            ),
            paper_ledger_path=Path(os.environ.get("PM_ROBOT_PAPER_LEDGER", "data/paper_ledger.jsonl")),
            execution_mode=os.environ.get("PM_ROBOT_MODE", "paper").lower(),
            log_dir=Path(os.environ.get("PM_ROBOT_LOG_DIR", "logs")),
            backup_dir=Path(os.environ.get("PM_ROBOT_BACKUP_DIR", "backups")),
            paper_bankroll_usd=float(os.environ.get("PM_ROBOT_PAPER_BANKROLL_USD", "2000")),
        )

    def assert_safe(self) -> None:
        if self.execution_mode == "live":
            raise RuntimeError("live execution has been moved out of this research project")


def load_policy(path: Path = DEFAULT_POLICY_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def threshold(policy: dict[str, Any], name: str, default: float = 0.0) -> float:
    return float(policy.get("thresholds", {}).get(name, default))


def weight(policy: dict[str, Any], name: str, default: float = 0.0) -> float:
    return float(policy.get("weights", {}).get(name, default))


def penalty(policy: dict[str, Any], name: str, default: float = 0.0) -> float:
    return float(policy.get("penalties", {}).get(name, default))
