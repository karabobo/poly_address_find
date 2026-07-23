"""CSV helpers for curated wallet-address ingress."""

from __future__ import annotations

import csv
from pathlib import Path

from pm_robot.models import CandidateAddress


def load_candidate_addresses(path: Path) -> list[CandidateAddress]:
    with path.open(encoding="utf-8") as handle:
        rows = csv.DictReader(handle)
        return [
            CandidateAddress(
                address=(row.get("address") or "").strip().lower(),
                sources=row.get("sources", ""),
                labels=row.get("labels", ""),
                notes=row.get("notes", ""),
                links=row.get("links", ""),
                status=row.get("status", ""),
            )
            for row in rows
            if row.get("address")
        ]
