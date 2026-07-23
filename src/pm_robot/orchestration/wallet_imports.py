"""Trusted-source imports routed through the shared wallet ingress."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pm_robot.io import load_candidate_addresses
from pm_robot.orchestration.wallet_sightings import record_wallet_sighting
from pm_robot.research.polydata_features import extract_polydata
from pm_robot.storage.repository import upsert_wallet_feature


def import_candidates_from_csv(
    conn: sqlite3.Connection,
    *,
    addresses_path: Path,
) -> int:
    """Import a curated CSV and advance each valid trusted wallet to L1."""

    candidates = load_candidate_addresses(addresses_path)
    for candidate in candidates:
        record_wallet_sighting(
            conn,
            candidate,
            trusted_source=True,
        )
    conn.commit()
    return len(candidates)


def import_polydata_json(conn: sqlite3.Connection, *, polydata_path: Path) -> dict[str, int]:
    """Import Polydata provenance and compact features without raw history."""

    candidates, features = extract_polydata(polydata_path)
    candidate_wallets: set[str] = set()
    for candidate in candidates:
        result = record_wallet_sighting(
            conn,
            candidate,
            trusted_source=True,
        )
        candidate_wallets.add(result.wallet)
    feature_count = 0
    for feature in features:
        if feature.address not in candidate_wallets:
            continue
        upsert_wallet_feature(conn, feature)
        feature_count += 1
    conn.commit()
    return {"candidates": len(candidate_wallets), "features": feature_count}
