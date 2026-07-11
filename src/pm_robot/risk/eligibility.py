"""Shared wallet eligibility gates for research outputs.

The project intentionally keeps a broad discovery/review funnel, but paper,
publish, and "winner library" outputs must fail closed unless the wallet has
enough evidence.  This module centralizes those output gates so provisional
review states cannot bypass paper/publish through one-off flags.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from pm_robot.orchestration.evidence_readiness import paper_evidence_ready
from pm_robot.pipeline_terms import (
    PAPER_ELIGIBLE_CANDIDATE_STAGES,
    PROVISIONAL_CANDIDATE_STAGES,
    PUBLISHABLE_CANDIDATE_STAGE,
)
from pm_robot.risk.gates import stable_readiness_status


PAPER_ELIGIBLE_STAGES = PAPER_ELIGIBLE_CANDIDATE_STAGES
PUBLISHABLE_STAGE = PUBLISHABLE_CANDIDATE_STAGE
PROVISIONAL_STAGES = PROVISIONAL_CANDIDATE_STAGES
FATAL_PAPER_BLOCKERS = ("non_positive_settled_roi", "non_positive_total_roi")
MIN_PAPER_LEADER_SCORE = 45.0
MIN_PAPER_SOURCE_COUNT = 1
MIN_PAPER_TRADE_EVENTS = 100
MIN_PAPER_COPY_EVENTS = 1
MIN_PUBLISH_COPY_EVENTS = 5
MIN_PUBLISH_EDGE_RETENTION_PCT = 60.0
MIN_PUBLISH_WALK_FORWARD_CONSISTENCY_PCT = 55.0
ELIGIBLE_HYGIENE_STATUSES = ("clean", "screened")


@dataclass(frozen=True)
class EligibilityResult:
    """A reusable gate result with machine-readable blockers."""

    eligible: bool
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible": self.eligible,
            "reasons": list(self.reasons),
        }


def paper_eligibility_status(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    facts: dict[str, Any] | sqlite3.Row | None = None,
) -> EligibilityResult:
    """Return whether a wallet may emit paper orders.

    `needs_manual_review` is intentionally never paper-eligible: review and
    watchlist flags may prioritize evidence, but they do not grant paper access.
    """

    row = _wallet_eligibility_facts(conn, wallet, facts=facts)
    reasons: list[str] = []
    stage = _text(row.get("candidate_stage")).lower()
    if stage in PROVISIONAL_STAGES:
        reasons.append("provisional_review_stage")
    if stage not in PAPER_ELIGIBLE_STAGES:
        reasons.append(f"stage_not_paper_eligible:{stage or 'missing'}")
    if not paper_evidence_ready(row):
        reasons.append("paper_evidence_tier_incomplete")
    if _float(row.get("leader_score")) < MIN_PAPER_LEADER_SCORE:
        reasons.append("below_min_paper_leader_score")
    if _int(row.get("source_count")) < MIN_PAPER_SOURCE_COUNT:
        reasons.append("insufficient_source_count")
    if _int(row.get("trade_events")) < MIN_PAPER_TRADE_EVENTS:
        reasons.append("insufficient_trade_events")
    if _float(row.get("copy_event_count")) < MIN_PAPER_COPY_EVENTS:
        reasons.append("missing_copyability_evidence")
    hygiene_status = _text(row.get("hygiene_status")).lower()
    if hygiene_status not in ELIGIBLE_HYGIENE_STATUSES:
        reasons.append(f"hygiene_status:{hygiene_status or 'missing'}")
    paper_blockers = set(_json_list(row.get("paper_blockers_json")))
    for blocker in FATAL_PAPER_BLOCKERS:
        if blocker in paper_blockers:
            reasons.append(f"paper_blocker:{blocker}")
    return _deduped_result(reasons)


def publish_eligibility_status(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    facts: dict[str, Any] | sqlite3.Row | None = None,
) -> EligibilityResult:
    """Return whether a wallet may be active in published research leaders."""

    row = _wallet_eligibility_facts(conn, wallet, facts=facts)
    reasons = list(paper_eligibility_status(conn, wallet, facts=row).reasons)
    stage = _text(row.get("candidate_stage")).lower()
    if stage != PUBLISHABLE_STAGE:
        reasons.append(f"stage_not_publishable:{stage or 'missing'}")
    if _int(row.get("production_ready")) != 1:
        reasons.append("paper_quality_not_production_ready")
    readiness = stable_readiness_status(conn, wallet)
    if _int(readiness.get("stable_production_ready")) != 1:
        reasons.append("stable_readiness_not_production_ready")
    if row.get("maker_fraction") is None:
        reasons.append("maker_fraction_missing")
    if _text(_json_dict(row.get("feature_extra_json")).get("maker_fraction_source")) == (
        "public_activity_no_maker_flags_observed"
    ):
        reasons.append("maker_fraction_source_unverified")
    if _float(row.get("edge_retention_pct")) < MIN_PUBLISH_EDGE_RETENTION_PCT:
        reasons.append("insufficient_edge_retention")
    if _float(row.get("walk_forward_consistency_pct")) < MIN_PUBLISH_WALK_FORWARD_CONSISTENCY_PCT:
        reasons.append("insufficient_walk_forward_consistency")
    if _float(row.get("copy_event_count")) < MIN_PUBLISH_COPY_EVENTS:
        reasons.append("copy_event_sample_too_small")
    for blocker in _json_list(row.get("paper_blockers_json")):
        reasons.append(f"paper_blocker:{blocker}")
    return _deduped_result(reasons)


def winner_library_eligibility_status(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    facts: dict[str, Any] | sqlite3.Row | None = None,
) -> EligibilityResult:
    """Return whether a wallet belongs in the filtered copyable-winner library."""

    return publish_eligibility_status(conn, wallet, facts=facts)


def _wallet_eligibility_facts(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    facts: dict[str, Any] | sqlite3.Row | None = None,
) -> dict[str, Any]:
    base = dict(facts or {})
    normalized_wallet = _text(base.get("wallet") or base.get("address") or wallet).lower()
    row = conn.execute(
        """
        SELECT
            cw.address AS wallet,
            cw.candidate_stage,
            COALESCE(ls.leader_score, 0) AS leader_score,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wf.copy_event_count, 0) AS copy_event_count,
            COALESCE(wf.hygiene_status, '') AS hygiene_status,
            wf.maker_fraction,
            COALESCE(wf.edge_retention_pct, 0) AS edge_retention_pct,
            COALESCE(wf.walk_forward_consistency_pct, 0) AS walk_forward_consistency_pct,
            COALESCE(wf.extra_json, '{}') AS feature_extra_json,
            COALESCE(pwq.production_ready, 0) AS production_ready,
            COALESCE(pwq.blockers_json, '[]') AS paper_blockers_json,
            wps.discovery_tier,
            wps.evidence_status,
            wps.current_stage,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count
        FROM candidate_wallets cw
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN paper_wallet_quality pwq
          ON pwq.wallet = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN leader_scores ls
          ON ls.score_id = (
              SELECT score_id
              FROM leader_scores
              WHERE address = cw.address
              ORDER BY scored_at DESC, score_id DESC
              LIMIT 1
          )
        WHERE cw.address = ?
        """,
        (normalized_wallet,),
    ).fetchone()
    if row is not None:
        base.update(dict(row))
    base["wallet"] = normalized_wallet
    base.setdefault("source_count", _source_count(conn, normalized_wallet))
    base.setdefault("trade_events", _trade_events(conn, normalized_wallet))
    return base


def _source_count(conn: sqlite3.Connection, wallet: str) -> int:
    if _table_exists(conn, "candidate_source_events"):
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM candidate_source_events WHERE address = ?",
            (wallet,),
        ).fetchone()
        return _int(row["n"] if row else 0)
    row = conn.execute(
        "SELECT sources FROM candidate_wallets WHERE address = ?",
        (wallet,),
    ).fetchone()
    return 1 if row and _text(row["sources"]) else 0


def _trade_events(conn: sqlite3.Connection, wallet: str) -> int:
    if not _table_exists(conn, "wallet_activity"):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM wallet_activity
        WHERE address = ?
          AND type = 'TRADE'
        """,
        (wallet,),
    ).fetchone()
    return _int(row["n"] if row else 0)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _deduped_result(reasons: list[str]) -> EligibilityResult:
    clean = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return EligibilityResult(eligible=not clean, reasons=clean)


def _json_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item) for item in raw if item]
    try:
        value = json.loads(str(raw or "[]"))
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
