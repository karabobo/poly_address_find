"""Candidate review pipeline."""

from __future__ import annotations

import sqlite3
import time
import json
from pathlib import Path

from pm_robot.config import load_policy
from pm_robot.io import load_candidate_addresses, load_wallet_features, write_rows
from pm_robot.models import CandidateAddress, CandidateStage, ScoreBreakdown
from pm_robot.orchestration.evidence_readiness import paper_evidence_ready, paper_evidence_ready_sql
from pm_robot.research.polydata_features import extract_polydata
from pm_robot.research.scoring import review_row, score_candidate
from pm_robot.storage.repository import (
    apply_paper_quality_blocks,
    apply_copyability_no_signal_blocks,
    get_wallet_features,
    latest_review_rows,
    list_candidates,
    persist_score,
    upsert_candidates,
    upsert_wallet_feature,
)
from pm_robot.storage.db import retry_sqlite_locked


def build_review_queue(
    *,
    addresses_path: Path,
    policy_path: Path,
    out_path: Path,
    features_path: Path | None = None,
) -> dict[str, int]:
    policy = load_policy(policy_path)
    candidates = load_candidate_addresses(addresses_path)
    features_by_address = load_wallet_features(features_path) if features_path else {}
    rows = []
    counts: dict[str, int] = {}
    for candidate in candidates:
        score = score_candidate(candidate, features_by_address.get(candidate.address), policy)
        rows.append(review_row(candidate, score))
        counts[score.stage.value] = counts.get(score.stage.value, 0) + 1
    write_rows(out_path, rows)
    return counts


def import_candidates_from_csv(
    conn: sqlite3.Connection,
    *,
    addresses_path: Path,
    source_event_mode: str = "upsert_source",
) -> int:
    return upsert_candidates(
        conn,
        load_candidate_addresses(addresses_path),
        source_event_mode=source_event_mode,
    )


def import_features_from_csv(conn: sqlite3.Connection, *, features_path: Path) -> int:
    features = load_wallet_features(features_path)
    for feature in features.values():
        upsert_wallet_feature(conn, feature)
    conn.commit()
    return len(features)


def import_polydata_json(conn: sqlite3.Connection, *, polydata_path: Path) -> dict[str, int]:
    candidates, features = extract_polydata(polydata_path)
    candidate_count = upsert_candidates(conn, candidates)
    for feature in features:
        upsert_wallet_feature(conn, feature)
    conn.commit()
    return {"candidates": candidate_count, "features": len(features)}


def score_database(
    conn: sqlite3.Connection,
    *,
    policy_path: Path,
    export_path: Path | None = None,
    incremental: bool = False,
    limit: int = 0,
) -> dict[str, int]:
    policy = load_policy(policy_path)
    policy_version = str(policy.get("version", ""))
    candidates = _list_score_candidates(conn, incremental=incremental, limit=limit, policy_version=policy_version)
    features_by_address = get_wallet_features(conn)
    skipped_incomplete_overwrites = 0
    skipped_unchanged_scores = 0
    written_scores = 0
    for candidate in candidates:
        score = score_candidate(candidate, features_by_address.get(candidate.address), policy)
        score = apply_paper_evidence_guard(conn, score)
        if _should_skip_incomplete_overwrite(conn, score, policy_version=policy_version):
            skipped_incomplete_overwrites += 1
            continue
        if _write_with_retry(
            conn,
            lambda score=score: _should_skip_unchanged_score(conn, score, policy_version=policy_version),
        ):
            skipped_unchanged_scores += 1
            continue
        _write_with_retry(conn, lambda score=score: persist_score(conn, score, policy_version=policy_version))
        written_scores += 1
    restored = _write_with_retry(conn, lambda: restore_masked_valid_scores(conn, policy_version=policy_version))
    blocked = _write_with_retry(conn, lambda: apply_paper_quality_blocks(conn))
    no_signal_blocked = _write_with_retry(conn, lambda: apply_copyability_no_signal_blocks(conn))
    evidence_guarded = _write_with_retry(conn, lambda: repair_paper_stage_evidence_incomplete(conn, policy_version=policy_version))
    synced = _write_with_retry(conn, lambda: sync_candidate_stages_from_latest_scores(conn))
    if export_path:
        write_rows(export_path, latest_review_rows(conn))
    counts = _candidate_stage_counts(conn)
    counts["score_candidates_considered"] = len(candidates)
    counts["scores_written"] = written_scores
    if skipped_incomplete_overwrites:
        counts["incomplete_rescore_skipped"] = skipped_incomplete_overwrites
    if skipped_unchanged_scores:
        counts["unchanged_score_skipped"] = skipped_unchanged_scores
    if restored:
        counts["masked_valid_scores_restored"] = restored
    if blocked:
        counts["paper_quality_blocked_this_run"] = blocked
    if no_signal_blocked:
        counts["copyability_no_signal_blocked_this_run"] = no_signal_blocked
    if evidence_guarded:
        counts["paper_evidence_incomplete_downgraded"] = evidence_guarded
    if synced:
        counts["candidate_stage_synced_from_latest_score"] = synced
    return counts


def apply_paper_evidence_guard(conn: sqlite3.Connection, score: ScoreBreakdown) -> ScoreBreakdown:
    """Keep a scored wallet out of paper stages until shared L3 evidence is ready."""

    if score.stage.value not in PAPER_REQUIRES_L3_STAGES:
        return score
    if _paper_evidence_ready(conn, score.address):
        return score
    return ScoreBreakdown(
        address=score.address,
        leader_score=score.leader_score,
        stage=CandidateStage.NEEDS_REVIEW,
        reason="paper_evidence_tier_incomplete",
        components=score.components,
        penalties=score.penalties,
    )


def _paper_evidence_ready(conn: sqlite3.Connection, address: str) -> bool:
    row = conn.execute(
        """
        SELECT
            discovery_tier,
            evidence_status,
            current_stage,
            activity_count,
            distinct_markets,
            non_fast_trade_count
        FROM wallet_processing_state
        WHERE wallet = ?
        """,
        (address,),
    ).fetchone()
    return paper_evidence_ready(row)


def repair_paper_stage_evidence_incomplete(
    conn: sqlite3.Connection,
    *,
    policy_version: str = "",
    now: int | None = None,
) -> int:
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT
                ls.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ls.address
                    ORDER BY ls.scored_at DESC, ls.score_id DESC
                ) AS rn
            FROM leader_scores ls
        )
        SELECT
            cw.address,
            cw.candidate_stage AS current_stage,
            latest.leader_score,
            latest.review_stage,
            latest.components_json,
            latest.penalties_json,
            latest.scored_at,
            COALESCE(latest.policy_version, '') AS policy_version
        FROM candidate_wallets cw
        JOIN latest
          ON latest.address = cw.address
         AND latest.rn = 1
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
          AND NOT {paper_ready_sql}
        """
        .format(paper_ready_sql=paper_evidence_ready_sql("wps"))
    ).fetchall()
    repaired_at_base = now or int(time.time())
    repaired = 0
    for row in rows:
        # The guard row must sort after the invalid paper-stage score it repairs.
        repaired_at = max(repaired_at_base, int(row["scored_at"] or 0) + 1)
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["address"],
                row["leader_score"],
                CandidateStage.NEEDS_REVIEW.value,
                "paper_evidence_tier_incomplete",
                row["components_json"],
                row["penalties_json"],
                policy_version or row["policy_version"],
                repaired_at,
            ),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            (CandidateStage.NEEDS_REVIEW.value, repaired_at, row["address"]),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["address"],
                row["current_stage"],
                CandidateStage.NEEDS_REVIEW.value,
                "paper_evidence_tier_incomplete",
                repaired_at,
            ),
        )
        repaired += 1
    return repaired


def _write_with_retry(conn: sqlite3.Connection, operation):
    """Run a short scoring write section and commit it independently."""

    def _operation():
        result = operation()
        conn.commit()
        return result

    return retry_sqlite_locked(_operation, rollback=conn.rollback, attempts=4, sleep_seconds=2.0)


def _list_score_candidates(
    conn: sqlite3.Connection,
    *,
    incremental: bool,
    limit: int,
    policy_version: str,
) -> list[CandidateAddress]:
    if not incremental:
        candidates = list_candidates(conn)
        return candidates[:limit] if limit > 0 else candidates

    rows = conn.execute(
        """
        WITH latest_score AS (
            SELECT address, MAX(score_id) AS score_id
            FROM leader_scores
            GROUP BY address
        ),
        latest AS (
            SELECT ls.address, ls.scored_at, COALESCE(ls.policy_version, '') AS policy_version
            FROM leader_scores ls
            JOIN latest_score latest_score
              ON latest_score.score_id = ls.score_id
        )
        SELECT cw.address, cw.sources, cw.labels, cw.notes, cw.links, cw.status
        FROM candidate_wallets cw
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN latest
          ON latest.address = cw.address
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND (
              latest.address IS NULL
              OR latest.policy_version != ?
              OR COALESCE(wf.updated_at, 0) > latest.scored_at
              OR COALESCE(wps.updated_at, 0) > latest.scored_at
              OR COALESCE(cw.updated_at, 0) > latest.scored_at
          )
          AND (
              wps.next_action = 'score_wallet'
              OR wps.evidence_status = 'summary_ready'
              OR wps.current_stage IN ('medium_done', 'deep_done')
              OR cw.candidate_stage IN ('needs_manual_review', 'paper_candidate', 'paper_approved', 'live_eligible')
              OR (
                  wf.address IS NOT NULL
                  AND (
                      wf.maker_fraction IS NOT NULL
                      OR wf.leader_in_degree IS NOT NULL
                      OR wf.copy_event_count IS NOT NULL
                      OR wf.single_market_pnl_share IS NOT NULL
                      OR wf.net_to_gross_exposure IS NOT NULL
                  )
              )
          )
        ORDER BY
            CASE
                WHEN wps.next_action = 'score_wallet' AND wps.current_stage = 'deep_done' THEN 0
                WHEN wps.next_action = 'score_wallet' AND wps.current_stage = 'medium_done' THEN 1
                WHEN wps.next_action = 'score_wallet' THEN 2
                WHEN wps.evidence_status = 'summary_ready' THEN 3
                WHEN cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible') THEN 4
                WHEN cw.candidate_stage = 'needs_manual_review' THEN 5
                ELSE 6
            END ASC,
            COALESCE(wps.priority, 100) ASC,
            COALESCE(wps.activity_count, 0) DESC,
            COALESCE(wf.updated_at, cw.updated_at, 0) DESC,
            cw.address ASC
        LIMIT CASE WHEN ? > 0 THEN ? ELSE 9223372036854775807 END
        """,
        (policy_version, limit, limit),
    ).fetchall()
    return [
        CandidateAddress(
            address=row["address"],
            sources=row["sources"],
            labels=row["labels"],
            notes=row["notes"],
            links=row["links"],
            status=row["status"],
        )
        for row in rows
    ]


INCOMPLETE_NEEDS_DATA_REASONS = (
    "no_wallet_metrics_attached",
    "hygiene_evidence_incomplete",
    "missing_required_score_components:",
)
RESTORABLE_STAGES = {
    CandidateStage.NEEDS_REVIEW.value,
    CandidateStage.PAPER_CANDIDATE.value,
    CandidateStage.PAPER_APPROVED.value,
    CandidateStage.LIVE_ELIGIBLE.value,
}
NON_RESTORABLE_CURRENT_STAGES = {
    CandidateStage.REJECTED.value,
    CandidateStage.BLOCKED_HYGIENE.value,
    CandidateStage.BLOCKED_COPYABILITY.value,
}
BLOCKING_STAGES = {
    CandidateStage.REJECTED.value,
    CandidateStage.BLOCKED_HYGIENE.value,
    CandidateStage.BLOCKED_COPYABILITY.value,
}
PAPER_READY_STAGES = {
    CandidateStage.PAPER_CANDIDATE.value,
    CandidateStage.PAPER_APPROVED.value,
    CandidateStage.LIVE_ELIGIBLE.value,
}
PAPER_REQUIRES_L3_STAGES = {
    CandidateStage.PAPER_CANDIDATE.value,
    CandidateStage.PAPER_APPROVED.value,
    CandidateStage.LIVE_ELIGIBLE.value,
}


def _is_incomplete_needs_data(score: ScoreBreakdown) -> bool:
    return score.stage == CandidateStage.NEEDS_DATA and score.leader_score == 0 and _is_incomplete_reason(score.reason)


def _is_incomplete_reason(reason: str) -> bool:
    return any(reason == marker or reason.startswith(marker) for marker in INCOMPLETE_NEEDS_DATA_REASONS)


def _should_skip_incomplete_overwrite(
    conn: sqlite3.Connection,
    score: ScoreBreakdown,
    *,
    policy_version: str,
) -> bool:
    if not _is_incomplete_needs_data(score):
        return False
    current = conn.execute(
        "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
        (score.address,),
    ).fetchone()
    if current and current["candidate_stage"] in NON_RESTORABLE_CURRENT_STAGES:
        return True
    return _latest_restorable_score(conn, score.address, policy_version=policy_version) is not None


def _should_skip_unchanged_score(
    conn: sqlite3.Connection,
    score: ScoreBreakdown,
    *,
    policy_version: str,
) -> bool:
    latest = conn.execute(
        """
        SELECT *
        FROM leader_scores
        WHERE address = ?
        ORDER BY score_id DESC
        LIMIT 1
        """,
        (score.address,),
    ).fetchone()
    if latest is None:
        return False
    if float(latest["leader_score"]) != float(score.leader_score):
        return False
    if latest["review_stage"] != score.stage.value:
        return False
    if latest["review_reason"] != score.reason:
        return False
    if latest["policy_version"] != policy_version:
        return False
    if _json_obj(latest["components_json"]) != score.components:
        return False
    if _json_obj(latest["penalties_json"]) != score.penalties:
        return False

    current = conn.execute(
        "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
        (score.address,),
    ).fetchone()
    current_stage = current["candidate_stage"] if current else None
    target_stage = _score_target_stage(
        current_stage,
        score.stage.value,
        paper_evidence_ready=_paper_evidence_ready(conn, score.address),
    )
    if current_stage != target_stage:
        now = int(time.time())
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            (target_stage, now, score.address),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (score.address, current_stage, target_stage, "sync_stage_from_unchanged_score", now),
        )
    source_updated_at = _latest_score_source_updated_at(conn, score.address)
    if source_updated_at > int(latest["scored_at"] or 0):
        conn.execute(
            "UPDATE leader_scores SET scored_at = ? WHERE score_id = ?",
            (int(time.time()), latest["score_id"]),
        )
    return True


def _latest_score_source_updated_at(conn: sqlite3.Connection, address: str) -> int:
    row = conn.execute(
        """
        SELECT
            MAX(
                COALESCE(cw.updated_at, 0),
                COALESCE(wf.updated_at, 0),
                COALESCE(wps.updated_at, 0)
            ) AS updated_at
        FROM candidate_wallets cw
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        WHERE cw.address = ?
        """,
        (address,),
    ).fetchone()
    return int(row["updated_at"] or 0) if row else 0


def _json_obj(value: str) -> dict[str, float]:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _score_target_stage(
    current_stage: str | None,
    scored_stage: str,
    *,
    paper_evidence_ready: bool = True,
) -> str:
    if current_stage == CandidateStage.REJECTED.value:
        return current_stage
    if current_stage in {
        CandidateStage.BLOCKED_HYGIENE.value,
        CandidateStage.BLOCKED_COPYABILITY.value,
    } and scored_stage not in BLOCKING_STAGES:
        return current_stage
    if current_stage == CandidateStage.LIVE_ELIGIBLE.value and scored_stage in PAPER_READY_STAGES:
        return current_stage
    if scored_stage in PAPER_REQUIRES_L3_STAGES and not paper_evidence_ready:
        return CandidateStage.NEEDS_REVIEW.value
    return scored_stage


def sync_candidate_stages_from_latest_scores(conn: sqlite3.Connection, *, now: int | None = None) -> int:
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT
                ls.address,
                ls.review_stage,
                ROW_NUMBER() OVER (
                    PARTITION BY ls.address
                    ORDER BY ls.scored_at DESC, ls.score_id DESC
                ) AS rn
            FROM leader_scores ls
        )
        SELECT
            cw.address,
            cw.candidate_stage AS current_stage,
            latest.review_stage AS scored_stage
        FROM candidate_wallets cw
        JOIN latest
          ON latest.address = cw.address
         AND latest.rn = 1
        WHERE cw.candidate_stage != latest.review_stage
        """
    ).fetchall()
    synced_at = now or int(time.time())
    synced = 0
    for row in rows:
        current_stage = row["current_stage"]
        target_stage = _score_target_stage(
            current_stage,
            row["scored_stage"],
            paper_evidence_ready=_paper_evidence_ready(conn, row["address"]),
        )
        if current_stage == target_stage:
            continue
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            (target_stage, synced_at, row["address"]),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["address"],
                current_stage,
                target_stage,
                "sync_stage_from_latest_score",
                synced_at,
            ),
        )
        synced += 1
    return synced


def restore_masked_valid_scores(conn: sqlite3.Connection, *, policy_version: str = "") -> int:
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT
                ls.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ls.address
                    ORDER BY ls.scored_at DESC, ls.score_id DESC
                ) AS rn
            FROM leader_scores ls
        )
        SELECT
            latest.address,
            latest.review_reason AS masked_reason,
            cw.candidate_stage AS current_stage
        FROM latest
        JOIN candidate_wallets cw
          ON cw.address = latest.address
        WHERE latest.rn = 1
          AND latest.leader_score = 0
          AND latest.review_stage = 'needs_data'
          AND (
              latest.review_reason = 'no_wallet_metrics_attached'
              OR latest.review_reason LIKE 'missing_required_score_components:%'
          )
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
        """
    ).fetchall()
    restored = 0
    for row in rows:
        prior = _latest_restorable_score(conn, row["address"], policy_version=policy_version)
        if prior is None:
            continue
        _insert_restored_score(conn, prior, current_stage=row["current_stage"], policy_version=policy_version)
        restored += 1
    return restored


def _latest_restorable_score(
    conn: sqlite3.Connection,
    address: str,
    *,
    policy_version: str = "",
) -> sqlite3.Row | None:
    policy_clause = "AND policy_version = ?" if policy_version else ""
    params: tuple[str, ...] = (address, policy_version) if policy_version else (address,)
    return conn.execute(
        """
        SELECT *
        FROM leader_scores
        WHERE address = ?
          AND leader_score > 0
          AND review_stage IN ('needs_manual_review', 'paper_candidate', 'paper_approved', 'live_eligible')
          AND review_reason != 'no_wallet_metrics_attached'
          AND review_reason NOT LIKE 'missing_required_score_components:%'
          {policy_clause}
        ORDER BY scored_at DESC, score_id DESC
        LIMIT 1
        """.format(policy_clause=policy_clause),
        params,
    ).fetchone()


def _insert_restored_score(
    conn: sqlite3.Connection,
    prior: sqlite3.Row,
    *,
    current_stage: str | None,
    policy_version: str,
) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO leader_scores(
            address, leader_score, review_stage, review_reason,
            components_json, penalties_json, policy_version, scored_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            prior["address"],
            prior["leader_score"],
            prior["review_stage"],
            prior["review_reason"],
            prior["components_json"],
            prior["penalties_json"],
            f"{policy_version}+restored_after_incomplete_rescore" if policy_version else "restored_after_incomplete_rescore",
            now,
        ),
    )
    if current_stage != prior["review_stage"]:
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            (prior["review_stage"], now, prior["address"]),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                prior["address"],
                current_stage,
                prior["review_stage"],
                "restored_valid_score_after_incomplete_rescore",
                now,
            ),
        )


def _candidate_stage_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT candidate_stage, COUNT(*) AS n
        FROM candidate_wallets
        GROUP BY candidate_stage
        ORDER BY candidate_stage ASC
        """
    ).fetchall()
    return {row["candidate_stage"]: int(row["n"]) for row in rows}
