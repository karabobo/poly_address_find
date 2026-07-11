"""Repository functions for candidates, features, scores, and orders."""

from __future__ import annotations

import json
import hashlib
import sqlite3
import time
from dataclasses import asdict
from typing import Any

from pm_robot.models import (
    CandidateAddress,
    ExecutionDecision,
    ScoreBreakdown,
    TradeSignal,
    WalletFeatures,
)
from pm_robot.orchestration.evidence_readiness import paper_evidence_ready_sql
from pm_robot.pipeline_terms import (
    EvidenceJobStage,
    EvidenceStatus,
    EvidenceTier,
    PAPER_ELIGIBLE_CANDIDATE_STAGES,
    evidence_promotion_approval_snapshot,
    evidence_promotion_is_approved,
)
from pm_robot.storage.db import retry_sqlite_locked


SOURCE_EVENT_APPEND = "append"
SOURCE_EVENT_UPSERT_SOURCE = "upsert_source"


def upsert_candidate(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    source_event_mode: str = SOURCE_EVENT_APPEND,
) -> None:
    now = int(time.time())
    existing = conn.execute(
        "SELECT sources, labels, notes, links FROM candidate_wallets WHERE address = ?",
        (candidate.address.lower(),),
    ).fetchone()
    sources = candidate.sources
    labels = candidate.labels
    notes = candidate.notes
    links = candidate.links
    if existing:
        sources = _merge_text(existing["sources"], candidate.sources)
        labels = _merge_text(existing["labels"], candidate.labels)
        notes = _merge_text(existing["notes"], candidate.notes)
        links = _merge_text(existing["links"], candidate.links)
    conn.execute(
        """
        INSERT INTO candidate_wallets(
            address, sources, labels, notes, links, status,
            candidate_stage, first_seen_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'needs_data', ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            sources = excluded.sources,
            labels = excluded.labels,
            notes = excluded.notes,
            links = excluded.links,
            status = excluded.status,
            updated_at = excluded.updated_at
        """,
        (
            candidate.address.lower(),
            sources,
            labels,
            notes,
            links,
            candidate.status,
            now,
            now,
        ),
    )
    record_candidate_source_event(
        conn,
        candidate,
        observed_at=now,
        recorded_at=now,
        mode=source_event_mode,
    )


def _merge_text(existing: str, incoming: str, *, sep: str = " | ", max_len: int = 4000) -> str:
    values: list[str] = []
    seen: set[str] = set()
    for raw in (existing or "", incoming or ""):
        for part in raw.split("|"):
            item = part.strip()
            if not item or item in seen:
                continue
            seen.add(item)
            values.append(item)
    text = sep.join(values)
    return text[:max_len]


def record_candidate_source_event(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    observed_at: int,
    recorded_at: int | None = None,
    evidence: dict[str, Any] | None = None,
    mode: str = SOURCE_EVENT_APPEND,
) -> None:
    """Append an immutable source observation for candidate provenance."""
    if mode == SOURCE_EVENT_UPSERT_SOURCE:
        _upsert_candidate_source_event_by_source(
            conn,
            candidate,
            observed_at=observed_at,
            recorded_at=recorded_at,
            evidence=evidence,
        )
        return
    if mode != SOURCE_EVENT_APPEND:
        raise ValueError(f"unknown source event mode: {mode}")
    conn.execute(
        """
        INSERT OR IGNORE INTO candidate_source_events(
            address, source, status, labels, notes, links,
            evidence_json, observed_at, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            candidate.address.lower(),
            candidate.sources,
            candidate.status,
            candidate.labels,
            candidate.notes,
            candidate.links,
            json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
            observed_at,
            recorded_at or int(time.time()),
        ),
    )


def _upsert_candidate_source_event_by_source(
    conn: sqlite3.Connection,
    candidate: CandidateAddress,
    *,
    observed_at: int,
    recorded_at: int | None = None,
    evidence: dict[str, Any] | None = None,
) -> None:
    """Keep one curated source record per wallet/source while refreshing details."""
    address = candidate.address.lower()
    source = candidate.sources
    existing = conn.execute(
        """
        SELECT event_id, observed_at
        FROM candidate_source_events
        WHERE address = ? AND source = ?
        ORDER BY event_id ASC
        LIMIT 1
        """,
        (address, source),
    ).fetchone()
    evidence_json = json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True)
    ts = recorded_at or int(time.time())
    if existing:
        conn.execute(
            """
            UPDATE candidate_source_events
            SET status = ?,
                labels = ?,
                notes = ?,
                links = ?,
                evidence_json = ?,
                observed_at = MIN(observed_at, ?),
                recorded_at = ?
            WHERE event_id = ?
            """,
            (
                candidate.status,
                candidate.labels,
                candidate.notes,
                candidate.links,
                evidence_json,
                observed_at,
                ts,
                existing["event_id"],
            ),
        )
        conn.execute(
            """
            DELETE FROM candidate_source_events
            WHERE address = ?
              AND source = ?
              AND event_id != ?
            """,
            (address, source, existing["event_id"]),
        )
        return
    conn.execute(
        """
        INSERT INTO candidate_source_events(
            address, source, status, labels, notes, links,
            evidence_json, observed_at, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            address,
            source,
            candidate.status,
            candidate.labels,
            candidate.notes,
            candidate.links,
            evidence_json,
            observed_at,
            ts,
        ),
    )


def upsert_candidates(
    conn: sqlite3.Connection,
    candidates: list[CandidateAddress],
    *,
    source_event_mode: str = SOURCE_EVENT_APPEND,
) -> int:
    for candidate in candidates:
        upsert_candidate(conn, candidate, source_event_mode=source_event_mode)
    conn.commit()
    return len(candidates)


def list_candidates(conn: sqlite3.Connection) -> list[CandidateAddress]:
    rows = conn.execute(
        "SELECT address, sources, labels, notes, links, status FROM candidate_wallets "
        "ORDER BY first_seen_at ASC, address ASC"
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


def summary_only_wallets(conn: sqlite3.Connection, wallets: Any) -> set[str]:
    """Return wallets whose raw evidence lifecycle is intentionally closed."""

    normalized = sorted({str(wallet).lower() for wallet in wallets if wallet})
    archived: set[str] = set()
    for offset in range(0, len(normalized), 500):
        batch = normalized[offset : offset + 500]
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(
            f"""
            SELECT address
            FROM wallet_registry
            WHERE address IN ({placeholders})
              AND raw_retention_tier = 'summary_only'
            """,
            batch,
        ).fetchall()
        archived.update(str(row["address"]).lower() for row in rows)
    return archived


def list_ingest_targets(conn: sqlite3.Connection, *, limit: int = 50) -> list[str]:
    rows = conn.execute(
        """
        SELECT cw.address
        FROM candidate_wallets cw
        LEFT JOIN wallet_registry wr ON wr.address = cw.address
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        ORDER BY
            CASE cw.candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END ASC,
            COALESCE(cw.last_ingested_at, 0) ASC,
            cw.updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [row["address"] for row in rows]


def list_paper_activity_targets(conn: sqlite3.Connection, *, limit: int = 50) -> list[str]:
    """Return paper-stage wallets for active observation refresh only."""

    placeholders = ",".join("?" for _ in PAPER_ELIGIBLE_CANDIDATE_STAGES)
    rows = conn.execute(
        f"""
        SELECT address FROM candidate_wallets
        WHERE candidate_stage IN ({placeholders})
        ORDER BY
            CASE candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END ASC,
            COALESCE(last_ingested_at, 0) ASC,
            updated_at DESC
        LIMIT ?
        """,
        (*PAPER_ELIGIBLE_CANDIDATE_STAGES, limit),
    ).fetchall()
    return [row["address"] for row in rows]


def list_activity_backfill_targets(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    target_events_per_wallet: int = 1000,
) -> list[str]:
    rows = conn.execute(
        """
        SELECT cw.address, COUNT(wa.activity_id) AS raw_activity_count
        FROM candidate_wallets cw
        LEFT JOIN wallet_activity wa
          ON wa.address = cw.address
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        GROUP BY cw.address
        HAVING raw_activity_count < ?
        ORDER BY raw_activity_count ASC, COALESCE(cw.last_ingested_at, 0) ASC, cw.updated_at DESC
        LIMIT ?
        """,
        (target_events_per_wallet, limit),
    ).fetchall()
    return [row["address"] for row in rows]


def activity_coverage(conn: sqlite3.Connection, *, limit: int = 25) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            cw.address,
            cw.candidate_stage,
            COUNT(wa.activity_id) AS activity_count,
            MIN(wa.timestamp) AS oldest_activity_ts,
            MAX(wa.timestamp) AS newest_activity_ts
        FROM candidate_wallets cw
        LEFT JOIN wallet_activity wa
          ON wa.address = cw.address
        GROUP BY cw.address
        ORDER BY activity_count ASC, cw.updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def activity_coverage_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    if _wallet_processing_state_ready(conn):
        row = conn.execute(
            """
            WITH counts AS (
                SELECT
                    cw.address,
                    COALESCE(wps.activity_count, ebb.current_depth, 0) AS activity_count
                FROM candidate_wallets cw
                LEFT JOIN wallet_processing_state wps
                  ON wps.wallet = cw.address
                LEFT JOIN evidence_backfill_budget ebb
                  ON ebb.wallet = cw.address
            )
            SELECT
                COUNT(*) AS wallet_count,
                SUM(CASE WHEN activity_count > 0 THEN 1 ELSE 0 END) AS wallets_with_activity,
                SUM(CASE WHEN activity_count >= 200 THEN 1 ELSE 0 END) AS wallets_ge_200,
                SUM(CASE WHEN activity_count >= 1000 THEN 1 ELSE 0 END) AS wallets_ge_1000,
                MIN(activity_count) AS min_events,
                MAX(activity_count) AS max_events,
                AVG(activity_count) AS avg_events,
                SUM(activity_count) AS total_events
            FROM counts
            """
        ).fetchone()
        return dict(row)
    row = conn.execute(
        """
        WITH actual_counts AS (
            SELECT address, COUNT(activity_id) AS activity_count
            FROM wallet_activity
            GROUP BY address
        ),
        counts AS (
            SELECT
                cw.address,
                COALESCE(
                    NULLIF(wps.activity_count, 0),
                    ac.activity_count,
                    ebb.current_depth,
                    0
                ) AS activity_count
            FROM candidate_wallets cw
            LEFT JOIN wallet_processing_state wps
              ON wps.wallet = cw.address
            LEFT JOIN actual_counts ac
              ON ac.address = cw.address
            LEFT JOIN evidence_backfill_budget ebb
              ON ebb.wallet = cw.address
        )
        SELECT
            COUNT(*) AS wallet_count,
            SUM(CASE WHEN activity_count > 0 THEN 1 ELSE 0 END) AS wallets_with_activity,
            SUM(CASE WHEN activity_count >= 200 THEN 1 ELSE 0 END) AS wallets_ge_200,
            SUM(CASE WHEN activity_count >= 1000 THEN 1 ELSE 0 END) AS wallets_ge_1000,
            MIN(activity_count) AS min_events,
            MAX(activity_count) AS max_events,
            AVG(activity_count) AS avg_events,
            SUM(activity_count) AS total_events
        FROM counts
        """
    ).fetchone()
    return dict(row)


def seed_evidence_backfill_budget(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    source: str,
    priority: int = 100,
    target_depth: int = 200,
    evidence: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    ts = now or int(time.time())
    current = _wallet_activity_count(conn, wallet)
    conn.execute(
        """
        INSERT INTO evidence_backfill_budget(
            wallet, source, priority, stage, target_depth, current_depth,
            next_attempt_at, evidence_json, created_at, updated_at
        ) VALUES (?, ?, ?, 'light_pending', ?, ?, 0, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            source = CASE
                WHEN evidence_backfill_budget.source = '' THEN excluded.source
                WHEN instr(evidence_backfill_budget.source, excluded.source) > 0 THEN evidence_backfill_budget.source
                ELSE evidence_backfill_budget.source || ' | ' || excluded.source
            END,
            priority = MIN(evidence_backfill_budget.priority, excluded.priority),
            target_depth = MAX(evidence_backfill_budget.target_depth, excluded.target_depth),
            current_depth = excluded.current_depth,
            evidence_json = excluded.evidence_json,
            updated_at = excluded.updated_at
        """,
        (
            wallet.lower(),
            source,
            priority,
            target_depth,
            current,
            json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
            ts,
            ts,
        ),
    )


def seed_missing_evidence_backfill_budgets(
    conn: sqlite3.Connection,
    *,
    source_like: str = "%polymarket_trades_global%",
    limit: int = 1000,
    now: int | None = None,
) -> int:
    ts = now or int(time.time())
    rows = conn.execute(
        """
        SELECT cw.address, cw.sources, COUNT(wa.activity_id) AS activity_count
        FROM candidate_wallets cw
        LEFT JOIN wallet_activity wa
          ON wa.address = cw.address
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = cw.address
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        WHERE cw.sources LIKE ?
          AND ebb.wallet IS NULL
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        GROUP BY cw.address
        ORDER BY cw.updated_at DESC
        LIMIT ?
        """,
        (source_like, limit),
    ).fetchall()
    for row in rows:
        conn.execute(
            """
            INSERT INTO evidence_backfill_budget(
                wallet, source, priority, stage, target_depth, current_depth,
                next_attempt_at, evidence_json, created_at, updated_at
            ) VALUES (?, 'polymarket_trades_global', 50, 'light_pending', 200, ?, 0, '{}', ?, ?)
            """,
            (row["address"], int(row["activity_count"] or 0), ts, ts),
        )
    conn.commit()
    return len(rows)


def list_evidence_backfill_targets(
    conn: sqlite3.Connection,
    *,
    stage: str,
    limit: int,
    now: int | None = None,
    expected_policy_version: str | None = None,
    expected_materializer_version: str | None = None,
) -> list[dict[str, Any]]:
    ts = now or int(time.time())
    rows = conn.execute(
        """
        SELECT
            ebb.*,
            cw.candidate_stage,
            cw.sources,
            COUNT(wa.activity_id) AS activity_count
        FROM evidence_backfill_budget ebb
        JOIN candidate_wallets cw
          ON cw.address = ebb.wallet
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        LEFT JOIN wallet_activity wa
          ON wa.address = ebb.wallet
        WHERE ebb.stage = ?
          AND ebb.next_attempt_at <= ?
          AND (
                ebb.stage = 'light_pending'
                OR ebb.stop_reason LIKE 'promotion_approved:' || ebb.stage || ':%'
          )
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        GROUP BY ebb.wallet
        ORDER BY ebb.priority ASC, ebb.updated_at ASC, ebb.wallet ASC
        LIMIT ?
        """,
        (stage, ts, max(limit * 16, limit + 100)),
    ).fetchall()
    return [
        dict(row)
        for row in rows
        if stage == EvidenceJobStage.LIGHT_PENDING.value
        or evidence_promotion_approval_is_current(
            conn,
            wallet=str(row["wallet"]),
            job_action=stage,
            expected_policy_version=expected_policy_version,
            expected_materializer_version=expected_materializer_version,
        )
    ][:limit]


def evidence_promotion_approval_is_current(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    job_action: str,
    expected_policy_version: str | None = None,
    expected_materializer_version: str | None = None,
) -> bool:
    """Verify that a depth approval still matches policy, features, and raw activity."""

    if job_action == EvidenceJobStage.LIGHT_PENDING.value:
        return True
    row = conn.execute(
        """
        SELECT
            ebb.stop_reason,
            ebb.evidence_json,
            wf.updated_at AS feature_updated_at,
            wf.extra_json AS feature_extra_json,
            COALESCE(ls.policy_version, '') AS latest_score_policy_version,
            (
                SELECT COUNT(*)
                FROM wallet_activity wa
                WHERE wa.address = ebb.wallet
                  AND wa.type = 'TRADE'
            ) AS raw_activity_count
        FROM evidence_backfill_budget ebb
        LEFT JOIN wallet_features wf
          ON wf.address = ebb.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = ebb.wallet
        WHERE ebb.wallet = ?
        """,
        (wallet.lower(),),
    ).fetchone()
    if row is None or row["feature_updated_at"] is None:
        return False
    snapshot = evidence_promotion_approval_snapshot(
        str(row["stop_reason"] or ""),
        job_action,
    )
    if snapshot is None:
        return False
    policy_version = str(snapshot["policy_version"])
    if expected_policy_version is not None and policy_version != expected_policy_version:
        return False
    latest_score_policy = str(row["latest_score_policy_version"] or "")
    if (
        job_action == EvidenceJobStage.DEEP_PENDING.value
        and policy_version != latest_score_policy
    ):
        return False
    evidence = _json_object(row["evidence_json"])
    promotion = evidence.get("promotion")
    if not isinstance(promotion, dict) or not bool(promotion.get("approved")):
        return False
    feature_extra = _json_object(row["feature_extra_json"])
    promotion_materializer = str(promotion.get("materializer_version") or "")
    feature_materializer = str(
        feature_extra.get("feature_materializer_version") or ""
    )
    return (
        str(promotion.get("job_action") or "") == job_action
        and str(promotion.get("policy_version") or "") == policy_version
        and int(promotion.get("feature_updated_at") or 0)
        == int(row["feature_updated_at"] or 0)
        == int(snapshot["feature_updated_at"])
        and int(
            promotion["activity_count"]
            if promotion.get("activity_count") is not None
            else -1
        )
        == int(row["raw_activity_count"] or 0)
        == int(snapshot["activity_count"])
        and promotion_materializer == feature_materializer
        and (
            expected_materializer_version is None
            or promotion_materializer == expected_materializer_version
        )
    )


def update_evidence_backfill_budget(
    conn: sqlite3.Connection,
    wallet: str,
    *,
    stage: str,
    target_depth: int,
    current_depth: int,
    stop_reason: str = "",
    evidence: dict[str, Any] | None = None,
    next_attempt_at: int = 0,
    error: str = "",
    now: int | None = None,
) -> None:
    ts = now or int(time.time())
    conn.execute(
        """
        UPDATE evidence_backfill_budget
        SET stage = ?,
            target_depth = ?,
            current_depth = ?,
            last_attempt_at = ?,
            next_attempt_at = ?,
            error_count = CASE WHEN ? != '' THEN error_count + 1 ELSE error_count END,
            stop_reason = ?,
            evidence_json = ?,
            updated_at = ?
        WHERE wallet = ?
        """,
        (
            stage,
            target_depth,
            current_depth,
            ts,
            next_attempt_at,
            error,
            (error or stop_reason)[:240],
            json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True),
            ts,
            wallet.lower(),
        ),
    )


def evidence_backfill_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT stage, COUNT(*) AS count
        FROM evidence_backfill_budget
        GROUP BY stage
        ORDER BY count DESC, stage ASC
        """
    ).fetchall()
    source_rows = conn.execute(
        """
        SELECT source, COUNT(*) AS count
        FROM evidence_backfill_budget
        GROUP BY source
        ORDER BY count DESC, source ASC
        LIMIT 10
        """
    ).fetchall()
    return {
        "stages": [dict(row) for row in rows],
        "sources": [dict(row) for row in source_rows],
    }


def enqueue_evidence_backfill_job(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    stage: str,
    target_depth: int,
    priority: int,
    shard: int,
    now: int | None = None,
) -> bool:
    ts = now or int(time.time())
    wallet = wallet.lower()
    before = conn.total_changes
    conn.execute(
        """
        INSERT INTO evidence_backfill_jobs(
            wallet, stage, target_depth, priority, shard, status,
            lease_owner, lease_until, attempts, next_attempt_at,
            last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'queued', NULL, 0, 0, 0, '', ?, ?)
        ON CONFLICT(wallet, stage, target_depth) DO UPDATE SET
            priority = MIN(evidence_backfill_jobs.priority, excluded.priority),
            shard = excluded.shard,
            status = 'queued',
            next_attempt_at = MIN(evidence_backfill_jobs.next_attempt_at, excluded.next_attempt_at),
            updated_at = excluded.updated_at
        WHERE evidence_backfill_jobs.status NOT IN ('done', 'running')
        """,
        (wallet, stage, target_depth, priority, shard, ts, ts),
    )
    return conn.total_changes > before


def claim_evidence_backfill_job(
    conn: sqlite3.Connection,
    *,
    shard_index: int,
    worker_id: str,
    lease_seconds: int,
    now: int | None = None,
) -> dict[str, Any] | None:
    ts = now or int(time.time())
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        """
        SELECT
            job.*,
            ebb.current_depth AS budget_current_depth,
            ebb.error_count AS budget_error_count,
            cw.candidate_stage
        FROM evidence_backfill_jobs job
        JOIN evidence_backfill_budget ebb
          ON ebb.wallet = job.wallet
         AND ebb.stage = job.stage
         AND ebb.target_depth = job.target_depth
        JOIN candidate_wallets cw
          ON cw.address = job.wallet
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        WHERE job.shard = ?
          AND job.next_attempt_at <= ?
          AND (
                job.status = 'queued'
                OR (job.status = 'running' AND job.lease_until <= ?)
          )
          AND ebb.next_attempt_at <= ?
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
        ORDER BY job.priority ASC, job.updated_at ASC, job.job_id ASC
        LIMIT 1
        """,
        (shard_index, ts, ts, ts),
    ).fetchone()
    if row is None:
        conn.commit()
        return None
    conn.execute(
        """
        UPDATE evidence_backfill_jobs
        SET status = 'running',
            lease_owner = ?,
            lease_until = ?,
            attempts = attempts + 1,
            last_attempt_at = ?,
            updated_at = ?
        WHERE job_id = ?
        """,
        (worker_id, ts + lease_seconds, ts, ts, row["job_id"]),
    )
    conn.commit()
    out = dict(row)
    out["attempts"] = int(out.get("attempts") or 0) + 1
    out["lease_owner"] = worker_id
    out["lease_until"] = ts + lease_seconds
    return out


def complete_evidence_backfill_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    now: int | None = None,
) -> None:
    ts = now or int(time.time())
    conn.execute(
        """
        UPDATE evidence_backfill_jobs
        SET status = 'done',
            lease_owner = NULL,
            lease_until = 0,
            last_error = '',
            completed_at = ?,
            updated_at = ?
        WHERE job_id = ?
        """,
        (ts, ts, job_id),
    )


def supersede_evidence_backfill_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    reason: str,
    now: int | None = None,
) -> None:
    """Close a claimed legacy job that no longer has valid depth approval."""

    ts = now or int(time.time())
    conn.execute(
        """
        UPDATE evidence_backfill_jobs
        SET status = 'superseded',
            lease_owner = NULL,
            lease_until = 0,
            last_error = ?,
            completed_at = ?,
            updated_at = ?
        WHERE job_id = ?
          AND status = 'running'
        """,
        (reason[:1000], ts, ts, job_id),
    )


def retry_evidence_backfill_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    error: str,
    next_attempt_at: int,
    failed: bool = False,
    count_attempt: bool = True,
    now: int | None = None,
) -> None:
    ts = now or int(time.time())
    conn.execute(
        """
        UPDATE evidence_backfill_jobs
        SET status = ?,
            lease_owner = NULL,
            lease_until = 0,
            next_attempt_at = ?,
            last_error = ?,
            attempts = CASE WHEN ? THEN attempts ELSE MAX(0, attempts - 1) END,
            updated_at = ?
        WHERE job_id = ?
        """,
        (
            "failed" if failed else "queued",
            next_attempt_at,
            error[:1000],
            1 if count_attempt else 0,
            ts,
            job_id,
        ),
    )


def evidence_backfill_job_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    status_rows = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM evidence_backfill_jobs
        GROUP BY status
        ORDER BY count DESC, status ASC
        """
    ).fetchall()
    shard_rows = conn.execute(
        """
        SELECT shard, status, COUNT(*) AS count
        FROM evidence_backfill_jobs
        WHERE status IN ('queued', 'running')
        GROUP BY shard, status
        ORDER BY shard ASC, status ASC
        """
    ).fetchall()
    stale_rows = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM evidence_backfill_jobs job
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = job.wallet
         AND ebb.stage = job.stage
         AND ebb.target_depth = job.target_depth
        WHERE job.status IN ('queued', 'running')
          AND ebb.wallet IS NULL
        """
    ).fetchone()
    return {
        "statuses": [dict(row) for row in status_rows],
        "active_by_shard": [dict(row) for row in shard_rows],
        "stale_active_jobs": int(stale_rows["count"] if stale_rows else 0),
    }


def wallet_pipeline_tier(
    activity_count: int,
    distinct_markets: int,
    non_fast_trade_count: int,
    fast_market_share: float,
) -> str:
    """Return evidence depth; this is not candidate_stage or queue state."""
    if activity_count <= 0:
        return EvidenceTier.L0_DISCOVERED.value
    if activity_count >= 1_000 and distinct_markets >= 10 and non_fast_trade_count >= 50:
        return EvidenceTier.L3_DEEP.value
    if activity_count >= 200 and (
        distinct_markets >= 3 or non_fast_trade_count >= 10
    ) and fast_market_share < 0.85:
        return EvidenceTier.L2_MEDIUM.value
    return EvidenceTier.L1_LIGHT.value


def upsert_data_artifact(
    conn: sqlite3.Connection,
    *,
    artifact_type: str,
    uri: str,
    storage_backend: str = "sqlite",
    partition_key: str = "",
    content_format: str = "json",
    row_count: int = 0,
    byte_size: int = 0,
    checksum: str = "",
    min_ts: int | None = None,
    max_ts: int | None = None,
    source: str = "",
    schema_version: str = "v1",
    metadata: dict[str, Any] | None = None,
    now: int | None = None,
) -> None:
    ts = now or int(time.time())
    conn.execute(
        """
        INSERT INTO data_artifacts(
            artifact_type, uri, storage_backend, partition_key, content_format,
            row_count, byte_size, checksum, min_ts, max_ts, source,
            schema_version, metadata_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(uri) DO UPDATE SET
            artifact_type = excluded.artifact_type,
            storage_backend = excluded.storage_backend,
            partition_key = excluded.partition_key,
            content_format = excluded.content_format,
            row_count = excluded.row_count,
            byte_size = excluded.byte_size,
            checksum = excluded.checksum,
            min_ts = excluded.min_ts,
            max_ts = excluded.max_ts,
            source = excluded.source,
            schema_version = excluded.schema_version,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        (
            artifact_type,
            uri,
            storage_backend,
            partition_key,
            content_format,
            int(row_count or 0),
            int(byte_size or 0),
            checksum,
            min_ts,
            max_ts,
            source,
            schema_version,
            _json_dump(metadata or {}),
            ts,
            ts,
        ),
    )


def upsert_wallet_evidence_summary(
    conn: sqlite3.Connection,
    wallet: str,
    evidence: dict[str, Any],
    *,
    source_artifacts: list[str] | dict[str, Any] | None = None,
    computed_at: int | None = None,
) -> None:
    ts = computed_at or int(time.time())
    wallet = wallet.lower()
    activity_count = _evidence_int(evidence, "activity_count")
    distinct_markets = _evidence_int(evidence, "distinct_markets")
    non_fast_trade_count = _evidence_int(evidence, "non_fast_trade_count")
    non_fast_distinct_markets = _evidence_int(evidence, "non_fast_distinct_markets")
    fast_market_share = _evidence_float(evidence, "fast_market_share")
    buy_count = _evidence_int(evidence, "buy_count")
    sell_count = _evidence_int(evidence, "sell_count")
    total_usdc_volume = _evidence_float(evidence, "total_usdc_volume")
    median_gap_sec = evidence.get("median_gap_sec")
    oldest_ts = evidence.get("oldest_ts")
    latest_ts = evidence.get("latest_ts")
    evidence_tier = wallet_pipeline_tier(
        activity_count,
        distinct_markets,
        non_fast_trade_count,
        fast_market_share,
    )
    tags = _strategy_tags(
        activity_count=activity_count,
        distinct_markets=distinct_markets,
        non_fast_trade_count=non_fast_trade_count,
        fast_market_share=fast_market_share,
    )
    risk_flags = _risk_flags(
        activity_count=activity_count,
        distinct_markets=distinct_markets,
        non_fast_trade_count=non_fast_trade_count,
        fast_market_share=fast_market_share,
    )
    copyability = {
        "tier": evidence_tier,
        "confidence": _evidence_confidence(
            activity_count=activity_count,
            distinct_markets=distinct_markets,
            non_fast_trade_count=non_fast_trade_count,
            fast_market_share=fast_market_share,
        ),
        "usable_for_copyability": (
            evidence_tier in {EvidenceTier.L2_MEDIUM.value, EvidenceTier.L3_DEEP.value}
            and fast_market_share < 0.85
            and non_fast_trade_count >= 10
        ),
        "needs_raw_history": evidence_tier != EvidenceTier.L3_DEEP.value,
    }
    representative = [
        {"market_slug": market}
        for market in evidence.get("sample_markets", [])
        if str(market or "")
    ][:20]
    artifacts = source_artifacts or []
    if isinstance(artifacts, dict):
        artifacts = [artifacts]
    conn.execute(
        """
        INSERT INTO wallet_evidence_summary(
            wallet, summary_version, activity_count, distinct_markets,
            non_fast_trade_count, non_fast_distinct_markets, fast_market_share,
            buy_count, sell_count, total_usdc_volume, median_gap_sec,
            oldest_ts, latest_ts, strategy_tags_json, risk_flags_json,
            copyability_json, representative_trades_json, source_artifacts_json,
            computed_at, updated_at
        ) VALUES (?, 'v1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            summary_version = excluded.summary_version,
            activity_count = excluded.activity_count,
            distinct_markets = excluded.distinct_markets,
            non_fast_trade_count = excluded.non_fast_trade_count,
            non_fast_distinct_markets = excluded.non_fast_distinct_markets,
            fast_market_share = excluded.fast_market_share,
            buy_count = excluded.buy_count,
            sell_count = excluded.sell_count,
            total_usdc_volume = excluded.total_usdc_volume,
            median_gap_sec = excluded.median_gap_sec,
            oldest_ts = excluded.oldest_ts,
            latest_ts = excluded.latest_ts,
            strategy_tags_json = excluded.strategy_tags_json,
            risk_flags_json = excluded.risk_flags_json,
            copyability_json = excluded.copyability_json,
            representative_trades_json = excluded.representative_trades_json,
            source_artifacts_json = excluded.source_artifacts_json,
            computed_at = excluded.computed_at,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            activity_count,
            distinct_markets,
            non_fast_trade_count,
            non_fast_distinct_markets,
            fast_market_share,
            buy_count,
            sell_count,
            total_usdc_volume,
            median_gap_sec,
            oldest_ts,
            latest_ts,
            _json_dump(tags),
            _json_dump(risk_flags),
            _json_dump(copyability),
            _json_dump(representative),
            _json_dump(artifacts),
            ts,
            ts,
        ),
    )


def sync_wallet_processing_state(
    conn: sqlite3.Connection,
    wallet: str,
    evidence: dict[str, Any],
    *,
    source: str = "",
    now: int | None = None,
) -> dict[str, Any]:
    ts = now or int(time.time())
    wallet = wallet.lower()
    budget = conn.execute(
        """
        SELECT
            stage, target_depth, current_depth, priority, next_attempt_at,
            last_attempt_at, stop_reason, evidence_json
        FROM evidence_backfill_budget
        WHERE wallet = ?
        """,
        (wallet,),
    ).fetchone()
    watermark = conn.execute(
        """
        SELECT newest_timestamp, newest_activity_key
        FROM wallet_activity_watermarks
        WHERE address = ?
        """,
        (wallet,),
    ).fetchone()
    existing = conn.execute(
        """
        SELECT last_light_backfill_at, last_medium_backfill_at, last_deep_backfill_at
        FROM wallet_processing_state
        WHERE wallet = ?
        """,
        (wallet,),
    ).fetchone()

    activity_count = _evidence_int(evidence, "activity_count")
    distinct_markets = _evidence_int(evidence, "distinct_markets")
    non_fast_trade_count = _evidence_int(evidence, "non_fast_trade_count")
    fast_market_share = _evidence_float(evidence, "fast_market_share")
    evidence_tier = wallet_pipeline_tier(
        activity_count,
        distinct_markets,
        non_fast_trade_count,
        fast_market_share,
    )
    confidence = _evidence_confidence(
        activity_count=activity_count,
        distinct_markets=distinct_markets,
        non_fast_trade_count=non_fast_trade_count,
        fast_market_share=fast_market_share,
    )
    current_stage = str(budget["stage"] if budget else "")
    target_depth = int(budget["target_depth"] if budget else 0)
    budget_depth = int(budget["current_depth"] if budget else 0)
    priority = int(budget["priority"] if budget else 100)
    next_action_at = int(budget["next_attempt_at"] if budget else 0)
    stop_reason = str(budget["stop_reason"] if budget else "")
    if stop_reason and not evidence_promotion_approval_is_current(
        conn,
        wallet=wallet,
        job_action=current_stage,
    ):
        stop_reason = ""
    evidence_status, next_action = _next_pipeline_action(
        evidence_tier=evidence_tier,
        current_stage=current_stage,
        stop_reason=stop_reason,
        activity_count=activity_count,
        distinct_markets=distinct_markets,
        non_fast_trade_count=non_fast_trade_count,
        fast_market_share=fast_market_share,
    )
    last_attempt_at = int(budget["last_attempt_at"] if budget and budget["last_attempt_at"] else 0)
    last_light_at = existing["last_light_backfill_at"] if existing else None
    last_medium_at = existing["last_medium_backfill_at"] if existing else None
    last_deep_at = existing["last_deep_backfill_at"] if existing else None
    if last_attempt_at:
        if target_depth <= 200 or current_stage.startswith("light"):
            last_light_at = last_attempt_at
        elif target_depth <= 1_000 or current_stage.startswith("medium"):
            last_medium_at = last_attempt_at
        else:
            last_deep_at = last_attempt_at
    raw_artifact_uri = f"sqlite://wallet_activity/{wallet}"
    summary_artifact_uri = f"sqlite://wallet_evidence_summary/{wallet}"
    if source:
        upsert_data_artifact(
            conn,
            artifact_type="wallet_evidence_summary",
            uri=summary_artifact_uri,
            storage_backend="sqlite",
            partition_key=wallet[:6],
            content_format="sqlite_row",
            row_count=1,
            min_ts=evidence.get("oldest_ts"),
            max_ts=evidence.get("latest_ts"),
            source=source,
            metadata={"wallet": wallet, "tier": evidence_tier},
            now=ts,
        )
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, newest_activity_ts, oldest_activity_ts,
            newest_activity_key, activity_count, non_fast_trade_count,
            distinct_markets, last_light_backfill_at, last_medium_backfill_at,
            last_deep_backfill_at, raw_artifact_uri, summary_artifact_uri, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            discovery_tier = excluded.discovery_tier,
            evidence_status = excluded.evidence_status,
            evidence_depth = excluded.evidence_depth,
            evidence_confidence = excluded.evidence_confidence,
            priority = excluded.priority,
            current_stage = excluded.current_stage,
            next_action = excluded.next_action,
            next_action_at = excluded.next_action_at,
            newest_activity_ts = excluded.newest_activity_ts,
            oldest_activity_ts = excluded.oldest_activity_ts,
            newest_activity_key = excluded.newest_activity_key,
            activity_count = excluded.activity_count,
            non_fast_trade_count = excluded.non_fast_trade_count,
            distinct_markets = excluded.distinct_markets,
            last_light_backfill_at = COALESCE(excluded.last_light_backfill_at, wallet_processing_state.last_light_backfill_at),
            last_medium_backfill_at = COALESCE(excluded.last_medium_backfill_at, wallet_processing_state.last_medium_backfill_at),
            last_deep_backfill_at = COALESCE(excluded.last_deep_backfill_at, wallet_processing_state.last_deep_backfill_at),
            raw_artifact_uri = excluded.raw_artifact_uri,
            summary_artifact_uri = excluded.summary_artifact_uri,
            updated_at = excluded.updated_at
        """,
        (
            wallet,
            evidence_tier,
            evidence_status,
            max(activity_count, budget_depth),
            confidence,
            priority,
            current_stage,
            next_action,
            next_action_at,
            int(watermark["newest_timestamp"] if watermark else evidence.get("latest_ts") or 0) or None,
            evidence.get("oldest_ts"),
            str(watermark["newest_activity_key"] if watermark else ""),
            activity_count,
            non_fast_trade_count,
            distinct_markets,
            last_light_at,
            last_medium_at,
            last_deep_at,
            raw_artifact_uri,
            summary_artifact_uri,
            ts,
        ),
    )
    return {
        "wallet": wallet,
        "discovery_tier": evidence_tier,
        "evidence_status": evidence_status,
        "next_action": next_action,
        "evidence_confidence": confidence,
    }


def materialize_wallet_processing_state(
    conn: sqlite3.Connection,
    *,
    limit: int = 0,
    source: str = "materialize_wallet_processing_state",
    commit_every: int = 100,
    stale_only: bool = False,
) -> dict[str, Any]:
    from pm_robot.orchestration.evidence_backfill import summarize_wallet_evidence

    sql = """
        SELECT cw.address
        FROM candidate_wallets cw
        LEFT JOIN evidence_backfill_budget ebb
          ON ebb.wallet = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN wallet_activity_watermarks waw
          ON waw.address = cw.address
        LEFT JOIN wallet_registry wr
          ON wr.address = cw.address
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'
    """
    if stale_only:
        sql += """
          AND (
              wps.wallet IS NULL
              OR COALESCE(cw.updated_at, 0) > COALESCE(wps.updated_at, 0)
              OR COALESCE(ebb.updated_at, 0) > COALESCE(wps.updated_at, 0)
              OR COALESCE(waw.updated_at, 0) > COALESCE(wps.updated_at, 0)
              OR COALESCE(waw.newest_timestamp, 0) != COALESCE(wps.newest_activity_ts, 0)
              OR COALESCE(waw.newest_activity_key, '') != COALESCE(wps.newest_activity_key, '')
              OR COALESCE(ebb.stage, '') != COALESCE(wps.current_stage, '')
              OR COALESCE(ebb.priority, 100) != COALESCE(wps.priority, 100)
              OR COALESCE(ebb.next_attempt_at, 0) != COALESCE(wps.next_action_at, 0)
              OR COALESCE(ebb.current_depth, 0) > COALESCE(wps.evidence_depth, 0)
          )
        """
    sql += """
        ORDER BY
            CASE WHEN wps.wallet IS NULL THEN 0 ELSE 1 END ASC,
            COALESCE(ebb.priority, 100) ASC,
            cw.updated_at DESC,
            cw.address ASC
    """
    params: tuple[Any, ...] = ()
    if limit > 0:
        sql += " LIMIT ?"
        params = (limit,)
    rows = conn.execute(sql, params).fetchall()
    wallets = [str(row["address"]).lower() for row in rows]
    batch_size = max(1, commit_every) if commit_every > 0 else max(1, len(wallets))
    materialized = 0

    for offset in range(0, len(wallets), batch_size):
        batch = wallets[offset : offset + batch_size]

        def materialize_batch() -> int:
            for wallet in batch:
                evidence = summarize_wallet_evidence(conn, wallet)
                upsert_wallet_evidence_summary(
                    conn,
                    wallet,
                    evidence,
                    source_artifacts=[f"sqlite://wallet_activity/{wallet}"],
                )
                sync_wallet_processing_state(conn, wallet, evidence, source=source)
            conn.commit()
            return len(batch)

        materialized += retry_sqlite_locked(
            materialize_batch,
            rollback=conn.rollback,
            attempts=4,
            sleep_seconds=2.0,
        )

    summary = wallet_processing_state_summary(conn)
    summary["wallets_seen"] = len(rows)
    summary["wallets_materialized"] = materialized
    return summary


def wallet_processing_state_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    tier_rows = conn.execute(
        """
        SELECT discovery_tier, COUNT(*) AS count
        FROM wallet_processing_state
        GROUP BY discovery_tier
        ORDER BY discovery_tier ASC
        """
    ).fetchall()
    status_rows = conn.execute(
        """
        SELECT evidence_status, COUNT(*) AS count
        FROM wallet_processing_state
        GROUP BY evidence_status
        ORDER BY count DESC, evidence_status ASC
        """
    ).fetchall()
    action_rows = conn.execute(
        """
        SELECT next_action, COUNT(*) AS count
        FROM wallet_processing_state
        GROUP BY next_action
        ORDER BY count DESC, next_action ASC
        LIMIT 20
        """
    ).fetchall()
    return {
        "tiers": [dict(row) for row in tier_rows],
        "statuses": [dict(row) for row in status_rows],
        "next_actions": [dict(row) for row in action_rows],
    }


def enqueue_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    wallet: str = "",
    subject_key: str = "",
    tier: str = "",
    priority: int = 100,
    shard: int = 0,
    input_data: dict[str, Any] | None = None,
    max_attempts: int = 3,
    next_attempt_at: int = 0,
    now: int | None = None,
) -> bool:
    """Enqueue a scope, reopening failed work only after its cooldown with fresh attempts."""
    ts = now or int(time.time())
    wallet = wallet.lower()
    before = conn.total_changes
    conn.execute(
        """
        INSERT INTO pipeline_jobs(
            job_type, wallet, subject_key, tier, priority, shard, status,
            lease_owner, lease_until, attempts, max_attempts, next_attempt_at,
            input_json, output_json, last_error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'queued', NULL, 0, 0, ?, ?, ?, '{}', '', ?, ?)
        ON CONFLICT(job_type, wallet, tier, subject_key) DO UPDATE SET
            priority = MIN(pipeline_jobs.priority, excluded.priority),
            shard = excluded.shard,
            status = 'queued',
            attempts = CASE
                WHEN pipeline_jobs.status = 'failed' THEN 0
                ELSE pipeline_jobs.attempts
            END,
            max_attempts = excluded.max_attempts,
            next_attempt_at = CASE
                WHEN pipeline_jobs.status = 'failed' THEN excluded.next_attempt_at
                ELSE MIN(pipeline_jobs.next_attempt_at, excluded.next_attempt_at)
            END,
            input_json = excluded.input_json,
            updated_at = excluded.updated_at
        WHERE pipeline_jobs.status NOT IN ('running', 'done')
          AND (
                pipeline_jobs.status != 'failed'
                OR pipeline_jobs.next_attempt_at <= excluded.updated_at
          )
        """,
        (
            job_type,
            wallet,
            subject_key,
            tier,
            int(priority),
            int(shard),
            int(max_attempts),
            int(next_attempt_at),
            _json_dump(input_data or {}),
            ts,
            ts,
        ),
    )
    return conn.total_changes > before


def claim_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    shard: int,
    worker_id: str,
    lease_seconds: int,
    priority_aging_seconds: int = 0,
    now: int | None = None,
) -> dict[str, Any] | None:
    """Claim one job atomically, promoting sufficiently old work ahead of normal priority."""
    ts = now or int(time.time())
    aging_seconds = max(0, int(priority_aging_seconds))
    aging_cutoff = ts - aging_seconds
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        """
        SELECT *
        FROM pipeline_jobs
        WHERE job_type = ?
          AND shard = ?
          AND next_attempt_at <= ?
          AND attempts < max_attempts
          AND (
                status = 'queued'
                OR (status = 'running' AND lease_until <= ?)
          )
        ORDER BY
            CASE WHEN status = 'running' AND lease_until <= ? THEN 0 ELSE 1 END ASC,
            CASE WHEN ? > 0 AND updated_at <= ? THEN 0 ELSE 1 END ASC,
            CASE WHEN ? > 0 AND updated_at <= ? THEN updated_at END ASC,
            priority ASC,
            updated_at ASC,
            job_id ASC
        LIMIT 1
        """,
        (
            job_type,
            shard,
            ts,
            ts,
            ts,
            aging_seconds,
            aging_cutoff,
            aging_seconds,
            aging_cutoff,
        ),
    ).fetchone()
    if row is None:
        conn.commit()
        return None
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'running',
            lease_owner = ?,
            lease_until = ?,
            attempts = attempts + 1,
            last_error = '',
            updated_at = ?
        WHERE job_id = ?
        """,
        (worker_id, ts + lease_seconds, ts, row["job_id"]),
    )
    conn.commit()
    out = dict(row)
    out["attempts"] = int(out.get("attempts") or 0) + 1
    out["lease_owner"] = worker_id
    out["lease_until"] = ts + lease_seconds
    out["last_error"] = ""
    return out


def renew_pipeline_job_lease(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    lease_seconds: int,
    now: int | None = None,
) -> bool:
    """Extend a running queue job lease without changing its execution state."""
    ts = now or int(time.time())
    updated = conn.execute(
        """
        UPDATE pipeline_jobs
        SET lease_until = ?,
            updated_at = ?
        WHERE job_id = ?
          AND status = 'running'
          AND lease_owner = ?
        """,
        (ts + lease_seconds, ts, int(job_id), worker_id),
    ).rowcount
    return bool(updated)


class PipelineJobLeaseLost(RuntimeError):
    """Raised when a queue worker no longer owns the job it is processing."""


def complete_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    output_data: dict[str, Any] | None = None,
    now: int | None = None,
) -> bool:
    """Complete a running job only when the caller still owns its lease."""
    ts = now or int(time.time())
    updated = conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = 'done',
            lease_owner = NULL,
            lease_until = 0,
            output_json = ?,
            last_error = '',
            completed_at = ?,
            updated_at = ?
        WHERE job_id = ?
          AND status = 'running'
          AND lease_owner = ?
        """,
        (_json_dump(output_data or {}), ts, ts, job_id, worker_id),
    ).rowcount
    return bool(updated)


def supersede_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    reason: str,
    worker_id: str | None = None,
    now: int | None = None,
) -> bool:
    """Close obsolete queued work, optionally requiring the active worker lease."""

    ts = now or int(time.time())
    params: list[Any] = [reason[:1000], ts, ts, int(job_id)]
    lease_clause = ""
    if worker_id is not None:
        lease_clause = "AND status = 'running' AND lease_owner = ?"
        params.append(worker_id)
    else:
        lease_clause = "AND status = 'queued'"
    updated = conn.execute(
        f"""
        UPDATE pipeline_jobs
        SET status = 'superseded',
            lease_owner = NULL,
            lease_until = 0,
            last_error = ?,
            completed_at = ?,
            updated_at = ?
        WHERE job_id = ?
          {lease_clause}
        """,
        tuple(params),
    ).rowcount
    return bool(updated)


def retry_pipeline_job(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    worker_id: str,
    error: str,
    next_attempt_at: int,
    count_attempt: bool = True,
    now: int | None = None,
) -> bool:
    """Release a failed job only when the caller still owns its lease."""
    ts = now or int(time.time())
    row = conn.execute("SELECT attempts, max_attempts FROM pipeline_jobs WHERE job_id = ?", (job_id,)).fetchone()
    attempts = int(row["attempts"] or 0) if row else 0
    if not count_attempt:
        attempts = max(0, attempts - 1)
    failed = bool(row and attempts >= int(row["max_attempts"] or 3))
    params: tuple[Any, ...] = (
        "failed" if failed else "queued",
        next_attempt_at,
        error[:1000],
        1 if count_attempt else 0,
        ts,
        job_id,
        worker_id,
    )
    updated = conn.execute(
        """
        UPDATE pipeline_jobs
        SET status = ?,
            lease_owner = NULL,
            lease_until = 0,
            next_attempt_at = ?,
            last_error = ?,
            attempts = CASE WHEN ? THEN attempts ELSE MAX(0, attempts - 1) END,
            updated_at = ?
        WHERE job_id = ?
          AND status = 'running'
          AND lease_owner = ?
        """,
        params,
    ).rowcount
    return bool(updated)


def pipeline_job_summary(conn: sqlite3.Connection, *, job_type: str = "") -> dict[str, Any]:
    params: tuple[Any, ...] = ()
    where = ""
    if job_type:
        where = "WHERE job_type = ?"
        params = (job_type,)
    status_rows = conn.execute(
        f"""
        SELECT job_type, status, COUNT(*) AS count
        FROM pipeline_jobs
        {where}
        GROUP BY job_type, status
        ORDER BY job_type ASC, count DESC, status ASC
        """,
        params,
    ).fetchall()
    shard_rows = conn.execute(
        f"""
        SELECT job_type, shard, status, COUNT(*) AS count
        FROM pipeline_jobs
        {where}
        GROUP BY job_type, shard, status
        ORDER BY job_type ASC, shard ASC, status ASC
        """,
        params,
    ).fetchall()
    return {
        "statuses": [dict(row) for row in status_rows],
        "by_shard": [dict(row) for row in shard_rows],
    }


def _evidence_int(evidence: dict[str, Any], key: str) -> int:
    try:
        return int(evidence.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _evidence_float(evidence: dict[str, Any], key: str) -> float:
    try:
        return float(evidence.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _evidence_confidence(
    *,
    activity_count: int,
    distinct_markets: int,
    non_fast_trade_count: int,
    fast_market_share: float,
) -> float:
    depth_score = min(activity_count / 1_000, 1.0)
    diversity_score = min(distinct_markets / 10, 1.0)
    non_fast_score = min(non_fast_trade_count / 50, 1.0)
    fast_penalty = max(0.0, fast_market_share - 0.5) * 0.6
    return round(max(0.0, min(1.0, depth_score * 0.45 + diversity_score * 0.3 + non_fast_score * 0.25 - fast_penalty)), 4)


def _strategy_tags(
    *,
    activity_count: int,
    distinct_markets: int,
    non_fast_trade_count: int,
    fast_market_share: float,
) -> list[str]:
    tags: list[str] = []
    if activity_count <= 0:
        tags.append("unproven")
    if activity_count >= 1_000:
        tags.append("deep_history")
    elif activity_count >= 200:
        tags.append("medium_history")
    elif activity_count > 0:
        tags.append("light_history")
    if distinct_markets >= 10:
        tags.append("multi_market")
    elif distinct_markets <= 2 and activity_count >= 25:
        tags.append("concentrated")
    if non_fast_trade_count >= 50:
        tags.append("non_fast_validated")
    if fast_market_share >= 0.85 and activity_count >= 50:
        tags.append("fast_market_specialist")
    return tags


def _risk_flags(
    *,
    activity_count: int,
    distinct_markets: int,
    non_fast_trade_count: int,
    fast_market_share: float,
) -> list[str]:
    flags: list[str] = []
    if activity_count < 25:
        flags.append("thin_history")
    if activity_count >= 50 and fast_market_share >= 0.85:
        flags.append("fast_market_dominant")
    if activity_count >= 25 and distinct_markets < 3 and non_fast_trade_count < 10:
        flags.append("low_strategy_diversity")
    return flags


def _next_pipeline_action(
    *,
    evidence_tier: str,
    current_stage: str,
    stop_reason: str = "",
    activity_count: int,
    distinct_markets: int,
    non_fast_trade_count: int,
    fast_market_share: float,
) -> tuple[str, str]:
    """Map evidence truth to queue work without deciding depth promotion policy."""
    if current_stage == "paused_fast_market_specialist" or (
        activity_count >= 50 and fast_market_share >= 0.85
    ):
        return EvidenceStatus.PAUSED.value, "manual_review_fast_market"
    if current_stage in {
        EvidenceJobStage.LIGHT_DONE.value,
        EvidenceJobStage.MEDIUM_DONE.value,
        EvidenceJobStage.DEEP_DONE.value,
    }:
        return EvidenceStatus.SUMMARY_READY.value, "score_wallet"
    if evidence_tier == EvidenceTier.L3_DEEP.value:
        return EvidenceStatus.SUMMARY_READY.value, "score_wallet"
    if current_stage == EvidenceJobStage.LIGHT_PENDING.value:
        if activity_count < 25:
            return EvidenceStatus.QUEUED.value, current_stage
        return EvidenceStatus.SUMMARY_READY.value, "score_wallet"
    if current_stage in {
        EvidenceJobStage.MEDIUM_PENDING.value,
        EvidenceJobStage.DEEP_PENDING.value,
    }:
        if evidence_promotion_is_approved(stop_reason, current_stage):
            return EvidenceStatus.QUEUED.value, current_stage
        return EvidenceStatus.SUMMARY_READY.value, "score_wallet"
    if evidence_tier == EvidenceTier.L0_DISCOVERED.value or activity_count < 25:
        return EvidenceStatus.NEEDS_LIGHT.value, EvidenceJobStage.LIGHT_PENDING.value
    return EvidenceStatus.SUMMARY_READY.value, "score_wallet"


def _wallet_processing_state_ready(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM candidate_wallets) AS candidate_count,
            (SELECT COUNT(*) FROM wallet_processing_state) AS state_count
        """
    ).fetchone()
    if not row:
        return False
    candidate_count = int(row["candidate_count"] or 0)
    state_count = int(row["state_count"] or 0)
    return candidate_count > 0 and state_count >= candidate_count


def _wallet_activity_count(conn: sqlite3.Connection, wallet: str) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM wallet_activity WHERE address = ?",
            (wallet.lower(),),
        ).fetchone()[0]
    )


def start_ingest_run(conn: sqlite3.Connection, ingest_type: str) -> int:
    now = int(time.time())
    conn.execute(
        """
        UPDATE ingest_runs
        SET finished_at = ?, status = 'interrupted', error = 'superseded_by_new_run'
        WHERE ingest_type = ? AND status = 'running'
        """,
        (now, ingest_type),
    )
    cur = conn.execute(
        "INSERT INTO ingest_runs(ingest_type, started_at) VALUES (?, ?)",
        (ingest_type, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def record_runtime_heartbeat(
    conn: sqlite3.Connection,
    ingest_type: str,
    *,
    status: str = "ok",
    rows_written: int = 0,
    error: str = "",
    now: int | None = None,
    started_at: int | None = None,
    finished_at: int | None = None,
) -> int:
    """Record a finished loop heartbeat without claiming ownership of worker runs."""

    ts = int(time.time()) if now is None else int(now)
    started_ts = ts if started_at is None else int(started_at)
    finished_ts = ts if finished_at is None else int(finished_at)
    finished_ts = max(started_ts, finished_ts)
    def write_heartbeat() -> int:
        cur = conn.execute(
            """
            INSERT INTO ingest_runs(
                ingest_type, started_at, finished_at, status,
                wallets_attempted, wallets_succeeded, rows_written, error
            ) VALUES (?, ?, ?, ?, 0, 0, ?, ?)
            """,
            (ingest_type, started_ts, finished_ts, status, int(rows_written), error[:1000]),
        )
        conn.commit()
        return int(cur.lastrowid)

    return retry_sqlite_locked(
        write_heartbeat,
        rollback=conn.rollback,
        attempts=2,
        sleep_seconds=0.5,
    )


def finish_ingest_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    wallets_attempted: int,
    wallets_succeeded: int,
    rows_written: int,
    error: str = "",
) -> None:
    conn.execute(
        """
        UPDATE ingest_runs
        SET finished_at = ?, status = ?, wallets_attempted = ?,
            wallets_succeeded = ?, rows_written = ?, error = ?
        WHERE run_id = ?
        """,
        (
            int(time.time()),
            status,
            wallets_attempted,
            wallets_succeeded,
            rows_written,
            error[:1000],
            run_id,
        ),
    )
    conn.commit()


def log_api_request(
    conn: sqlite3.Connection,
    *,
    base_url: str,
    endpoint: str,
    status_code: int | None,
    latency_ms: int,
    retry_count: int,
    error_type: str = "",
    ok: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO api_request_log(
            ts, base_url, endpoint, status_code, latency_ms,
            retry_count, error_type, ok
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            base_url,
            endpoint,
            status_code,
            latency_ms,
            retry_count,
            error_type[:120],
            1 if ok else 0,
        ),
    )
    conn.commit()


def api_request_summary(conn: sqlite3.Connection, *, since_seconds: int = 3600) -> dict[str, Any]:
    since = int(time.time()) - since_seconds
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS request_count,
            SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS error_count,
            AVG(latency_ms) AS avg_latency_ms,
            MAX(latency_ms) AS max_latency_ms
        FROM api_request_log
        WHERE ts >= ?
        """,
        (since,),
    ).fetchone()
    by_error = conn.execute(
        """
        SELECT error_type, COUNT(*) AS count
        FROM api_request_log
        WHERE ts >= ? AND ok = 0
        GROUP BY error_type
        ORDER BY count DESC, error_type ASC
        LIMIT 10
        """,
        (since,),
    ).fetchall()
    out = dict(row)
    out["window_seconds"] = since_seconds
    out["errors_by_type"] = [dict(r) for r in by_error]
    return out


def list_gamma_market_backfill_targets(
    conn: sqlite3.Connection,
    *,
    limit: int = 100,
    now: int | None = None,
    paper_only: bool = False,
) -> list[str]:
    now = now or int(time.time())
    if paper_only:
        rows = conn.execute(
            """
            WITH paper_slugs AS (
                SELECT DISTINCT market_slug
                FROM paper_fills
                WHERE market_slug IS NOT NULL AND market_slug != ''
            )
            SELECT paper_slugs.market_slug
            FROM paper_slugs
            LEFT JOIN gamma_market_cache gmc
              ON gmc.market_slug = paper_slugs.market_slug
            WHERE gmc.market_slug IS NULL
               OR gmc.expires_at <= ?
            ORDER BY COALESCE(gmc.expires_at, 0) ASC, paper_slugs.market_slug ASC
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
        return [row["market_slug"] for row in rows]
    rows = conn.execute(
        """
        WITH slugs AS (
            SELECT DISTINCT wa.market_slug, 0 AS priority
            FROM copy_trade_links ctl
            JOIN copy_pair_stats cps
              ON cps.leader_wallet = ctl.leader_wallet
             AND cps.follower_wallet = ctl.follower_wallet
             AND cps.qualifies = 1
            JOIN wallet_activity wa
              ON wa.activity_id = ctl.leader_activity_id
            WHERE wa.market_slug IS NOT NULL AND wa.market_slug != ''
            UNION
            SELECT market_slug, 1 AS priority FROM paper_fills
            WHERE market_slug IS NOT NULL AND market_slug != ''
            UNION
            SELECT market_slug, 2 AS priority FROM wallet_episodes
            WHERE market_slug IS NOT NULL AND market_slug != ''
              AND status = 'open'
              AND bought_usdc > 0
              AND ABS(net_shares) > 0.0000001
            UNION
            SELECT market_slug, 3 AS priority FROM paper_orders
            WHERE market_slug IS NOT NULL AND market_slug != '' AND accepted = 1
            UNION
            SELECT market_slug, 4 AS priority FROM wallet_activity
            WHERE market_slug IS NOT NULL AND market_slug != ''
            UNION
            SELECT market_slug, 5 AS priority FROM wallet_positions
            WHERE market_slug IS NOT NULL AND market_slug != ''
            UNION
            SELECT market_slug, 6 AS priority FROM wallet_episodes
            WHERE market_slug IS NOT NULL AND market_slug != ''
        ),
        ranked_slugs AS (
            SELECT market_slug, MIN(priority) AS priority
            FROM slugs
            GROUP BY market_slug
        )
        SELECT ranked_slugs.market_slug
        FROM ranked_slugs
        LEFT JOIN gamma_market_cache gmc
          ON gmc.market_slug = ranked_slugs.market_slug
        WHERE gmc.market_slug IS NULL
           OR gmc.expires_at <= ?
        ORDER BY ranked_slugs.priority ASC, COALESCE(gmc.expires_at, 0) ASC, ranked_slugs.market_slug ASC
        LIMIT ?
        """,
        (now, limit),
    ).fetchall()
    return [row["market_slug"] for row in rows]


def upsert_gamma_market_cache(
    conn: sqlite3.Connection,
    *,
    market_slug: str,
    market: dict[str, Any],
    fetched_at: int,
    ttl_seconds: int,
) -> None:
    closed = bool(market.get("closed") or market.get("resolved"))
    expires_at = 4_102_444_800 if closed else fetched_at + ttl_seconds
    conn.execute(
        """
        INSERT INTO gamma_market_cache(
            market_slug, condition_id, event_slug, question, title, category,
            end_date, closed, active, archived, clob_token_ids_json,
            outcomes_json, outcome_prices_json, raw_json, fetched_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(market_slug) DO UPDATE SET
            condition_id = excluded.condition_id,
            event_slug = excluded.event_slug,
            question = excluded.question,
            title = excluded.title,
            category = excluded.category,
            end_date = excluded.end_date,
            closed = excluded.closed,
            active = excluded.active,
            archived = excluded.archived,
            clob_token_ids_json = excluded.clob_token_ids_json,
            outcomes_json = excluded.outcomes_json,
            outcome_prices_json = excluded.outcome_prices_json,
            raw_json = excluded.raw_json,
            fetched_at = excluded.fetched_at,
            expires_at = excluded.expires_at
        """,
        (
            market_slug,
            market.get("conditionId") or market.get("condition_id"),
            _event_slug_from_market(market),
            market.get("question"),
            market.get("title") or market.get("question"),
            _category_from_market(market),
            market.get("endDate") or market.get("end_date"),
            1 if closed else 0,
            1 if market.get("active") else 0,
            1 if market.get("archived") else 0,
            _json_list_field(market.get("clobTokenIds") or market.get("clob_token_ids")),
            _json_list_field(market.get("outcomes")),
            _json_list_field(market.get("outcomePrices") or market.get("outcome_prices")),
            json.dumps(market, ensure_ascii=False),
            fetched_at,
            expires_at,
        ),
    )
    conn.commit()


def upsert_gamma_market_failure(
    conn: sqlite3.Connection,
    *,
    market_slug: str,
    error: str,
    fetched_at: int,
    ttl_seconds: int,
) -> None:
    conn.execute(
        """
        INSERT INTO gamma_market_cache(
            market_slug, condition_id, event_slug, question, title, category,
            end_date, closed, active, archived, clob_token_ids_json,
            outcomes_json, outcome_prices_json, raw_json, fetched_at, expires_at
        ) VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 1, '[]', '[]', '[]', ?, ?, ?)
        ON CONFLICT(market_slug) DO UPDATE SET
            condition_id = excluded.condition_id,
            event_slug = excluded.event_slug,
            question = excluded.question,
            title = excluded.title,
            category = excluded.category,
            end_date = excluded.end_date,
            closed = excluded.closed,
            active = excluded.active,
            archived = excluded.archived,
            clob_token_ids_json = excluded.clob_token_ids_json,
            outcomes_json = excluded.outcomes_json,
            outcome_prices_json = excluded.outcome_prices_json,
            raw_json = excluded.raw_json,
            fetched_at = excluded.fetched_at,
            expires_at = excluded.expires_at
        """,
        (
            market_slug,
            json.dumps({"error": error}, ensure_ascii=False),
            fetched_at,
            fetched_at + ttl_seconds,
        ),
    )
    conn.commit()


def gamma_market_cache_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        WITH slugs AS (
            SELECT market_slug FROM wallet_activity
            WHERE market_slug IS NOT NULL AND market_slug != ''
            UNION
            SELECT market_slug FROM wallet_positions
            WHERE market_slug IS NOT NULL AND market_slug != ''
            UNION
            SELECT market_slug FROM wallet_episodes
            WHERE market_slug IS NOT NULL AND market_slug != ''
        ),
        paper_slugs AS (
            SELECT DISTINCT market_slug FROM paper_fills
            WHERE market_slug IS NOT NULL AND market_slug != ''
        )
        SELECT
            (SELECT COUNT(*) FROM slugs) AS referenced_market_slugs,
            (SELECT COUNT(*) FROM paper_slugs) AS paper_market_slugs,
            (
                SELECT COUNT(*)
                FROM paper_slugs ps
                JOIN gamma_market_cache gmc ON gmc.market_slug = ps.market_slug
                WHERE gmc.expires_at > strftime('%s','now')
            ) AS cached_paper_markets,
            (SELECT COUNT(*) FROM gamma_market_cache) AS cached_markets,
            (SELECT COUNT(*) FROM gamma_market_cache WHERE closed = 1) AS cached_closed_markets,
            (SELECT COUNT(*) FROM gamma_market_cache WHERE raw_json LIKE '{"error":%') AS cached_error_markets,
            (SELECT COUNT(*) FROM gamma_market_cache WHERE expires_at <= strftime('%s','now')) AS expired_markets
        """
    ).fetchone()
    return dict(row)


def persist_wallet_positions(
    conn: sqlite3.Connection,
    address: str,
    positions: list[dict[str, Any]],
    *,
    captured_at: int,
    commit: bool = True,
) -> int:
    address = address.lower()
    if _raw_evidence_write_suppressed(conn, address):
        return 0
    rows = []
    for position in positions:
        asset_id = str(position.get("asset") or position.get("asset_id") or position.get("token_id") or "")
        if not asset_id:
            continue
        rows.append(
            (
                address,
                asset_id,
                position.get("conditionId") or position.get("condition_id"),
                position.get("marketSlug") or position.get("market_slug"),
                position.get("eventSlug") or position.get("event_slug"),
                position.get("title"),
                position.get("outcome"),
                _float(position.get("size")),
                _float(position.get("avgPrice") or position.get("avg_price")),
                _float(position.get("curPrice") or position.get("current_price")),
                _float(position.get("currentValue") or position.get("current_value")),
                _float(position.get("initialValue") or position.get("initial_value")),
                _float(position.get("cashPnl") or position.get("cash_pnl")),
                _float(position.get("realizedPnl") or position.get("realized_pnl")),
                _float(position.get("percentPnl") or position.get("percent_pnl")),
                position.get("endDate") or position.get("end_date"),
                1 if position.get("negRisk") or position.get("neg_risk") else 0,
                captured_at,
                json.dumps(position, ensure_ascii=False),
            )
        )
    if rows:
        conn.execute("DELETE FROM wallet_positions WHERE address = ?", (address,))
        conn.executemany(
            """
            INSERT OR REPLACE INTO wallet_positions(
                address, asset_id, condition_id, market_slug, event_slug, title,
                outcome, size, avg_price, current_price, current_value,
                initial_value, cash_pnl, realized_pnl, percent_pnl, end_date,
                neg_risk, captured_at, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    conn.execute(
        "UPDATE candidate_wallets SET last_ingested_at = ?, updated_at = ? WHERE address = ?",
        (captured_at, captured_at, address),
    )
    _merge_position_feature(conn, address, positions)
    if commit:
        conn.commit()
    return len(rows)


def persist_wallet_activity(
    conn: sqlite3.Connection,
    address: str,
    events: list[dict[str, Any]],
    *,
    ingested_at: int,
    source: str = "",
    commit: bool = True,
) -> int:
    address = address.lower()
    if _raw_evidence_write_suppressed(conn, address):
        return 0
    rows = []
    for event in events:
        raw_event = dict(event)
        if source and not raw_event.get("source"):
            raw_event["source"] = source
        timestamp = int(event.get("timestamp") or 0)
        condition_id = event.get("conditionId") or event.get("condition_id")
        event_slug = event.get("eventSlug") or event.get("event_slug")
        market_slug = event.get("slug") or event.get("marketSlug") or event.get("market_slug")
        asset_id = str(event.get("asset") or event.get("asset_id") or "")
        outcome = event.get("outcome")
        event_type = str(event.get("type") or "")
        side = event.get("side")
        price = _float(event.get("price"))
        size = _float(event.get("size"))
        usdc_size = _float(event.get("usdcSize") or event.get("usdc_size"))
        transaction_hash = event.get("transactionHash") or event.get("transaction_hash")
        activity_key = _activity_key(
            timestamp=timestamp,
            condition_id=condition_id,
            event_slug=event_slug,
            market_slug=market_slug,
            asset_id=asset_id,
            outcome=outcome,
            event_type=event_type,
            side=side,
            price=price,
            size=size,
            usdc_size=usdc_size,
            transaction_hash=transaction_hash,
        )
        rows.append(
            (
                address,
                activity_key,
                _legacy_activity_key(
                    timestamp=timestamp,
                    condition_id=condition_id,
                    event_slug=event_slug,
                    market_slug=market_slug,
                    asset_id=asset_id,
                    outcome=outcome,
                    event_type=event_type,
                    side=side,
                    price=price,
                    size=size,
                    usdc_size=usdc_size,
                    transaction_hash=transaction_hash,
                ),
                timestamp,
                condition_id,
                event_slug,
                market_slug,
                asset_id,
                outcome,
                event_type,
                side,
                price,
                size,
                usdc_size,
                transaction_hash,
                json.dumps(raw_event, ensure_ascii=False),
                ingested_at,
            )
        )
    if rows:
        keys = [row[1] for row in rows] + [row[2] for row in rows]
        placeholders = ",".join("?" for _ in keys)
        existing = {
            row["activity_key"]
            for row in conn.execute(
                f"""
                SELECT activity_key FROM wallet_activity
                WHERE address = ? AND activity_key IN ({placeholders})
                """,
                (address, *keys),
            ).fetchall()
        }
        rows = [row for row in rows if row[1] not in existing and row[2] not in existing]
    before_insert = conn.total_changes
    if rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO wallet_activity(
                address, activity_key, timestamp, condition_id, event_slug, market_slug,
                asset_id, outcome, type, side, price, size, usdc_size,
                transaction_hash, raw_json, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [row[:2] + row[3:] for row in rows],
        )
    inserted = conn.total_changes - before_insert
    update_activity_watermark(conn, address, events, updated_at=ingested_at)
    conn.execute(
        "UPDATE candidate_wallets SET last_ingested_at = ?, updated_at = ? WHERE address = ?",
        (ingested_at, ingested_at, address),
    )
    if commit:
        conn.commit()
    return inserted


def _raw_evidence_write_suppressed(conn: sqlite3.Connection, address: str) -> bool:
    """Prevent a frozen or pruned wallet from silently repopulating the hot store."""

    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'wallet_registry'"
    ).fetchone() is None:
        return False
    row = conn.execute(
        "SELECT raw_retention_tier FROM wallet_registry WHERE address = ?",
        (address.lower(),),
    ).fetchone()
    return row is not None and str(row["raw_retention_tier"] or "") == "summary_only"


def activity_watermark(conn: sqlite3.Connection, address: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT newest_timestamp, newest_activity_key, last_full_backfill_at
        FROM wallet_activity_watermarks
        WHERE address = ?
        """,
        (address.lower(),),
    ).fetchone()
    if not row:
        return {"newest_timestamp": 0, "newest_activity_key": "", "last_full_backfill_at": None}
    return dict(row)


def update_activity_watermark(
    conn: sqlite3.Connection,
    address: str,
    events: list[dict[str, Any]],
    *,
    updated_at: int,
) -> None:
    address = address.lower()
    newest_ts = 0
    newest_key = ""
    for event in events:
        timestamp = int(event.get("timestamp") or 0)
        key = _activity_key_from_event(event)
        if timestamp > newest_ts:
            newest_ts = timestamp
            newest_key = key
    if newest_ts <= 0:
        conn.execute(
            """
            INSERT INTO wallet_activity_watermarks(address, updated_at)
            VALUES (?, ?)
            ON CONFLICT(address) DO UPDATE SET updated_at = excluded.updated_at
            """,
            (address, updated_at),
        )
        return
    conn.execute(
        """
        INSERT INTO wallet_activity_watermarks(
            address, newest_timestamp, newest_activity_key, updated_at
        ) VALUES (?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            newest_timestamp = CASE
                WHEN excluded.newest_timestamp > wallet_activity_watermarks.newest_timestamp
                THEN excluded.newest_timestamp
                ELSE wallet_activity_watermarks.newest_timestamp
            END,
            newest_activity_key = CASE
                WHEN excluded.newest_timestamp >= wallet_activity_watermarks.newest_timestamp
                THEN excluded.newest_activity_key
                ELSE wallet_activity_watermarks.newest_activity_key
            END,
            updated_at = excluded.updated_at
        """,
        (address, newest_ts, newest_key, updated_at),
    )


def activity_event_key(event: dict[str, Any]) -> str:
    return _activity_key_from_event(event)


def rebuild_wallet_episodes(
    conn: sqlite3.Connection,
    address: str,
    *,
    commit: bool = True,
) -> int:
    address = address.lower()
    now = int(time.time())
    rows = conn.execute(
        """
        SELECT * FROM wallet_activity
        WHERE address = ? AND type = 'TRADE'
        ORDER BY timestamp ASC, activity_id ASC
        """,
        (address,),
    ).fetchall()
    grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in rows:
        key = (row["condition_id"] or "", row["asset_id"] or "")
        grouped.setdefault(key, []).append(row)

    conn.execute("DELETE FROM wallet_episodes WHERE address = ?", (address,))
    episode_rows = []
    settlement_cache: dict[tuple[str, str], float | None] = {}
    for (_condition, _asset), events in grouped.items():
        if not events:
            continue
        buy_events = [e for e in events if str(e["side"]).upper() == "BUY"]
        sell_events = [e for e in events if str(e["side"]).upper() == "SELL"]
        bought_usdc = sum(float(e["usdc_size"] or 0) for e in buy_events)
        sold_usdc = sum(float(e["usdc_size"] or 0) for e in sell_events)
        bought_shares = sum(float(e["size"] or 0) for e in buy_events)
        sold_shares = sum(float(e["size"] or 0) for e in sell_events)
        net_shares = bought_shares - sold_shares
        avg_entry = bought_usdc / bought_shares if bought_shares > 0 else None
        realized_est = sold_usdc - (avg_entry or 0) * sold_shares
        status = "closed" if bought_shares > 0 and abs(net_shares) <= max(1e-9, bought_shares * 0.01) else "open"
        first = events[0]
        last = events[-1]
        settlement_price = _cached_gamma_settlement_price(
            conn,
            settlement_cache,
            market_slug=str(first["market_slug"] or ""),
            asset_id=str(first["asset_id"] or ""),
        )
        if bought_shares > 0 and settlement_price is not None:
            realized_est = sold_usdc + net_shares * settlement_price - bought_usdc
            status = "closed"
        episode_rows.append(
            (
                address,
                first["condition_id"],
                first["event_slug"],
                first["market_slug"],
                first["asset_id"],
                first["outcome"],
                first["timestamp"],
                last["timestamp"],
                len(buy_events),
                len(sell_events),
                len(buy_events),
                bought_usdc,
                sold_usdc,
                net_shares,
                avg_entry,
                realized_est,
                status,
                now,
            )
        )
    if episode_rows:
        conn.executemany(
            """
            INSERT INTO wallet_episodes(
                address, condition_id, event_slug, market_slug, asset_id, outcome,
                first_ts, last_ts, buy_count, sell_count, dca_entries,
                bought_usdc, sold_usdc, net_shares, avg_entry_price,
                realized_pnl_est, status, rebuilt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            episode_rows,
        )
    _merge_episode_features(conn, address)
    if commit:
        conn.commit()
    return len(episode_rows)


def _cached_gamma_settlement_price(
    conn: sqlite3.Connection,
    cache: dict[tuple[str, str], float | None],
    *,
    market_slug: str,
    asset_id: str,
) -> float | None:
    key = (market_slug, asset_id)
    if key not in cache:
        cache[key] = _gamma_settlement_price(conn, market_slug=market_slug, asset_id=asset_id)
    return cache[key]


def _gamma_settlement_price(conn: sqlite3.Connection, *, market_slug: str, asset_id: str) -> float | None:
    if not market_slug or not asset_id:
        return None
    row = conn.execute(
        """
        SELECT *
        FROM gamma_market_cache
        WHERE market_slug = ?
          AND closed = 1
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (market_slug,),
    ).fetchone()
    if row is None:
        return None
    price = _gamma_asset_price(row, asset_id)
    if price is None:
        return None
    return price if price <= 0.001 or price >= 0.999 else None


def _gamma_asset_price(row: sqlite3.Row, asset_id: str) -> float | None:
    token_ids = [str(item) for item in _json_list(row["clob_token_ids_json"])]
    prices = [_float(item) for item in _json_list(row["outcome_prices_json"])]
    if asset_id in token_ids:
        idx = token_ids.index(asset_id)
        if idx < len(prices):
            return prices[idx]

    raw = _json_object(row["raw_json"])
    tokens = raw.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = str(token.get("token_id") or token.get("tokenId") or token.get("id") or "")
            if token_id != asset_id:
                continue
            return _float(token.get("price") or token.get("last_price") or token.get("lastPrice"))
    return None


def upsert_wallet_feature(conn: sqlite3.Connection, feature: WalletFeatures) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO wallet_features(
            address, cumulative_win_rate, recent_30d_volume_usdc, net_pnl_usdc,
            total_volume_usdc, event_win_rate, trade_win_rate, avg_dca_entries,
            sell_pct, bot_score, trades_per_day, median_gap_sec, maker_fraction,
            leader_in_degree, copy_event_count, copy_market_count,
            containment_pct_median, copy_stream_roi, edge_retention_pct,
            walk_forward_consistency_pct, survival_score, single_market_pnl_share,
            net_to_gross_exposure, hygiene_status, primary_category,
            last_active_days_ago, extra_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            cumulative_win_rate = COALESCE(excluded.cumulative_win_rate, wallet_features.cumulative_win_rate),
            recent_30d_volume_usdc = COALESCE(excluded.recent_30d_volume_usdc, wallet_features.recent_30d_volume_usdc),
            net_pnl_usdc = COALESCE(excluded.net_pnl_usdc, wallet_features.net_pnl_usdc),
            total_volume_usdc = COALESCE(excluded.total_volume_usdc, wallet_features.total_volume_usdc),
            event_win_rate = COALESCE(excluded.event_win_rate, wallet_features.event_win_rate),
            trade_win_rate = COALESCE(excluded.trade_win_rate, wallet_features.trade_win_rate),
            avg_dca_entries = COALESCE(excluded.avg_dca_entries, wallet_features.avg_dca_entries),
            sell_pct = COALESCE(excluded.sell_pct, wallet_features.sell_pct),
            bot_score = COALESCE(excluded.bot_score, wallet_features.bot_score),
            trades_per_day = COALESCE(excluded.trades_per_day, wallet_features.trades_per_day),
            median_gap_sec = COALESCE(excluded.median_gap_sec, wallet_features.median_gap_sec),
            maker_fraction = COALESCE(excluded.maker_fraction, wallet_features.maker_fraction),
            leader_in_degree = COALESCE(excluded.leader_in_degree, wallet_features.leader_in_degree),
            copy_event_count = COALESCE(excluded.copy_event_count, wallet_features.copy_event_count),
            copy_market_count = COALESCE(excluded.copy_market_count, wallet_features.copy_market_count),
            containment_pct_median = COALESCE(excluded.containment_pct_median, wallet_features.containment_pct_median),
            copy_stream_roi = COALESCE(excluded.copy_stream_roi, wallet_features.copy_stream_roi),
            edge_retention_pct = COALESCE(excluded.edge_retention_pct, wallet_features.edge_retention_pct),
            walk_forward_consistency_pct = COALESCE(excluded.walk_forward_consistency_pct, wallet_features.walk_forward_consistency_pct),
            survival_score = COALESCE(excluded.survival_score, wallet_features.survival_score),
            single_market_pnl_share = COALESCE(excluded.single_market_pnl_share, wallet_features.single_market_pnl_share),
            net_to_gross_exposure = COALESCE(excluded.net_to_gross_exposure, wallet_features.net_to_gross_exposure),
            hygiene_status = CASE
                WHEN excluded.hygiene_status != '' THEN excluded.hygiene_status
                ELSE wallet_features.hygiene_status
            END,
            primary_category = CASE
                WHEN excluded.primary_category != '' THEN excluded.primary_category
                ELSE wallet_features.primary_category
            END,
            last_active_days_ago = COALESCE(excluded.last_active_days_ago, wallet_features.last_active_days_ago),
            extra_json = excluded.extra_json,
            updated_at = excluded.updated_at
        """,
        (
            feature.address.lower(),
            feature.cumulative_win_rate,
            feature.recent_30d_volume_usdc,
            feature.net_pnl_usdc,
            feature.total_volume_usdc,
            feature.event_win_rate,
            feature.trade_win_rate,
            feature.avg_dca_entries,
            feature.sell_pct,
            feature.bot_score,
            feature.trades_per_day,
            feature.median_gap_sec,
            feature.maker_fraction,
            feature.leader_in_degree,
            feature.copy_event_count,
            feature.copy_market_count,
            feature.containment_pct_median,
            feature.copy_stream_roi,
            feature.edge_retention_pct,
            feature.walk_forward_consistency_pct,
            feature.survival_score,
            feature.single_market_pnl_share,
            feature.net_to_gross_exposure,
            feature.hygiene_status,
            feature.primary_category,
            feature.last_active_days_ago,
            json.dumps(feature.extra, ensure_ascii=False),
            now,
        ),
    )


def get_wallet_features(conn: sqlite3.Connection) -> dict[str, WalletFeatures]:
    rows = conn.execute("SELECT * FROM wallet_features").fetchall()
    return {row["address"]: _feature_from_row(row) for row in rows}


def persist_score(
    conn: sqlite3.Connection,
    score: ScoreBreakdown,
    *,
    policy_version: str = "",
) -> None:
    now = int(time.time())
    current = conn.execute(
        "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
        (score.address,),
    ).fetchone()
    from_stage = current["candidate_stage"] if current else None
    next_stage = _score_target_stage(from_stage, score.stage.value)
    conn.execute(
        """
        INSERT INTO leader_scores(
            address, leader_score, review_stage, review_reason,
            components_json, penalties_json, policy_version, scored_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            score.address,
            score.leader_score,
            score.stage.value,
            score.reason,
            json.dumps(score.components, ensure_ascii=False),
            json.dumps(score.penalties, ensure_ascii=False),
            policy_version,
            now,
        ),
    )
    conn.execute(
        "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
        (next_stage, now, score.address),
    )
    if from_stage != next_stage:
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (score.address, from_stage, next_stage, score.reason, now),
        )


BLOCKING_SCORE_STAGES = {"rejected", "blocked_hygiene", "blocked_copyability"}
PAPER_READY_SCORE_STAGES = {"paper_candidate", "paper_approved", "live_eligible"}


def _score_target_stage(current_stage: str | None, scored_stage: str) -> str:
    if current_stage == "rejected":
        return current_stage
    if current_stage in {"blocked_hygiene", "blocked_copyability"} and scored_stage not in BLOCKING_SCORE_STAGES:
        return current_stage
    if current_stage == "live_eligible" and scored_stage in PAPER_READY_SCORE_STAGES:
        return current_stage
    return scored_stage


MIN_STABLE_READY_OBSERVATIONS = 3
MIN_STABLE_READY_SPAN_SECONDS = 3600


def apply_paper_quality_blocks(conn: sqlite3.Connection, *, now: int | None = None) -> int:
    blocked_at = now or int(time.time())
    rows = conn.execute(
        """
        SELECT cw.address, cw.candidate_stage, pwq.blockers_json
        FROM paper_wallet_quality pwq
        JOIN candidate_wallets cw
          ON cw.address = pwq.wallet
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND EXISTS (
              SELECT 1
              FROM json_each(CASE
                  WHEN json_valid(COALESCE(pwq.blockers_json, '[]')) THEN pwq.blockers_json
                  ELSE '[]'
              END) blocker
              WHERE blocker.value IN (
                  'non_positive_settled_roi',
                  'max_drawdown_exceeded',
                  'market_concentration_exceeded'
              )
          )
        """
    ).fetchall()
    for row in rows:
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            ("blocked_copyability", blocked_at, row["address"]),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["address"],
                row["candidate_stage"],
                "blocked_copyability",
                "paper_quality_risk_block",
                blocked_at,
            ),
        )
    ready_rows = conn.execute(
        """
        SELECT cw.address, cw.candidate_stage
        FROM paper_wallet_quality pwq
        JOIN candidate_wallets cw
          ON cw.address = pwq.wallet
        JOIN wallet_features wf
          ON wf.address = pwq.wallet
        JOIN wallet_processing_state wps
          ON wps.wallet = pwq.wallet
        WHERE pwq.production_ready = 1
          AND {paper_ready_sql}
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'live_eligible')
          AND lower(COALESCE(wf.hygiene_status, '')) IN ('clean', 'screened')
          AND wf.maker_fraction IS NOT NULL
          AND COALESCE(json_extract(wf.extra_json, '$.maker_fraction_source'), '')
              != 'public_activity_no_maker_flags_observed'
          AND COALESCE(wf.edge_retention_pct, 0) >= 60
          AND COALESCE(wf.walk_forward_consistency_pct, 0) >= 55
          AND COALESCE(wf.copy_event_count, 0) >= 5
          AND (
              NOT EXISTS (
                  SELECT 1
                  FROM sqlite_master
                  WHERE type = 'table'
                    AND name = 'paper_readiness_observations'
              )
              OR cw.address IN (
                  SELECT stable.wallet
                  FROM (
                      SELECT
                          pro.wallet AS wallet,
                          COUNT(*) AS observations,
                          MIN(observed_at) AS first_ready_at,
                          MAX(observed_at) AS last_ready_at
                      FROM paper_readiness_observations pro
                      WHERE pro.wallet = cw.address
                        AND pro.production_ready = 1
                        AND NOT EXISTS (
                            SELECT 1
                            FROM paper_readiness_observations newer_bad
                            WHERE newer_bad.wallet = pro.wallet
                              AND newer_bad.production_ready = 0
                              AND (
                                  newer_bad.observed_at > pro.observed_at
                                  OR (
                                      newer_bad.observed_at = pro.observed_at
                                      AND newer_bad.observation_id > pro.observation_id
                                  )
                              )
                        )
                      GROUP BY pro.wallet
                  ) stable
                  WHERE stable.observations >= ?
                    AND stable.last_ready_at - stable.first_ready_at >= ?
              )
          )
          AND NOT EXISTS (
              SELECT 1
              FROM json_each(CASE
                  WHEN json_valid(COALESCE(pwq.blockers_json, '[]')) THEN pwq.blockers_json
                  ELSE '[]'
              END) blocker
          )
          AND (
              cw.candidate_stage != 'blocked_copyability'
              OR EXISTS (
                  SELECT 1
                  FROM review_events re
                  WHERE re.address = cw.address
                    AND re.reason IN (
                        'paper_quality_non_positive_settled_roi',
                        'paper_quality_risk_block'
                    )
              )
          )
        """.format(paper_ready_sql=paper_evidence_ready_sql("wps")),
        (
            MIN_STABLE_READY_OBSERVATIONS,
            MIN_STABLE_READY_SPAN_SECONDS,
        ),
    ).fetchall()
    for row in ready_rows:
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            ("live_eligible", blocked_at, row["address"]),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["address"],
                row["candidate_stage"],
                "live_eligible",
                "paper_quality_production_ready",
                blocked_at,
            ),
        )
    return len(rows)


def apply_copyability_no_signal_blocks(
    conn: sqlite3.Connection,
    *,
    wallet: str = "",
    allow_running: bool = False,
    now: int | None = None,
) -> int:
    """Block manual-review wallets after a completed copyability scan finds no signal."""

    blocked_at = now or int(time.time())
    wallet = wallet.lower().strip()
    wallet_filter = "AND cw.address = ?" if wallet else ""
    status_filter = "cj.status IN ('done', 'running')" if wallet and allow_running else "cj.status = 'done'"
    rows = conn.execute(
        f"""
        WITH latest_copy_job AS (
            SELECT pj.*
            FROM pipeline_jobs pj
            JOIN (
                SELECT wallet, MAX(job_id) AS job_id
                FROM pipeline_jobs
                WHERE job_type = 'copyability_evidence'
                GROUP BY wallet
            ) latest
              ON latest.job_id = pj.job_id
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            latest_score.leader_score,
            latest_score.components_json,
            latest_score.penalties_json,
            latest_score.policy_version
        FROM candidate_wallets cw
        JOIN wallet_features wf
          ON wf.address = cw.address
        JOIN leader_latest_scores latest_score
          ON latest_score.address = cw.address
        JOIN latest_copy_job cj
          ON cj.wallet = cw.address
        WHERE cw.candidate_stage = 'needs_manual_review'
          AND latest_score.review_stage = 'needs_manual_review'
          AND latest_score.review_reason != 'copyability_scan_no_signal'
          AND {status_filter}
          AND COALESCE(json_extract(cj.input_json, '$.graph_scan_mode'), 'default') IN ('default', 'deep')
          AND COALESCE(wf.leader_in_degree, 0) = 0
          AND COALESCE(wf.copy_event_count, 0) = 0
          AND COALESCE(wf.copy_market_count, 0) = 0
          AND COALESCE(json_extract(wf.extra_json, '$.copy_candidate_pair_count'), -1) = 0
          AND COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), -1) = 0
          AND COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), -1) = 0
          AND COALESCE(json_extract(wf.extra_json, '$.copy_validated_pair_count'), 0) = 0
          {wallet_filter}
        """,
        (wallet,) if wallet else (),
    ).fetchall()
    for row in rows:
        conn.execute(
            """
            INSERT INTO leader_scores(
                address, leader_score, review_stage, review_reason,
                components_json, penalties_json, policy_version, scored_at
            ) VALUES (?, ?, 'blocked_copyability', 'copyability_scan_no_signal', ?, ?, ?, ?)
            """,
            (
                row["address"],
                row["leader_score"],
                row["components_json"],
                row["penalties_json"],
                row["policy_version"],
                blocked_at,
            ),
        )
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ?, updated_at = ? WHERE address = ?",
            ("blocked_copyability", blocked_at, row["address"]),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["address"],
                row["candidate_stage"],
                "blocked_copyability",
                "copyability_scan_no_signal",
                blocked_at,
            ),
        )
    return len(rows)


def latest_review_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT cw.address, cw.candidate_stage AS review_stage,
               COALESCE(ls.review_reason, '') AS review_reason,
               COALESCE(ls.leader_score, 0) AS leader_score,
               cw.sources, cw.labels, cw.notes, cw.links, cw.status,
               COALESCE(ls.components_json, '{}') AS components_json,
               COALESCE(ls.penalties_json, '{}') AS penalties_json
        FROM candidate_wallets cw
        LEFT JOIN leader_scores ls
          ON ls.score_id = (
              SELECT score_id FROM leader_scores
              WHERE address = cw.address
              ORDER BY scored_at DESC, score_id DESC
              LIMIT 1
          )
        ORDER BY cw.updated_at DESC, cw.address ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def persist_paper_order(
    conn: sqlite3.Connection,
    order_id: str,
    signal: TradeSignal,
    decision: ExecutionDecision,
) -> None:
    conn.execute(
        """
        INSERT INTO paper_orders(
            order_id, signal_id, wallet, market_slug, asset_id, outcome, side,
            price, stake_usd, route, accepted, reason, created_at,
            leader_price, executable_price, best_bid, best_ask,
            fillable_stake_usd, fee_usd, slippage_bps, quote_snapshot_at,
            quote_latency_ms, quote_source, quote_json, validation_cohort
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id) DO UPDATE SET
            price = excluded.price,
            stake_usd = excluded.stake_usd,
            route = excluded.route,
            accepted = excluded.accepted,
            reason = excluded.reason,
            created_at = excluded.created_at,
            leader_price = excluded.leader_price,
            executable_price = excluded.executable_price,
            best_bid = excluded.best_bid,
            best_ask = excluded.best_ask,
            fillable_stake_usd = excluded.fillable_stake_usd,
            fee_usd = excluded.fee_usd,
            slippage_bps = excluded.slippage_bps,
            quote_snapshot_at = excluded.quote_snapshot_at,
            quote_latency_ms = excluded.quote_latency_ms,
            quote_source = excluded.quote_source,
            quote_json = excluded.quote_json,
            validation_cohort = excluded.validation_cohort
        """,
        (
            order_id,
            signal.signal_id,
            signal.wallet,
            signal.market_slug,
            signal.asset_id,
            signal.outcome,
            signal.side,
            decision.executable_price or signal.price,
            decision.stake_usd,
            decision.route,
            1 if decision.accepted else 0,
            decision.reason,
            int(time.time()),
            signal.price,
            decision.executable_price,
            signal.best_bid,
            signal.best_ask,
            signal.fillable_stake_usd,
            decision.fee_usd,
            decision.slippage_bps,
            signal.quote_snapshot_at,
            signal.quote_latency_ms,
            signal.quote_source,
            signal.quote_json,
            signal.validation_cohort,
        ),
    )
    conn.commit()


def persist_paper_signal_evaluations(
    conn: sqlite3.Connection,
    evaluations: list[dict[str, Any]],
    *,
    evaluated_at: int,
) -> int:
    """Persist read-only quoteability evidence without creating paper orders."""

    rows = []
    for item in evaluations:
        signal_id = str(item.get("signal_id") or "")
        wallet = str(item.get("wallet") or "").lower()
        if not signal_id or not wallet:
            continue
        rows.append(
            (
                signal_id,
                wallet,
                str(item.get("candidate_stage") or ""),
                str(item.get("validation_cohort") or ""),
                str(item.get("market_slug") or ""),
                str(item.get("asset_id") or ""),
                str(item.get("outcome") or ""),
                str(item.get("side") or ""),
                _int(item.get("detected_at")),
                _int(item.get("signal_age_sec")),
                _int(item.get("max_actionable_signal_age_sec")),
                _float(item.get("leader_price")),
                _float(item.get("requested_stake_usd")) or 0.0,
                _float(item.get("best_bid")),
                _float(item.get("best_ask")),
                _float(item.get("executable_price")),
                _float(item.get("fillable_stake_usd")) or 0.0,
                _int(item.get("quote_snapshot_at")),
                _int(item.get("quote_latency_ms")),
                str(item.get("quote_source") or ""),
                str(item.get("quote_error") or ""),
                1 if item.get("accepted") else 0,
                1 if item.get("actionable") else 0,
                str(item.get("actionability_reason") or ""),
                str(item.get("decision_reason") or ""),
                _float(item.get("stake_usd")) or 0.0,
                str(item.get("route") or ""),
                _float(item.get("fee_usd")) or 0.0,
                _float(item.get("slippage_bps")),
                _float(item.get("leader_score")) or 0.0,
                _float(item.get("copy_event_count")) or 0.0,
                str(item.get("hygiene_status") or ""),
                int(evaluated_at),
                json.dumps(item, ensure_ascii=False, sort_keys=True)[:100_000],
            )
        )
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO paper_signal_evaluations(
            signal_id, wallet, candidate_stage, validation_cohort, market_slug,
            asset_id, outcome, side, detected_at, signal_age_sec,
            max_actionable_signal_age_sec, leader_price,
            requested_stake_usd, best_bid, best_ask, executable_price,
            fillable_stake_usd, quote_snapshot_at, quote_latency_ms,
            quote_source, quote_error, accepted, actionable,
            actionability_reason, decision_reason, stake_usd,
            route, fee_usd, slippage_bps, leader_score, copy_event_count,
            hygiene_status, evaluated_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id) DO UPDATE SET
            wallet = excluded.wallet,
            candidate_stage = excluded.candidate_stage,
            validation_cohort = excluded.validation_cohort,
            market_slug = excluded.market_slug,
            asset_id = excluded.asset_id,
            outcome = excluded.outcome,
            side = excluded.side,
            detected_at = excluded.detected_at,
            signal_age_sec = excluded.signal_age_sec,
            max_actionable_signal_age_sec = excluded.max_actionable_signal_age_sec,
            leader_price = excluded.leader_price,
            requested_stake_usd = excluded.requested_stake_usd,
            best_bid = excluded.best_bid,
            best_ask = excluded.best_ask,
            executable_price = excluded.executable_price,
            fillable_stake_usd = excluded.fillable_stake_usd,
            quote_snapshot_at = excluded.quote_snapshot_at,
            quote_latency_ms = excluded.quote_latency_ms,
            quote_source = excluded.quote_source,
            quote_error = excluded.quote_error,
            accepted = excluded.accepted,
            actionable = excluded.actionable,
            actionability_reason = excluded.actionability_reason,
            decision_reason = excluded.decision_reason,
            stake_usd = excluded.stake_usd,
            route = excluded.route,
            fee_usd = excluded.fee_usd,
            slippage_bps = excluded.slippage_bps,
            leader_score = excluded.leader_score,
            copy_event_count = excluded.copy_event_count,
            hygiene_status = excluded.hygiene_status,
            evaluated_at = excluded.evaluated_at,
            raw_json = excluded.raw_json
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def _feature_from_row(row: sqlite3.Row) -> WalletFeatures:
    extra = {}
    if row["extra_json"]:
        try:
            extra = json.loads(row["extra_json"])
        except json.JSONDecodeError:
            extra = {}
    return WalletFeatures(
        address=row["address"],
        cumulative_win_rate=row["cumulative_win_rate"],
        recent_30d_volume_usdc=row["recent_30d_volume_usdc"],
        net_pnl_usdc=row["net_pnl_usdc"],
        total_volume_usdc=row["total_volume_usdc"],
        event_win_rate=row["event_win_rate"],
        trade_win_rate=row["trade_win_rate"],
        avg_dca_entries=row["avg_dca_entries"],
        sell_pct=row["sell_pct"],
        bot_score=row["bot_score"],
        trades_per_day=row["trades_per_day"],
        median_gap_sec=row["median_gap_sec"],
        maker_fraction=row["maker_fraction"],
        leader_in_degree=row["leader_in_degree"],
        copy_event_count=row["copy_event_count"],
        copy_market_count=row["copy_market_count"],
        containment_pct_median=row["containment_pct_median"],
        copy_stream_roi=row["copy_stream_roi"],
        edge_retention_pct=row["edge_retention_pct"],
        walk_forward_consistency_pct=row["walk_forward_consistency_pct"],
        survival_score=row["survival_score"],
        single_market_pnl_share=row["single_market_pnl_share"],
        net_to_gross_exposure=row["net_to_gross_exposure"],
        hygiene_status=row["hygiene_status"],
        primary_category=row["primary_category"],
        last_active_days_ago=row["last_active_days_ago"],
        extra=extra,
    )


def _merge_position_feature(
    conn: sqlite3.Connection,
    address: str,
    positions: list[dict[str, Any]],
) -> None:
    if not positions:
        return
    total_current_value = sum(
        _float(p.get("currentValue") or p.get("current_value")) or 0.0
        for p in positions
    )
    total_cash_pnl = sum(
        _float(p.get("cashPnl") or p.get("cash_pnl")) or 0.0
        for p in positions
    )
    primary_category = _infer_primary_category(positions)
    existing = conn.execute(
        "SELECT * FROM wallet_features WHERE address = ?",
        (address,),
    ).fetchone()
    feature = _feature_from_row(existing) if existing else WalletFeatures(address=address)
    merged = WalletFeatures(
        address=address,
        cumulative_win_rate=feature.cumulative_win_rate,
        recent_30d_volume_usdc=feature.recent_30d_volume_usdc,
        net_pnl_usdc=feature.net_pnl_usdc if feature.net_pnl_usdc is not None else total_cash_pnl,
        total_volume_usdc=feature.total_volume_usdc,
        event_win_rate=feature.event_win_rate,
        trade_win_rate=feature.trade_win_rate,
        avg_dca_entries=feature.avg_dca_entries,
        sell_pct=feature.sell_pct,
        bot_score=feature.bot_score,
        trades_per_day=feature.trades_per_day,
        median_gap_sec=feature.median_gap_sec,
        maker_fraction=feature.maker_fraction,
        leader_in_degree=feature.leader_in_degree,
        copy_event_count=feature.copy_event_count,
        copy_market_count=feature.copy_market_count,
        containment_pct_median=feature.containment_pct_median,
        copy_stream_roi=feature.copy_stream_roi,
        edge_retention_pct=feature.edge_retention_pct,
        walk_forward_consistency_pct=feature.walk_forward_consistency_pct,
        survival_score=feature.survival_score,
        single_market_pnl_share=feature.single_market_pnl_share,
        net_to_gross_exposure=feature.net_to_gross_exposure,
        hygiene_status=feature.hygiene_status or "incomplete",
        primary_category=feature.primary_category or primary_category,
        last_active_days_ago=feature.last_active_days_ago,
        extra={
            **feature.extra,
            "open_positions_count": len(positions),
            "open_positions_current_value": round(total_current_value, 4),
            "open_positions_cash_pnl": round(total_cash_pnl, 4),
        },
    )
    upsert_wallet_feature(conn, merged)


def _merge_episode_features(conn: sqlite3.Connection, address: str) -> None:
    rows = conn.execute(
        "SELECT * FROM wallet_episodes WHERE address = ?",
        (address,),
    ).fetchall()
    if not rows:
        return
    buy_count = sum(int(r["buy_count"] or 0) for r in rows)
    sell_count = sum(int(r["sell_count"] or 0) for r in rows)
    total_trades = buy_count + sell_count
    total_bought = sum(float(r["bought_usdc"] or 0) for r in rows)
    total_sold = sum(float(r["sold_usdc"] or 0) for r in rows)
    closed = [r for r in rows if r["status"] == "closed"]
    wins = [r for r in closed if float(r["realized_pnl_est"] or 0) > 0]
    trade_win_rate = None
    if total_trades > 0:
        profitable_sells = sum(
            1
            for r in rows
            if int(r["sell_count"] or 0) > 0 and float(r["realized_pnl_est"] or 0) > 0
        )
        trade_win_rate = profitable_sells / total_trades
    event_win_rate = (len(wins) / len(closed)) if closed else None
    avg_dca = buy_count / len(rows) if rows else None
    sell_pct = (sell_count / total_trades * 100.0) if total_trades else None
    net_pnl = sum(float(r["realized_pnl_est"] or 0) for r in rows)
    existing = conn.execute("SELECT * FROM wallet_features WHERE address = ?", (address,)).fetchone()
    feature = _feature_from_row(existing) if existing else WalletFeatures(address=address)
    merged = WalletFeatures(
        address=address,
        cumulative_win_rate=_first_not_none(
            feature.cumulative_win_rate,
            event_win_rate,
            trade_win_rate,
        ),
        recent_30d_volume_usdc=feature.recent_30d_volume_usdc,
        net_pnl_usdc=feature.net_pnl_usdc if feature.net_pnl_usdc is not None else net_pnl,
        total_volume_usdc=feature.total_volume_usdc if feature.total_volume_usdc is not None else total_bought + total_sold,
        event_win_rate=_first_not_none(feature.event_win_rate, event_win_rate),
        trade_win_rate=_first_not_none(feature.trade_win_rate, trade_win_rate),
        avg_dca_entries=_first_not_none(feature.avg_dca_entries, avg_dca),
        sell_pct=feature.sell_pct if feature.sell_pct is not None else sell_pct,
        bot_score=feature.bot_score,
        trades_per_day=feature.trades_per_day,
        median_gap_sec=feature.median_gap_sec,
        maker_fraction=feature.maker_fraction,
        leader_in_degree=feature.leader_in_degree,
        copy_event_count=feature.copy_event_count,
        copy_market_count=feature.copy_market_count,
        containment_pct_median=feature.containment_pct_median,
        copy_stream_roi=feature.copy_stream_roi,
        edge_retention_pct=feature.edge_retention_pct,
        walk_forward_consistency_pct=feature.walk_forward_consistency_pct,
        survival_score=feature.survival_score,
        single_market_pnl_share=feature.single_market_pnl_share,
        net_to_gross_exposure=feature.net_to_gross_exposure,
        hygiene_status=feature.hygiene_status or "incomplete",
        primary_category=feature.primary_category,
        last_active_days_ago=feature.last_active_days_ago,
        extra={
            **feature.extra,
            "activity_trade_count": total_trades,
            "episode_count": len(rows),
            "closed_episode_count": len(closed),
            "episode_realized_pnl_est": round(net_pnl, 4),
        },
    )
    upsert_wallet_feature(conn, merged)


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _activity_key(
    *,
    timestamp: int,
    condition_id: Any,
    event_slug: Any,
    market_slug: Any,
    asset_id: Any,
    outcome: Any,
    event_type: Any,
    side: Any,
    price: float | None,
    size: float | None,
    usdc_size: float | None,
    transaction_hash: Any,
) -> str:
    payload = {
        "transaction_hash": str(transaction_hash or "").lower(),
        "timestamp": int(timestamp or 0),
        "condition_id": str(condition_id or ""),
        "event_slug": str(event_slug or ""),
        "market_slug": str(market_slug or ""),
        "asset_id": str(asset_id or ""),
        "outcome": str(outcome or ""),
        "event_type": str(event_type or ""),
        "side": str(side or ""),
        "price": None if price is None else str(price),
        "size": None if size is None else str(size),
        "usdc_size": None if usdc_size is None else str(usdc_size),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _legacy_activity_key(
    *,
    timestamp: int,
    condition_id: Any,
    event_slug: Any,
    market_slug: Any,
    asset_id: Any,
    outcome: Any,
    event_type: Any,
    side: Any,
    price: float | None,
    size: float | None,
    usdc_size: float | None,
    transaction_hash: Any,
) -> str:
    parts = (
        str(transaction_hash or "").lower(),
        str(timestamp or ""),
        str(condition_id or ""),
        str(event_slug or ""),
        str(market_slug or ""),
        str(asset_id or ""),
        str(outcome or ""),
        str(event_type or ""),
        str(side or ""),
        "" if price is None else str(price),
        "" if size is None else str(size),
        "" if usdc_size is None else str(usdc_size),
    )
    return "|".join(parts)


def _activity_key_from_event(event: dict[str, Any]) -> str:
    return _activity_key(
        timestamp=int(event.get("timestamp") or 0),
        condition_id=event.get("conditionId") or event.get("condition_id"),
        event_slug=event.get("eventSlug") or event.get("event_slug"),
        market_slug=event.get("slug") or event.get("marketSlug") or event.get("market_slug"),
        asset_id=str(event.get("asset") or event.get("asset_id") or ""),
        outcome=event.get("outcome"),
        event_type=str(event.get("type") or ""),
        side=event.get("side"),
        price=_float(event.get("price")),
        size=_float(event.get("size")),
        usdc_size=_float(event.get("usdcSize") or event.get("usdc_size")),
        transaction_hash=event.get("transactionHash") or event.get("transaction_hash"),
    )


def _first_not_none(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _infer_primary_category(positions: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for p in positions:
        text = " ".join(
            str(p.get(k) or "")
            for k in ("marketSlug", "eventSlug", "title", "market_slug", "event_slug")
        ).lower()
        category = "other"
        if any(k in text for k in ("trump", "biden", "harris", "election", "senate", "politic")):
            category = "politics"
        elif any(k in text for k in ("nba", "nfl", "mlb", "nhl", "ufc", "tennis", "soccer")):
            category = "sports"
        elif any(k in text for k in ("btc", "bitcoin", "eth", "crypto", "solana")):
            category = "crypto"
        elif any(k in text for k in ("weather", "hurricane", "temperature", "rain")):
            category = "weather"
        elif any(k in text for k in ("iran", "israel", "ukraine", "russia", "china", "war")):
            category = "geopolitics"
        counts[category] = counts.get(category, 0) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda item: item[1])[0]


def _json_list_field(value: Any) -> str:
    if value is None or value == "":
        return "[]"
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return json.dumps(parsed if isinstance(parsed, list) else [parsed], ensure_ascii=False)
        except json.JSONDecodeError:
            return json.dumps([value], ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return json.dumps([value], ensure_ascii=False)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _event_slug_from_market(market: dict[str, Any]) -> str | None:
    event = market.get("event")
    if isinstance(event, dict):
        return event.get("slug")
    events = market.get("events")
    if isinstance(events, list) and events and isinstance(events[0], dict):
        return events[0].get("slug")
    return market.get("eventSlug") or market.get("event_slug")


def _category_from_market(market: dict[str, Any]) -> str | None:
    category = market.get("category")
    if isinstance(category, dict):
        return category.get("slug") or category.get("label") or category.get("name")
    if category:
        return str(category)
    event = market.get("event")
    if isinstance(event, dict) and event.get("category"):
        return str(event.get("category"))
    return None
