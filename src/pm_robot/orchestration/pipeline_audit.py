"""Read-only end-to-end wallet discovery pipeline audit."""

from __future__ import annotations

import sqlite3
import time
from typing import Any

from pm_robot.orchestration.evidence_readiness import paper_evidence_ready_sql
from pm_robot.orchestration.feature_materializer import MATERIALIZER_VERSION
from pm_robot.pipeline_terms import (
    EvidenceJobStage,
    PipelineJobType,
)


V2_REQUIRED_TABLES = (
    "observed_wallets",
    "candidate_wallets",
    "wallet_processing_state",
    "pipeline_jobs",
    "wallet_evidence_summary",
    "wallet_features",
    "leader_scores",
)
ACTIVE_JOB_STATUSES = ("queued", "running")
PENDING_EVIDENCE_ACTIONS = (
    EvidenceJobStage.LIGHT_PENDING.value,
    EvidenceJobStage.MEDIUM_PENDING.value,
    EvidenceJobStage.DEEP_PENDING.value,
)
HIGH_PRIORITY_PENDING_JOB_PRIORITY = 10
PIPELINE_HANDOFF_GRACE_SECONDS = 600
SCORE_STALE_GRACE_SECONDS = PIPELINE_HANDOFF_GRACE_SECONDS
BLOCKING_CANDIDATE_STAGES = ("rejected", "blocked_hygiene", "blocked_copyability")
PAPER_READY_CANDIDATE_STAGES = ("paper_candidate", "paper_approved", "live_eligible")
ADDRESS_TABLE_COLUMNS = (
    ("candidate_wallets", "address"),
    ("candidate_source_events", "address"),
    ("observed_wallets", "wallet"),
    ("wallet_processing_state", "wallet"),
    ("pipeline_jobs", "wallet"),
)


def pipeline_audit_report(
    conn: sqlite3.Connection,
    *,
    top: int = 20,
    min_score: float = 40.0,
    paper_min_score: float = 70.0,
    policy_version: str = "",
    now: int | None = None,
) -> dict[str, Any]:
    """Return a read-only funnel report from observation through research export."""

    started_snapshot = False
    if not conn.in_transaction:
        conn.execute("BEGIN")
        started_snapshot = True
    generated_at = now or int(time.time())
    try:
        tables = _table_names(conn)
        schema = _schema_report(conn, tables)
        observation = _observation_report(conn, tables)
        candidates = _candidate_report(conn, tables, now=generated_at)
        address_quality = address_quality_report(conn, tables=tables)
        evidence = _evidence_report(
            conn,
            tables,
            policy_version=policy_version,
            now=generated_at,
        )
        queues = _queue_report(conn, tables, now=generated_at)
        scoring = _scoring_report(
            conn,
            tables,
            review_min_score=min_score,
            paper_min_score=paper_min_score,
        )
        export = _export_report(conn, tables)
        issues = _issues(
            schema=schema,
            observation=observation,
            candidates=candidates,
            address_quality=address_quality,
            evidence=evidence,
            queues=queues,
            scoring=scoring,
        )
        report = {
            "ok": True,
            "generated_at": generated_at,
            "min_score": min_score,
            "review_min_score": min_score,
            "paper_min_score": paper_min_score,
            "top": top,
            "schema": schema,
            "funnel": {
                "observation": observation,
                "candidates": candidates,
                "address_quality": address_quality,
                "evidence": evidence,
                "queues": queues,
                "scoring": scoring,
                "research_export": export,
            },
            "issues": issues,
            "samples": _samples(
                conn,
                tables,
                top=top,
                policy_version=policy_version,
                now=generated_at,
            ),
            "next_steps": _next_steps(issues),
        }
        return report
    finally:
        if started_snapshot:
            conn.execute("COMMIT")


def _schema_report(conn: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    missing = sorted(set(V2_REQUIRED_TABLES) - tables)
    return {
        "latest_migration": _latest_migration(conn, tables),
        "v2_ready": not missing,
        "missing_tables": missing,
        "table_count": len(tables),
    }


def _observation_report(conn: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    if "observed_wallets" not in tables:
        return {"available": False}
    total = _count(conn, "observed_wallets")
    promoted = _scalar(
        conn,
        "SELECT COUNT(*) FROM observed_wallets WHERE promoted_at IS NOT NULL",
    )
    promoted_missing_candidate = 0
    if "candidate_wallets" in tables:
        promoted_missing_candidate = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM observed_wallets ow
            LEFT JOIN candidate_wallets cw ON cw.address = ow.wallet
            WHERE ow.promoted_at IS NOT NULL
              AND cw.address IS NULL
            """,
        )
    return {
        "available": True,
        "total": total,
        "promoted": promoted,
        "unpromoted": max(total - promoted, 0),
        "promoted_missing_candidate": promoted_missing_candidate,
    }


def _candidate_report(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    now: int,
) -> dict[str, Any]:
    if "candidate_wallets" not in tables:
        return {"available": False}
    total = _count(conn, "candidate_wallets")
    report = {
        "available": True,
        "total": total,
        "stage_counts": _counts_by(conn, "candidate_wallets", "candidate_stage"),
        "without_processing_state": 0,
        "active_without_processing_state": 0,
        "active_without_processing_state_stale": 0,
        "handoff_grace_seconds": PIPELINE_HANDOFF_GRACE_SECONDS,
        "without_latest_score": 0,
        "without_wallet_features": 0,
        "without_source_events": 0,
        "without_activity_watermark": 0,
        "unvalidated_core_copy_active": 0,
        "unvalidated_core_copy_all": 0,
    }
    if "wallet_processing_state" in tables:
        report["without_processing_state"] = _candidate_missing(conn, "wallet_processing_state", "wallet")
        report["active_without_processing_state"] = _candidate_missing(
            conn, "wallet_processing_state", "wallet", active_candidates_only=True
        )
        report["active_without_processing_state_stale"] = _candidate_missing(
            conn,
            "wallet_processing_state",
            "wallet",
            active_candidates_only=True,
            first_seen_before=now - PIPELINE_HANDOFF_GRACE_SECONDS,
        )
    elif total:
        report["without_processing_state"] = total
        report["active_without_processing_state"] = total
        report["active_without_processing_state_stale"] = total
    if "leader_scores" in tables:
        report["without_latest_score"] = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM candidate_wallets cw
            WHERE NOT EXISTS (
                SELECT 1 FROM leader_scores ls WHERE ls.address = cw.address
            )
            """,
        )
    if "wallet_features" in tables:
        report["without_wallet_features"] = _candidate_missing(conn, "wallet_features", "address")
        report["unvalidated_core_copy_active"] = _unvalidated_core_copy_count(
            conn, tables, active_candidates_only=True
        )
        report["unvalidated_core_copy_all"] = _unvalidated_core_copy_count(
            conn, tables, active_candidates_only=False
        )
    if "candidate_source_events" in tables:
        report["without_source_events"] = _candidate_missing(conn, "candidate_source_events", "address")
    if "wallet_activity_watermarks" in tables:
        report["without_activity_watermark"] = _candidate_missing(
            conn, "wallet_activity_watermarks", "address"
        )
    return report


def address_quality_report(
    conn: sqlite3.Connection,
    *,
    tables: set[str] | None = None,
) -> dict[str, Any]:
    """Audit wallet-like fields for canonical EVM address shape."""

    current_tables = tables if tables is not None else _table_names(conn)
    by_table: dict[str, int] = {}
    total = 0
    for table, column in ADDRESS_TABLE_COLUMNS:
        if table not in current_tables:
            continue
        count = _invalid_address_count(conn, table, column)
        by_table[f"{table}.{column}"] = count
        total += count
    return {
        "available": bool(by_table),
        "invalid_address_rows": total,
        "by_table": by_table,
    }


def _evidence_report(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    policy_version: str,
    now: int,
) -> dict[str, Any]:
    if "wallet_processing_state" not in tables:
        return {"available": False}
    pending_without_job = _pending_without_active_job_count(
        conn,
        tables,
        policy_version=policy_version,
    )
    high_priority_pending_without_job = _pending_without_active_job_count(
        conn,
        tables,
        policy_version=policy_version,
        priority_ceiling=HIGH_PRIORITY_PENDING_JOB_PRIORITY,
    )
    return {
        "available": True,
        "total": _count(conn, "wallet_processing_state"),
        "tier_counts": _counts_by(conn, "wallet_processing_state", "discovery_tier"),
        "status_counts": _counts_by(conn, "wallet_processing_state", "evidence_status"),
        "next_action_counts": _counts_by(conn, "wallet_processing_state", "next_action"),
        "pending_without_active_job": pending_without_job,
        "high_priority_pending_without_active_job": high_priority_pending_without_job,
        "summary_ready_without_score": _summary_ready_without_score(conn, tables),
        "summary_ready_without_score_stale": _summary_ready_without_score(
            conn,
            tables,
            updated_before=now - PIPELINE_HANDOFF_GRACE_SECONDS,
        ),
        "summary_ready_score_stale": _summary_ready_score_stale(conn, tables, now=now),
        "handoff_grace_seconds": PIPELINE_HANDOFF_GRACE_SECONDS,
        "paused_fast_market": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM wallet_processing_state
            WHERE evidence_status = 'paused'
               OR next_action = 'manual_review_fast_market'
            """,
        ),
        "stale_next_action": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM wallet_processing_state
            WHERE next_action IN ('light_pending', 'medium_pending', 'deep_pending')
              AND next_action_at > 0
              AND next_action_at < ?
            """,
            (now - 86_400,),
        ),
    }


def _queue_report(conn: sqlite3.Connection, tables: set[str], *, now: int) -> dict[str, Any]:
    if "pipeline_jobs" not in tables:
        return {"available": False}
    return {
        "available": True,
        "status_counts": _pipeline_status_counts(conn),
        "active_by_type_action": _active_by_type_action(conn),
        "stale_running": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM pipeline_jobs
            WHERE status = 'running'
              AND lease_until <= ?
            """,
            (now,),
        ),
        "failed": _scalar(conn, "SELECT COUNT(*) FROM pipeline_jobs WHERE status = 'failed'"),
        "queued": _scalar(conn, "SELECT COUNT(*) FROM pipeline_jobs WHERE status = 'queued'"),
    }


def _scoring_report(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    review_min_score: float,
    paper_min_score: float,
) -> dict[str, Any]:
    if not {"candidate_wallets", "leader_scores"}.issubset(tables):
        return {"available": False}
    return {
        "available": True,
        "review_min_score": review_min_score,
        "paper_min_score": paper_min_score,
        "latest_score_stage_counts": _latest_candidate_score_stage_counts(conn),
        "high_score_manual_review": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM candidate_wallets cw
            WHERE cw.candidate_stage = 'needs_manual_review'
              AND COALESCE((
                  SELECT leader_score
                  FROM leader_scores ls
                  WHERE ls.address = cw.address
                  ORDER BY scored_at DESC, score_id DESC
                  LIMIT 1
              ), -1) >= ?
            """,
            (review_min_score,),
        ),
        "paper_score_manual_review": _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM candidate_wallets cw
            WHERE cw.candidate_stage = 'needs_manual_review'
              AND COALESCE((
                  SELECT leader_score
                  FROM leader_scores ls
                  WHERE ls.address = cw.address
                  ORDER BY scored_at DESC, score_id DESC
                  LIMIT 1
              ), -1) >= ?
            """,
            (paper_min_score,),
        ),
        "paper_candidate": _scalar(
            conn,
            "SELECT COUNT(*) FROM candidate_wallets WHERE candidate_stage = 'paper_candidate'",
        ),
        "live_eligible": _scalar(
            conn,
            "SELECT COUNT(*) FROM candidate_wallets WHERE candidate_stage = 'live_eligible'",
        ),
        "candidate_stage_differs_latest_review": _actionable_candidate_stage_drift_count(conn),
        "paper_stage_evidence_incomplete": _paper_stage_evidence_incomplete_count(conn, tables),
    }


def _export_report(conn: sqlite3.Connection, tables: set[str]) -> dict[str, Any]:
    report: dict[str, Any] = {"available": True}
    if "leader_publish" in tables:
        report["active_published_leaders"] = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM leader_publish
            WHERE revoked_at IS NULL
              AND expires_at > strftime('%s','now')
            """,
        )
    else:
        report["active_published_leaders"] = 0
    if "wallet_registry" in tables:
        report["winner_library_rows"] = _scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM wallet_registry
            WHERE registry_status IN ('publish_ready', 'paper_ready', 'retain_summary')
            """,
        )
    else:
        report["winner_library_rows"] = 0
    return report


def _issues(
    *,
    schema: dict[str, Any],
    observation: dict[str, Any],
    candidates: dict[str, Any],
    address_quality: dict[str, Any],
    evidence: dict[str, Any],
    queues: dict[str, Any],
    scoring: dict[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if schema.get("missing_tables"):
        issues.append(
            {
                "severity": "critical",
                "code": "schema_missing_v2_tables",
                "count": len(schema["missing_tables"]),
                "detail": schema["missing_tables"],
            }
        )
    _add_count_issue(
        issues,
        "warning",
        "observed_promoted_missing_candidate",
        observation.get("promoted_missing_candidate", 0),
    )
    _add_count_issue(
        issues,
        "warning",
        "candidate_missing_processing_state",
        candidates.get("active_without_processing_state_stale", 0),
    )
    _add_count_issue(
        issues,
        "warning",
        "candidate_unvalidated_core_copy_credit",
        candidates.get("unvalidated_core_copy_active", 0),
    )
    _add_count_issue(
        issues,
        "warning",
        "address_quality_invalid_wallet_rows",
        address_quality.get("invalid_address_rows", 0),
    )
    _add_count_issue(
        issues,
        "warning",
        "evidence_high_priority_pending_without_active_job",
        evidence.get("high_priority_pending_without_active_job", 0),
    )
    _add_count_issue(
        issues,
        "warning",
        "evidence_summary_ready_without_score",
        evidence.get("summary_ready_without_score_stale", 0),
    )
    _add_count_issue(
        issues,
        "warning",
        "evidence_summary_ready_score_stale",
        evidence.get("summary_ready_score_stale", 0),
    )
    _add_count_issue(issues, "warning", "pipeline_jobs_stale_running", queues.get("stale_running", 0))
    _add_count_issue(issues, "warning", "pipeline_jobs_failed", queues.get("failed", 0))
    _add_count_issue(
        issues,
        "info",
        "high_score_manual_review",
        scoring.get("high_score_manual_review", 0),
    )
    _add_count_issue(
        issues,
        "critical",
        "paper_stage_evidence_incomplete",
        scoring.get("paper_stage_evidence_incomplete", 0),
    )
    return issues


def _samples(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    top: int,
    policy_version: str,
    now: int,
) -> dict[str, Any]:
    return {
        "invalid_address_rows": _invalid_address_samples(conn, tables, top),
        "promoted_missing_candidate": _promoted_missing_candidate_samples(conn, tables, top),
        "pending_without_active_job": _pending_without_active_job_samples(
            conn,
            tables,
            top,
            policy_version=policy_version,
        ),
        "high_priority_pending_without_active_job": _pending_without_active_job_samples(
            conn,
            tables,
            top,
            policy_version=policy_version,
            priority_ceiling=HIGH_PRIORITY_PENDING_JOB_PRIORITY,
        ),
        "summary_ready_without_recent_score": _summary_ready_without_recent_score_samples(
            conn, tables, top, now=now
        ),
        "stale_running_jobs": _stale_running_job_samples(conn, tables, top, now=now),
        "unvalidated_core_copy_credit": _unvalidated_core_copy_samples(conn, tables, top),
        "paper_stage_evidence_incomplete": _paper_stage_evidence_incomplete_samples(conn, tables, top),
    }


def _next_steps(issues: list[dict[str, Any]]) -> list[str]:
    codes = {str(issue.get("code")) for issue in issues}
    steps: list[str] = []
    if "schema_missing_v2_tables" in codes:
        steps.append("run migrate before diagnosing current v2 wallet flow")
    if "observed_promoted_missing_candidate" in codes:
        steps.append("repair observed_wallets.promoted_at rows that do not have candidate_wallets")
    if "candidate_missing_processing_state" in codes:
        steps.append("run wallet-pipeline-state --materialize to rebuild wallet_processing_state")
    if "candidate_unvalidated_core_copy_credit" in codes:
        steps.append("run materialize-features with copyability refresh, then build-review --incremental")
    if "address_quality_invalid_wallet_rows" in codes:
        steps.append("repair or quarantine invalid wallet addresses before scheduling evidence jobs")
    if "evidence_high_priority_pending_without_active_job" in codes:
        steps.append("run wallet-pipeline-plan, then wallet-pipeline-worker shards")
    if "evidence_summary_ready_without_score" in codes or "evidence_summary_ready_score_stale" in codes:
        steps.append("run materialize-features, then build-review --incremental")
    if "pipeline_jobs_stale_running" in codes:
        steps.append("let pipeline workers reclaim expired running jobs or reset stale leases")
    if "pipeline_jobs_failed" in codes:
        steps.append("inspect failed pipeline_jobs.last_error before retrying")
    if "high_score_manual_review" in codes:
        steps.append("run pipeline-smoothness to separate copyability/hygiene/manual blockers")
    if "paper_stage_evidence_incomplete" in codes:
        steps.append("downgrade paper/live labels without L3 evidence, then resume wallet-pipeline backfill")
    if not steps:
        steps.append("no high-priority handoff break found; keep workers running and monitor copyability/backlog")
    return steps


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]) for row in rows}


def _latest_migration(conn: sqlite3.Connection, tables: set[str]) -> int:
    if "schema_migrations" not in tables:
        return 0
    return _scalar(conn, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations")


def _count(conn: sqlite3.Connection, table: str) -> int:
    return _scalar(conn, f"SELECT COUNT(*) FROM {table}")


def _counts_by(conn: sqlite3.Connection, table: str, column: str) -> dict[str, int]:
    rows = conn.execute(
        f"""
        SELECT COALESCE({column}, '') AS name, COUNT(*) AS count
        FROM {table}
        GROUP BY {column}
        ORDER BY count DESC, name ASC
        """
    ).fetchall()
    return {str(row["name"] or ""): int(row["count"] or 0) for row in rows}


def _candidate_missing(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    *,
    active_candidates_only: bool = False,
    first_seen_before: int | None = None,
) -> int:
    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    active_filter = f"AND cw.candidate_stage NOT IN ({blocking_placeholders})" if active_candidates_only else ""
    age_filter = "AND cw.first_seen_at < ?" if first_seen_before is not None else ""
    params: tuple[Any, ...] = BLOCKING_CANDIDATE_STAGES if active_candidates_only else ()
    if first_seen_before is not None:
        params = (*params, first_seen_before)
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM candidate_wallets cw
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} target WHERE target.{column} = cw.address
        )
        {active_filter}
        {age_filter}
        """,
        params,
    )


def _unvalidated_core_copy_count(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    active_candidates_only: bool,
) -> int:
    if not {"candidate_wallets", "wallet_features"}.issubset(tables):
        return 0
    stage_filter = (
        "AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')"
        if active_candidates_only
        else ""
    )
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM candidate_wallets cw
        JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN copy_leader_stats cls
          ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        WHERE (COALESCE(wf.leader_in_degree, 0) > 0
            OR COALESCE(wf.copy_event_count, 0) > 0
            OR COALESCE(wf.copy_market_count, 0) > 0)
          AND COALESCE(cls.qualified_follower_count, 0) = 0
          AND COALESCE(clp.backtest_trade_count, 0) = 0
          AND COALESCE(clp.edge_retention_pct, 0) = 0
          AND COALESCE(clp.walk_forward_consistency_pct, 0) = 0
          {stage_filter}
        """,
    )


def _valid_evm_address_sql(column: str) -> str:
    """SQLite predicate for canonical 0x-prefixed 20-byte hex addresses."""

    return (
        f"length(COALESCE({column}, '')) = 42 "
        f"AND substr(COALESCE({column}, ''), 1, 2) = '0x' "
        f"AND lower(substr(COALESCE({column}, ''), 3)) NOT GLOB '*[^0-9a-f]*'"
    )


def _invalid_address_count(conn: sqlite3.Connection, table: str, column: str) -> int:
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE COALESCE({column}, '') != ''
          AND NOT ({_valid_evm_address_sql(column)})
        """,
    )


def _summary_ready_without_score(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    updated_before: int | None = None,
) -> int:
    if not {"candidate_wallets", "wallet_processing_state", "leader_scores"}.issubset(tables):
        return 0
    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    registry_join, retention_filter = _active_raw_evidence_scope_sql(tables)
    age_filter = "AND COALESCE(wps.updated_at, 0) < ?" if updated_before is not None else ""
    params: tuple[Any, ...] = BLOCKING_CANDIDATE_STAGES
    if updated_before is not None:
        params = (*params, updated_before)
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        {registry_join}
        WHERE (wps.evidence_status = 'summary_ready' OR wps.next_action = 'score_wallet')
          AND cw.candidate_stage NOT IN ({blocking_placeholders})
          {retention_filter}
          {age_filter}
          AND NOT EXISTS (
              SELECT 1 FROM leader_scores ls WHERE ls.address = wps.wallet
          )
        """,
        params,
    )


def _summary_ready_score_stale(conn: sqlite3.Connection, tables: set[str], *, now: int) -> int:
    if not {"candidate_wallets", "wallet_processing_state", "leader_scores"}.issubset(tables):
        return 0
    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    registry_join, retention_filter = _active_raw_evidence_scope_sql(tables)
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        {registry_join}
        WHERE (wps.evidence_status = 'summary_ready' OR wps.next_action = 'score_wallet')
          AND cw.candidate_stage NOT IN ({blocking_placeholders})
          {retention_filter}
          AND COALESCE(wps.updated_at, 0) < ?
          AND EXISTS (
              SELECT 1 FROM leader_scores existing_score
              WHERE existing_score.address = wps.wallet
          )
          AND COALESCE((
              SELECT MAX(scored_at)
              FROM leader_scores ls
              WHERE ls.address = wps.wallet
          ), 0) < COALESCE(wps.updated_at, 0)
        """,
        (*BLOCKING_CANDIDATE_STAGES, now - SCORE_STALE_GRACE_SECONDS),
    )


def _pending_without_active_job_count(
    conn: sqlite3.Connection,
    tables: set[str],
    *,
    policy_version: str,
    priority_ceiling: int | None = None,
) -> int:
    if not {"wallet_processing_state", "pipeline_jobs"}.issubset(tables):
        return 0
    placeholders = ",".join("?" for _ in PENDING_EVIDENCE_ACTIONS)
    active_placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
    join_candidate = "JOIN candidate_wallets cw ON cw.address = wps.wallet" if "candidate_wallets" in tables else ""
    join_budget = (
        "LEFT JOIN evidence_backfill_budget ebb ON ebb.wallet = wps.wallet"
        if "evidence_backfill_budget" in tables
        else ""
    )
    join_registry, retention_filter = _active_raw_evidence_scope_sql(tables)
    approval_join, promotion_filter = _evidence_promotion_audit_sql(
        tables,
        policy_version=policy_version,
    )
    candidate_filter = ""
    params: list[Any] = [*PENDING_EVIDENCE_ACTIONS]
    if "candidate_wallets" in tables:
        blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
        candidate_filter = f"AND cw.candidate_stage NOT IN ({blocking_placeholders})"
        params.extend(BLOCKING_CANDIDATE_STAGES)
    priority_filter = ""
    if priority_ceiling is not None:
        priority_filter = "AND COALESCE(wps.priority, 100) <= ?"
        params.append(priority_ceiling)
    params.extend([PipelineJobType.WALLET_EVIDENCE_BACKFILL.value, *ACTIVE_JOB_STATUSES])
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM wallet_processing_state wps
        {join_candidate}
        {join_budget}
        {join_registry}
        {approval_join}
        WHERE wps.next_action IN ({placeholders})
          AND wps.evidence_status NOT IN ('paused', 'summary_ready')
          {retention_filter}
          {promotion_filter}
          {candidate_filter}
          {priority_filter}
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs pj
              WHERE pj.job_type = ?
                AND pj.wallet = wps.wallet
                AND pj.subject_key = wps.next_action
                AND pj.status IN ({active_placeholders})
          )
        """,
        tuple(params),
    )


def _pipeline_status_counts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT job_type, status, COUNT(*) AS count
        FROM pipeline_jobs
        GROUP BY job_type, status
        ORDER BY job_type ASC, count DESC, status ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _active_by_type_action(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT job_type, subject_key AS job_action, tier AS job_scope, status, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE status IN ('queued', 'running')
        GROUP BY job_type, subject_key, tier, status
        ORDER BY job_type ASC, subject_key ASC, tier ASC, status ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _latest_candidate_score_stage_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Count latest review stages for current candidates, not the full score history."""

    rows = conn.execute(
        """
        WITH ranked_scores AS (
            SELECT
                ls.address,
                ls.review_stage,
                ROW_NUMBER() OVER (
                    PARTITION BY ls.address
                    ORDER BY ls.scored_at DESC, ls.score_id DESC
                ) AS rn
            FROM leader_scores ls
        ),
        latest AS (
            SELECT
                ranked_scores.review_stage AS review_stage
            FROM candidate_wallets cw
            JOIN ranked_scores
              ON ranked_scores.address = cw.address
             AND ranked_scores.rn = 1
        )
        SELECT COALESCE(review_stage, '') AS review_stage, COUNT(*) AS count
        FROM latest
        GROUP BY review_stage
        ORDER BY count DESC, review_stage ASC
        """
    ).fetchall()
    return {str(row["review_stage"] or ""): int(row["count"] or 0) for row in rows}


def _actionable_candidate_stage_drift_count(conn: sqlite3.Connection) -> int:
    """Count only stage drift that the review sync is allowed to correct."""

    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    paper_ready_placeholders = ",".join("?" for _ in PAPER_READY_CANDIDATE_STAGES)
    return _scalar(
        conn,
        f"""
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
        SELECT COUNT(*)
        FROM candidate_wallets cw
        JOIN latest
          ON latest.address = cw.address
         AND latest.rn = 1
        WHERE cw.candidate_stage != latest.review_stage
          AND cw.candidate_stage != 'rejected'
          AND NOT (
              cw.candidate_stage IN ('blocked_hygiene', 'blocked_copyability')
              AND latest.review_stage NOT IN ({blocking_placeholders})
          )
          AND NOT (
              cw.candidate_stage = 'live_eligible'
              AND latest.review_stage IN ({paper_ready_placeholders})
          )
        """,
        (*BLOCKING_CANDIDATE_STAGES, *PAPER_READY_CANDIDATE_STAGES),
    )


def _paper_stage_evidence_incomplete_count(conn: sqlite3.Connection, tables: set[str]) -> int:
    if not {"candidate_wallets", "wallet_processing_state"}.issubset(tables):
        return 0
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM candidate_wallets cw
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
          AND NOT {paper_evidence_ready_sql('wps')}
        """,
    )


def _paper_stage_evidence_incomplete_samples(
    conn: sqlite3.Connection,
    tables: set[str],
    top: int,
) -> list[dict[str, Any]]:
    if not {"candidate_wallets", "wallet_processing_state"}.issubset(tables):
        return []
    rows = conn.execute(
        f"""
        SELECT
            cw.address AS wallet,
            cw.candidate_stage,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.current_stage, '') AS evidence_job_stage,
            COALESCE(wps.activity_count, 0) AS activity_count
        FROM candidate_wallets cw
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
          AND NOT {paper_evidence_ready_sql('wps')}
        ORDER BY cw.updated_at ASC, cw.address ASC
        LIMIT ?
        """,
        (top,),
    ).fetchall()
    return [dict(row) for row in rows]


def _promoted_missing_candidate_samples(
    conn: sqlite3.Connection, tables: set[str], top: int
) -> list[dict[str, Any]]:
    if not {"observed_wallets", "candidate_wallets"}.issubset(tables):
        return []
    rows = conn.execute(
        """
        SELECT ow.wallet, ow.promotion_reason, ow.promoted_at, ow.recent_max_trade_usdc
        FROM observed_wallets ow
        LEFT JOIN candidate_wallets cw ON cw.address = ow.wallet
        WHERE ow.promoted_at IS NOT NULL
          AND cw.address IS NULL
        ORDER BY ow.promoted_at DESC
        LIMIT ?
        """,
        (top,),
    ).fetchall()
    return [dict(row) for row in rows]


def _invalid_address_samples(
    conn: sqlite3.Connection, tables: set[str], top: int
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for table, column in ADDRESS_TABLE_COLUMNS:
        if table not in tables or len(samples) >= top:
            continue
        remaining = max(top - len(samples), 0)
        rows = conn.execute(
            f"""
            SELECT {column} AS wallet
            FROM {table}
            WHERE COALESCE({column}, '') != ''
              AND NOT ({_valid_evm_address_sql(column)})
            ORDER BY {column} ASC
            LIMIT ?
            """,
            (remaining,),
        ).fetchall()
        for row in rows:
            samples.append(
                {
                    "table": table,
                    "column": column,
                    "wallet": row["wallet"],
                }
            )
    return samples


def _pending_without_active_job_samples(
    conn: sqlite3.Connection,
    tables: set[str],
    top: int,
    *,
    policy_version: str,
    priority_ceiling: int | None = None,
) -> list[dict[str, Any]]:
    if not {"wallet_processing_state", "pipeline_jobs"}.issubset(tables):
        return []
    placeholders = ",".join("?" for _ in PENDING_EVIDENCE_ACTIONS)
    active_placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
    join_candidate = "JOIN candidate_wallets cw ON cw.address = wps.wallet" if "candidate_wallets" in tables else ""
    join_budget = (
        "LEFT JOIN evidence_backfill_budget ebb ON ebb.wallet = wps.wallet"
        if "evidence_backfill_budget" in tables
        else ""
    )
    join_registry, retention_filter = _active_raw_evidence_scope_sql(tables)
    approval_join, promotion_filter = _evidence_promotion_audit_sql(
        tables,
        policy_version=policy_version,
    )
    candidate_filter = ""
    params: list[Any] = [*PENDING_EVIDENCE_ACTIONS]
    if "candidate_wallets" in tables:
        blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
        candidate_filter = f"AND cw.candidate_stage NOT IN ({blocking_placeholders})"
        params.extend(BLOCKING_CANDIDATE_STAGES)
    priority_filter = ""
    if priority_ceiling is not None:
        priority_filter = "AND COALESCE(wps.priority, 100) <= ?"
        params.append(priority_ceiling)
    params.extend([PipelineJobType.WALLET_EVIDENCE_BACKFILL.value, *ACTIVE_JOB_STATUSES, top])
    rows = conn.execute(
        f"""
        SELECT wps.wallet, wps.discovery_tier AS evidence_tier, wps.evidence_status,
               wps.next_action, wps.priority, wps.updated_at
        FROM wallet_processing_state wps
        {join_candidate}
        {join_budget}
        {join_registry}
        {approval_join}
        WHERE wps.next_action IN ({placeholders})
          AND wps.evidence_status NOT IN ('paused', 'summary_ready')
          {retention_filter}
          {promotion_filter}
          {candidate_filter}
          {priority_filter}
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs pj
              WHERE pj.job_type = ?
                AND pj.wallet = wps.wallet
                AND pj.subject_key = wps.next_action
                AND pj.status IN ({active_placeholders})
          )
        ORDER BY wps.priority ASC, wps.updated_at ASC, wps.wallet ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _evidence_promotion_audit_sql(
    tables: set[str],
    *,
    policy_version: str,
) -> tuple[str, str]:
    """Keep audit backlog semantics aligned with the network worker guard."""

    required = {
        "evidence_backfill_budget",
        "leader_scores",
        "wallet_features",
        "wallet_activity",
    }
    if not required.issubset(tables):
        return "", "AND wps.next_action = 'light_pending'"
    materializer_version = MATERIALIZER_VERSION.replace("'", "''")
    current_policy_version = policy_version.replace("'", "''")
    join_sql = "LEFT JOIN wallet_features wf ON wf.address = wps.wallet"
    predicate = f"""
        AND (
            wps.next_action = 'light_pending'
            OR (
                COALESCE(CAST(json_extract(
                    ebb.evidence_json,
                    '$.promotion.approved'
                ) AS INTEGER), 0) = 1
                AND COALESCE(json_extract(
                    ebb.evidence_json,
                    '$.promotion.job_action'
                ), '') = wps.next_action
                AND COALESCE(json_extract(
                    ebb.evidence_json,
                    '$.promotion.policy_version'
                ), '') = '{current_policy_version}'
                AND COALESCE(CAST(json_extract(
                    ebb.evidence_json,
                    '$.promotion.feature_updated_at'
                ) AS INTEGER), 0) = COALESCE(wf.updated_at, 0)
                AND COALESCE(CAST(json_extract(
                    ebb.evidence_json,
                    '$.promotion.activity_count'
                ) AS INTEGER), -1) = (
                    SELECT COUNT(*)
                    FROM wallet_activity wa
                    WHERE wa.address = wps.wallet AND wa.type = 'TRADE'
                )
                AND COALESCE(json_extract(
                    ebb.evidence_json,
                    '$.promotion.materializer_version'
                ), '') = '{materializer_version}'
                AND COALESCE(json_extract(
                    wf.extra_json,
                    '$.feature_materializer_version'
                ), '') = '{materializer_version}'
                AND (
                    wps.next_action = 'medium_pending'
                    OR COALESCE((
                        SELECT ls.policy_version
                        FROM leader_scores ls
                        WHERE ls.address = wps.wallet
                        ORDER BY ls.scored_at DESC, ls.score_id DESC
                        LIMIT 1
                    ), '') = '{current_policy_version}'
                )
                AND COALESCE(ebb.stop_reason, '') =
                    'promotion_approved:' || wps.next_action || ':' ||
                    COALESCE(json_extract(
                        ebb.evidence_json,
                        '$.promotion.policy_version'
                    ), '') || ':' || CAST(COALESCE(wf.updated_at, 0) AS TEXT) || ':' ||
                    CAST((
                        SELECT COUNT(*)
                        FROM wallet_activity wa
                        WHERE wa.address = wps.wallet AND wa.type = 'TRADE'
                    ) AS TEXT)
            )
        )
    """
    return join_sql, predicate


def _summary_ready_without_recent_score_samples(
    conn: sqlite3.Connection, tables: set[str], top: int, *, now: int
) -> list[dict[str, Any]]:
    if not {"candidate_wallets", "wallet_processing_state", "leader_scores"}.issubset(tables):
        return []
    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    registry_join, retention_filter = _active_raw_evidence_scope_sql(tables)
    rows = conn.execute(
        f"""
        SELECT
            wps.wallet,
            wps.discovery_tier AS evidence_tier,
            wps.evidence_status,
            wps.next_action,
            wps.updated_at,
            COALESCE((SELECT MAX(scored_at) FROM leader_scores ls WHERE ls.address = wps.wallet), 0) AS latest_scored_at
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        {registry_join}
        WHERE (wps.evidence_status = 'summary_ready' OR wps.next_action = 'score_wallet')
          AND cw.candidate_stage NOT IN ({blocking_placeholders})
          {retention_filter}
          AND COALESCE(wps.updated_at, 0) < ?
          AND COALESCE((
              SELECT MAX(scored_at)
              FROM leader_scores ls
              WHERE ls.address = wps.wallet
          ), 0) < COALESCE(wps.updated_at, 0)
        ORDER BY wps.updated_at DESC
        LIMIT ?
        """,
        (*BLOCKING_CANDIDATE_STAGES, now - SCORE_STALE_GRACE_SECONDS, top),
    ).fetchall()
    return [dict(row) for row in rows]


def _active_raw_evidence_scope_sql(tables: set[str]) -> tuple[str, str]:
    """Exclude intentionally retired raw evidence from actionable audit backlogs."""

    if "wallet_registry" not in tables:
        return "", ""
    return (
        "LEFT JOIN wallet_registry wr ON wr.address = wps.wallet",
        "AND COALESCE(wr.raw_retention_tier, '') != 'summary_only'",
    )


def _stale_running_job_samples(
    conn: sqlite3.Connection, tables: set[str], top: int, *, now: int
) -> list[dict[str, Any]]:
    if "pipeline_jobs" not in tables:
        return []
    rows = conn.execute(
        """
        SELECT job_id, job_type, wallet, subject_key AS job_action, tier AS job_scope, lease_owner, lease_until, attempts
        FROM pipeline_jobs
        WHERE status = 'running'
          AND lease_until <= ?
        ORDER BY lease_until ASC, priority ASC
        LIMIT ?
        """,
        (now, top),
    ).fetchall()
    return [dict(row) for row in rows]


def _unvalidated_core_copy_samples(
    conn: sqlite3.Connection, tables: set[str], top: int
) -> list[dict[str, Any]]:
    if not {"candidate_wallets", "wallet_features"}.issubset(tables):
        return []
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT ls.*
            FROM leader_scores ls
            JOIN (
                SELECT address, MAX(score_id) AS score_id
                FROM leader_scores
                GROUP BY address
            ) latest_id
              ON latest_id.address = ls.address
             AND latest_id.score_id = ls.score_id
        )
        SELECT
            cw.address AS wallet,
            cw.candidate_stage,
            COALESCE(latest.leader_score, 0) AS leader_score,
            COALESCE(wf.leader_in_degree, 0) AS leader_in_degree,
            COALESCE(wf.copy_event_count, 0) AS copy_event_count,
            COALESCE(wf.copy_market_count, 0) AS copy_market_count,
            COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0) AS raw_copy_event_count,
            COALESCE(json_extract(wf.extra_json, '$.copy_validated_pair_count'), 0) AS validated_pair_count
        FROM candidate_wallets cw
        JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN latest
          ON latest.address = cw.address
        LEFT JOIN copy_leader_stats cls
          ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        WHERE cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
          AND (COALESCE(wf.leader_in_degree, 0) > 0
            OR COALESCE(wf.copy_event_count, 0) > 0
            OR COALESCE(wf.copy_market_count, 0) > 0)
          AND COALESCE(cls.qualified_follower_count, 0) = 0
          AND COALESCE(clp.backtest_trade_count, 0) = 0
          AND COALESCE(clp.edge_retention_pct, 0) = 0
          AND COALESCE(clp.walk_forward_consistency_pct, 0) = 0
        ORDER BY COALESCE(latest.leader_score, 0) DESC, cw.updated_at DESC, cw.address ASC
        LIMIT ?
        """,
        (top,),
    ).fetchall()
    return [dict(row) for row in rows]


def _add_count_issue(
    issues: list[dict[str, Any]],
    severity: str,
    code: str,
    count: Any,
) -> None:
    numeric_count = int(count or 0)
    if numeric_count > 0:
        issues.append({"severity": severity, "code": code, "count": numeric_count})


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        return 0
    return int(row[0] or 0)
