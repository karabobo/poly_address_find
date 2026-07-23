"""Low-volume queue for independent L6 validation of current L5 wallets."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.clients.polymarket_public import (
    MAX_ACTIVITY_LIMIT,
    MAX_CLOSED_POSITIONS_LIMIT,
    MAX_CURRENT_POSITIONS_LIMIT,
    PublicPolymarketClient,
)
from pm_robot.orchestration.retry_policy import (
    is_upstream_scheduling_error,
    upstream_aware_retry_at,
)
from pm_robot.orchestration.wallet_level_selection import SELECTION_POLICY_VERSION
from pm_robot.pipeline_terms import PipelineJobType
from pm_robot.research.current_elite import CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS
from pm_robot.research.l6_validation import (
    L6_VALIDATION_POLICY_VERSION,
    L6ValidationDecision,
    L6ValidationPolicy,
    L6ValidationResult,
    evaluate_l6_validation,
)
from pm_robot.research.wallet_history_summary import METHODOLOGY_VERSION
from pm_robot.storage.l6_validation_store import (
    L6ValidationArtifact,
    discard_l6_validation_artifact,
    persist_l6_validation_artifact,
)
from pm_robot.storage.repository import (
    claim_pipeline_job,
    complete_pipeline_job,
    enqueue_pipeline_job,
    retry_pipeline_job,
)
from pm_robot.storage.wallet_levels import advance_wallet_level, get_wallet_level
from pm_robot.wallet_levels import WalletLevel


JOB_TYPE = PipelineJobType.WALLET_L6_VALIDATE.value
DEFAULT_REFRESH_SECONDS = 14 * 86_400
DEFAULT_MAX_ACTIVE_JOBS = 10
MAX_CLOSED_POSITION_ROWS = 5_000
MAX_ACTIVITY_ROWS = 10_000
MAX_CURRENT_POSITION_ROWS = 5_000
MAX_HISTORICAL_ACTIVITY_OFFSET = 5_000
MAX_ACTIVITY_SPLIT_DEPTH = 24


@dataclass(frozen=True)
class L6ValidationPlanSummary:
    targets_seen: int
    jobs_enqueued: int
    active_jobs: int
    max_active_jobs: int
    status: str


@dataclass(frozen=True)
class L6ValidationWorkerSummary:
    jobs_attempted: int
    jobs_succeeded: int
    jobs_failed: int
    jobs_deferred: int
    validations_passed: int
    validations_warned: int
    validations_failed: int
    promoted_l6: int
    status: str
    error: str = ""


def plan_l6_validation_jobs(
    conn: sqlite3.Connection,
    *,
    limit: int = 5,
    max_active_jobs: int = DEFAULT_MAX_ACTIVE_JOBS,
    shard_count: int = 1,
    refresh_seconds: int = DEFAULT_REFRESH_SECONDS,
    validation_policy_version: str = L6_VALIDATION_POLICY_VERSION,
    selection_policy_version: str = SELECTION_POLICY_VERSION,
    now: int | None = None,
) -> L6ValidationPlanSummary:
    """Queue only current L5/L6 evidence; this does not run validation or alter levels."""

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    ts = int(time.time()) if now is None else int(now)
    active_jobs = int(
        conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE job_type = ? "
            "AND (status = 'running' OR (status = 'queued' AND attempts < max_attempts))",
            (JOB_TYPE,),
        ).fetchone()[0]
    )
    slots = max(0, int(limit))
    if max_active_jobs > 0:
        slots = min(slots, max(0, int(max_active_jobs) - active_jobs))
    if slots == 0:
        return L6ValidationPlanSummary(0, 0, active_jobs, max(0, int(max_active_jobs)), "ok")

    rows = conn.execute(
        """
        SELECT
            levels.wallet,
            levels.level,
            summary.artifact_id,
            summary.research_score,
            COALESCE(latest.validated_at, 0) AS latest_validated_at
        FROM wallet_levels AS levels
        JOIN wallet_history_summaries AS summary
          ON summary.wallet = levels.wallet
         AND summary.history_depth = 'deep'
        JOIN wallet_level_selections AS selection
          ON selection.wallet = levels.wallet
         AND selection.target_level = 'l5'
         AND selection.evidence_artifact_id = summary.artifact_id
         AND selection.policy_version = ?
         AND selection.selected = 1
        LEFT JOIN wallet_l6_validations AS latest
          ON latest.validation_id = (
              SELECT prior.validation_id
              FROM wallet_l6_validations AS prior
              WHERE prior.wallet = levels.wallet
                AND prior.policy_version = ?
              ORDER BY prior.validated_at DESC, prior.validation_id DESC
              LIMIT 1
          )
        WHERE levels.level IN ('l5', 'l6')
          AND levels.hard_risk_block = 0
          AND summary.methodology_version = ?
          AND summary.updated_at >= ?
          AND (
                latest.validation_id IS NULL
             OR latest.evidence_artifact_id != summary.artifact_id
             OR latest.validated_at <= ?
          )
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs AS active_job
              WHERE active_job.job_type = ?
                AND active_job.wallet = levels.wallet
                AND (
                      active_job.status = 'running'
                   OR (active_job.status = 'queued' AND active_job.attempts < active_job.max_attempts)
                )
          )
        ORDER BY
            CASE levels.level WHEN 'l5' THEN 0 ELSE 1 END,
            summary.research_score DESC,
            levels.wallet ASC
        LIMIT ?
        """,
        (
            selection_policy_version,
            validation_policy_version,
            METHODOLOGY_VERSION,
            ts - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
            ts - max(0, int(refresh_seconds)),
            JOB_TYPE,
            slots,
        ),
    ).fetchall()
    enqueued = 0
    refresh_bucket = ts // max(1, int(refresh_seconds))
    for row in rows:
        wallet = str(row["wallet"])
        artifact_id = str(row["artifact_id"])
        job_action = (
            f"validate_l6:{validation_policy_version}:{artifact_id}:{refresh_bucket}"
        )
        enqueued += int(
            enqueue_pipeline_job(
                conn,
                job_type=JOB_TYPE,
                wallet=wallet,
                job_action=job_action,
                job_scope="l6",
                priority=5 if str(row["level"]) == WalletLevel.L5.value else 10,
                shard=_wallet_shard(wallet, shard_count),
                input_data={
                    "evidence_artifact_id": artifact_id,
                    "validation_policy_version": validation_policy_version,
                    "selection_policy_version": selection_policy_version,
                    "planned_at": ts,
                },
                max_attempts=3,
                now=ts,
            )
        )
    return L6ValidationPlanSummary(
        targets_seen=len(rows),
        jobs_enqueued=enqueued,
        active_jobs=active_jobs,
        max_active_jobs=max(0, int(max_active_jobs)),
        status="ok",
    )


def run_l6_validation_worker(
    conn: sqlite3.Connection,
    *,
    archive_dir: Path,
    shard_index: int = 0,
    shard_count: int = 1,
    limit: int = 1,
    lease_seconds: int = 1_800,
    sleep_seconds: float = 0.05,
    worker_id: str = "",
    client: PublicPolymarketClient | None = None,
    policy: L6ValidationPolicy | None = None,
) -> L6ValidationWorkerSummary:
    """Fetch independent evidence, persist one verdict, and promote only passing L5 wallets."""

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be in [0, shard_count)")
    client = client or PublicPolymarketClient(conn=conn)
    active_policy = policy or L6ValidationPolicy()
    worker_id = worker_id or f"l6-validation-{shard_index}-{int(time.time())}"
    attempted = succeeded = failed = deferred = 0
    passed = warned = validation_failed = promoted = 0
    error = ""

    for index in range(max(0, int(limit))):
        if index and sleep_seconds > 0:
            time.sleep(sleep_seconds)
        job = claim_pipeline_job(
            conn,
            job_type=JOB_TYPE,
            shard=shard_index,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )
        if job is None:
            break
        attempted += 1
        wallet = str(job["wallet"])
        raw_artifact: L6ValidationArtifact | None = None
        try:
            input_data = _json_dict(job.get("input_json"))
            evidence_artifact_id = str(input_data.get("evidence_artifact_id") or "")
            selection_policy_version = str(
                input_data.get("selection_policy_version") or SELECTION_POLICY_VERSION
            )
            level = get_wallet_level(conn, wallet)
            if level.level not in {WalletLevel.L5, WalletLevel.L6} or level.hard_risk_block:
                _complete_skipped_job(conn, job=job, worker_id=worker_id, level=level.level)
                succeeded += 1
                continue
            now = int(time.time())
            if not _evidence_is_current(
                conn,
                wallet=wallet,
                artifact_id=evidence_artifact_id,
                selection_policy_version=selection_policy_version,
                now=now,
            ):
                _complete_skipped_job(conn, job=job, worker_id=worker_id, level=level.level)
                succeeded += 1
                continue

            coverage_start = now - active_policy.window_seconds
            positions, positions_complete = _fetch_positions(
                client, wallet, sleep_seconds=sleep_seconds
            )
            closed, closed_complete = _fetch_closed_positions(
                client, wallet, sleep_seconds=sleep_seconds
            )
            activity, activity_complete = _fetch_activity_window(
                client,
                wallet,
                start=coverage_start,
                end=now,
                sleep_seconds=sleep_seconds,
            )
            leaderboard = _fetch_leaderboard_cross_checks(client, wallet)
            result = evaluate_l6_validation(
                current_positions=positions,
                closed_positions=closed,
                activity=activity,
                leaderboard_rows=leaderboard,
                current_positions_complete=positions_complete,
                closed_positions_complete=closed_complete,
                activity_complete=activity_complete,
                now=now,
                policy=active_policy,
            )
            raw_artifact = persist_l6_validation_artifact(
                archive_dir=archive_dir,
                wallet=wallet,
                source_rows={
                    "activity": activity,
                    "closed_positions": closed,
                    "current_positions": positions,
                    "leaderboard": leaderboard,
                },
                now=now,
            )
            _persist_validation(
                conn,
                wallet=wallet,
                evidence_artifact_id=evidence_artifact_id,
                result=result,
                artifact=raw_artifact,
                now=now,
            )
            if result.decision is L6ValidationDecision.PASS:
                passed += 1
                if level.level is WalletLevel.L5:
                    decision = advance_wallet_level(
                        conn,
                        wallet,
                        to_level=WalletLevel.L6,
                        reason="independent_validation_passed",
                        policy_version=result.policy_version,
                        facts={
                            "validation_id": raw_artifact.artifact_id,
                            "evidence_artifact_id": evidence_artifact_id,
                            "realized_pnl_usdc": result.realized_pnl_usdc,
                            "recent_realized_pnl_usdc": result.recent_realized_pnl_usdc,
                            "official_all_pnl_usdc": result.official_all_pnl_usdc,
                            "official_profit_intensity": result.official_profit_intensity,
                            "active_weeks": result.active_weeks,
                            "positive_week_ratio": result.positive_week_ratio,
                            "abnormal_flags": list(result.abnormal_flags),
                        },
                        now=now,
                    )
                    promoted += int(decision.level is WalletLevel.L6)
            elif result.decision is L6ValidationDecision.WARNING:
                warned += 1
            else:
                validation_failed += 1
            if not complete_pipeline_job(
                conn,
                job_id=int(job["job_id"]),
                worker_id=worker_id,
                output_data={
                    "validation_id": raw_artifact.artifact_id,
                    "decision": result.decision.value,
                    "reason": result.reason,
                    "promoted_l6": bool(level.level is WalletLevel.L5 and result.decision is L6ValidationDecision.PASS),
                },
                now=now,
            ):
                raise RuntimeError("L6 validation job lease lost")
            conn.commit()
            succeeded += 1
        except Exception as exc:
            conn.rollback()
            if raw_artifact is not None:
                discard_l6_validation_artifact(archive_dir=archive_dir, artifact=raw_artifact)
            scheduler_deferred = is_upstream_scheduling_error(exc)
            deferred += int(scheduler_deferred)
            failed += int(not scheduler_deferred)
            error = str(exc)
            retry_pipeline_job(
                conn,
                job_id=int(job["job_id"]),
                worker_id=worker_id,
                error=error,
                next_attempt_at=upstream_aware_retry_at(
                    exc,
                    now=int(time.time()),
                    attempts=int(job.get("attempts") or 1),
                ),
                count_attempt=not scheduler_deferred,
            )
            conn.commit()
            if scheduler_deferred:
                break

    return L6ValidationWorkerSummary(
        jobs_attempted=attempted,
        jobs_succeeded=succeeded,
        jobs_failed=failed,
        jobs_deferred=deferred,
        validations_passed=passed,
        validations_warned=warned,
        validations_failed=validation_failed,
        promoted_l6=promoted,
        status="partial" if failed or deferred else "ok",
        error=error,
    )


def _fetch_positions(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    sleep_seconds: float,
) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while len(rows) < MAX_CURRENT_POSITION_ROWS:
        batch = client.positions(
            wallet,
            size_threshold=0.0,
            limit=MAX_CURRENT_POSITIONS_LIMIT,
            offset=offset,
        )
        rows.extend(batch[: MAX_CURRENT_POSITION_ROWS - len(rows)])
        if len(batch) < MAX_CURRENT_POSITIONS_LIMIT:
            return rows, True
        offset += MAX_CURRENT_POSITIONS_LIMIT
        _sleep(sleep_seconds)
    return rows, False


def _fetch_closed_positions(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    sleep_seconds: float,
) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while len(rows) < MAX_CLOSED_POSITION_ROWS:
        batch = client.closed_positions(
            wallet,
            limit=MAX_CLOSED_POSITIONS_LIMIT,
            offset=offset,
            size_threshold=0.0,
        )
        rows.extend(batch[: MAX_CLOSED_POSITION_ROWS - len(rows)])
        if len(batch) < MAX_CLOSED_POSITIONS_LIMIT:
            return rows, True
        offset += MAX_CLOSED_POSITIONS_LIMIT
        _sleep(sleep_seconds)
    return rows, False


def _fetch_activity_window(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    start: int,
    end: int,
    sleep_seconds: float,
) -> tuple[list[dict[str, Any]], bool]:
    if end < start:
        return [], True
    return _fetch_activity_range(
        client,
        wallet,
        start=start,
        end=end,
        max_rows=MAX_ACTIVITY_ROWS,
        sleep_seconds=sleep_seconds,
        split_depth=0,
    )


def _fetch_activity_range(
    client: PublicPolymarketClient,
    wallet: str,
    *,
    start: int,
    end: int,
    max_rows: int,
    sleep_seconds: float,
    split_depth: int,
) -> tuple[list[dict[str, Any]], bool]:
    """Page one time range, splitting it before the API's historical offset cap."""

    if max_rows <= 0:
        return [], False
    rows: list[dict[str, Any]] = []
    offset = 0
    while offset <= MAX_HISTORICAL_ACTIVITY_OFFSET:
        batch = client.activity(
            wallet,
            limit=MAX_ACTIVITY_LIMIT,
            offset=offset,
            start=start,
            end=end,
        )
        rows.extend(batch[: max_rows - len(rows)])
        if len(batch) < MAX_ACTIVITY_LIMIT:
            return rows, True
        if len(rows) >= max_rows:
            return rows, False
        if offset == MAX_HISTORICAL_ACTIVITY_OFFSET:
            break
        offset += MAX_ACTIVITY_LIMIT
        _sleep(sleep_seconds)

    if start >= end or split_depth >= MAX_ACTIVITY_SPLIT_DEPTH:
        return rows, False

    midpoint = start + (end - start) // 2
    _sleep(sleep_seconds)
    left_rows, left_complete = _fetch_activity_range(
        client,
        wallet,
        start=start,
        end=midpoint,
        max_rows=max_rows,
        sleep_seconds=sleep_seconds,
        split_depth=split_depth + 1,
    )
    if len(left_rows) >= max_rows:
        return left_rows, False
    right_rows, right_complete = _fetch_activity_range(
        client,
        wallet,
        start=midpoint + 1,
        end=end,
        max_rows=max_rows - len(left_rows),
        sleep_seconds=sleep_seconds,
        split_depth=split_depth + 1,
    )
    return left_rows + right_rows, left_complete and right_complete


def _fetch_leaderboard_cross_checks(
    client: PublicPolymarketClient,
    wallet: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period in ("WEEK", "MONTH", "ALL"):
        try:
            batch = client.trader_leaderboard(
                category="OVERALL",
                time_period=period,
                order_by="PNL",
                limit=1,
                offset=0,
                user=wallet,
            )
        except Exception:
            continue
        for row in batch:
            if isinstance(row, dict):
                enriched = dict(row)
                enriched["validationTimePeriod"] = period
                rows.append(enriched)
    return rows


def _persist_validation(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    evidence_artifact_id: str,
    result: L6ValidationResult,
    artifact: L6ValidationArtifact,
    now: int,
) -> None:
    conn.execute(
        """
        INSERT INTO wallet_l6_validations(
            validation_id, wallet, evidence_artifact_id, policy_version,
            decision, reason, coverage_start, coverage_end,
            closed_position_count, timestamped_closed_position_count,
            activity_count, active_weeks, positive_week_ratio,
            realized_pnl_usdc, recent_realized_pnl_usdc, open_pnl_usdc,
            max_drawdown_usdc, max_drawdown_ratio,
            top_market_profit_share, top_day_profit_share, churn_ratio,
            unrealized_profit_share, official_all_pnl_usdc,
            official_all_volume_usdc, official_profit_intensity,
            official_month_pnl_usdc, official_week_pnl_usdc,
            abnormal_flags_json, evidence_metrics_json,
            raw_relative_path, raw_byte_size, raw_checksum, validated_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            artifact.artifact_id,
            wallet,
            evidence_artifact_id,
            result.policy_version,
            result.decision.value,
            result.reason,
            result.coverage_start,
            result.coverage_end,
            result.closed_position_count,
            result.timestamped_closed_position_count,
            result.activity_count,
            result.active_weeks,
            result.positive_week_ratio,
            result.realized_pnl_usdc,
            result.recent_realized_pnl_usdc,
            result.open_pnl_usdc,
            result.max_drawdown_usdc,
            result.max_drawdown_ratio,
            result.top_market_profit_share,
            result.top_day_profit_share,
            result.churn_ratio,
            result.unrealized_profit_share,
            result.official_all_pnl_usdc,
            result.official_all_volume_usdc,
            result.official_profit_intensity,
            result.official_month_pnl_usdc,
            result.official_week_pnl_usdc,
            json.dumps(result.abnormal_flags),
            json.dumps(result.evidence_metrics, sort_keys=True),
            artifact.relative_path,
            artifact.byte_size,
            artifact.checksum,
            now,
            now,
        ),
    )


def _evidence_is_current(
    conn: sqlite3.Connection,
    *,
    wallet: str,
    artifact_id: str,
    selection_policy_version: str,
    now: int,
) -> bool:
    return (
        conn.execute(
            """
            SELECT 1
            FROM wallet_history_summaries AS summary
            JOIN wallet_level_selections AS selection
              ON selection.wallet = summary.wallet
             AND selection.target_level = 'l5'
             AND selection.evidence_artifact_id = summary.artifact_id
             AND selection.policy_version = ?
             AND selection.selected = 1
            WHERE summary.wallet = ?
              AND summary.artifact_id = ?
              AND summary.history_depth = 'deep'
              AND summary.methodology_version = ?
              AND summary.updated_at >= ?
            """,
            (
                selection_policy_version,
                wallet,
                artifact_id,
                METHODOLOGY_VERSION,
                int(now) - CURRENT_ELITE_EVIDENCE_MAX_AGE_SECONDS,
            ),
        ).fetchone()
        is not None
    )


def _complete_skipped_job(
    conn: sqlite3.Connection,
    *,
    job: dict[str, Any],
    worker_id: str,
    level: WalletLevel,
) -> None:
    complete_pipeline_job(
        conn,
        job_id=int(job["job_id"]),
        worker_id=worker_id,
        output_data={"status": "skipped", "level": level.value},
    )
    conn.commit()


def _wallet_shard(wallet: str, shard_count: int) -> int:
    return int(hashlib.sha256(wallet.encode("ascii")).hexdigest()[:8], 16) % shard_count


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
