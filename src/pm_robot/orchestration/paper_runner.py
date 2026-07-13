"""Paper execution loop for approved wallet-copy signals."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path

from pm_robot.clients.http import HttpClientError
from pm_robot.clients.polymarket_public import PublicPolymarketClient
from pm_robot.execution.paper_broker import PAPER_MAX_PRICE, PAPER_MIN_PRICE, PaperBroker
from pm_robot.execution.paper_quote import simulate_buy_quote
from pm_robot.models import CandidateStage, TradeSignal
from pm_robot.orchestration.evidence_readiness import paper_evidence_ready_sql
from pm_robot.orchestration.retry_policy import is_upstream_scheduling_error
from pm_robot.pipeline_terms import (
    COPYABILITY_OBSERVER_ACTIVITY_SOURCE,
    COPYABILITY_OBSERVER_REVIEW_REASON,
    EXPLORATORY_COPYABILITY_COHORT,
    PAPER_ELIGIBLE_CANDIDATE_STAGES,
)
from pm_robot.risk.eligibility import (
    exploratory_copyability_eligibility_status,
    paper_eligibility_status,
)
from pm_robot.storage.repository import persist_paper_observer_trials, persist_paper_signal_evaluations


PAPER_STAGES = PAPER_ELIGIBLE_CANDIDATE_STAGES
PAPER_BLOCKERS = ("non_positive_settled_roi", "non_positive_total_roi")
PAPER_OBSERVER_PREVIEW_SCHEMA_VERSION = "paper_observer_preview_v1"
PAPER_OBSERVER_EVALUATION_SCHEMA_VERSION = "paper_observer_evaluation_v1"
PAPER_OBSERVER_RETRY_COOLDOWN_SEC = 60
PAPER_OBSERVER_SELECTION_MODE = "incremental_unseen_market_first"
EXPLORATORY_COPYABILITY_STAGE = CandidateStage.BLOCKED_COPYABILITY.value
PAPER_OBSERVER_DIAGNOSTIC_WINDOWS = (
    (21_600, "6h"),
    (86_400, "24h"),
    (259_200, "72h"),
    (604_800, "168h"),
)


@dataclass(frozen=True)
class PaperRunSummary:
    signals_seen: int
    orders_recorded: int
    skipped: int
    rejections_recorded: int = 0


@dataclass(frozen=True)
class PaperObserverPreview:
    schema_version: str
    generated_at: int
    max_signal_age_sec: int
    exploratory_copyability_min_score: float | None
    min_timestamp: int
    paper_stage_wallets: int
    exploratory_copyability_wallets: int
    observer_wallets: int
    recent_buy_events: int
    paper_stage_recent_buy_events: int
    exploratory_copyability_recent_buy_events: int
    latest_activity_ts: int | None
    latest_buy_ts: int | None
    latest_activity_ingested_at: int | None
    latest_buy_ingested_at: int | None
    latest_activity_age_sec: int | None
    latest_buy_age_sec: int | None
    recent_buy_avg_ingest_lag_sec: float | None
    recent_buy_max_ingest_lag_sec: int | None
    no_signal_reason: str
    window_diagnostics: list[dict[str, object]]
    signals_seen: int
    signals: list[dict[str, object]]


@dataclass(frozen=True)
class PaperObserverEvaluation:
    schema_version: str
    generated_at: int
    max_signal_age_sec: int
    max_actionable_signal_age_sec: int
    retry_cooldown_sec: int
    selection_mode: str
    max_stake_usd: float
    exploratory_copyability_min_score: float | None
    signals_seen: int
    validation_signals: int
    exploratory_copyability_signals: int
    quotes_attempted: int
    quotes_succeeded: int
    accepted_signals: int
    actionable_signals: int
    rejected_signals: int
    stale_signal_rejections: int
    quote_error_signals: int
    actionable_rate_pct: float
    average_slippage_bps: float | None
    average_latency_ms: float | None
    evaluations_persisted: int
    trials_opened: int
    evaluations: list[dict[str, object]]


def preview_paper_observer(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    max_signal_age_sec: int = 21_600,
    exploratory_copyability_min_score: float | None = None,
    now: int | None = None,
) -> PaperObserverPreview:
    """Return eligible recent paper signals without quotes, orders, or writes."""

    generated_at = int(time.time()) if now is None else int(now)
    safe_limit = min(max(int(limit), 1), 250)
    safe_max_signal_age_sec = max(int(max_signal_age_sec), 0)
    min_timestamp = generated_at - safe_max_signal_age_sec if safe_max_signal_age_sec > 0 else 0
    rows = _candidate_activity_rows(
        conn,
        limit=safe_limit,
        min_timestamp=min_timestamp,
        include_watchlist_min_score=None,
        include_review_min_score=None,
        exploratory_copyability_min_score=exploratory_copyability_min_score,
    )
    signals = [_observation_from_row(row) for row in rows]
    context = _observer_activity_context(
        conn,
        min_timestamp=min_timestamp,
        generated_at=generated_at,
        exploratory_copyability_min_score=exploratory_copyability_min_score,
    )
    no_signal_reason = "" if signals else _observer_no_signal_reason(context, min_timestamp=min_timestamp)
    return PaperObserverPreview(
        schema_version=PAPER_OBSERVER_PREVIEW_SCHEMA_VERSION,
        generated_at=generated_at,
        max_signal_age_sec=safe_max_signal_age_sec,
        exploratory_copyability_min_score=(
            max(0.0, float(exploratory_copyability_min_score))
            if exploratory_copyability_min_score is not None
            else None
        ),
        min_timestamp=min_timestamp,
        paper_stage_wallets=int(context.get("paper_stage_wallets") or 0),
        exploratory_copyability_wallets=int(
            context.get("exploratory_copyability_wallets") or 0
        ),
        observer_wallets=int(context.get("observer_wallets") or 0),
        recent_buy_events=int(context.get("recent_buy_events") or 0),
        paper_stage_recent_buy_events=int(
            context.get("paper_stage_recent_buy_events") or 0
        ),
        exploratory_copyability_recent_buy_events=int(
            context.get("exploratory_copyability_recent_buy_events") or 0
        ),
        latest_activity_ts=context.get("latest_activity_ts"),
        latest_buy_ts=context.get("latest_buy_ts"),
        latest_activity_ingested_at=context.get("latest_activity_ingested_at"),
        latest_buy_ingested_at=context.get("latest_buy_ingested_at"),
        latest_activity_age_sec=context.get("latest_activity_age_sec"),
        latest_buy_age_sec=context.get("latest_buy_age_sec"),
        recent_buy_avg_ingest_lag_sec=context.get("recent_buy_avg_ingest_lag_sec"),
        recent_buy_max_ingest_lag_sec=context.get("recent_buy_max_ingest_lag_sec"),
        no_signal_reason=no_signal_reason,
        window_diagnostics=_observer_window_diagnostics(
            conn,
            generated_at=generated_at,
            limit=safe_limit,
            exploratory_copyability_min_score=exploratory_copyability_min_score,
        ),
        signals_seen=len(signals),
        signals=signals,
    )


def evaluate_paper_observer(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    max_stake_usd: float = 40.0,
    max_signal_age_sec: int = 21_600,
    max_actionable_signal_age_sec: int = 300,
    retry_cooldown_sec: int = PAPER_OBSERVER_RETRY_COOLDOWN_SEC,
    exploratory_copyability_min_score: float | None = None,
    now: int | None = None,
    persist: bool = False,
    client: PublicPolymarketClient | None = None,
) -> PaperObserverEvaluation:
    """Quote eligible paper-stage signals without writing paper orders."""

    generated_at = int(time.time()) if now is None else int(now)
    safe_limit = min(max(int(limit), 1), 250)
    safe_max_signal_age_sec = max(int(max_signal_age_sec), 0)
    safe_max_actionable_signal_age_sec = max(int(max_actionable_signal_age_sec), 0)
    safe_retry_cooldown_sec = max(int(retry_cooldown_sec), 0)
    safe_max_stake_usd = max(float(max_stake_usd), 1.0)
    min_timestamp = generated_at - safe_max_signal_age_sec if safe_max_signal_age_sec > 0 else 0
    rows = _candidate_activity_rows(
        conn,
        limit=safe_limit,
        min_timestamp=min_timestamp,
        include_watchlist_min_score=None,
        include_review_min_score=None,
        exploratory_copyability_min_score=exploratory_copyability_min_score,
        observer_retry_after=(
            generated_at - safe_retry_cooldown_sec
            if persist
            else None
        ),
    )
    broker = PaperBroker(ledger_path=None, conn=None, max_stake_usd=safe_max_stake_usd)
    client = client or PublicPolymarketClient(conn=conn)
    evaluations: list[dict[str, object]] = []
    for row in rows:
        evaluations.append(
            _evaluate_observation_row(
                row,
                broker=broker,
                client=client,
                generated_at=generated_at,
                max_actionable_signal_age_sec=safe_max_actionable_signal_age_sec,
            )
        )
    evaluations_persisted = 0
    trials_opened = 0
    if persist:
        evaluations_persisted = persist_paper_signal_evaluations(conn, evaluations, evaluated_at=generated_at)
        trials_opened = persist_paper_observer_trials(conn, evaluations, evaluated_at=generated_at)
    accepted = [row for row in evaluations if row.get("accepted")]
    actionable = [row for row in evaluations if row.get("actionable")]
    stale = [row for row in evaluations if row.get("actionability_reason") == "signal_too_old"]
    quote_errors = [row for row in evaluations if row.get("quote_error")]
    validation = [
        row for row in evaluations if row.get("validation_cohort") == "validation"
    ]
    exploratory = [
        row
        for row in evaluations
        if row.get("validation_cohort") == EXPLORATORY_COPYABILITY_COHORT
    ]
    latencies = [float(row["quote_latency_ms"]) for row in evaluations if row.get("quote_latency_ms") is not None]
    slippages = [float(row["slippage_bps"]) for row in accepted if row.get("slippage_bps") is not None]
    return PaperObserverEvaluation(
        schema_version=PAPER_OBSERVER_EVALUATION_SCHEMA_VERSION,
        generated_at=generated_at,
        max_signal_age_sec=safe_max_signal_age_sec,
        max_actionable_signal_age_sec=safe_max_actionable_signal_age_sec,
        retry_cooldown_sec=safe_retry_cooldown_sec,
        selection_mode=(PAPER_OBSERVER_SELECTION_MODE if persist else "snapshot_latest_first"),
        max_stake_usd=round(safe_max_stake_usd, 2),
        exploratory_copyability_min_score=(
            max(0.0, float(exploratory_copyability_min_score))
            if exploratory_copyability_min_score is not None
            else None
        ),
        signals_seen=len(evaluations),
        validation_signals=len(validation),
        exploratory_copyability_signals=len(exploratory),
        quotes_attempted=len(evaluations),
        quotes_succeeded=len(evaluations) - len(quote_errors),
        accepted_signals=len(accepted),
        actionable_signals=len(actionable),
        rejected_signals=len(evaluations) - len(accepted),
        stale_signal_rejections=len(stale),
        quote_error_signals=len(quote_errors),
        actionable_rate_pct=round((len(actionable) / len(evaluations)) * 100, 2) if evaluations else 0.0,
        average_slippage_bps=_average(slippages),
        average_latency_ms=_average(latencies),
        evaluations_persisted=evaluations_persisted,
        trials_opened=trials_opened,
        evaluations=evaluations,
    )


def run_paper(
    conn: sqlite3.Connection,
    *,
    ledger_path: Path | None = None,
    limit: int = 50,
    max_stake_usd: float = 40.0,
    max_signal_age_sec: int = 21_600,
    include_watchlist_min_score: float | None = None,
    include_review_min_score: float | None = None,
    client: PublicPolymarketClient | None = None,
) -> PaperRunSummary:
    broker = PaperBroker(ledger_path=ledger_path, conn=conn, max_stake_usd=max_stake_usd)
    client = client or PublicPolymarketClient(conn=conn)
    min_timestamp = int(time.time()) - max_signal_age_sec if max_signal_age_sec > 0 else 0
    rows = _candidate_activity_rows(
        conn,
        limit=limit,
        min_timestamp=min_timestamp,
        include_watchlist_min_score=include_watchlist_min_score,
        include_review_min_score=include_review_min_score,
    )
    recorded = 0
    skipped = 0
    rejections = 0
    for row in rows:
        signal = _signal_from_row(row)
        quote_started = time.monotonic()
        try:
            quote = simulate_buy_quote(client.book(signal.asset_id), broker.requested_stake(signal))
            signal = replace(
                signal,
                best_bid=quote.best_bid,
                best_ask=quote.best_ask,
                executable_price=quote.executable_price,
                fillable_stake_usd=quote.fillable_stake_usd,
                quote_snapshot_at=quote.snapshot_at,
                quote_latency_ms=int((time.monotonic() - quote_started) * 1000),
                quote_source=quote.source,
                quote_json=quote.raw_json,
            )
        except HttpClientError as exc:
            if is_upstream_scheduling_error(exc):
                raise
            signal = _signal_with_quote_error(signal, exc, quote_started=quote_started)
        except Exception as exc:
            signal = _signal_with_quote_error(signal, exc, quote_started=quote_started)
        decision = broker.evaluate(signal)
        broker.submit(signal, decision)
        if not decision.accepted:
            skipped += 1
            rejections += 1
            continue
        recorded += 1
    return PaperRunSummary(len(rows), recorded, skipped, rejections)


def _evaluate_observation_row(
    row: sqlite3.Row,
    *,
    broker: PaperBroker,
    client: PublicPolymarketClient,
    generated_at: int,
    max_actionable_signal_age_sec: int,
) -> dict[str, object]:
    signal = _signal_from_row(row)
    requested_stake = broker.requested_stake(signal)
    quote_started = time.monotonic()
    quote_error = ""
    try:
        quote = simulate_buy_quote(client.book(signal.asset_id), requested_stake)
        signal = replace(
            signal,
            best_bid=quote.best_bid,
            best_ask=quote.best_ask,
            executable_price=quote.executable_price,
            fillable_stake_usd=quote.fillable_stake_usd,
            quote_snapshot_at=quote.snapshot_at,
            quote_latency_ms=int((time.monotonic() - quote_started) * 1000),
            quote_source=quote.source,
            quote_json=quote.raw_json,
        )
    except HttpClientError as exc:
        if is_upstream_scheduling_error(exc):
            raise
        quote_error = f"{type(exc).__name__}: {str(exc)[:180]}"
        signal = _signal_with_quote_error(signal, exc, quote_started=quote_started)
    except Exception as exc:
        quote_error = f"{type(exc).__name__}: {str(exc)[:180]}"
        signal = _signal_with_quote_error(signal, exc, quote_started=quote_started)
    decision = broker.evaluate(signal)
    signal_age_sec = max(0, generated_at - signal.detected_at)
    signal_fresh = max_actionable_signal_age_sec <= 0 or signal_age_sec <= max_actionable_signal_age_sec
    actionable = bool(decision.accepted and signal_fresh)
    actionability_reason = _actionability_reason(
        accepted=decision.accepted,
        signal_fresh=signal_fresh,
        decision_reason=decision.reason,
    )
    return {
        **_observation_from_row(row),
        "signal_age_sec": signal_age_sec,
        "max_actionable_signal_age_sec": max_actionable_signal_age_sec,
        "requested_stake_usd": round(requested_stake, 2),
        "best_bid": signal.best_bid,
        "best_ask": signal.best_ask,
        "executable_price": signal.executable_price,
        "fillable_stake_usd": round(signal.fillable_stake_usd, 6),
        "quote_snapshot_at": signal.quote_snapshot_at,
        "quote_latency_ms": signal.quote_latency_ms,
        "quote_source": signal.quote_source,
        "quote_error": quote_error,
        "accepted": decision.accepted,
        "actionable": actionable,
        "actionability_reason": actionability_reason,
        "decision_reason": decision.reason,
        "stake_usd": decision.stake_usd,
        "route": decision.route,
        "fee_usd": decision.fee_usd,
        "slippage_bps": decision.slippage_bps if decision.accepted else None,
        "observer_action": "external_paper_evaluate_no_order",
    }


def _signal_with_quote_error(
    signal: TradeSignal,
    error: BaseException,
    *,
    quote_started: float,
) -> TradeSignal:
    return replace(
        signal,
        quote_snapshot_at=int(time.time()),
        quote_latency_ms=int((time.monotonic() - quote_started) * 1000),
        quote_source=f"quote_error:{type(error).__name__}",
    )


def _actionability_reason(*, accepted: bool, signal_fresh: bool, decision_reason: str) -> str:
    if not accepted:
        return decision_reason
    if not signal_fresh:
        return "signal_too_old"
    return "actionable_quote"


def _observation_from_row(row: sqlite3.Row) -> dict[str, object]:
    signal = _signal_from_row(row)
    return {
        "signal_id": signal.signal_id,
        "wallet": signal.wallet,
        "market_slug": signal.market_slug,
        "asset_id": signal.asset_id,
        "outcome": signal.outcome,
        "side": signal.side,
        "leader_price": signal.price,
        "detected_at": signal.detected_at,
        "ingested_at": int(row["ingested_at"] or 0),
        "ingest_lag_sec": max(0, int(row["ingested_at"] or 0) - signal.detected_at),
        "source": signal.source,
        "validation_cohort": signal.validation_cohort,
        "candidate_stage": row["candidate_stage"],
        "leader_score": float(row["leader_score"] or 0),
        "review_reason": row["review_reason"] or "",
        "copy_event_count": float(row["copy_event_count"] or 0),
        "hygiene_status": row["hygiene_status"] or "",
        "trade_events": int(row["trade_events"] or 0),
        "source_count": int(row["source_count"] or 0),
        "observer_action": "external_paper_quote_and_evaluate",
    }


def _observer_activity_context(
    conn: sqlite3.Connection,
    *,
    min_timestamp: int,
    generated_at: int,
    exploratory_copyability_min_score: float | None = None,
) -> dict[str, object]:
    """Summarize activity for the exact formal and research observer scopes."""

    placeholders = ",".join("?" for _ in PAPER_STAGES)
    params: list[object] = [*PAPER_STAGES]
    exploratory_scope_sql = "SELECT NULL AS address WHERE 0"
    if exploratory_copyability_min_score is not None:
        exploratory_scope_sql = _exploratory_copyability_scope_sql()
        params.extend(
            _exploratory_copyability_scope_params(
                exploratory_copyability_min_score
            )
        )
    params.extend([min_timestamp] * 5)
    row = conn.execute(
        f"""
        WITH observer_wallets AS (
            SELECT cw.address, 'validation' AS validation_cohort
            FROM candidate_wallets cw
            WHERE cw.candidate_stage IN ({placeholders})

            UNION ALL

            SELECT exploratory.address, '{EXPLORATORY_COPYABILITY_COHORT}' AS validation_cohort
            FROM ({exploratory_scope_sql}) exploratory
        )
        SELECT
            COUNT(DISTINCT CASE
                WHEN ow.validation_cohort = 'validation' THEN ow.address
                ELSE NULL
            END) AS paper_stage_wallets,
            COUNT(DISTINCT CASE
                WHEN ow.validation_cohort = '{EXPLORATORY_COPYABILITY_COHORT}' THEN ow.address
                ELSE NULL
            END) AS exploratory_copyability_wallets,
            COUNT(DISTINCT ow.address) AS observer_wallets,
            MAX(wa.timestamp) AS latest_activity_ts,
            MAX(wa.ingested_at) AS latest_activity_ingested_at,
            MAX(CASE
                WHEN wa.type = 'TRADE' AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                THEN wa.timestamp
                ELSE NULL
            END) AS latest_buy_ts,
            MAX(CASE
                WHEN wa.type = 'TRADE' AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                THEN wa.ingested_at
                ELSE NULL
            END) AS latest_buy_ingested_at,
            SUM(CASE
                WHEN wa.type = 'TRADE'
                  AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                  AND wa.timestamp >= ?
                THEN 1
                ELSE 0
            END) AS recent_buy_events
            ,
            SUM(CASE
                WHEN ow.validation_cohort = 'validation'
                  AND wa.type = 'TRADE'
                  AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                  AND wa.timestamp >= ?
                THEN 1
                ELSE 0
            END) AS paper_stage_recent_buy_events,
            SUM(CASE
                WHEN ow.validation_cohort = '{EXPLORATORY_COPYABILITY_COHORT}'
                  AND wa.type = 'TRADE'
                  AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                  AND wa.timestamp >= ?
                THEN 1
                ELSE 0
            END) AS exploratory_copyability_recent_buy_events,
            AVG(CASE
                WHEN wa.type = 'TRADE'
                  AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                  AND wa.timestamp >= ?
                THEN MAX(0, wa.ingested_at - wa.timestamp)
                ELSE NULL
            END) AS recent_buy_avg_ingest_lag_sec,
            MAX(CASE
                WHEN wa.type = 'TRADE'
                  AND UPPER(COALESCE(wa.side, '')) = 'BUY'
                  AND wa.timestamp >= ?
                THEN MAX(0, wa.ingested_at - wa.timestamp)
                ELSE NULL
            END) AS recent_buy_max_ingest_lag_sec
        FROM observer_wallets ow
        LEFT JOIN wallet_activity wa
          ON wa.address = ow.address
        """,
        params,
    ).fetchone()
    latest_activity_ts = _optional_int(row["latest_activity_ts"] if row else None)
    latest_buy_ts = _optional_int(row["latest_buy_ts"] if row else None)
    latest_activity_ingested_at = _optional_int(row["latest_activity_ingested_at"] if row else None)
    latest_buy_ingested_at = _optional_int(row["latest_buy_ingested_at"] if row else None)
    return {
        "paper_stage_wallets": int(row["paper_stage_wallets"] or 0) if row else 0,
        "exploratory_copyability_wallets": (
            int(row["exploratory_copyability_wallets"] or 0) if row else 0
        ),
        "observer_wallets": int(row["observer_wallets"] or 0) if row else 0,
        "recent_buy_events": int(row["recent_buy_events"] or 0) if row else 0,
        "paper_stage_recent_buy_events": (
            int(row["paper_stage_recent_buy_events"] or 0) if row else 0
        ),
        "exploratory_copyability_recent_buy_events": (
            int(row["exploratory_copyability_recent_buy_events"] or 0) if row else 0
        ),
        "exploratory_scope_enabled": exploratory_copyability_min_score is not None,
        "latest_activity_ts": latest_activity_ts,
        "latest_buy_ts": latest_buy_ts,
        "latest_activity_ingested_at": latest_activity_ingested_at,
        "latest_buy_ingested_at": latest_buy_ingested_at,
        "latest_activity_age_sec": _age_seconds(latest_activity_ts, generated_at),
        "latest_buy_age_sec": _age_seconds(latest_buy_ts, generated_at),
        "recent_buy_avg_ingest_lag_sec": _optional_float(row["recent_buy_avg_ingest_lag_sec"] if row else None),
        "recent_buy_max_ingest_lag_sec": _optional_int(row["recent_buy_max_ingest_lag_sec"] if row else None),
    }


def _exploratory_copyability_scope_sql() -> str:
    """Return the shared research-only wallet scope used by observer metrics."""

    return f"""
        SELECT cw.address
        FROM candidate_wallets cw
        LEFT JOIN leader_scores ls
          ON ls.score_id = (
              SELECT score_id
              FROM leader_scores
              WHERE address = cw.address
              ORDER BY scored_at DESC, score_id DESC
              LIMIT 1
          )
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN paper_wallet_quality pwq
          ON pwq.wallet = cw.address
        WHERE cw.candidate_stage = ?
          AND COALESCE(ls.review_reason, '') = ?
          AND COALESCE(ls.leader_score, 0) >= ?
          AND LOWER(COALESCE(wf.hygiene_status, '')) IN ('clean', 'screened')
          AND {paper_evidence_ready_sql('wps')}
          AND (
                COALESCE(wps.non_fast_trade_count, 0) >= 100
                OR (
                    SELECT COUNT(*)
                    FROM wallet_activity trade_events
                    WHERE trade_events.address = cw.address
                      AND trade_events.type = 'TRADE'
                ) >= 100
          )
          AND EXISTS (
              SELECT 1
              FROM candidate_source_events cse
              WHERE cse.address = cw.address
          )
          AND NOT EXISTS (
              SELECT 1
              FROM json_each(CASE
                  WHEN json_valid(COALESCE(pwq.blockers_json, '[]')) THEN pwq.blockers_json
                  ELSE '[]'
              END) blocker
              WHERE blocker.value IN ({','.join('?' for _ in PAPER_BLOCKERS)})
          )
    """


def _exploratory_copyability_scope_params(min_score: float) -> tuple[object, ...]:
    return (
        EXPLORATORY_COPYABILITY_STAGE,
        COPYABILITY_OBSERVER_REVIEW_REASON,
        max(0.0, float(min_score)),
        *PAPER_BLOCKERS,
    )


def _observer_no_signal_reason(context: dict[str, object], *, min_timestamp: int) -> str:
    if int(context.get("observer_wallets") or 0) <= 0:
        if context.get("exploratory_scope_enabled"):
            return "no_observer_wallets"
        return "no_paper_stage_wallets"
    latest_buy_ts = context.get("latest_buy_ts")
    if latest_buy_ts is None:
        return "no_buy_activity"
    if int(latest_buy_ts) < min_timestamp:
        return "latest_buy_outside_window"
    if int(context.get("recent_buy_events") or 0) > 0:
        return "recent_buys_failed_eligibility_or_deduped"
    return "no_recent_buy_activity"


def _observer_window_diagnostics(
    conn: sqlite3.Connection,
    *,
    generated_at: int,
    limit: int,
    exploratory_copyability_min_score: float | None = None,
) -> list[dict[str, object]]:
    diagnostics: list[dict[str, object]] = []
    for seconds, label in PAPER_OBSERVER_DIAGNOSTIC_WINDOWS:
        min_timestamp = generated_at - seconds
        rows = _candidate_activity_rows(
            conn,
            limit=limit,
            min_timestamp=min_timestamp,
            include_watchlist_min_score=None,
            include_review_min_score=None,
            exploratory_copyability_min_score=exploratory_copyability_min_score,
        )
        context = _observer_activity_context(
            conn,
            min_timestamp=min_timestamp,
            generated_at=generated_at,
            exploratory_copyability_min_score=exploratory_copyability_min_score,
        )
        diagnostics.append(
            {
                "window_label": label,
                "max_signal_age_sec": seconds,
                "recent_buy_events": int(context.get("recent_buy_events") or 0),
                "paper_stage_recent_buy_events": int(
                    context.get("paper_stage_recent_buy_events") or 0
                ),
                "exploratory_copyability_recent_buy_events": int(
                    context.get("exploratory_copyability_recent_buy_events") or 0
                ),
                "eligible_signals": len(rows),
                "avg_ingest_lag_sec": context.get("recent_buy_avg_ingest_lag_sec"),
                "max_ingest_lag_sec": context.get("recent_buy_max_ingest_lag_sec"),
                "no_signal_reason": "" if rows else _observer_no_signal_reason(context, min_timestamp=min_timestamp),
            }
        )
    return diagnostics


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _age_seconds(value: int | None, now: int) -> int | None:
    if value is None:
        return None
    return max(0, int(now) - int(value))


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _candidate_activity_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    min_timestamp: int,
    include_watchlist_min_score: float | None,
    include_review_min_score: float | None,
    exploratory_copyability_min_score: float | None = None,
    observer_retry_after: int | None = None,
) -> list[sqlite3.Row]:
    placeholders = ",".join("?" for _ in PAPER_STAGES)
    params: list[object] = [*PAPER_STAGES]
    _ = include_watchlist_min_score, include_review_min_score
    stage_filter = f"cw.candidate_stage IN ({placeholders})"
    if exploratory_copyability_min_score is not None:
        stage_filter = f"""(
            {stage_filter}
            OR (
                   cw.candidate_stage = ?
               AND COALESCE(ls.review_reason, '') = ?
               AND COALESCE(ls.leader_score, 0) >= ?
               AND LOWER(COALESCE(wf.hygiene_status, '')) IN ('clean', 'screened')
               AND {paper_evidence_ready_sql('wps')}
            )
        )"""
        params.extend(
            [
                EXPLORATORY_COPYABILITY_STAGE,
                COPYABILITY_OBSERVER_REVIEW_REASON,
                max(0.0, float(exploratory_copyability_min_score)),
            ]
        )
    params.extend([*PAPER_BLOCKERS, PAPER_MIN_PRICE, PAPER_MAX_PRICE, min_timestamp])
    observer_filter = ""
    paper_stage_order_values = ",".join(f"'{stage}'" for stage in PAPER_STAGES)
    observer_order = f"""
        CASE WHEN cw.candidate_stage IN ({paper_stage_order_values}) THEN 0 ELSE 1 END ASC,
        wa.timestamp DESC,
        wa.activity_id DESC
    """
    if observer_retry_after is not None:
        # Persisted observer cycles consume accepted signals once and retry failures after a cooldown.
        observer_filter = """
          AND NOT EXISTS (
              SELECT 1
              FROM paper_signal_evaluations pse
              WHERE pse.signal_id = 'activity-' || wa.activity_id
                AND (
                    pse.accepted = 1
                    OR pse.evaluated_at >= ?
                )
          )
        """
        observer_order = """
            CASE WHEN cw.candidate_stage IN ({paper_stage_order_values}) THEN 0 ELSE 1 END ASC,
            CASE WHEN EXISTS (
                SELECT 1
                FROM paper_observer_trials pot
                WHERE pot.wallet = wa.address
                  AND pot.market_slug = wa.market_slug
            ) THEN 1 ELSE 0 END ASC,
            CASE WHEN EXISTS (
                SELECT 1
                FROM paper_signal_evaluations prior_pse
                WHERE prior_pse.signal_id = 'activity-' || wa.activity_id
            ) THEN 1 ELSE 0 END ASC,
            wa.timestamp DESC,
            wa.activity_id DESC
        """.format(paper_stage_order_values=paper_stage_order_values)
        params.append(max(0, int(observer_retry_after)))
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT
            wa.activity_id,
            wa.address,
            wa.market_slug,
            wa.asset_id,
            wa.outcome,
            wa.side,
            wa.price,
            wa.timestamp,
            wa.ingested_at,
            cw.candidate_stage,
            COALESCE(ls.leader_score, 0) AS leader_score,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wf.copy_event_count, 0) AS copy_event_count,
            COALESCE(wf.hygiene_status, '') AS hygiene_status,
            COALESCE(pwq.blockers_json, '[]') AS paper_blockers_json,
            wps.discovery_tier,
            wps.evidence_status,
            wps.current_stage,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            (
                SELECT COUNT(*)
                FROM wallet_activity trade_events
                WHERE trade_events.address = wa.address
                  AND trade_events.type = 'TRADE'
            ) AS trade_events,
            (
                SELECT COUNT(*)
                FROM candidate_source_events cse
                WHERE cse.address = wa.address
            ) AS source_count
        FROM wallet_activity wa
        JOIN candidate_wallets cw
          ON cw.address = wa.address
        LEFT JOIN leader_scores ls
          ON ls.score_id = (
              SELECT score_id
              FROM leader_scores
              WHERE address = cw.address
              ORDER BY scored_at DESC, score_id DESC
              LIMIT 1
          )
        LEFT JOIN paper_wallet_quality pwq
          ON pwq.wallet = wa.address
        LEFT JOIN wallet_features wf
          ON wf.address = wa.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = wa.address
        WHERE {stage_filter}
          AND NOT EXISTS (
              SELECT 1
              FROM json_each(CASE
                  WHEN json_valid(COALESCE(pwq.blockers_json, '[]')) THEN pwq.blockers_json
                  ELSE '[]'
              END) blocker
              WHERE blocker.value IN ({",".join("?" for _ in PAPER_BLOCKERS)})
          )
          AND wa.type = 'TRADE'
          AND UPPER(COALESCE(wa.side, '')) = 'BUY'
          AND wa.price >= ?
          AND wa.price <= ?
          AND wa.timestamp >= ?
          AND COALESCE(wa.market_slug, '') != ''
          AND COALESCE(wa.asset_id, '') != ''
          AND NOT EXISTS (
              SELECT 1 FROM paper_orders po
              WHERE po.signal_id = 'activity-' || wa.activity_id
                AND (
                    po.accepted = 1
                    OR po.created_at >= strftime('%s','now') - 300
                )
          )
          {observer_filter}
        ORDER BY {observer_order}
        LIMIT ?
        """,
        params,
    ).fetchall()
    eligible_rows: list[sqlite3.Row] = []
    for row in rows:
        if row["candidate_stage"] in PAPER_STAGES:
            if paper_eligibility_status(conn, row["address"], facts=row).eligible:
                eligible_rows.append(row)
            continue
        if exploratory_copyability_min_score is not None and (
            exploratory_copyability_eligibility_status(
                conn,
                row["address"],
                min_score=exploratory_copyability_min_score,
                facts=row,
            ).eligible
        ):
            eligible_rows.append(row)
    return eligible_rows


def _signal_from_row(row: sqlite3.Row) -> TradeSignal:
    return TradeSignal(
        signal_id=f"activity-{row['activity_id']}",
        wallet=row["address"],
        market_slug=row["market_slug"],
        asset_id=row["asset_id"],
        outcome=row["outcome"] or "",
        side=row["side"],
        price=float(row["price"]),
        detected_at=int(row["timestamp"]),
        source=_signal_source(row),
        confidence=1.0,
        validation_cohort=(
            EXPLORATORY_COPYABILITY_COHORT
            if row["candidate_stage"] == EXPLORATORY_COPYABILITY_STAGE
            else "validation"
        ),
    )


def _signal_source(row: sqlite3.Row) -> str:
    if row["candidate_stage"] == EXPLORATORY_COPYABILITY_STAGE:
        return COPYABILITY_OBSERVER_ACTIVITY_SOURCE
    return "paper_wallet_activity"
