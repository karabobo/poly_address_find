"""Read-only checks for the opt-in paper execution profile.

This module intentionally avoids importing the wider app model layer so the NAS
host helper can run it with older system Python versions.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set


SCHEMA_VERSION = "execution_preflight_v1"
PAPER_REALTIME_AUDIT_SCHEMA_VERSION = "paper_realtime_audit_v1"
RTDS_WATCH_AUDIT_SCHEMA_VERSION = "rtds_watch_audit_v1"
DEFAULT_MAX_SIGNAL_AGE_SEC = 300
DEFAULT_ACTIVITY_LOOKBACK_SEC = 86_400
DEFAULT_RTDS_WATCH_MIN_SCORE = 65.0
PAPER_STAGE_WALLET_STAGES = ("paper_candidate", "paper_approved", "live_eligible")
RTDS_WATCH_CANDIDATE_STAGES = ("needs_manual_review",)
RTDS_WATCH_ACTIVITY_SOURCE = "polymarket_rtds_watch_activity"
DEFAULT_EXECUTION_SERVICES = ("paper-runner-loop", "paper-settle-loop", "publish-loop")
DEFAULT_HEARTBEATS = (
    "loop_paper_observer_activity",
    "loop_paper_observer_preview",
    "loop_paper_observer_evaluation",
    "loop_rtds_discovery",
    "loop_paper_runner",
    "loop_paper_settle",
    "loop_publish",
)


def parse_compose_rows(raw: str) -> List[Dict[str, Any]]:
    """Parse Docker Compose JSON output from either array or JSONL format."""

    text = (raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        rows = []
        for line in text.splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
        return rows
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


def running_execution_services(
    compose_rows: Sequence[Dict[str, Any]],
    execution_services: Iterable[str] = DEFAULT_EXECUTION_SERVICES,
) -> List[str]:
    """Return execution-profile service names that Compose reports as running."""

    service_set = set(execution_services)
    running = []
    for row in compose_rows:
        service = str(row.get("Service") or "")
        if service not in service_set:
            continue
        if str(row.get("State") or "").lower() == "running":
            running.append(service)
    return running


def execution_preflight_from_env() -> Dict[str, Any]:
    """Build the NAS helper preflight payload from environment variables."""

    root = Path(os.environ.get("PM_ROBOT_RUNTIME_ROOT") or ".")
    db_path = root / "data" / "pm_robot.sqlite"
    max_signal_age_sec = _int_env("PM_ROBOT_PAPER_RUN_MAX_SIGNAL_AGE_SEC", DEFAULT_MAX_SIGNAL_AGE_SEC)
    execution_services = tuple((os.environ.get("PM_ROBOT_EXECUTION_SERVICES") or "").split()) or DEFAULT_EXECUTION_SERVICES
    compose_json = os.environ.get("PM_ROBOT_EXECUTION_COMPOSE_PS_JSON") or ""
    compose_error = os.environ.get("PM_ROBOT_EXECUTION_COMPOSE_PS_ERROR") or ""
    return execution_preflight_for_db_path(
        db_path,
        max_signal_age_sec=max_signal_age_sec,
        execution_services=execution_services,
        compose_ps_json=compose_json,
        compose_error=compose_error,
    )


def execution_preflight_for_db_path(
    db_path: Path,
    *,
    now: Optional[int] = None,
    max_signal_age_sec: int = DEFAULT_MAX_SIGNAL_AGE_SEC,
    execution_services: Iterable[str] = DEFAULT_EXECUTION_SERVICES,
    compose_ps_json: str = "",
    compose_error: str = "",
) -> Dict[str, Any]:
    """Open the SQLite database read-only and return execution preflight status."""

    now_ts = int(now if now is not None else time.time())
    services = tuple(execution_services)
    compose_rows = parse_compose_rows(compose_ps_json)
    running = running_execution_services(compose_rows, services)
    if not db_path.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": now_ts,
            "state": "db_missing",
            "ready_to_start_execution": False,
            "recommended_action": "数据库不存在，先恢复 research/scoring 数据库。",
            "db_path": str(db_path),
            "execution_profile": {
                "services": sorted(services),
                "running_services": running,
                "compose_error": compose_error,
            },
        }

    uri = "%s?mode=ro" % db_path.resolve().as_uri()
    conn = sqlite3.connect(uri, uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA query_only = ON")
        return execution_preflight_status(
            conn,
            now=now_ts,
            max_signal_age_sec=max_signal_age_sec,
            execution_services=services,
            compose_rows=compose_rows,
            compose_error=compose_error,
        )
    finally:
        conn.close()


def execution_preflight_status(
    conn: sqlite3.Connection,
    *,
    now: Optional[int] = None,
    max_signal_age_sec: int = DEFAULT_MAX_SIGNAL_AGE_SEC,
    execution_services: Iterable[str] = DEFAULT_EXECUTION_SERVICES,
    compose_rows: Optional[Sequence[Dict[str, Any]]] = None,
    compose_error: str = "",
) -> Dict[str, Any]:
    """Return the read-only start gate for the opt-in execution profile."""

    now_ts = int(now if now is not None else time.time())
    max_age = int(max_signal_age_sec)
    signal_cutoff = 0 if max_age <= 0 else now_ts - max_age
    services = tuple(execution_services)
    running = running_execution_services(compose_rows or [], services)

    stage_counts = _rows(
        conn,
        """
        SELECT candidate_stage AS stage, COUNT(*) AS count
        FROM candidate_wallets
        GROUP BY candidate_stage
        ORDER BY count DESC, stage ASC
        """,
    ) if _table_exists(conn, "candidate_wallets") else []
    stage_count_map = {str(row["stage"]): int(row["count"] or 0) for row in stage_counts}
    paper_stage_wallets = sum(stage_count_map.get(stage, 0) for stage in PAPER_STAGE_WALLET_STAGES)

    paper_stage_quality = _one(
        conn,
        """
        SELECT
            SUM(CASE WHEN pwq.wallet IS NOT NULL THEN 1 ELSE 0 END) AS with_quality,
            SUM(CASE WHEN pwq.wallet IS NULL THEN 1 ELSE 0 END) AS missing_quality,
            SUM(CASE WHEN COALESCE(pwq.production_ready, 0) = 1 THEN 1 ELSE 0 END) AS production_ready
        FROM candidate_wallets cw
        LEFT JOIN paper_wallet_quality pwq ON pwq.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        """,
    ) if _tables_exist(conn, ("candidate_wallets", "paper_wallet_quality")) else {
        "with_quality": 0,
        "missing_quality": paper_stage_wallets,
        "production_ready": 0,
    }

    recent_buy = _one(
        conn,
        """
        SELECT
            COUNT(*) AS events,
            MAX(wa.timestamp) AS latest_ts,
            MAX(wa.ingested_at) AS latest_ingested_at,
            MAX(MAX(0, wa.ingested_at - wa.timestamp)) AS max_ingest_lag_sec
        FROM wallet_activity wa
        JOIN candidate_wallets cw ON cw.address = wa.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
          AND wa.type = 'TRADE'
          AND UPPER(COALESCE(wa.side, '')) = 'BUY'
          AND wa.timestamp >= ?
        """,
        (signal_cutoff,),
    ) if _tables_exist(conn, ("wallet_activity", "candidate_wallets")) else {}

    observer = _one(
        conn,
        """
        SELECT
            COUNT(*) AS evaluations,
            SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS accepted,
            SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS actionable,
            SUM(CASE WHEN actionability_reason = 'signal_too_old' THEN 1 ELSE 0 END) AS stale,
            SUM(CASE WHEN COALESCE(quote_error, '') != '' THEN 1 ELSE 0 END) AS quote_errors,
            COUNT(DISTINCT wallet) AS wallets,
            MAX(evaluated_at) AS latest_evaluated_at
        FROM paper_signal_evaluations
        WHERE evaluated_at >= ?
          AND wallet IN (
              SELECT address
              FROM candidate_wallets
              WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
          )
        """,
        (signal_cutoff,),
    ) if _tables_exist(conn, ("paper_signal_evaluations", "candidate_wallets")) else {}

    paper_orders = _one(
        conn,
        """
        SELECT
            COUNT(*) AS orders,
            SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS recent_orders,
            SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS accepted_orders,
            MAX(created_at) AS latest_order_at,
            SUM(CASE
                  WHEN wallet IN (
                      SELECT address
                      FROM candidate_wallets
                      WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
                  )
                  THEN 1 ELSE 0
                END) AS paper_stage_orders,
            SUM(CASE
                  WHEN wallet IN (
                      SELECT address
                      FROM candidate_wallets
                      WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
                  )
                   AND created_at >= ?
                  THEN 1 ELSE 0
                END) AS paper_stage_recent_orders,
            SUM(CASE
                  WHEN wallet IN (
                      SELECT address
                      FROM candidate_wallets
                      WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
                  )
                   AND accepted = 1
                  THEN 1 ELSE 0
                END) AS paper_stage_accepted_orders,
            MAX(CASE
                  WHEN wallet IN (
                      SELECT address
                      FROM candidate_wallets
                      WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
                  )
                  THEN created_at ELSE NULL
                END) AS paper_stage_latest_order_at
        FROM paper_orders
        """,
        (signal_cutoff, signal_cutoff),
    ) if _tables_exist(conn, ("paper_orders", "candidate_wallets")) else {}

    publish = _one(
        conn,
        """
        SELECT
            SUM(CASE
                  WHEN status = 'active'
                   AND revoked_at IS NULL
                   AND (expires_at = 0 OR expires_at > ?)
                  THEN 1 ELSE 0
                END) AS active,
            SUM(CASE WHEN status = 'revoked' THEN 1 ELSE 0 END) AS revoked,
            MAX(published_at) AS latest_published_at
        FROM leader_publish
        """,
        (now_ts,),
    ) if _table_exists(conn, "leader_publish") else {}
    realtime_coverage = _paper_realtime_coverage(
        conn,
        now=now_ts,
        signal_cutoff=signal_cutoff,
        max_signal_age_sec=max_age,
        lookback_sec=DEFAULT_ACTIVITY_LOOKBACK_SEC,
    )
    heartbeats = [_heartbeat(conn, name) for name in DEFAULT_HEARTBEATS]
    rtds_runtime = _rtds_runtime_diagnostics(
        heartbeats,
        now=now_ts,
        progress=_rtds_recent_progress(conn, now=now_ts),
    )

    recent_buy_events = _int(recent_buy.get("events"))
    observer_evaluations = _int(observer.get("evaluations"))
    observer_actionable = _int(observer.get("actionable"))
    observer_accepted = _int(observer.get("accepted"))
    observer_stale = _int(observer.get("stale"))
    observer_quote_errors = _int(observer.get("quote_errors"))

    if running:
        state = "execution_already_running"
        ready = False
        action = "execution profile 已在运行；继续观察 paper_orders、paper_wallet_quality 和 leader_publish。"
    elif paper_stage_wallets <= 0:
        state = "no_paper_stage_wallets"
        ready = False
        action = "没有 paper-stage 钱包；继续 research/scoring，不启动 execution。"
    elif observer_actionable > 0:
        state = "ready_to_start_execution"
        ready = True
        action = "已有当前窗口内 actionable 信号；可以考虑启动 execution-up 记录纸面订单。"
    elif recent_buy_events > 0 and observer_evaluations <= 0:
        state = "recent_buy_waiting_quote_evaluation"
        ready = False
        action = "有当前窗口 BUY，但 observer 还没形成报价评估；先等 paper-observer-loop。"
    elif recent_buy_events > 0:
        state = "recent_buy_not_actionable"
        ready = False
        action = "有当前窗口 BUY，但报价/滑点/时效未通过；暂不启动 execution。"
    elif observer_accepted > 0 and observer_stale >= observer_accepted:
        state = "quoteable_but_stale"
        ready = False
        action = "历史信号可报价但已过时；等待新的实时 BUY。"
    else:
        state = "waiting_fresh_buy_signal"
        ready = False
        action = "已有 paper 钱包，但当前没有可跟的新 BUY；启动 execution 只会空转。"

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_ts,
        "state": state,
        "ready_to_start_execution": ready,
        "recommended_action": action,
        "execution_profile": {
            "services": sorted(services),
            "running_services": running,
            "compose_error": compose_error,
        },
        "signal_window": {
            "max_signal_age_sec": max_age,
            "cutoff": signal_cutoff,
        },
        "wallets": {
            "paper_stage_wallets": paper_stage_wallets,
            "paper_candidate": stage_count_map.get("paper_candidate", 0),
            "paper_approved": stage_count_map.get("paper_approved", 0),
            "live_eligible": stage_count_map.get("live_eligible", 0),
            "paper_stage_with_quality": _int(paper_stage_quality.get("with_quality")),
            "paper_stage_missing_quality": _int(paper_stage_quality.get("missing_quality")),
            "paper_stage_production_ready": _int(paper_stage_quality.get("production_ready")),
        },
        "recent_paper_stage_buy": {
            "events": recent_buy_events,
            "latest_ts": recent_buy.get("latest_ts"),
            "latest_ingested_at": recent_buy.get("latest_ingested_at"),
            "max_ingest_lag_sec": recent_buy.get("max_ingest_lag_sec"),
        },
        "observer": {
            "evaluations": observer_evaluations,
            "accepted": observer_accepted,
            "actionable": observer_actionable,
            "stale": observer_stale,
            "quote_errors": observer_quote_errors,
            "wallets": _int(observer.get("wallets")),
            "latest_evaluated_at": observer.get("latest_evaluated_at"),
        },
        "paper_orders": {
            "orders": _int(paper_orders.get("orders")),
            "recent_orders": _int(paper_orders.get("recent_orders")),
            "accepted_orders": _int(paper_orders.get("accepted_orders")),
            "latest_order_at": paper_orders.get("latest_order_at"),
            "paper_stage_orders": _int(paper_orders.get("paper_stage_orders")),
            "paper_stage_recent_orders": _int(paper_orders.get("paper_stage_recent_orders")),
            "paper_stage_accepted_orders": _int(paper_orders.get("paper_stage_accepted_orders")),
            "paper_stage_latest_order_at": paper_orders.get("paper_stage_latest_order_at"),
        },
        "publish": {
            "active": _int(publish.get("active")),
            "revoked": _int(publish.get("revoked")),
            "latest_published_at": publish.get("latest_published_at"),
        },
        "paper_realtime_coverage": realtime_coverage,
        "rtds_runtime_diagnostics": rtds_runtime,
        "heartbeats": heartbeats,
        "write_boundary": (
            "preflight is read-only; execution-up is the separate switch that can write "
            "paper_orders, paper_wallet_quality, and leader_publish"
        ),
    }


def paper_realtime_audit_status(
    conn: sqlite3.Connection,
    *,
    now: Optional[int] = None,
    max_signal_age_sec: int = DEFAULT_MAX_SIGNAL_AGE_SEC,
    lookback_sec: int = DEFAULT_ACTIVITY_LOOKBACK_SEC,
    limit: int = 50,
) -> Dict[str, Any]:
    """Return per-wallet realtime blockers without changing execution state."""

    now_ts = int(now if now is not None else time.time())
    max_age = int(max_signal_age_sec)
    signal_cutoff = 0 if max_age <= 0 else now_ts - max_age
    capped_limit = max(1, min(int(limit), 250))
    rows = _paper_realtime_audit_rows(
        conn,
        now=now_ts,
        signal_cutoff=signal_cutoff,
        max_signal_age_sec=max_age,
        lookback_sec=lookback_sec,
        limit=capped_limit,
    )
    blocker_counts: Dict[str, int] = {}
    for row in rows:
        blocker = str(row.get("realtime_blocker") or "unknown")
        blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    return {
        "schema_version": PAPER_REALTIME_AUDIT_SCHEMA_VERSION,
        "generated_at": now_ts,
        "signal_window": {
            "max_signal_age_sec": max_age,
            "cutoff": signal_cutoff,
            "lookback_sec": int(lookback_sec),
        },
        "wallet_count": len(rows),
        "blocker_counts": [
            {"blocker": key, "count": blocker_counts[key]}
            for key in sorted(blocker_counts, key=lambda item: (-blocker_counts[item], item))
        ],
        "wallets": rows,
        "write_boundary": "paper realtime audit is read-only; it never writes paper orders or publish rows",
    }


def rtds_watch_audit_status(
    conn: sqlite3.Connection,
    *,
    now: Optional[int] = None,
    min_score: float = DEFAULT_RTDS_WATCH_MIN_SCORE,
    lookback_sec: int = DEFAULT_ACTIVITY_LOOKBACK_SEC,
    limit: int = 50,
) -> Dict[str, Any]:
    """Return near-paper RTDS watch wallets and their realtime evidence."""

    now_ts = int(now if now is not None else time.time())
    capped_limit = max(1, min(int(limit), 250))
    rows = _rtds_watch_audit_rows(
        conn,
        now=now_ts,
        min_score=float(min_score),
        lookback_sec=int(lookback_sec),
        limit=capped_limit,
    )
    state_counts: Dict[str, int] = {}
    for row in rows:
        state = str(row.get("watch_state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
    return {
        "schema_version": RTDS_WATCH_AUDIT_SCHEMA_VERSION,
        "generated_at": now_ts,
        "scope": {
            "candidate_stages": list(RTDS_WATCH_CANDIDATE_STAGES),
            "min_score": float(min_score),
            "lookback_sec": int(lookback_sec),
            "activity_source": RTDS_WATCH_ACTIVITY_SOURCE,
        },
        "wallet_count": len(rows),
        "state_counts": [
            {"state": key, "count": state_counts[key]}
            for key in sorted(state_counts, key=lambda item: (-state_counts[item], item))
        ],
        "wallets": rows,
        "write_boundary": "RTDS watch audit is read-only; watch wallets are not paper approved or published",
    }


def _paper_realtime_coverage(
    conn: sqlite3.Connection,
    *,
    now: int,
    signal_cutoff: int,
    max_signal_age_sec: int,
    lookback_sec: int,
) -> Dict[str, Any]:
    """Summarize whether paper-stage wallets are producing low-latency signals."""

    if not _tables_exist(conn, ("candidate_wallets", "wallet_activity")):
        return {
            "state": "missing_activity_tables",
            "next_action": "缺少候选或活动表，无法判断 paper 实时覆盖。",
        }
    lookback_start = max(0, int(now) - max(0, int(lookback_sec)))
    max_lag = max(0, int(max_signal_age_sec))
    row = _one(
        conn,
        """
        WITH paper AS (
            SELECT address
            FROM candidate_wallets
            WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        ),
        scoped AS (
            SELECT
                wa.*,
                COALESCE(
                    json_extract(CASE WHEN json_valid(COALESCE(wa.raw_json, '{}')) THEN wa.raw_json ELSE '{}' END, '$.source'),
                    ''
                ) AS source_name,
                MAX(0, wa.ingested_at - wa.timestamp) AS ingest_lag_sec
            FROM wallet_activity wa
            JOIN paper p
              ON p.address = wa.address
            WHERE wa.timestamp >= ?
        )
        SELECT
            (SELECT COUNT(*) FROM paper) AS paper_stage_wallets,
            COUNT(*) AS events_24h,
            SUM(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN 1 ELSE 0 END) AS buy_events_24h,
            SUM(CASE
                  WHEN type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                   AND ingest_lag_sec <= ?
                  THEN 1 ELSE 0
                END) AS timely_buy_events_24h,
            SUM(CASE
                  WHEN type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                   AND ingest_lag_sec > ?
                  THEN 1 ELSE 0
                END) AS delayed_buy_events_24h,
            SUM(CASE WHEN source_name = 'polymarket_rtds_activity' THEN 1 ELSE 0 END) AS rtds_events_24h,
            SUM(CASE
                  WHEN source_name = 'polymarket_rtds_activity'
                   AND type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                  THEN 1 ELSE 0
                END) AS rtds_buy_events_24h,
            SUM(CASE
                  WHEN type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                   AND timestamp >= ?
                  THEN 1 ELSE 0
                END) AS current_buy_events,
            SUM(CASE
                  WHEN type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                   AND timestamp >= ?
                   AND ingest_lag_sec <= ?
                  THEN 1 ELSE 0
                END) AS timely_buy_events,
            SUM(CASE
                  WHEN source_name = 'polymarket_rtds_activity'
                   AND type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                   AND timestamp >= ?
                  THEN 1 ELSE 0
                END) AS current_rtds_buy_events,
            SUM(CASE
                  WHEN type = 'TRADE'
                   AND UPPER(COALESCE(side, '')) = 'BUY'
                   AND timestamp >= ?
                   AND ingest_lag_sec > ?
                  THEN 1 ELSE 0
                END) AS delayed_current_buy_events,
            MAX(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN timestamp ELSE NULL END) AS latest_buy_ts,
            MAX(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingested_at ELSE NULL END) AS latest_buy_ingested_at,
            MAX(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS max_buy_ingest_lag_sec,
            AVG(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS avg_buy_ingest_lag_sec,
            MAX(CASE WHEN source_name = 'polymarket_rtds_activity' THEN timestamp ELSE NULL END) AS latest_rtds_ts,
            MAX(timestamp) AS latest_activity_ts,
            MAX(ingested_at) AS latest_activity_ingested_at
        FROM scoped
        """,
        (
            lookback_start,
            max_lag,
            max_lag,
            signal_cutoff,
            signal_cutoff,
            max_lag,
            signal_cutoff,
            signal_cutoff,
            max_lag,
        ),
    )
    paper_wallets = _int(row.get("paper_stage_wallets"))
    buy_events = _int(row.get("buy_events_24h"))
    current_buy_events = _int(row.get("current_buy_events"))
    timely_buy_events = _int(row.get("timely_buy_events"))
    current_rtds_buy_events = _int(row.get("current_rtds_buy_events"))
    rtds_buy_events = _int(row.get("rtds_buy_events_24h"))
    delayed_current_buy_events = _int(row.get("delayed_current_buy_events"))
    delayed_buy_events_24h = _int(row.get("delayed_buy_events_24h"))
    timely_buy_events_24h = _int(row.get("timely_buy_events_24h"))
    if paper_wallets <= 0:
        state = "no_paper_stage_wallets"
        action = "还没有 paper-stage 钱包；实时覆盖等待研究侧产生 paper_approved。"
    elif current_rtds_buy_events > 0:
        state = "rtds_current_buy_seen"
        action = "RTDS 已捕捉当前窗口 paper BUY；进入报价评估或 execution-preflight 判断。"
    elif timely_buy_events > 0:
        state = "timely_non_rtds_buy_seen"
        action = "当前窗口有及时入库 BUY，但不是 RTDS 来源；优先检查 observer 报价结果。"
    elif current_buy_events > 0 and delayed_current_buy_events >= current_buy_events:
        state = "current_buy_delayed_ingest"
        action = "当前窗口内有 BUY，但入库延迟超过可跟窗口；需要继续依赖 RTDS 或更低延迟来源。"
    elif buy_events > 0 and rtds_buy_events <= 0 and delayed_buy_events_24h >= buy_events and timely_buy_events_24h <= 0:
        state = "paper_buy_delayed_without_rtds"
        action = "24h 内 paper BUY 全部是延迟补进且 RTDS 未命中；这些不能作为可跟实时信号。"
    elif buy_events > 0 and rtds_buy_events <= 0:
        state = "paper_traded_but_no_rtds_hit"
        action = "24h 内 paper 钱包有 BUY，但 RTDS 未命中；继续观察 RTDS，必要时检查 RTDS 字段匹配或订阅质量。"
    elif buy_events > 0:
        state = "stale_buy_only"
        action = "24h 内有 paper BUY，但当前窗口没有可跟新信号。"
    else:
        state = "no_paper_buy_24h"
        action = "24h 内 paper 钱包没有 BUY；等待它们重新交易或扩大 paper 候选池。"
    return {
        "state": state,
        "next_action": action,
        "lookback_sec": int(lookback_sec),
        "paper_stage_wallets": paper_wallets,
        "events_24h": _int(row.get("events_24h")),
        "buy_events_24h": buy_events,
        "timely_buy_events_24h": timely_buy_events_24h,
        "delayed_buy_events_24h": delayed_buy_events_24h,
        "rtds_events_24h": _int(row.get("rtds_events_24h")),
        "rtds_buy_events_24h": rtds_buy_events,
        "current_buy_events": current_buy_events,
        "current_rtds_buy_events": current_rtds_buy_events,
        "timely_buy_events": timely_buy_events,
        "delayed_current_buy_events": delayed_current_buy_events,
        "latest_activity_ts": row.get("latest_activity_ts"),
        "latest_activity_ingested_at": row.get("latest_activity_ingested_at"),
        "latest_buy_ts": row.get("latest_buy_ts"),
        "latest_buy_ingested_at": row.get("latest_buy_ingested_at"),
        "latest_rtds_ts": row.get("latest_rtds_ts"),
        "max_buy_ingest_lag_sec": row.get("max_buy_ingest_lag_sec"),
        "avg_buy_ingest_lag_sec": _round_optional(row.get("avg_buy_ingest_lag_sec")),
    }


def _paper_realtime_audit_rows(
    conn: sqlite3.Connection,
    *,
    now: int,
    signal_cutoff: int,
    max_signal_age_sec: int,
    lookback_sec: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """Audit current paper-stage wallets and explain each realtime blocker."""

    if not _tables_exist(conn, ("candidate_wallets", "wallet_activity")):
        return []
    lookback_start = max(0, int(now) - max(0, int(lookback_sec)))
    max_lag = max(0, int(max_signal_age_sec))
    has_scores = _table_exists(conn, "leader_scores")
    has_quality = _table_exists(conn, "paper_wallet_quality")
    has_evaluations = _table_exists(conn, "paper_signal_evaluations")
    score_columns = (
        "ls.leader_score AS leader_score, ls.review_stage AS review_stage, ls.review_reason AS review_reason"
        if has_scores else
        "NULL AS leader_score, '' AS review_stage, '' AS review_reason"
    )
    score_join = (
        """
        LEFT JOIN leader_scores ls
          ON ls.address = p.address
         AND ls.score_id = (
             SELECT ls2.score_id
             FROM leader_scores ls2
             WHERE ls2.address = p.address
             ORDER BY ls2.scored_at DESC, ls2.score_id DESC
             LIMIT 1
         )
        """
        if has_scores else ""
    )
    quality_columns = (
        "COALESCE(pwq.production_ready, 0) AS production_ready, pwq.updated_at AS quality_updated_at"
        if has_quality else
        "0 AS production_ready, NULL AS quality_updated_at"
    )
    quality_join = "LEFT JOIN paper_wallet_quality pwq ON pwq.wallet = p.address" if has_quality else ""
    eval_columns = (
        """
        COALESCE(obs.evaluations, 0) AS observer_evaluations,
        COALESCE(obs.actionable, 0) AS observer_actionable,
        COALESCE(obs.accepted, 0) AS observer_accepted,
        obs.latest_evaluated_at AS latest_observer_at
        """
        if has_evaluations else
        """
        0 AS observer_evaluations,
        0 AS observer_actionable,
        0 AS observer_accepted,
        NULL AS latest_observer_at
        """
    )
    eval_cte = (
        """
        , observer AS (
            SELECT
                pse.wallet AS address,
                COUNT(*) AS evaluations,
                SUM(CASE WHEN pse.actionable = 1 THEN 1 ELSE 0 END) AS actionable,
                SUM(CASE WHEN pse.accepted = 1 THEN 1 ELSE 0 END) AS accepted,
                MAX(pse.evaluated_at) AS latest_evaluated_at
            FROM paper_signal_evaluations pse
            WHERE pse.evaluated_at >= ?
            GROUP BY pse.wallet
        )
        """
        if has_evaluations else ""
    )
    eval_join = "LEFT JOIN observer obs ON obs.address = p.address" if has_evaluations else ""
    params: List[Any] = [
        lookback_start,
        max_lag,
        max_lag,
        signal_cutoff,
        signal_cutoff,
        max_lag,
        signal_cutoff,
        signal_cutoff,
        max_lag,
    ]
    if has_evaluations:
        params.append(max(0, int(now) - max(0, int(lookback_sec))))
    params.append(limit)
    rows = _rows(
        conn,
        """
        WITH paper AS (
            SELECT address, candidate_stage, sources, updated_at
            FROM candidate_wallets
            WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        ),
        scoped AS (
            SELECT
                wa.address,
                wa.timestamp,
                wa.ingested_at,
                wa.type,
                wa.side,
                COALESCE(
                    json_extract(CASE WHEN json_valid(COALESCE(wa.raw_json, '{}')) THEN wa.raw_json ELSE '{}' END, '$.source'),
                    ''
                ) AS source_name,
                MAX(0, wa.ingested_at - wa.timestamp) AS ingest_lag_sec
            FROM wallet_activity wa
            JOIN paper p
              ON p.address = wa.address
            WHERE wa.timestamp >= ?
        ),
        activity AS (
            SELECT
                address,
                COUNT(*) AS events_24h,
                SUM(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN 1 ELSE 0 END) AS buy_events_24h,
                SUM(CASE
                      WHEN type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                       AND ingest_lag_sec <= ?
                      THEN 1 ELSE 0
                    END) AS timely_buy_events_24h,
                SUM(CASE
                      WHEN type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                       AND ingest_lag_sec > ?
                      THEN 1 ELSE 0
                    END) AS delayed_buy_events_24h,
                SUM(CASE
                      WHEN source_name = 'polymarket_rtds_activity'
                       AND type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                      THEN 1 ELSE 0
                    END) AS rtds_buy_events_24h,
                SUM(CASE
                      WHEN type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                       AND timestamp >= ?
                      THEN 1 ELSE 0
                    END) AS current_buy_events,
                SUM(CASE
                      WHEN type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                       AND timestamp >= ?
                       AND ingest_lag_sec <= ?
                      THEN 1 ELSE 0
                    END) AS timely_buy_events,
                SUM(CASE
                      WHEN source_name = 'polymarket_rtds_activity'
                       AND type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                       AND timestamp >= ?
                      THEN 1 ELSE 0
                    END) AS current_rtds_buy_events,
                SUM(CASE
                      WHEN type = 'TRADE'
                       AND UPPER(COALESCE(side, '')) = 'BUY'
                       AND timestamp >= ?
                       AND ingest_lag_sec > ?
                      THEN 1 ELSE 0
                    END) AS delayed_current_buy_events,
                MAX(timestamp) AS latest_activity_ts,
                MAX(ingested_at) AS latest_activity_ingested_at,
                MAX(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN timestamp ELSE NULL END) AS latest_buy_ts,
                MAX(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingested_at ELSE NULL END) AS latest_buy_ingested_at,
                MAX(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS max_buy_ingest_lag_sec,
                AVG(CASE WHEN type = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS avg_buy_ingest_lag_sec,
                MAX(CASE WHEN source_name = 'polymarket_rtds_activity' THEN timestamp ELSE NULL END) AS latest_rtds_ts
            FROM scoped
            GROUP BY address
        )
        """ + eval_cte + """
        SELECT
            p.address,
            p.candidate_stage,
            p.sources,
            p.updated_at,
            """ + score_columns + """,
            """ + quality_columns + """,
            COALESCE(a.events_24h, 0) AS events_24h,
            COALESCE(a.buy_events_24h, 0) AS buy_events_24h,
            COALESCE(a.timely_buy_events_24h, 0) AS timely_buy_events_24h,
            COALESCE(a.delayed_buy_events_24h, 0) AS delayed_buy_events_24h,
            COALESCE(a.rtds_buy_events_24h, 0) AS rtds_buy_events_24h,
            COALESCE(a.current_buy_events, 0) AS current_buy_events,
            COALESCE(a.current_rtds_buy_events, 0) AS current_rtds_buy_events,
            COALESCE(a.timely_buy_events, 0) AS timely_buy_events,
            COALESCE(a.delayed_current_buy_events, 0) AS delayed_current_buy_events,
            a.latest_activity_ts,
            a.latest_activity_ingested_at,
            a.latest_buy_ts,
            a.latest_buy_ingested_at,
            a.latest_rtds_ts,
            a.max_buy_ingest_lag_sec,
            a.avg_buy_ingest_lag_sec,
            """ + eval_columns + """
        FROM paper p
        LEFT JOIN activity a ON a.address = p.address
        """ + score_join + """
        """ + quality_join + """
        """ + eval_join + """
        ORDER BY
            CASE p.candidate_stage
              WHEN 'live_eligible' THEN 0
              WHEN 'paper_approved' THEN 1
              WHEN 'paper_candidate' THEN 2
              ELSE 3
            END,
            COALESCE(leader_score, 0) DESC,
            p.updated_at DESC
        LIMIT ?
        """,
        params,
    )
    audited = []
    for row in rows:
        item = dict(row)
        item["leader_score"] = _round_optional(item.get("leader_score"))
        item["production_ready"] = _int(item.get("production_ready"))
        for key in (
            "events_24h",
            "buy_events_24h",
            "timely_buy_events_24h",
            "delayed_buy_events_24h",
            "rtds_buy_events_24h",
            "current_buy_events",
            "current_rtds_buy_events",
            "timely_buy_events",
            "delayed_current_buy_events",
            "observer_evaluations",
            "observer_actionable",
            "observer_accepted",
        ):
            item[key] = _int(item.get(key))
        item["avg_buy_ingest_lag_sec"] = _round_optional(item.get("avg_buy_ingest_lag_sec"))
        blocker, action = _paper_realtime_blocker(item)
        item["realtime_blocker"] = blocker
        item["next_action"] = action
        audited.append(item)
    return audited


def _paper_realtime_blocker(row: Dict[str, Any]) -> tuple[str, str]:
    """Map one paper-stage wallet to the next realtime action."""

    if _int(row.get("observer_actionable")) > 0:
        return "ready_actionable_signal", "已有可报价的新信号；交给 execution-preflight 判断是否启动执行面。"
    if _int(row.get("current_rtds_buy_events")) > 0:
        return "rtds_buy_waiting_observer", "RTDS 已捕捉当前 BUY；等待 paper observer 报价评估。"
    if _int(row.get("timely_buy_events")) > 0:
        return "timely_buy_waiting_observer", "有及时入库 BUY；检查 observer 是否报价、滑点或时效未通过。"
    if _int(row.get("current_buy_events")) > 0 and _int(row.get("delayed_current_buy_events")) >= _int(row.get("current_buy_events")):
        return "buy_delayed_ingest", "有当前窗口 BUY，但入库延迟超过可跟窗口；需要 RTDS 或更低延迟来源。"
    if (
        _int(row.get("buy_events_24h")) > 0
        and _int(row.get("rtds_buy_events_24h")) <= 0
        and _int(row.get("delayed_buy_events_24h")) >= _int(row.get("buy_events_24h"))
        and _int(row.get("timely_buy_events_24h")) <= 0
    ):
        return "paper_buy_delayed_without_rtds", "24h 内 BUY 全部是延迟补进且 RTDS 未命中；这些不能作为可跟实时信号。"
    if _int(row.get("buy_events_24h")) > 0 and _int(row.get("rtds_buy_events_24h")) <= 0:
        return "paper_buy_without_rtds_hit", "24h 内有 BUY，但 RTDS 未命中；检查 RTDS 字段匹配、订阅质量或继续观察。"
    if _int(row.get("buy_events_24h")) > 0:
        return "stale_buy_only", "24h 内有 BUY，但已经不在可跟窗口；等待新交易。"
    return "no_buy_24h", "24h 内没有 BUY；等待该钱包交易或扩大 paper 候选池。"


def _rtds_watch_audit_rows(
    conn: sqlite3.Connection,
    *,
    now: int,
    min_score: float,
    lookback_sec: int,
    limit: int,
) -> List[Dict[str, Any]]:
    """List near-paper wallets observed by the RTDS research-only watch scope."""

    if not _tables_exist(conn, ("candidate_wallets", "leader_scores")):
        return []
    has_activity = _table_exists(conn, "wallet_activity")
    has_features = _table_exists(conn, "wallet_features")
    has_state = _table_exists(conn, "wallet_processing_state")
    lookback_start = max(0, int(now) - max(0, int(lookback_sec)))
    stage_placeholders = ",".join("?" for _ in RTDS_WATCH_CANDIDATE_STAGES)
    feature_columns = (
        """
        COALESCE(wf.copy_event_count, 0) AS copy_event_count,
        COALESCE(wf.copy_market_count, 0) AS copy_market_count,
        COALESCE(wf.copy_stream_roi, 0) AS copy_stream_roi,
        COALESCE(wf.hygiene_status, '') AS hygiene_status,
        COALESCE(wf.net_pnl_usdc, 0) AS net_pnl_usdc,
        COALESCE(wf.total_volume_usdc, 0) AS total_volume_usdc
        """
        if has_features else
        """
        0 AS copy_event_count,
        0 AS copy_market_count,
        0 AS copy_stream_roi,
        '' AS hygiene_status,
        0 AS net_pnl_usdc,
        0 AS total_volume_usdc
        """
    )
    feature_join = "LEFT JOIN wallet_features wf ON wf.address = w.address" if has_features else ""
    state_columns = (
        """
        COALESCE(wps.discovery_tier, '') AS discovery_tier,
        COALESCE(wps.evidence_status, '') AS evidence_status,
        COALESCE(wps.activity_count, 0) AS activity_count,
        COALESCE(wps.distinct_markets, 0) AS distinct_markets,
        COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count
        """
        if has_state else
        """
        '' AS discovery_tier,
        '' AS evidence_status,
        0 AS activity_count,
        0 AS distinct_markets,
        0 AS non_fast_trade_count
        """
    )
    state_join = "LEFT JOIN wallet_processing_state wps ON wps.wallet = w.address" if has_state else ""
    activity_cte = (
        """
        , watch_activity AS (
            SELECT
                wa.address,
                COUNT(*) AS watch_events_24h,
                SUM(CASE WHEN wa.type = 'TRADE' AND UPPER(COALESCE(wa.side, '')) = 'BUY' THEN 1 ELSE 0 END) AS watch_buy_events_24h,
                COUNT(DISTINCT COALESCE(wa.market_slug, '')) AS watch_markets_24h,
                SUM(CASE WHEN wa.timestamp >= ? THEN 1 ELSE 0 END) AS current_watch_events,
                MAX(wa.timestamp) AS latest_watch_ts,
                MAX(wa.ingested_at) AS latest_watch_ingested_at,
                MAX(MAX(0, wa.ingested_at - wa.timestamp)) AS max_watch_ingest_lag_sec
            FROM wallet_activity wa
            JOIN watch w
              ON w.address = wa.address
            WHERE wa.timestamp >= ?
              AND COALESCE(
                    json_extract(CASE WHEN json_valid(COALESCE(wa.raw_json, '{}')) THEN wa.raw_json ELSE '{}' END, '$.source'),
                    ''
                  ) = ?
            GROUP BY wa.address
        )
        """
        if has_activity else ""
    )
    activity_columns = (
        """
        COALESCE(a.watch_events_24h, 0) AS watch_events_24h,
        COALESCE(a.watch_buy_events_24h, 0) AS watch_buy_events_24h,
        COALESCE(a.watch_markets_24h, 0) AS watch_markets_24h,
        COALESCE(a.current_watch_events, 0) AS current_watch_events,
        a.latest_watch_ts,
        a.latest_watch_ingested_at,
        a.max_watch_ingest_lag_sec
        """
        if has_activity else
        """
        0 AS watch_events_24h,
        0 AS watch_buy_events_24h,
        0 AS watch_markets_24h,
        0 AS current_watch_events,
        NULL AS latest_watch_ts,
        NULL AS latest_watch_ingested_at,
        NULL AS max_watch_ingest_lag_sec
        """
    )
    activity_join = "LEFT JOIN watch_activity a ON a.address = w.address" if has_activity else ""
    params: List[Any] = [*RTDS_WATCH_CANDIDATE_STAGES, float(min_score), limit]
    if has_activity:
        params.extend([max(0, int(now) - DEFAULT_MAX_SIGNAL_AGE_SEC), lookback_start, RTDS_WATCH_ACTIVITY_SOURCE])
    rows = _rows(
        conn,
        """
        WITH latest AS (
            SELECT ls.*
            FROM leader_scores ls
            JOIN (
                SELECT address, MAX(score_id) AS score_id
                FROM leader_scores
                GROUP BY address
            ) x
              ON x.address = ls.address
             AND x.score_id = ls.score_id
        ),
        watch AS (
            SELECT
                cw.address,
                cw.candidate_stage,
                cw.sources,
                cw.updated_at,
                latest.leader_score,
                latest.review_stage,
                latest.review_reason,
                latest.scored_at
            FROM candidate_wallets cw
            JOIN latest
              ON latest.address = cw.address
            WHERE cw.candidate_stage IN (""" + stage_placeholders + """)
              AND latest.leader_score >= ?
            ORDER BY latest.leader_score DESC, cw.updated_at DESC
            LIMIT ?
        )
        """ + activity_cte + """
        SELECT
            w.address,
            w.candidate_stage,
            w.sources,
            w.updated_at,
            w.leader_score,
            w.review_stage,
            w.review_reason,
            w.scored_at,
            """ + feature_columns + """,
            """ + state_columns + """,
            """ + activity_columns + """
        FROM watch w
        """ + feature_join + """
        """ + state_join + """
        """ + activity_join + """
        ORDER BY w.leader_score DESC, w.updated_at DESC
        """,
        params,
    )
    audited = []
    for row in rows:
        item = dict(row)
        item["leader_score"] = _round_optional(item.get("leader_score"))
        item["copy_stream_roi"] = _round_optional(item.get("copy_stream_roi"))
        item["net_pnl_usdc"] = _round_optional(item.get("net_pnl_usdc"))
        item["total_volume_usdc"] = _round_optional(item.get("total_volume_usdc"))
        for key in (
            "copy_event_count",
            "copy_market_count",
            "activity_count",
            "distinct_markets",
            "non_fast_trade_count",
            "watch_events_24h",
            "watch_buy_events_24h",
            "watch_markets_24h",
            "current_watch_events",
        ):
            item[key] = _int(item.get(key))
        state, action = _rtds_watch_state(item)
        item["watch_state"] = state
        item["next_action"] = action
        audited.append(item)
    return audited


def _rtds_watch_state(row: Dict[str, Any]) -> tuple[str, str]:
    """Explain how a near-paper RTDS watch wallet should be handled next."""

    if _int(row.get("current_watch_events")) > 0:
        return "current_watch_hit", "RTDS 已命中近 paper 钱包；复查该交易的 copyability，并考虑升级 paper 观察。"
    if _int(row.get("watch_events_24h")) > 0:
        return "watch_hit_24h", "24h 内有 RTDS watch 交易；补实时交易证据后重新评分。"
    if _int(row.get("copy_market_count")) <= 1:
        return "waiting_more_copy_markets", "分数接近 paper，但 copyability 市场覆盖偏窄；继续等待更多实时市场证据。"
    return "waiting_watch_hit", "已进入 RTDS watch 范围；等待该钱包在实时流中再次交易。"


def _rtds_runtime_diagnostics(
    heartbeats: Sequence[Dict[str, Any]],
    *,
    now: Optional[int] = None,
    progress: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Parse latest RTDS heartbeat counters into dashboard-friendly fields."""

    heartbeat = next((row for row in heartbeats if row.get("name") == "loop_rtds_discovery"), None)
    if not heartbeat:
        return {"state": "missing", "next_action": "没有 RTDS 心跳；先检查 rtds-discovery 服务。"}
    now_ts = int(now if now is not None else time.time())
    finished_at = _int(heartbeat.get("finished_at"))
    heartbeat_age_sec = max(0, now_ts - finished_at) if finished_at > 0 else None
    fresh = heartbeat_age_sec is not None and heartbeat_age_sec <= 120
    counters = _parse_key_value_counters(str(heartbeat.get("error") or ""))
    paper_rows = _int(counters.get("paper_rows"))
    paper_wallet_rows = _int(counters.get("paper_wallet_rows"))
    paper_matches = _int(counters.get("paper_matches"))
    eligible = _int(counters.get("paper_eligible"))
    paper_events = _int(counters.get("paper_events"))
    wallet_keys = counters.get("paper_wallet_keys") or ""
    progress_payload = progress or {}
    stream_state = str(progress_payload.get("state") or "unknown")
    stream_action = str(progress_payload.get("next_action") or "")
    if str(heartbeat.get("status") or "") != "ok":
        state = "rtds_unhealthy"
        action = "RTDS 心跳非 ok；先看服务日志和代理连接。"
    elif not fresh:
        state = "rtds_heartbeat_stale"
        action = "RTDS 心跳已经变旧；先检查 rtds-discovery 服务是否仍在推进。"
    elif stream_state == "stream_not_progressing":
        state = "rtds_stream_not_progressing"
        action = stream_action or "RTDS 心跳新鲜但消息数没有增长；检查 websocket 或代理连接。"
    elif paper_rows <= 0:
        state = "waiting_rtds_activity"
        action = "RTDS 暂未记录 paper-scope 行；继续等待实时流。"
    elif paper_wallet_rows <= 0:
        state = "wallet_field_missing"
        action = "RTDS 有交易但没有可识别钱包字段；需要检查 payload 字段映射。"
    elif eligible > 0 and paper_matches <= 0:
        state = "no_paper_wallet_match"
        action = "RTDS 钱包字段正常，但当前 paper 钱包未出现在实时流里；等待交易或扩大 paper 池。"
    elif paper_events <= 0:
        state = "paper_match_no_new_events"
        action = "RTDS 命中 paper 钱包但没有写入新事件，可能是重复交易或已入库。"
    else:
        state = "paper_rtds_writing"
        action = "RTDS 已经为 paper 钱包写入实时事件。"
    return {
        "state": state,
        "next_action": action,
        "status": heartbeat.get("status") or "",
        "finished_at": heartbeat.get("finished_at"),
        "heartbeat_age_sec": heartbeat_age_sec,
        "heartbeat_fresh": fresh,
        "stream_state": stream_state,
        "stream_next_action": stream_action,
        "progress_window_sec": _int(progress_payload.get("window_sec")),
        "progress_samples": _int(progress_payload.get("samples")),
        "message_delta": _int(progress_payload.get("message_delta")),
        "selected_delta": _int(progress_payload.get("selected_delta")),
        "messages": _int(counters.get("messages")),
        "trades": _int(counters.get("trades")),
        "selected": _int(counters.get("selected")),
        "batches": _int(counters.get("batches")),
        "paper_wallets": _int(counters.get("paper_wallets")),
        "paper_events": paper_events,
        "paper_rows": paper_rows,
        "paper_wallet_rows": paper_wallet_rows,
        "paper_matches": paper_matches,
        "paper_eligible": eligible,
        "paper_wallet_keys": wallet_keys,
        "watch_wallets": _int(counters.get("watch_wallets")),
        "watch_events": _int(counters.get("watch_events")),
        "watch_matches": _int(counters.get("watch_matches")),
        "watch_eligible": _int(counters.get("watch_eligible")),
    }


def _rtds_recent_progress(conn: sqlite3.Connection, *, now: int, window_sec: int = 300) -> Dict[str, Any]:
    """Measure whether recent RTDS heartbeat counters are still increasing."""

    if not _table_exists(conn, "ingest_runs"):
        return {
            "state": "missing_table",
            "next_action": "缺少 ingest_runs，无法判断 RTDS 进度。",
            "window_sec": int(window_sec),
            "samples": 0,
        }
    rows = _rows(
        conn,
        """
        SELECT finished_at, error
        FROM ingest_runs
        WHERE ingest_type = 'loop_rtds_discovery'
          AND status = 'ok'
          AND finished_at >= ?
        ORDER BY finished_at ASC, run_id ASC
        """,
        (max(0, int(now) - max(0, int(window_sec))),),
    )
    samples = []
    for row in rows:
        counters = _parse_key_value_counters(str(row.get("error") or ""))
        samples.append(
            {
                "finished_at": _int(row.get("finished_at")),
                "messages": _int(counters.get("messages")),
                "selected": _int(counters.get("selected")),
                "paper_matches": _int(counters.get("paper_matches")),
                "watch_matches": _int(counters.get("watch_matches")),
            }
        )
    if len(samples) < 2:
        return {
            "state": "insufficient_samples",
            "next_action": "RTDS 最近样本不足；继续等待下一次心跳。",
            "window_sec": int(window_sec),
            "samples": len(samples),
        }
    first = samples[0]
    last = samples[-1]
    message_delta = max(0, int(last["messages"]) - int(first["messages"]))
    selected_delta = max(0, int(last["selected"]) - int(first["selected"]))
    paper_match_delta = max(0, int(last["paper_matches"]) - int(first["paper_matches"]))
    watch_match_delta = max(0, int(last["watch_matches"]) - int(first["watch_matches"]))
    if message_delta > 0:
        state = "stream_progressing"
        action = "RTDS 消息数持续增长；实时流正常，目标钱包未命中就表示它们暂未实时交易。"
    else:
        state = "stream_not_progressing"
        action = "RTDS 最近心跳存在但消息数没有增长；检查 websocket、代理或服务循环。"
    return {
        "state": state,
        "next_action": action,
        "window_sec": int(window_sec),
        "samples": len(samples),
        "first_finished_at": first["finished_at"],
        "latest_finished_at": last["finished_at"],
        "message_delta": message_delta,
        "selected_delta": selected_delta,
        "paper_match_delta": paper_match_delta,
        "watch_match_delta": watch_match_delta,
    }


def _parse_key_value_counters(text: str) -> Dict[str, str]:
    values = {}
    for token in (text or "").split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _round_optional(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except ValueError:
        return default


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _tables_exist(conn: sqlite3.Connection, names: Iterable[str]) -> bool:
    return all(_table_exists(conn, name) for name in names)


def _one(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> Dict[str, Any]:
    row = conn.execute(sql, tuple(params)).fetchone()
    return dict(row) if row else {}


def _rows(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def _heartbeat(conn: sqlite3.Connection, name: str) -> Dict[str, Any]:
    if not _table_exists(conn, "ingest_runs"):
        return {"name": name, "status": "missing_table"}
    row = conn.execute(
        """
        SELECT ingest_type AS name, status, finished_at, rows_written, error
        FROM ingest_runs
        WHERE ingest_type = ?
        ORDER BY finished_at DESC, run_id DESC
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    return dict(row) if row else {"name": name, "status": "missing"}
