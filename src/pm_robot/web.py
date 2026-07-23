"""Read-only L0-L6 wallet research console."""

from __future__ import annotations

import hashlib
import html
import json
import os
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from pm_robot.config import RobotSettings
from pm_robot.research.current_elite import (
    current_elite_wallets,
    current_verified_l6_wallets,
)
from pm_robot.storage.db import connect_readonly


SESSION_COOKIE = "pm_robot_token"
SCHEMA_VERSION = "wallet_research_v2"
DETAIL_SCHEMA_VERSION = "wallet_research_detail_v2"
MAX_LIST_LIMIT = 250
DASHBOARD_CACHE_TTL_SEC = 30

LEVEL_DEFINITIONS = (
    ("l0", "来源流", "首次捕获的钱包地址"),
    ("l1", "资源准入", "可信来源，或最近最多 10 笔观测成交累计达到 100 USDC"),
    ("l2", "样本通过", "最近最多 10 笔成交金额合计达到 100 USDC"),
    ("l3", "轻历史优选", "轻量历史同组相对排名入选"),
    ("l4", "深历史优选", "深度历史同组相对排名入选"),
    ("l5", "评分精英", "当前评分体系筛出的最高等级钱包"),
    ("l6", "独立复核", "收益、持续性和异常交易检查已独立通过"),
)
LEVEL_VALUES = tuple(level for level, _, _ in LEVEL_DEFINITIONS)
HIGH_LEVEL_VALUES = ("l3", "l4", "l5", "l6")
QUEUE_DEFINITIONS = (
    ("wallet_recent_screen", "快速初筛队列", "只读取最近最多 10 笔成交样本"),
    ("wallet_history_collect", "历史采集队列", "补充收益概况并将历史直接写入列式归档"),
    ("wallet_l6_validate", "L6 独立复核队列", "只复核少量当前 L5/L6 钱包"),
)
ACTIVE_JOB_STATUSES = ("queued", "running")
JOB_STATUS_ORDER = ("queued", "running", "done", "failed", "cancelled", "superseded")

_DASHBOARD_CACHE_LOCK = threading.Lock()
_DASHBOARD_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_DASHBOARD_REFRESHING: set[str] = set()


@dataclass(frozen=True)
class WebConsoleConfig:
    settings: RobotSettings
    host: str = "127.0.0.1"
    port: int = 8787
    token: str = ""
    auth_required: bool = True


def run_web_console(config: WebConsoleConfig) -> None:
    conn = connect_readonly(config.settings.db_path)
    conn.close()
    server = ThreadingHTTPServer((config.host, config.port), _handler_factory(config))
    _start_dashboard_cache_prewarm(config.settings)
    print(f"pm-robot wallet research console listening on http://{config.host}:{config.port}")
    server.serve_forever()


def dashboard_data(
    settings: RobotSettings,
    *,
    include_pair_quality: bool = False,
    include_heavy_audits: bool = False,
) -> dict[str, Any]:
    del include_pair_quality, include_heavy_audits
    conn = connect_readonly(settings.db_path)
    try:
        schema_ready = _wallet_research_schema_ready(conn)
        level_counts = _level_counts(conn) if schema_ready else _empty_level_counts()
        elite_wallets = current_elite_wallets(conn) if schema_ready else set()
        verified_l6_wallets = current_verified_l6_wallets(conn) if schema_ready else set()
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": int(time.time()),
            "schema_ready": schema_ready,
            "runtime": _runtime_build_info(),
            "database_size_bytes": settings.db_path.stat().st_size if settings.db_path.exists() else 0,
            "wallet_count": sum(int(row["count"]) for row in level_counts),
            "level_counts": level_counts,
            "current_elite_wallet_count": len(elite_wallets),
            "verified_l6_wallet_count": len(verified_l6_wallets),
            "queues": _queue_summaries(conn),
            "high_level_wallets": (
                _wallet_rows(
                    conn,
                    levels=HIGH_LEVEL_VALUES,
                    limit=50,
                    elite_wallets=elite_wallets,
                    verified_l6_wallets=verified_l6_wallets,
                    exclude_stale_l5=True,
                )
                if schema_ready
                else []
            ),
            "selection_summary": _selection_summary(conn) if schema_ready else [],
            "recent_level_changes": _recent_level_changes(conn) if schema_ready else [],
        }
    finally:
        conn.close()


def dashboard_summary_data(
    settings: RobotSettings,
    *,
    fresh: bool = False,
    full: bool = False,
) -> dict[str, Any]:
    del full
    return _dashboard_data_cached(settings, force_refresh=fresh)


def wallet_levels_data(settings: RobotSettings) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        ready = _wallet_research_schema_ready(conn)
        counts = _level_counts(conn) if ready else _empty_level_counts()
        elite_wallets = current_elite_wallets(conn) if ready else set()
        verified_l6_wallets = current_verified_l6_wallets(conn) if ready else set()
        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": int(time.time()),
            "schema_ready": ready,
            "wallet_count": sum(int(row["count"]) for row in counts),
            "level_counts": counts,
            "current_elite_wallet_count": len(elite_wallets),
            "verified_l6_wallet_count": len(verified_l6_wallets),
        }
    finally:
        conn.close()


def wallet_table_rows(
    settings: RobotSettings,
    *,
    level: str = "",
    query: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    conn = connect_readonly(settings.db_path)
    try:
        if not _wallet_research_schema_ready(conn):
            return []
        levels = (level.lower(),) if level.lower() in LEVEL_VALUES else LEVEL_VALUES
        return _wallet_rows(conn, levels=levels, query=query, limit=limit)
    finally:
        conn.close()


def discovery_data(
    settings: RobotSettings,
    *,
    level: str = "",
    query: str = "",
    limit: int = 150,
) -> dict[str, Any]:
    rows = wallet_table_rows(settings, level=level, query=query, limit=limit)
    levels = wallet_levels_data(settings)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": int(time.time()),
        "filters": {"level": level if level in LEVEL_VALUES else "", "query": query, "limit": limit},
        "level_counts": levels["level_counts"],
        "wallet_count": len(rows),
        "wallets": rows,
    }


def wallet_detail_data(settings: RobotSettings, address: str) -> dict[str, Any]:
    wallet = address.strip().lower()
    conn = connect_readonly(settings.db_path)
    try:
        if not _wallet_research_schema_ready(conn):
            return {
                "schema_version": DETAIL_SCHEMA_VERSION,
                "generated_at": int(time.time()),
                "address": wallet,
                "found": False,
            }
        level = _one(
            conn,
            """
            SELECT level, level_reason, policy_version, hard_risk_block,
                   first_seen_at, last_seen_at, level_updated_at, updated_at
            FROM wallet_levels
            WHERE wallet = ?
            """,
            (wallet,),
        )
        if not level:
            return {
                "schema_version": DETAIL_SCHEMA_VERSION,
                "generated_at": int(time.time()),
                "address": wallet,
                "found": False,
            }
        level["current_elite"] = wallet in current_elite_wallets(conn, wallets=(wallet,))
        level["verified_l6"] = wallet in current_verified_l6_wallets(conn, wallets=(wallet,))

        source = _one(
            conn,
            """
            SELECT sources, labels, status, observed_trade_count, recent_trade_count,
                   recent_usdc_total, recent_max_trade_usdc, first_seen_at, updated_at
            FROM observed_wallets
            WHERE wallet = ?
            """,
            (wallet,),
        )
        screen = _one(
            conn,
            """
            SELECT sample_limit, sample_trade_count, sample_volume_usdc,
                   sample_market_count, latest_trade_at, screen_complete,
                   screen_qualified, screen_reason, computed_at, updated_at
            FROM wallet_screen_summaries
            WHERE wallet = ?
            """,
            (wallet,),
        )
        pnl = _one(
            conn,
            """
            SELECT current_position_value_usdc, open_estimated_pnl_usdc,
                   closed_realized_pnl_usdc, total_estimated_pnl_usdc,
                   capital_basis_usdc, cost_roi_estimate, open_position_count,
                   closed_position_count, coverage, methodology_version,
                   captured_at, updated_at
            FROM wallet_pnl_summaries
            WHERE wallet = ?
            """,
            (wallet,),
        )
        history = _one(
            conn,
            """
            SELECT artifact_id, history_depth, activity_count, distinct_markets,
                   non_fast_trade_count, fast_market_share, total_volume_usdc,
                   buy_count, sell_count, median_gap_sec, trades_per_day,
                   market_volume_top_share, oldest_timestamp, latest_timestamp,
                   strategy_tags_json, risk_flags_json, research_score,
                   score_components_json, methodology_version, computed_at, updated_at
            FROM wallet_history_summaries
            WHERE wallet = ?
            """,
            (wallet,),
        )
        if history:
            history["strategy_tags"] = _json_list(history.pop("strategy_tags_json", "[]"))
            history["risk_flags"] = _json_list(history.pop("risk_flags_json", "[]"))
            history["score_components"] = _json_dict(history.pop("score_components_json", "{}"))
            if int(level.get("hard_risk_block") or 0) and "hard_risk_block" not in history["risk_flags"]:
                history["risk_flags"].insert(0, "hard_risk_block")

        artifact: dict[str, Any] = {}
        artifact_id = str((history or {}).get("artifact_id") or "")
        if artifact_id:
            artifact = _one(
                conn,
                """
                SELECT artifact_id, history_depth, storage_version, row_count,
                       byte_size, checksum, status, created_at, updated_at
                FROM wallet_history_artifacts
                WHERE artifact_id = ?
                """,
                (artifact_id,),
            )

        return {
            "schema_version": DETAIL_SCHEMA_VERSION,
            "generated_at": int(time.time()),
            "address": wallet,
            "found": True,
            "level": level,
            "source": source,
            "screen": screen,
            "pnl": pnl,
            "history": history,
            "artifact": artifact,
            "l6_validations": _rows(
                conn,
                """
                SELECT validation_id, evidence_artifact_id, policy_version,
                       decision, reason, coverage_start, coverage_end,
                       closed_position_count, activity_count, active_weeks,
                       positive_week_ratio, realized_pnl_usdc,
                       recent_realized_pnl_usdc, open_pnl_usdc,
                       max_drawdown_usdc, max_drawdown_ratio,
                       top_market_profit_share, top_day_profit_share,
                       churn_ratio, unrealized_profit_share,
                       official_all_pnl_usdc, official_all_volume_usdc,
                       official_profit_intensity, official_month_pnl_usdc,
                       official_week_pnl_usdc,
                       abnormal_flags_json, validated_at
                FROM wallet_l6_validations
                WHERE wallet = ?
                ORDER BY validated_at DESC, validation_id DESC
                LIMIT 20
                """,
                (wallet,),
            ),
            "selections": _rows(
                conn,
                """
                SELECT target_level, evidence_artifact_id, policy_version,
                       selected, rank_in_cohort, cohort_size, source_bucket,
                       strategy_bucket, reason, decided_at, updated_at
                FROM wallet_level_selections
                WHERE wallet = ?
                ORDER BY decided_at DESC, target_level DESC
                LIMIT 20
                """,
                (wallet,),
            ),
            "pipeline_jobs": _rows(
                conn,
                """
                SELECT job_id, job_type, job_action, job_scope,
                       status, priority, shard, attempts,
                       max_attempts, next_attempt_at, created_at, updated_at,
                       completed_at
                FROM pipeline_jobs
                WHERE wallet = ?
                  AND job_type IN ('wallet_recent_screen', 'wallet_history_collect', 'wallet_l6_validate')
                ORDER BY updated_at DESC, job_id DESC
                LIMIT 30
                """,
                (wallet,),
            ),
            "level_events": _rows(
                conn,
                """
                SELECT from_level, to_level, reason, policy_version, created_at
                FROM wallet_level_events
                WHERE wallet = ?
                ORDER BY created_at DESC, event_id DESC
                LIMIT 30
                """,
                (wallet,),
            ),
        }
    finally:
        conn.close()


def _handler_factory(config: WebConsoleConfig) -> type[BaseHTTPRequestHandler]:
    class WebConsoleHandler(BaseHTTPRequestHandler):
        server_version = "PMRobotWeb/1.0"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if not self._authorize(parsed):
                return

            if parsed.path == "/":
                self._send_html(_render_dashboard(config.settings))
                return
            if parsed.path == "/wallets":
                params = parse_qs(parsed.query)
                self._send_html(
                    _render_wallets(
                        config.settings,
                        level=_first(params, "level"),
                        query=_first(params, "q"),
                    )
                )
                return
            if parsed.path.startswith("/wallet/"):
                address = parsed.path.removeprefix("/wallet/").strip().lower()
                self._send_html(_render_wallet_detail(config.settings, address))
                return
            if parsed.path == "/api/runtime":
                self._send_json({"runtime": _runtime_build_info(), "health": "ok"})
                return
            if parsed.path == "/api/summary":
                params = parse_qs(parsed.query)
                self._send_json(
                    dashboard_summary_data(
                        config.settings,
                        fresh=_first(params, "fresh") == "1",
                    )
                )
                return
            if parsed.path == "/api/wallet-levels":
                self._send_json(wallet_levels_data(config.settings))
                return
            if parsed.path == "/api/wallets":
                params = parse_qs(parsed.query)
                self._send_json(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "generated_at": int(time.time()),
                        "wallets": wallet_table_rows(
                            config.settings,
                            level=_first(params, "level"),
                            query=_first(params, "q"),
                            limit=_int_param(params, "limit", 100),
                        ),
                    }
                )
                return
            if parsed.path.startswith("/api/wallet/"):
                address = parsed.path.removeprefix("/api/wallet/").strip().lower()
                self._send_json(wallet_detail_data(config.settings, address))
                return
            if parsed.path == "/logout":
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Set-Cookie", f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
                self.send_header("Location", "/login")
                self._security_headers()
                self.end_headers()
                return
            if parsed.path == "/login":
                self._send_html(_render_login(error=""))
                return
            self._send_html(_render_page("Not Found", "<main class=\"empty-page\"><h1>页面不存在</h1></main>"), status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/login":
                self._send_html(_render_page("Not Found", "<main class=\"empty-page\"><h1>页面不存在</h1></main>"), status=HTTPStatus.NOT_FOUND)
                return
            body_len = int(self.headers.get("Content-Length", "0") or "0")
            body = self.rfile.read(min(body_len, 4096)).decode("utf-8")
            submitted = parse_qs(body).get("token", [""])[0]
            if config.token and secrets.compare_digest(submitted, config.token):
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Set-Cookie", _auth_cookie(config.token))
                self.send_header("Location", "/")
                self._security_headers()
                self.end_headers()
                return
            self._send_html(_render_login(error="访问令牌不正确"), status=HTTPStatus.UNAUTHORIZED)

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"{self.address_string()} - {fmt % args}")

        def _authorize(self, parsed: Any) -> bool:
            if not config.auth_required:
                return True
            if not config.token:
                self._send_html(_render_missing_token(), status=HTTPStatus.SERVICE_UNAVAILABLE)
                return False
            if parsed.path == "/login":
                return True
            query_token = parse_qs(parsed.query).get("token", [""])[0]
            if query_token and secrets.compare_digest(query_token, config.token):
                clean_query = parse_qs(parsed.query)
                clean_query.pop("token", None)
                clean_path = parsed.path
                encoded = urlencode({key: value[0] for key, value in clean_query.items() if value})
                if encoded:
                    clean_path = f"{clean_path}?{encoded}"
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Set-Cookie", _auth_cookie(config.token))
                self.send_header("Location", clean_path)
                self._security_headers()
                self.end_headers()
                return False
            header_token = self.headers.get("X-PM-Robot-Token", "")
            if header_token and secrets.compare_digest(header_token, config.token):
                return True
            cookie_token = _cookie_value(self.headers.get("Cookie", ""), SESSION_COOKIE)
            if cookie_token and secrets.compare_digest(cookie_token, config.token):
                return True
            self._send_html(_render_login(error="请使用访问令牌登录"), status=HTTPStatus.UNAUTHORIZED)
            return False

        def _send_html(self, body: str, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self._security_headers()
            self.end_headers()
            self.wfile.write(data)

        def _send_json(self, body: Any, *, status: HTTPStatus = HTTPStatus.OK) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self._security_headers()
            self.end_headers()
            self.wfile.write(data)

        def _security_headers(self) -> None:
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'none'; form-action 'self'",
            )

    return WebConsoleHandler


def _wallet_research_schema_ready(conn: sqlite3.Connection) -> bool:
    required = {
        "observed_wallets",
        "wallet_history_artifacts",
        "wallet_level_events",
        "wallet_levels",
        "wallet_screen_summaries",
        "wallet_pnl_summaries",
        "wallet_history_summaries",
        "wallet_level_selections",
        "wallet_l6_validations",
        "pipeline_jobs",
    }
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ({})".format(
            ",".join("?" for _ in required)
        ),
        tuple(sorted(required)),
    ).fetchall()
    return {str(row[0]) for row in rows} == required


def _empty_level_counts() -> list[dict[str, Any]]:
    return [
        {"level": level, "label": label, "description": description, "count": 0}
        for level, label, description in LEVEL_DEFINITIONS
    ]


def _level_counts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    counts = {
        str(row["level"]): int(row["count"])
        for row in conn.execute(
            "SELECT level, COUNT(*) AS count FROM wallet_levels GROUP BY level"
        ).fetchall()
    }
    return [
        {
            "level": level,
            "label": label,
            "description": description,
            "count": counts.get(level, 0),
        }
        for level, label, description in LEVEL_DEFINITIONS
    ]


def _queue_summaries(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "pipeline_jobs"):
        grouped: dict[str, list[dict[str, Any]]] = {}
    else:
        grouped = {}
        rows = _rows(
            conn,
            """
            SELECT job_type, status, COUNT(*) AS count,
                   MIN(CASE WHEN status = 'queued' THEN created_at END) AS oldest_queued_at,
                   MAX(updated_at) AS latest_updated_at,
                   SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) AS completed_24h
            FROM pipeline_jobs
            WHERE job_type IN ('wallet_recent_screen', 'wallet_history_collect', 'wallet_l6_validate')
            GROUP BY job_type, status
            """,
            (int(time.time()) - 86_400,),
        )
        for row in rows:
            grouped.setdefault(str(row["job_type"]), []).append(row)

    result = []
    now = int(time.time())
    for job_type, label, description in QUEUE_DEFINITIONS:
        job_rows = grouped.get(job_type, [])
        status_counts = {
            status: sum(int(row["count"] or 0) for row in job_rows if row["status"] == status)
            for status in JOB_STATUS_ORDER
        }
        status_counts = {key: value for key, value in status_counts.items() if value}
        oldest = min(
            (int(row["oldest_queued_at"]) for row in job_rows if row.get("oldest_queued_at")),
            default=0,
        )
        latest = max((int(row["latest_updated_at"] or 0) for row in job_rows), default=0)
        result.append(
            {
                "job_type": job_type,
                "label": label,
                "description": description,
                "status_counts": status_counts,
                "active": sum(status_counts.get(status, 0) for status in ACTIVE_JOB_STATUSES),
                "completed_24h": sum(int(row["completed_24h"] or 0) for row in job_rows),
                "oldest_wait_seconds": max(0, now - oldest) if oldest else 0,
                "latest_updated_at": latest,
            }
        )
    return result


def _wallet_rows(
    conn: sqlite3.Connection,
    *,
    levels: tuple[str, ...],
    query: str = "",
    limit: int = 100,
    elite_wallets: set[str] | None = None,
    verified_l6_wallets: set[str] | None = None,
    exclude_stale_l5: bool = False,
) -> list[dict[str, Any]]:
    clean_levels = tuple(level for level in levels if level in LEVEL_VALUES) or LEVEL_VALUES
    where = [f"wl.level IN ({','.join('?' for _ in clean_levels)})"]
    params: list[Any] = list(clean_levels)
    current_elites = current_elite_wallets(conn) if elite_wallets is None else set(elite_wallets)
    verified_l6 = (
        current_verified_l6_wallets(conn)
        if verified_l6_wallets is None
        else set(verified_l6_wallets)
    )
    if exclude_stale_l5:
        current_parts = ["wl.level NOT IN ('l5', 'l6')"]
        if current_elites:
            current_parts.append(
                f"(wl.level = 'l5' AND wl.wallet IN ({','.join('?' for _ in current_elites)}))"
            )
            params.extend(sorted(current_elites))
        if verified_l6:
            current_parts.append(
                f"(wl.level = 'l6' AND wl.wallet IN ({','.join('?' for _ in verified_l6)}))"
            )
            params.extend(sorted(verified_l6))
        where.append("(" + " OR ".join(current_parts) + ")")
    clean_query = query.strip().lower()
    if clean_query:
        where.append("(LOWER(wl.wallet) LIKE ? OR LOWER(COALESCE(ow.sources, '')) LIKE ?)")
        needle = f"%{clean_query}%"
        params.extend((needle, needle))
    params.append(min(max(int(limit), 1), MAX_LIST_LIMIT))
    rows = _rows(
        conn,
        f"""
        SELECT
            wl.wallet,
            wl.level,
            wl.level_reason,
            wl.hard_risk_block,
            COALESCE(ow.sources, '') AS sources,
            COALESCE(wp.total_estimated_pnl_usdc, 0) AS total_estimated_pnl_usdc,
            wp.cost_roi_estimate,
            COALESCE(wp.current_position_value_usdc, 0) AS current_position_value_usdc,
            COALESCE(wh.history_depth, 'none') AS history_depth,
            COALESCE(wh.activity_count, 0) AS activity_count,
            COALESCE(wh.distinct_markets, 0) AS distinct_markets,
            COALESCE(wh.research_score, 0) AS research_score,
            COALESCE(wh.strategy_tags_json, '[]') AS strategy_tags_json,
            COALESCE(wh.risk_flags_json, '[]') AS risk_flags_json,
            COALESCE(sel.rank_in_cohort, 0) AS rank_in_cohort,
            COALESCE(sel.cohort_size, 0) AS cohort_size,
            COALESCE(sel.policy_version, wl.policy_version, '') AS selection_policy_version,
            COALESCE(validation.decision, '') AS l6_validation_decision,
            COALESCE(validation.reason, '') AS l6_validation_reason,
            COALESCE(validation.validated_at, 0) AS l6_validated_at,
            COALESCE(wh.updated_at, wp.updated_at, ws.updated_at, wl.updated_at, 0) AS updated_at
        FROM wallet_levels wl
        LEFT JOIN observed_wallets ow ON ow.wallet = wl.wallet
        LEFT JOIN wallet_screen_summaries ws ON ws.wallet = wl.wallet
        LEFT JOIN wallet_pnl_summaries wp ON wp.wallet = wl.wallet
        LEFT JOIN wallet_history_summaries wh ON wh.wallet = wl.wallet
        LEFT JOIN wallet_level_selections sel
          ON sel.rowid = (
              SELECT candidate.rowid
              FROM wallet_level_selections candidate
              WHERE candidate.wallet = wl.wallet
              ORDER BY candidate.decided_at DESC, candidate.target_level DESC
              LIMIT 1
          )
        LEFT JOIN wallet_l6_validations validation
          ON validation.validation_id = (
              SELECT latest.validation_id
              FROM wallet_l6_validations latest
              WHERE latest.wallet = wl.wallet
              ORDER BY latest.validated_at DESC, latest.validation_id DESC
              LIMIT 1
          )
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE wl.level
                WHEN 'l6' THEN 6 WHEN 'l5' THEN 5 WHEN 'l4' THEN 4 WHEN 'l3' THEN 3
                WHEN 'l2' THEN 2 WHEN 'l1' THEN 1 ELSE 0
            END DESC,
            COALESCE(wh.research_score, 0) DESC,
            wl.level_updated_at DESC,
            wl.wallet ASC
        LIMIT ?
        """,
        tuple(params),
    )
    return [
        _normalize_wallet_row(
            row,
            current_elite=str(row.get("wallet") or "") in current_elites,
            verified_l6=str(row.get("wallet") or "") in verified_l6,
        )
        for row in rows
    ]


def _normalize_wallet_row(
    row: dict[str, Any],
    *,
    current_elite: bool,
    verified_l6: bool,
) -> dict[str, Any]:
    risk_flags = _json_list(row.pop("risk_flags_json", "[]"))
    if int(row.pop("hard_risk_block", 0) or 0) and "hard_risk_block" not in risk_flags:
        risk_flags.insert(0, "hard_risk_block")
    return {
        "wallet": str(row.get("wallet") or ""),
        "level": str(row.get("level") or "l0"),
        "level_reason": str(row.get("level_reason") or ""),
        "current_elite": bool(current_elite),
        "verified_l6": bool(verified_l6),
        "sources": str(row.get("sources") or ""),
        "total_estimated_pnl_usdc": float(row.get("total_estimated_pnl_usdc") or 0),
        "cost_roi_estimate": (
            float(row["cost_roi_estimate"]) if row.get("cost_roi_estimate") is not None else None
        ),
        "current_position_value_usdc": float(row.get("current_position_value_usdc") or 0),
        "history_depth": str(row.get("history_depth") or "none"),
        "activity_count": int(row.get("activity_count") or 0),
        "distinct_markets": int(row.get("distinct_markets") or 0),
        "research_score": float(row.get("research_score") or 0),
        "strategy_tags": _json_list(row.pop("strategy_tags_json", "[]")),
        "risk_flags": risk_flags,
        "rank_in_cohort": int(row.get("rank_in_cohort") or 0),
        "cohort_size": int(row.get("cohort_size") or 0),
        "selection_policy_version": str(row.get("selection_policy_version") or ""),
        "l6_validation_decision": str(row.get("l6_validation_decision") or ""),
        "l6_validation_reason": str(row.get("l6_validation_reason") or ""),
        "l6_validated_at": int(row.get("l6_validated_at") or 0),
        "updated_at": int(row.get("updated_at") or 0),
    }


def _selection_summary(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        SELECT target_level,
               COUNT(*) AS decisions,
               SUM(CASE WHEN selected = 1 THEN 1 ELSE 0 END) AS selected,
               COUNT(DISTINCT policy_version) AS policy_versions,
               MAX(decided_at) AS latest_decided_at
        FROM wallet_level_selections
        GROUP BY target_level
        ORDER BY target_level
        """,
    )


def _recent_level_changes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        SELECT wallet, from_level, to_level, reason, policy_version, created_at
        FROM wallet_level_events
        ORDER BY created_at DESC, event_id DESC
        LIMIT 12
        """,
    )


def _dashboard_data_cached(settings: RobotSettings, *, force_refresh: bool = False) -> dict[str, Any]:
    ttl = _dashboard_cache_ttl()
    if ttl <= 0:
        return dashboard_data(settings)
    key = str(settings.db_path.resolve())
    now = time.time()
    if not force_refresh:
        with _DASHBOARD_CACHE_LOCK:
            cached = _DASHBOARD_CACHE.get(key)
            if cached and now - cached[0] <= ttl:
                return cached[1]
    data = dashboard_data(settings)
    with _DASHBOARD_CACHE_LOCK:
        _DASHBOARD_CACHE[key] = (time.time(), data)
    return data


def _dashboard_cache_ttl() -> int:
    try:
        return int(os.environ.get("PM_ROBOT_WEB_DASHBOARD_CACHE_TTL_SEC", str(DASHBOARD_CACHE_TTL_SEC)))
    except ValueError:
        return DASHBOARD_CACHE_TTL_SEC


def _start_dashboard_cache_prewarm(settings: RobotSettings) -> None:
    key = str(settings.db_path.resolve())
    with _DASHBOARD_CACHE_LOCK:
        if key in _DASHBOARD_REFRESHING:
            return
        _DASHBOARD_REFRESHING.add(key)

    def prewarm() -> None:
        try:
            _dashboard_data_cached(settings, force_refresh=True)
        except Exception as exc:  # pragma: no cover - startup remains available for diagnostics.
            print(f"pm-robot dashboard cache prewarm skipped: {type(exc).__name__}: {exc}")
        finally:
            with _DASHBOARD_CACHE_LOCK:
                _DASHBOARD_REFRESHING.discard(key)

    threading.Thread(target=prewarm, name="pm-robot-dashboard-prewarm", daemon=True).start()


@lru_cache(maxsize=1)
def _runtime_build_info() -> dict[str, Any]:
    package_root = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    source_file_count = 0
    latest_mtime = 0
    for path in sorted(package_root.rglob("*.py"), key=lambda item: str(item.relative_to(package_root))):
        if "__pycache__" in path.parts:
            continue
        try:
            stat = path.stat()
            content = path.read_bytes()
        except OSError:
            continue
        digest.update(str(path.relative_to(package_root)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
        source_file_count += 1
        latest_mtime = max(latest_mtime, int(stat.st_mtime))
    return {
        "package_version": _package_version(),
        "source_fingerprint": digest.hexdigest()[:12] if source_file_count else "unknown",
        "source_file_count": source_file_count,
        "latest_source_mtime": latest_mtime,
        "computed_at": int(time.time()),
    }


def _package_version() -> str:
    try:
        return importlib_metadata.version("polymarket-wallet-research")
    except importlib_metadata.PackageNotFoundError:
        return _pyproject_version()


def _pyproject_version() -> str:
    for base in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]:
        pyproject = base / "pyproject.toml"
        if not pyproject.exists():
            continue
        try:
            lines = pyproject.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            key, separator, value = line.partition("=")
            if separator and key.strip() == "version":
                return value.strip().strip("\"'")
    return "unknown"


def _render_dashboard(settings: RobotSettings) -> str:
    data = _dashboard_data_cached(settings)
    levels = data["level_counts"]
    queues = data["queues"]
    high_rows = data["high_level_wallets"]
    schema_notice = ""
    if not data.get("schema_ready"):
        schema_notice = (
            '<section class="notice warn"><strong>数据库结构尚未就绪</strong>'
            '<span>请先完成最新数据库迁移。</span></section>'
        )
    body = (
        _top_nav("overview")
        + '<main class="shell">'
        + '<header class="page-head"><div><p class="eyebrow">POLYMARKET WALLET RESEARCH</p>'
        + '<h1>钱包研究分级</h1>'
        + f'<p>更新 {_fmt_ts(data.get("generated_at"))} · 数据库 {_fmt_bytes(data.get("database_size_bytes"))}</p></div>'
        + '<div class="actions"><a class="button secondary" href="/api/summary">JSON</a>'
        + '<a class="button" href="/wallets">钱包目录</a></div></header>'
        + schema_notice
        + '<section class="section-head"><div><h2>L0-L6 分布</h2>'
        + f'<p>当前共 {_fmt_int(data.get("wallet_count"))} 个钱包，等级仅表达研究资源优先级。</p></div></section>'
        + _level_grid(levels)
        + '<section class="section-head split"><div><h2>处理队列</h2><p>统计初筛、历史采集和少量 L6 独立复核任务。</p></div>'
        + '<span class="section-meta">24 小时完成量</span></section>'
        + _queue_board(queues)
        + '<section class="section-head split"><div><h2>高等级钱包</h2>'
        + f'<p>L3-L5 当前优选与 L6 独立复核钱包；当前 L5/L6 {_fmt_int(data.get("current_elite_wallet_count"))} 个，已验证 L6 {_fmt_int(data.get("verified_l6_wallet_count"))} 个。</p></div>'
        + f'<a href="/wallets?level=l3">查看目录</a></section>'
        + _wallet_table(high_rows)
        + '<section class="grid two-col">'
        + _selection_section(data.get("selection_summary") or [])
        + _level_changes_section(data.get("recent_level_changes") or [])
        + '</section></main>'
    )
    return _render_page("钱包研究分级", body)


def _render_wallets(settings: RobotSettings, *, level: str = "", query: str = "") -> str:
    selected_level = level if level in LEVEL_VALUES else ""
    data = discovery_data(settings, level=selected_level, query=query, limit=150)
    options = ['<option value="">全部等级</option>']
    for value, label, _ in LEVEL_DEFINITIONS:
        selected = " selected" if selected_level == value else ""
        options.append(f'<option value="{value}"{selected}>{value.upper()} · {_e(label)}</option>')
    api_query = urlencode({"level": selected_level, "q": query, "limit": 150})
    body = (
        _top_nav("wallets")
        + '<main class="shell">'
        + '<header class="page-head"><div><p class="eyebrow">RESEARCH DIRECTORY</p>'
        + '<h1>钱包目录</h1><p>按等级、地址或来源检索研究摘要。</p></div>'
        + f'<div class="actions"><a class="button secondary" href="/api/wallets?{_e(api_query)}">JSON</a>'
        + '<a class="button" href="/">返回总览</a></div></header>'
        + '<form class="filter-bar" method="get" action="/wallets">'
        + f'<label><span>等级</span><select name="level">{"".join(options)}</select></label>'
        + f'<label class="grow"><span>地址或来源</span><input name="q" value="{_e(query)}" placeholder="0x... / rtds / leaderboard"></label>'
        + '<button class="button" type="submit">筛选</button>'
        + '<a class="button secondary" href="/wallets">重置</a></form>'
        + '<section class="section-head"><div><h2>研究摘要</h2>'
        + f'<p>当前显示 {_fmt_int(data.get("wallet_count"))} 个钱包。</p></div></section>'
        + _wallet_table(data.get("wallets") or [])
        + '</main>'
    )
    return _render_page("钱包目录", body)


def _render_wallet_detail(settings: RobotSettings, address: str) -> str:
    data = wallet_detail_data(settings, address)
    if not data.get("found"):
        return _render_page(
            "钱包不存在",
            _top_nav("wallets")
            + f'<main class="empty-page"><h1>未找到钱包</h1><p class="mono">{_e(address)}</p>'
            + '<a class="button" href="/wallets">返回目录</a></main>',
        )
    level = data.get("level") or {}
    source = data.get("source") or {}
    screen = data.get("screen") or {}
    pnl = data.get("pnl") or {}
    history = data.get("history") or {}
    artifact = data.get("artifact") or {}
    validations = data.get("l6_validations") or []
    latest_validation = validations[0] if validations else {}
    level_value = str(level.get("level") or "l0")
    body = (
        _top_nav("wallets")
        + '<main class="shell">'
        + '<header class="page-head wallet-head"><div>'
        + f'<div class="level-badge strong">{_e(level_value.upper())}</div>'
        + f'<h1 class="wallet-address">{_e(address)}</h1>'
        + f'<p>{_e(source.get("sources") or "来源待记录")} · 最近更新 {_fmt_ts(level.get("updated_at"))}</p></div>'
        + '<div class="actions"><a class="button secondary" href="/api/wallet/'
        + _e(address)
        + '">JSON</a><a class="button" href="/wallets">返回目录</a></div></header>'
        + '<section class="metric-row">'
        + _metric("预估总收益", _fmt_money(pnl.get("total_estimated_pnl_usdc")), "收益接口覆盖范围内")
        + _metric("成本口径 ROI", _fmt_pct(pnl.get("cost_roi_estimate")), "无成本基数时不估算")
        + _metric("研究分", _fmt_score(history.get("research_score")), "用于同组相对排序")
        + _metric("历史深度", _history_label(history.get("history_depth")), f'{_fmt_int(history.get("activity_count"))} 条活动')
        + '</section>'
        + '<section class="detail-grid">'
        + _detail_panel("等级状态", [
            ("当前等级", level_value.upper()),
            ("等级原因", _reason_label(level.get("level_reason"))),
            ("规则版本", level.get("policy_version")),
            ("首次发现", _fmt_ts(level.get("first_seen_at"))),
            ("最近发现", _fmt_ts(level.get("last_seen_at"))),
        ])
        + _detail_panel("快速初筛", [
            ("样本成交", f'{_fmt_int(screen.get("sample_trade_count"))} / {_fmt_int(screen.get("sample_limit"))}'),
            ("样本金额", _fmt_money(screen.get("sample_volume_usdc"))),
            ("样本市场", _fmt_int(screen.get("sample_market_count"))),
            ("初筛结果", "通过" if int(screen.get("screen_qualified") or 0) else "未通过或未完成"),
            ("计算时间", _fmt_ts(screen.get("computed_at"))),
        ])
        + _detail_panel("收益概况", [
            ("当前持仓价值", _fmt_money(pnl.get("current_position_value_usdc"))),
            ("持仓预估收益", _fmt_money(pnl.get("open_estimated_pnl_usdc"))),
            ("已结束持仓收益", _fmt_money(pnl.get("closed_realized_pnl_usdc"))),
            ("成本基数", _fmt_money(pnl.get("capital_basis_usdc"))),
            ("覆盖范围", pnl.get("coverage") or "none"),
        ])
        + _detail_panel("历史摘要", [
            ("活动数", _fmt_int(history.get("activity_count"))),
            ("市场数", _fmt_int(history.get("distinct_markets"))),
            ("总成交额", _fmt_money(history.get("total_volume_usdc"))),
            ("快盘占比", _fmt_pct(history.get("fast_market_share"))),
            ("最大市场占比", _fmt_pct(history.get("market_volume_top_share"))),
        ])
        + _detail_panel("L6 独立复核", [
            ("当前状态", _l6_status_label(level, latest_validation)),
            ("官方全历史 PnL", _fmt_money(latest_validation.get("official_all_pnl_usdc"))),
            ("官方累计成交量", _fmt_money(latest_validation.get("official_all_volume_usdc"))),
            ("利润强度（非 ROI）", _fmt_pct(latest_validation.get("official_profit_intensity"))),
            ("90 天已实现收益", _fmt_money(latest_validation.get("realized_pnl_usdc"))),
            ("最近 30 天收益", _fmt_money(latest_validation.get("recent_realized_pnl_usdc"))),
            ("盈利周占比", _fmt_pct(latest_validation.get("positive_week_ratio"))),
            ("复核时间", _fmt_ts(latest_validation.get("validated_at"))),
        ])
        + '</section>'
        + '<section class="grid two-col">'
        + _tag_section("策略标签", history.get("strategy_tags") or [], empty="暂无策略标签")
        + _tag_section("风险标签", history.get("risk_flags") or [], empty="暂无风险标签", risk=True)
        + '</section>'
        + '<section class="section-head"><div><h2>分级决策</h2><p>保留每次相对排名的规则版本与组内位置。</p></div></section>'
        + _simple_table(
            data.get("selections") or [],
            (("target_level", "目标"), ("selected", "入选"), ("rank_in_cohort", "组内排名"),
             ("cohort_size", "组规模"), ("source_bucket", "来源组"), ("strategy_bucket", "策略组"),
             ("policy_version", "规则版本"), ("decided_at", "决策时间")),
            time_keys={"decided_at"},
        )
        + '<section class="section-head"><div><h2>L6 复核记录</h2><p>独立复核不会改写 L5 评分；只有通过才升级到 L6。</p></div></section>'
        + _simple_table(
            _l6_validation_table_rows(validations),
            (("decision_label", "结论"), ("reason", "原因"),
             ("official_pnl", "官方全历史 PnL"), ("profit_intensity_label", "利润强度（非 ROI）"),
             ("realized_pnl", "90 天收益"), ("recent_pnl", "30 天收益"),
             ("active_weeks", "活跃周"), ("positive_week_ratio_label", "盈利周占比"),
             ("top_market_share_label", "最大市场盈利占比"), ("validated_at", "复核时间")),
            time_keys={"validated_at"},
        )
        + '<section class="section-head"><div><h2>处理记录</h2><p>显示快速初筛、历史采集与 L6 独立复核任务。</p></div></section>'
        + _simple_table(
            data.get("pipeline_jobs") or [],
            (("job_type", "任务"), ("job_scope", "范围"), ("status", "状态"),
             ("attempts", "尝试"), ("updated_at", "更新"), ("completed_at", "完成")),
            time_keys={"updated_at", "completed_at"},
        )
        + '<section class="grid two-col">'
        + _artifact_section(artifact)
        + _level_event_section(data.get("level_events") or [])
        + '</section></main>'
    )
    return _render_page(address, body)


def _top_nav(active: str) -> str:
    overview = " active" if active == "overview" else ""
    wallets = " active" if active == "wallets" else ""
    return (
        '<nav class="topbar"><a class="brand" href="/">PM ROBOT <span>RESEARCH</span></a>'
        + '<div class="nav-links">'
        + f'<a class="{overview.strip()}" href="/">研究总览</a>'
        + f'<a class="{wallets.strip()}" href="/wallets">钱包目录</a>'
        + '</div><a class="logout" href="/logout">退出</a></nav>'
    )


def _level_grid(rows: list[dict[str, Any]]) -> str:
    items = []
    for row in rows:
        level = str(row.get("level") or "l0")
        items.append(
            f'<a class="level-cell level-{_e(level)}" href="/wallets?level={_e(level)}">'
            f'<div><span class="level-code">{_e(level.upper())}</span><span>{_e(row.get("label"))}</span></div>'
            f'<strong>{_fmt_int(row.get("count"))}</strong>'
            f'<small>{_e(row.get("description"))}</small></a>'
        )
    return '<section class="level-grid">' + "".join(items) + '</section>'


def _queue_board(rows: list[dict[str, Any]]) -> str:
    blocks = []
    for row in rows:
        counts = row.get("status_counts") or {}
        state = "busy" if int(row.get("active") or 0) else "idle"
        blocks.append(
            f'<article class="queue-row {state}"><div class="queue-main"><span class="state-dot"></span><div>'
            f'<h3>{_e(row.get("label"))}</h3><p>{_e(row.get("description"))}</p></div></div>'
            f'<div class="queue-stat"><span>等待</span><strong>{_fmt_int(counts.get("queued"))}</strong></div>'
            f'<div class="queue-stat"><span>处理中</span><strong>{_fmt_int(counts.get("running"))}</strong></div>'
            f'<div class="queue-stat"><span>24h 完成</span><strong>{_fmt_int(row.get("completed_24h"))}</strong></div>'
            f'<div class="queue-stat"><span>最久等待</span><strong>{_duration(row.get("oldest_wait_seconds"))}</strong></div>'
            '</article>'
        )
    return '<section class="queue-board">' + "".join(blocks) + '</section>'


def _wallet_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<div class="empty-state"><strong>暂无匹配钱包</strong><span>等待发现与分级任务产生新结果。</span></div>'
    body = []
    for row in rows:
        wallet = str(row.get("wallet") or "")
        rank = "-"
        if int(row.get("rank_in_cohort") or 0):
            rank = f'{_fmt_int(row.get("rank_in_cohort"))}/{_fmt_int(row.get("cohort_size"))}'
        level_value = str(row.get("level") or "l0")
        level_note = _reason_label(row.get("level_reason"))
        if level_value == "l5":
            level_note = "评分精英，等待独立复核" if row.get("current_elite") else "历史 L5，等待新证据复核"
        elif level_value == "l6":
            level_note = "独立复核已通过" if row.get("verified_l6") else "历史 L6，等待定期复核"
        body.append(
            '<tr>'
            f'<td><a class="wallet-link mono" href="/wallet/{_e(wallet)}" title="{_e(wallet)}">{_e(_short_wallet(wallet))}</a>'
            f'<small>{_e(row.get("sources") or "来源待记录")}</small></td>'
            f'<td><span class="level-badge">{_e(level_value.upper())}</span><small class="level-reason">{_e(level_note)}</small></td>'
            f'<td class="num"><strong>{_fmt_money(row.get("total_estimated_pnl_usdc"))}</strong><small>ROI {_fmt_pct(row.get("cost_roi_estimate"))}</small></td>'
            f'<td class="num"><strong>{_fmt_score(row.get("research_score"))}</strong><small>组内 {rank}</small></td>'
            f'<td><strong>{_e(_history_label(row.get("history_depth")))}</strong><small>{_fmt_int(row.get("activity_count"))} 条 · {_fmt_int(row.get("distinct_markets"))} 市场</small></td>'
            f'<td>{_tag_inline(row.get("strategy_tags") or [], empty="未归类")}</td>'
            f'<td>{_tag_inline(row.get("risk_flags") or [], empty="无显著风险", risk=True)}</td>'
            f'<td><span>{_fmt_ts(row.get("updated_at"))}</span></td>'
            '</tr>'
        )
    return (
        '<div class="table-wrap"><table class="data-table"><thead><tr>'
        '<th>钱包 / 来源</th><th>等级</th><th>预估收益</th><th>研究分</th>'
        '<th>历史</th><th>策略</th><th>风险</th><th>更新</th>'
        '</tr></thead><tbody>' + "".join(body) + '</tbody></table></div>'
    )


def _selection_section(rows: list[dict[str, Any]]) -> str:
    if not rows:
        content = '<div class="empty-state compact"><span>暂无分级决策记录。</span></div>'
    else:
        content = _simple_table(
            rows,
            (("target_level", "目标"), ("decisions", "已评估"), ("selected", "已入选"),
             ("policy_versions", "规则版本数"), ("latest_decided_at", "最近决策")),
            time_keys={"latest_decided_at"},
        )
    return '<section><div class="section-head"><div><h2>相对优选</h2><p>没有永久总分门槛。</p></div></div>' + content + '</section>'


def _level_changes_section(rows: list[dict[str, Any]]) -> str:
    if not rows:
        content = '<div class="empty-state compact"><span>暂无等级变更记录。</span></div>'
    else:
        content = _simple_table(
            rows,
            (("wallet", "钱包"), ("from_level", "原等级"), ("to_level", "新等级"),
             ("reason", "原因"), ("created_at", "时间")),
            time_keys={"created_at"},
            wallet_keys={"wallet"},
        )
    return '<section><div class="section-head"><div><h2>最近升级</h2><p>等级只前进一个层级。</p></div></div>' + content + '</section>'


def _metric(label: str, value: str, note: str) -> str:
    return f'<div class="metric"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'


def _detail_panel(title: str, rows: list[tuple[str, Any]]) -> str:
    items = "".join(
        f'<div class="detail-row"><span>{_e(label)}</span><strong>{_e(value if value not in (None, "") else "-")}</strong></div>'
        for label, value in rows
    )
    return f'<section class="detail-panel"><h2>{_e(title)}</h2>{items}</section>'


def _l6_status_label(level: dict[str, Any], validation: dict[str, Any]) -> str:
    if bool(level.get("verified_l6")):
        return "已通过"
    decision = str(validation.get("decision") or "")
    if decision == "warning":
        return "需进一步复核，保留 L5"
    if decision == "fail":
        return "未通过，保留 L5"
    if decision == "pass":
        return "历史通过，等待刷新"
    return "等待复核" if str(level.get("level") or "") == "l5" else "未进入复核"


def _l6_validation_table_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    decision_labels = {"pass": "通过", "warning": "需复核", "fail": "未通过"}
    return [
        {
            **row,
            "decision_label": decision_labels.get(str(row.get("decision") or ""), "-"),
            "official_pnl": _fmt_money(row.get("official_all_pnl_usdc")),
            "profit_intensity_label": _fmt_pct(row.get("official_profit_intensity")),
            "realized_pnl": _fmt_money(row.get("realized_pnl_usdc")),
            "recent_pnl": _fmt_money(row.get("recent_realized_pnl_usdc")),
            "positive_week_ratio_label": _fmt_pct(row.get("positive_week_ratio")),
            "top_market_share_label": _fmt_pct(row.get("top_market_profit_share")),
        }
        for row in rows
    ]


def _tag_section(title: str, tags: list[str], *, empty: str, risk: bool = False) -> str:
    class_name = " tags-risk" if risk else ""
    content = _tag_inline(tags, empty=empty, risk=risk)
    return f'<section><div class="section-head"><div><h2>{_e(title)}</h2></div></div><div class="tag-band{class_name}">{content}</div></section>'


def _artifact_section(artifact: dict[str, Any]) -> str:
    rows = [
        ("归档编号", artifact.get("artifact_id")),
        ("历史深度", _history_label(artifact.get("history_depth"))),
        ("行数", _fmt_int(artifact.get("row_count"))),
        ("体积", _fmt_bytes(artifact.get("byte_size"))),
        ("存储版本", artifact.get("storage_version")),
        ("状态", artifact.get("status")),
    ]
    return _detail_panel("历史归档", rows)


def _level_event_section(rows: list[dict[str, Any]]) -> str:
    if not rows:
        content = '<div class="empty-state compact"><span>暂无等级事件。</span></div>'
    else:
        content = _simple_table(
            rows,
            (("from_level", "原等级"), ("to_level", "新等级"), ("reason", "原因"),
             ("policy_version", "规则版本"), ("created_at", "时间")),
            time_keys={"created_at"},
        )
    return '<section><div class="section-head"><div><h2>等级事件</h2></div></div>' + content + '</section>'


def _simple_table(
    rows: list[dict[str, Any]],
    columns: tuple[tuple[str, str], ...],
    *,
    time_keys: set[str] | None = None,
    wallet_keys: set[str] | None = None,
) -> str:
    if not rows:
        return '<div class="empty-state compact"><span>暂无记录。</span></div>'
    time_keys = time_keys or set()
    wallet_keys = wallet_keys or set()
    head = "".join(f'<th>{_e(label)}</th>' for _, label in columns)
    body = []
    for row in rows:
        cells = []
        for key, _ in columns:
            value: Any = row.get(key)
            if key in time_keys:
                value = _fmt_ts(value)
            elif key in wallet_keys:
                value = _short_wallet(str(value or ""))
            elif key == "selected":
                value = "是" if int(value or 0) else "否"
            elif key == "job_type":
                value = _job_label(value)
            elif key == "status":
                value = _status_label(value)
            elif key in {"target_level", "from_level", "to_level"}:
                value = str(value or "-").upper()
            cells.append(f'<td>{_e(value if value not in (None, "") else "-")}</td>')
        body.append('<tr>' + "".join(cells) + '</tr>')
    return f'<div class="table-wrap compact-table"><table class="data-table"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def _tag_inline(tags: list[str], *, empty: str, risk: bool = False) -> str:
    if not tags:
        return f'<span class="muted">{_e(empty)}</span>'
    class_name = "tag risk" if risk else "tag"
    return '<div class="tag-list">' + "".join(
        f'<span class="{class_name}">{_e(_tag_label(str(tag)))}</span>' for tag in tags[:5]
    ) + '</div>'


def _tag_label(value: str) -> str:
    return {
        "multi_market": "多市场",
        "high_frequency": "高频",
        "fast_market_specialist": "快盘专长",
        "two_sided": "双向交易",
        "thin_history": "历史偏薄",
        "market_concentration": "市场集中",
        "negative_pnl": "收益为负",
        "profit_concentration_watch": "收益集中待观察",
        "hard_risk_block": "硬风险阻断",
    }.get(value, value.replace("_", " "))


def _reason_label(value: Any) -> str:
    reason = str(value or "")
    return {
        "relative_rank_selected": "相对排名入选",
        "relative_rank_below_percentile": "相对排名低于本层晋级区间",
        "relative_rank_capacity_limited": "已达相对排名区间但本轮名额已满",
        "verified_trade": "有效成交初筛",
        "trusted_source": "可信来源初筛",
        "sample_volume_at_least_100_usdc": "样本金额通过",
        "legacy_candidate_backfill": "历史候选回填",
        "legacy_sighting_backfill": "历史来源回填",
    }.get(reason, reason.replace("_", " "))


def _job_label(value: Any) -> str:
    return {
        "wallet_recent_screen": "快速初筛",
        "wallet_history_collect": "历史采集",
    }.get(str(value or ""), str(value or "-"))


def _status_label(value: Any) -> str:
    return {
        "queued": "等待",
        "running": "处理中",
        "done": "完成",
        "failed": "失败",
        "cancelled": "已取消",
        "superseded": "已替换",
    }.get(str(value or ""), str(value or "-"))


def _render_login(error: str) -> str:
    message = f'<p class="form-error">{_e(error)}</p>' if error else ""
    return _render_page(
        "登录",
        '<main class="login-shell"><section class="login-box"><p class="eyebrow">PM ROBOT RESEARCH</p>'
        '<h1>钱包研究分级</h1><p>请输入访问令牌。</p>'
        + message
        + '<form method="post" action="/login"><label><span>访问令牌</span>'
        '<input type="password" name="token" autocomplete="current-password" required autofocus></label>'
        '<button class="button" type="submit">登录</button></form></section></main>',
    )


def _render_missing_token() -> str:
    return _render_page(
        "服务未配置",
        '<main class="login-shell"><section class="login-box"><h1>访问令牌尚未配置</h1>'
        '<p>请检查 Web 服务环境配置。</p></section></main>',
    )


def _render_page(title: str, body: str) -> str:
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<title>{_e(title)}</title><style>{_STYLES}</style></head><body>{body}</body></html>'
    )


def _auth_cookie(token: str) -> str:
    return f"{SESSION_COOKIE}={token}; Path=/; Max-Age=86400; HttpOnly; SameSite=Lax"


def _cookie_value(header: str, name: str) -> str:
    for item in header.split(";"):
        key, separator, value = item.strip().partition("=")
        if separator and key == name:
            return value
    return ""


def _first(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key) or [""]
    return str(values[0]).strip()


def _int_param(params: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int(_first(params, key) or default)
    except ValueError:
        return default


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone() is not None


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def _json_list(raw: Any) -> list[str]:
    try:
        value = json.loads(str(raw or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def _json_dict(raw: Any) -> dict[str, Any]:
    try:
        value = json.loads(str(raw or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def _fmt_score(value: Any) -> str:
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_money(value: Any) -> str:
    try:
        amount = float(value or 0)
    except (TypeError, ValueError):
        return "-"
    sign = "+" if amount > 0 else ""
    return f"{sign}${amount:,.2f}"


def _fmt_pct(value: Any) -> str:
    if value is None or value == "":
        return "-"
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def _fmt_ts(value: Any) -> str:
    try:
        timestamp = int(value or 0)
    except (TypeError, ValueError):
        return "-"
    if timestamp <= 0:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp))


def _fmt_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB")
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    return f"{size:.1f} {units[index]}"


def _duration(value: Any) -> str:
    try:
        seconds = max(0, int(value or 0))
    except (TypeError, ValueError):
        return "-"
    if not seconds:
        return "-"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86_400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86_400:.1f}d"


def _history_label(value: Any) -> str:
    return {"none": "无", "sample": "样本", "light": "轻量", "deep": "深度"}.get(
        str(value or "none"), str(value or "无")
    )


def _short_wallet(value: str) -> str:
    return f"{value[:8]}...{value[-6:]}" if len(value) > 18 else value


_STYLES = r"""
:root {
  --ink: #18211d;
  --muted: #69736e;
  --line: #dbe1dd;
  --soft: #f4f6f4;
  --surface: #ffffff;
  --brand: #176b4d;
  --brand-dark: #0d4e38;
  --blue: #2f6f99;
  --amber: #a46410;
  --red: #a23b36;
  --shadow: 0 6px 22px rgba(26, 43, 35, .07);
}
* { box-sizing: border-box; }
html { background: #eef1ef; color: var(--ink); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; letter-spacing: 0; }
body { margin: 0; min-width: 320px; }
a { color: var(--brand-dark); text-decoration: none; }
a:hover { text-decoration: underline; }
.topbar { height: 58px; display: flex; align-items: center; gap: 28px; padding: 0 max(20px, calc((100vw - 1440px) / 2)); background: #12241d; color: #fff; border-bottom: 1px solid #263b32; }
.brand { color: #fff; font-size: 14px; font-weight: 800; white-space: nowrap; }
.brand span { color: #7bc5a3; font-weight: 650; }
.nav-links { display: flex; align-items: stretch; align-self: stretch; }
.nav-links a { display: flex; align-items: center; padding: 0 16px; color: #c5d1cb; font-size: 14px; border-bottom: 3px solid transparent; }
.nav-links a:hover, .nav-links a.active { color: #fff; border-bottom-color: #58b98d; text-decoration: none; }
.logout { margin-left: auto; color: #c5d1cb; font-size: 13px; }
.shell { width: min(1440px, calc(100% - 40px)); margin: 0 auto; padding: 28px 0 56px; }
.page-head { display: flex; align-items: flex-end; justify-content: space-between; gap: 24px; padding-bottom: 22px; border-bottom: 1px solid var(--line); }
.page-head h1 { margin: 5px 0 5px; font-size: 30px; line-height: 1.2; }
.page-head p { margin: 0; color: var(--muted); font-size: 14px; }
.eyebrow { color: var(--brand) !important; font-size: 11px !important; font-weight: 800; }
.actions { display: flex; gap: 8px; flex-wrap: wrap; }
.button { min-height: 38px; display: inline-flex; align-items: center; justify-content: center; padding: 0 15px; border: 1px solid var(--brand); border-radius: 5px; background: var(--brand); color: #fff; font: inherit; font-size: 13px; font-weight: 700; cursor: pointer; }
.button:hover { background: var(--brand-dark); text-decoration: none; }
.button.secondary { background: #fff; color: var(--brand-dark); border-color: #b9c6bf; }
.section-head { display: flex; align-items: flex-end; justify-content: space-between; gap: 16px; margin: 30px 0 12px; }
.section-head h2 { margin: 0 0 3px; font-size: 18px; }
.section-head p { margin: 0; color: var(--muted); font-size: 13px; }
.section-meta { color: var(--muted); font-size: 12px; }
.notice { display: flex; gap: 12px; margin-top: 18px; padding: 13px 15px; border-left: 4px solid var(--amber); background: #fff7e8; color: #71450b; }
.notice span { color: #7c6647; }
.level-grid { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); background: var(--surface); border: 1px solid var(--line); box-shadow: var(--shadow); }
.level-cell { min-width: 0; padding: 17px 15px; color: var(--ink); border-right: 1px solid var(--line); }
.level-cell:last-child { border-right: 0; }
.level-cell:hover { background: #f8faf8; text-decoration: none; }
.level-cell > div { display: flex; align-items: center; gap: 7px; font-size: 12px; color: var(--muted); }
.level-cell strong { display: block; margin: 9px 0 6px; font-size: 27px; font-variant-numeric: tabular-nums; }
.level-cell small { display: block; min-height: 36px; color: var(--muted); line-height: 1.45; }
.level-code, .level-badge { display: inline-flex; align-items: center; justify-content: center; min-width: 35px; min-height: 23px; padding: 2px 7px; border: 1px solid #aebbb4; border-radius: 4px; background: #f2f5f3; color: #25352d; font-size: 11px; font-weight: 800; }
.level-l3 .level-code, .level-l3 .level-badge { color: var(--blue); border-color: #9abed5; background: #edf6fb; }
.level-l4 .level-code, .level-l4 .level-badge { color: var(--amber); border-color: #d5b178; background: #fff7e9; }
.level-l5 .level-code, .level-l5 .level-badge { color: var(--blue); border-color: #79a9c8; background: #e7f2f8; }
.level-l6 .level-code, .level-l6 .level-badge, .level-badge.strong { color: #fff; border-color: var(--brand); background: var(--brand); }
.queue-board { border: 1px solid var(--line); background: var(--surface); box-shadow: var(--shadow); }
.queue-row { display: grid; grid-template-columns: minmax(300px, 1.6fr) repeat(4, minmax(90px, .45fr)); align-items: center; min-height: 82px; border-bottom: 1px solid var(--line); }
.queue-row:last-child { border-bottom: 0; }
.queue-main { display: flex; align-items: center; gap: 12px; padding: 14px 18px; }
.queue-main h3 { margin: 0 0 4px; font-size: 14px; }
.queue-main p { margin: 0; color: var(--muted); font-size: 12px; }
.state-dot { width: 9px; height: 9px; flex: 0 0 auto; border-radius: 50%; background: #95a09a; }
.queue-row.busy .state-dot { background: #2b9a68; box-shadow: 0 0 0 4px #e2f3eb; }
.queue-stat { height: 100%; display: flex; flex-direction: column; justify-content: center; padding: 12px 16px; border-left: 1px solid var(--line); }
.queue-stat span { color: var(--muted); font-size: 11px; }
.queue-stat strong { margin-top: 4px; font-size: 18px; font-variant-numeric: tabular-nums; }
.table-wrap { overflow-x: auto; border: 1px solid var(--line); background: var(--surface); box-shadow: var(--shadow); }
.data-table { width: 100%; min-width: 880px; border-collapse: collapse; font-size: 13px; }
.data-table th { padding: 11px 12px; background: var(--soft); color: #57625d; font-size: 11px; text-align: left; white-space: nowrap; border-bottom: 1px solid var(--line); }
.data-table td { padding: 12px; vertical-align: top; border-bottom: 1px solid #e8ece9; }
.data-table tr:last-child td { border-bottom: 0; }
.data-table tbody tr:hover { background: #fbfcfb; }
.data-table td small { display: block; max-width: 220px; margin-top: 4px; color: var(--muted); line-height: 1.35; overflow-wrap: anywhere; }
.data-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.wallet-link { font-weight: 750; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.tag-list { display: flex; gap: 4px; flex-wrap: wrap; }
.tag { display: inline-flex; padding: 3px 6px; border: 1px solid #b7cec2; border-radius: 4px; background: #edf6f1; color: #235f45; font-size: 10px; line-height: 1.2; }
.tag.risk { border-color: #d8bbb7; background: #fff2f0; color: #8a3732; }
.muted { color: var(--muted); }
.empty-state { min-height: 150px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 5px; border: 1px dashed #bdc8c1; background: rgba(255,255,255,.55); color: var(--muted); }
.empty-state strong { color: var(--ink); }
.empty-state.compact { min-height: 90px; }
.grid { display: grid; gap: 28px; }
.grid > * { min-width: 0; }
.two-col { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.compact-table { box-shadow: none; }
.compact-table .data-table { min-width: 560px; }
.metric-row { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); margin: 22px 0 0; border: 1px solid var(--line); background: var(--surface); box-shadow: var(--shadow); }
.metric { min-width: 0; padding: 16px; border-right: 1px solid var(--line); }
.metric:last-child { border-right: 0; }
.metric span, .metric small { display: block; color: var(--muted); font-size: 11px; }
.metric strong { display: block; margin: 7px 0 5px; font-size: 22px; font-variant-numeric: tabular-nums; }
.detail-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 14px; margin-top: 18px; }
.detail-panel { border: 1px solid var(--line); background: var(--surface); box-shadow: var(--shadow); }
.detail-panel h2 { margin: 0; padding: 12px 14px; border-bottom: 1px solid var(--line); background: var(--soft); font-size: 14px; }
.detail-row { display: flex; justify-content: space-between; gap: 14px; padding: 9px 14px; border-bottom: 1px solid #edf0ee; font-size: 12px; }
.detail-row:last-child { border-bottom: 0; }
.detail-row span { color: var(--muted); }
.detail-row strong { max-width: 62%; text-align: right; overflow-wrap: anywhere; }
.wallet-head { align-items: center; }
.wallet-head > div:first-child { min-width: 0; }
.wallet-address { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 24px !important; overflow-wrap: anywhere; }
.tag-band { min-height: 64px; display: flex; align-items: center; padding: 13px; border: 1px solid var(--line); background: var(--surface); }
.filter-bar { display: flex; align-items: end; gap: 10px; margin-top: 20px; padding: 15px; border: 1px solid var(--line); background: var(--surface); box-shadow: var(--shadow); }
.filter-bar label, .login-box label { display: flex; flex-direction: column; gap: 5px; min-width: 180px; color: var(--muted); font-size: 11px; }
.filter-bar .grow { flex: 1; }
input, select { width: 100%; height: 38px; padding: 0 10px; border: 1px solid #b8c3bd; border-radius: 4px; background: #fff; color: var(--ink); font: inherit; }
input:focus, select:focus { outline: 2px solid #83bda2; outline-offset: 1px; }
.login-shell, .empty-page { min-height: calc(100vh - 58px); display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 30px; }
.login-box { width: min(420px, 100%); padding: 28px; border: 1px solid var(--line); border-radius: 6px; background: #fff; box-shadow: var(--shadow); }
.login-box h1 { margin: 8px 0; font-size: 25px; }
.login-box > p { color: var(--muted); }
.login-box form { display: grid; gap: 14px; margin-top: 22px; }
.form-error { padding: 9px 11px; background: #fff0ee; color: var(--red) !important; }
@media (max-width: 1100px) {
  .level-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .level-cell:nth-child(3) { border-right: 0; }
  .level-cell:nth-child(-n+3) { border-bottom: 1px solid var(--line); }
  .queue-row { grid-template-columns: minmax(260px, 1.4fr) repeat(2, minmax(90px, .45fr)); }
  .queue-stat:nth-last-child(-n+2) { display: none; }
  .detail-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 760px) {
  .topbar { padding: 0 14px; gap: 10px; }
  .brand span, .logout { display: none; }
  .nav-links { margin-left: auto; }
  .nav-links a { padding: 0 10px; }
  .shell { width: min(100% - 24px, 1440px); padding-top: 20px; }
  .page-head { align-items: flex-start; flex-direction: column; }
  .page-head h1 { font-size: 25px; }
  .actions { width: 100%; }
  .actions .button { flex: 1; }
  .level-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .level-cell, .level-cell:nth-child(3) { border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); }
  .level-cell:nth-child(2n) { border-right: 0; }
  .level-cell:nth-last-child(-n+2) { border-bottom: 0; }
  .queue-row { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .queue-main { grid-column: 1 / -1; padding: 12px; border-bottom: 1px solid var(--line); }
  .queue-main p { display: none; }
  .queue-stat { display: flex !important; min-height: 62px; padding: 10px; }
  .queue-stat:nth-child(2) { border-left: 0; }
  .level-reason { display: none !important; }
  .grid.two-col, .metric-row, .detail-grid { grid-template-columns: 1fr; }
  .metric { border-right: 0; border-bottom: 1px solid var(--line); }
  .metric:last-child { border-bottom: 0; }
  .filter-bar { align-items: stretch; flex-direction: column; }
  .filter-bar label { width: 100%; }
  .filter-bar .button { width: 100%; }
  .wallet-address { font-size: 18px !important; }
}
"""
