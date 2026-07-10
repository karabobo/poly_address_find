"""Read-only web console for pm-robot research operations."""

from __future__ import annotations

import csv
import hashlib
import html
import io
from functools import lru_cache
from importlib import metadata as importlib_metadata
import json
import os
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from pm_robot.config import RobotSettings, load_policy, threshold
from pm_robot.execution.preflight import (
    DEFAULT_ACTIVITY_LOOKBACK_SEC,
    DEFAULT_EXECUTION_SERVICES,
    DEFAULT_MAX_SIGNAL_AGE_SEC,
    DEFAULT_RTDS_WATCH_MIN_SCORE,
    execution_preflight_status,
    paper_realtime_audit_status,
    rtds_watch_audit_status,
)
from pm_robot.orchestration.pipeline_audit import (
    ACTIVE_JOB_STATUSES,
    BLOCKING_CANDIDATE_STAGES,
    HIGH_PRIORITY_PENDING_JOB_PRIORITY,
    PENDING_EVIDENCE_ACTIONS,
)
from pm_robot.orchestration.paper_runner import preview_paper_observer
from pm_robot.orchestration.evidence_readiness import paper_evidence_ready, paper_evidence_ready_sql
from pm_robot.orchestration.wallet_pipeline import (
    DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
    DEFAULT_PIPELINE_STAGE_WEIGHTS,
    wallet_pipeline_schedule_status,
)
from pm_robot.models import CandidateStage
from pm_robot.pipeline_terms import (
    PAPER_ELIGIBLE_CANDIDATE_STAGES,
    EvidenceJobStage,
    EvidenceStatus,
    EvidenceTier,
    PipelineJobType,
)
from pm_robot.storage.api_rate_limit import api_rate_limit_summary
from pm_robot.storage.db import connect_readonly
from pm_robot.storage.repository import evidence_backfill_summary


SESSION_COOKIE = "pm_robot_token"
MAX_LIST_LIMIT = 250
PAPER_HANDOFF_SCHEMA_VERSION = "paper_handoff_v1"
PAPER_OBSERVER_EVALUATION_SCHEMA_VERSION = "paper_observer_evaluation_v1"
PAPER_OBSERVER_CURRENT_EVALUATION_MAX_AGE_SEC = 600
PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC = 300
PAPER_OBSERVER_QUALITY_LOOKBACK_SEC = 7 * 86_400
PAPER_RTDS_BRIDGE_FRESH_SEC = 600
PAPER_RTDS_BRIDGE_RECENT_SEC = 86_400
DASHBOARD_CACHE_TTL_SEC = 30
WAL_WARN_BYTES = 1_000_000_000
WAL_CRITICAL_BYTES = 3_000_000_000
LOW_FREE_DISK_BYTES = 100_000_000_000
BACKUP_MAX_AGE_SECONDS = 26 * 3_600
_DASHBOARD_CACHE_LOCK = threading.Lock()
_DASHBOARD_CACHE: dict[tuple[str, bool], tuple[float, dict[str, Any]]] = {}
_DASHBOARD_REFRESHING: set[tuple[str, bool]] = set()

_CANDIDATE_STAGE_LABELS = {
    CandidateStage.IMPORTED.value: "已导入",
    CandidateStage.NEEDS_DATA.value: "待补证据",
    CandidateStage.NEEDS_REVIEW.value: "自动复核中",
    CandidateStage.PAPER_CANDIDATE.value: "Paper 候选",
    CandidateStage.PAPER_APPROVED.value: "Paper 已批准",
    CandidateStage.LIVE_ELIGIBLE.value: "可交接生产",
    CandidateStage.REJECTED.value: "已拒绝",
    CandidateStage.BLOCKED_HYGIENE.value: "Hygiene 阻断",
    CandidateStage.BLOCKED_COPYABILITY.value: "Copyability 阻断",
}
_EVIDENCE_TIER_LABELS = {
    EvidenceTier.L0_DISCOVERED.value: "L0 已发现",
    EvidenceTier.L1_LIGHT.value: "L1 轻量证据",
    EvidenceTier.L2_MEDIUM.value: "L2 中量证据",
    EvidenceTier.L3_DEEP.value: "L3 深度证据",
}
_EVIDENCE_STATUS_LABELS = {
    EvidenceStatus.PENDING.value: "待规划",
    EvidenceStatus.NEEDS_LIGHT.value: "需要轻量历史",
    EvidenceStatus.NEEDS_MEDIUM.value: "需要中量历史",
    EvidenceStatus.NEEDS_DEEP.value: "需要深度历史",
    EvidenceStatus.QUEUED.value: "已进入队列",
    EvidenceStatus.SUMMARY_READY.value: "证据摘要就绪",
    EvidenceStatus.PAUSED.value: "已暂停",
}
_EVIDENCE_ACTION_LABELS = {
    EvidenceJobStage.LIGHT_PENDING.value: "补轻量历史",
    EvidenceJobStage.LIGHT_DONE.value: "轻量历史完成",
    EvidenceJobStage.MEDIUM_PENDING.value: "补中量历史",
    EvidenceJobStage.MEDIUM_DONE.value: "中量历史完成",
    EvidenceJobStage.DEEP_PENDING.value: "补深度历史",
    EvidenceJobStage.DEEP_DONE.value: "深度历史完成",
    "score_wallet": "进入评分",
    "manual_review_fast_market": "复核快盘风险",
}
_PIPELINE_JOB_TYPE_LABELS = {
    PipelineJobType.WALLET_EVIDENCE_BACKFILL.value: "历史证据",
    PipelineJobType.COPYABILITY_EVIDENCE.value: "Copyability 证据",
}
_JOB_STATUS_LABELS = {
    "queued": "等待执行",
    "running": "执行中",
    "done": "已完成",
    "failed": "已失败",
    "cancelled": "已取消",
}


@dataclass(frozen=True)
class RuntimeLoopSpec:
    key: str
    label: str
    ingest_pattern: str
    max_age_seconds: int


# Web does not need Docker access. These specs summarize logical loop freshness
# from ingest_runs written by workers or by explicit runtime heartbeats.
_RUNTIME_LOOP_SPECS = (
    RuntimeLoopSpec("wallet_pipeline_workers", "钱包证据 workers", "wallet_pipeline_worker_%", 1_800),
    RuntimeLoopSpec("copyability_workers", "Copyability workers", "copyability_evidence_worker_%", 900),
    RuntimeLoopSpec("discovery_leaderboard", "Leaderboard 发现", "loop_discovery_leaderboard", 7_200),
    RuntimeLoopSpec("discovery_activity", "大额交易发现", "loop_discovery_activity", 7_200),
    RuntimeLoopSpec("research_control", "研究控制循环", "loop_research_control", 1_800),
    RuntimeLoopSpec("paper_observer_activity", "Paper 活动快刷", "loop_paper_observer_activity", 300),
    RuntimeLoopSpec("paper_observer_preview", "Paper 预览快刷", "loop_paper_observer_preview", 300),
    RuntimeLoopSpec("paper_observer_evaluation", "Paper 报价快评", "loop_paper_observer_evaluation", 300),
    RuntimeLoopSpec("maintenance", "维护循环", "loop_maintenance", 7_200),
    RuntimeLoopSpec("backup", "数据库备份", "loop_backup", BACKUP_MAX_AGE_SECONDS),
    RuntimeLoopSpec("rtds_discovery", "RTDS 实时发现", "loop_rtds_discovery", 900),
)

_RESEARCH_CONTROL_STEP_SPECS = (
    ("eligibility_repair_prepare", "资格修复"),
    ("wallet_pipeline_state_materialize", "钱包状态物化"),
    ("wallet_pipeline_plan", "钱包队列规划"),
    ("copyability_plan", "Copyability 规划"),
    ("materialize_features", "特征物化"),
    ("incremental_score", "增量评分"),
)
_RESEARCH_CONTROL_STEP_PREFIX = "loop_research_control_step_"


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
    print(f"pm-robot web console listening on http://{config.host}:{config.port}")
    server.serve_forever()


def dashboard_data(settings: RobotSettings, *, include_pair_quality: bool = True) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return _dashboard_data(conn, settings, include_pair_quality=include_pair_quality)
    finally:
        conn.close()


def execution_preflight_data(
    settings: RobotSettings,
    *,
    max_signal_age_sec: int = DEFAULT_MAX_SIGNAL_AGE_SEC,
) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return execution_preflight_status(
            conn,
            max_signal_age_sec=max_signal_age_sec,
            execution_services=DEFAULT_EXECUTION_SERVICES,
            compose_error="not_checked_from_web",
        )
    finally:
        conn.close()


def paper_realtime_audit_data(
    settings: RobotSettings,
    *,
    max_signal_age_sec: int = DEFAULT_MAX_SIGNAL_AGE_SEC,
    lookback_sec: int = DEFAULT_ACTIVITY_LOOKBACK_SEC,
    limit: int = 50,
) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return paper_realtime_audit_status(
            conn,
            max_signal_age_sec=max_signal_age_sec,
            lookback_sec=lookback_sec,
            limit=limit,
        )
    finally:
        conn.close()


def rtds_watch_audit_data(
    settings: RobotSettings,
    *,
    min_score: float = DEFAULT_RTDS_WATCH_MIN_SCORE,
    lookback_sec: int = DEFAULT_ACTIVITY_LOOKBACK_SEC,
    limit: int = 50,
) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return rtds_watch_audit_status(
            conn,
            min_score=min_score,
            lookback_sec=lookback_sec,
            limit=limit,
        )
    finally:
        conn.close()


def paper_pool_expansion_data(settings: RobotSettings, *, limit: int = 50) -> dict[str, Any]:
    paper_thresholds = _paper_candidate_thresholds(settings)
    conn = connect_readonly(settings.db_path)
    try:
        result = _paper_pool_expansion_audit(
            conn,
            paper_min_score=paper_thresholds["min_score"],
            min_copy_events=paper_thresholds["min_copy_events"],
            min_copy_markets=paper_thresholds["min_copy_markets"],
            limit=limit,
        )
        result["policy_loaded"] = paper_thresholds["policy_loaded"]
        result["policy_error"] = paper_thresholds["policy_error"]
        return result
    finally:
        conn.close()


def _dashboard_data_cached(
    settings: RobotSettings,
    *,
    include_pair_quality: bool = True,
    force_refresh: bool = False,
) -> dict[str, Any]:
    ttl = _dashboard_cache_ttl()
    if ttl <= 0:
        return dashboard_data(settings, include_pair_quality=include_pair_quality)
    key = (str(settings.db_path.resolve()), include_pair_quality)
    now = time.time()
    if not force_refresh:
        stale_data: dict[str, Any] | None = None
        should_refresh = False
        with _DASHBOARD_CACHE_LOCK:
            cached = _DASHBOARD_CACHE.get(key)
            if cached and now - cached[0] <= ttl:
                return cached[1]
            if cached:
                stale_data = cached[1]
                if key not in _DASHBOARD_REFRESHING:
                    _DASHBOARD_REFRESHING.add(key)
                    should_refresh = True
        if stale_data is not None:
            if should_refresh:
                _start_dashboard_cache_refresh(settings, include_pair_quality=include_pair_quality, key=key)
            return stale_data
    data = dashboard_data(settings, include_pair_quality=include_pair_quality)
    with _DASHBOARD_CACHE_LOCK:
        _DASHBOARD_CACHE[key] = (time.time(), data)
    return data


def _dashboard_cache_ttl() -> int:
    try:
        return int(os.environ.get("PM_ROBOT_WEB_DASHBOARD_CACHE_TTL_SEC", str(DASHBOARD_CACHE_TTL_SEC)))
    except ValueError:
        return DASHBOARD_CACHE_TTL_SEC


def _web_env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return int(default)


def _prewarm_dashboard_cache(settings: RobotSettings) -> None:
    try:
        _dashboard_data_cached(settings, force_refresh=True)
    except Exception as exc:  # pragma: no cover - startup should continue if diagnostics fail.
        print(f"pm-robot web dashboard cache prewarm skipped: {type(exc).__name__}: {exc}")


def _start_dashboard_cache_prewarm(settings: RobotSettings) -> None:
    """Warm expensive dashboard queries without delaying the listening socket."""

    threading.Thread(
        target=_prewarm_dashboard_cache,
        args=(settings,),
        name="pm-robot-dashboard-prewarm",
        daemon=True,
    ).start()


def _start_dashboard_cache_refresh(
    settings: RobotSettings,
    *,
    include_pair_quality: bool,
    key: tuple[str, bool],
) -> None:
    def refresh() -> None:
        try:
            data = dashboard_data(settings, include_pair_quality=include_pair_quality)
            with _DASHBOARD_CACHE_LOCK:
                _DASHBOARD_CACHE[key] = (time.time(), data)
        except Exception as exc:  # pragma: no cover - diagnostics should not block stale dashboard data.
            print(f"pm-robot web dashboard cache refresh skipped: {type(exc).__name__}: {exc}")
        finally:
            with _DASHBOARD_CACHE_LOCK:
                _DASHBOARD_REFRESHING.discard(key)

    threading.Thread(target=refresh, name="pm-robot-dashboard-refresh", daemon=True).start()


def paper_handoff_data(settings: RobotSettings, *, limit: int = 50) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return _paper_handoff_summary(conn, settings, limit=limit)
    finally:
        conn.close()


def paper_handoff_csv(settings: RobotSettings, *, limit: int = 250) -> str:
    summary = paper_handoff_data(settings, limit=limit)
    return _paper_handoff_csv(summary)


def paper_observer_preview_data(
    settings: RobotSettings,
    *,
    limit: int = 50,
    max_signal_age_sec: int = 21_600,
) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return _paper_observer_preview_summary(
            conn,
            limit=limit,
            max_signal_age_sec=max_signal_age_sec,
        )
    finally:
        conn.close()


def paper_observer_evaluation_data(settings: RobotSettings) -> dict[str, Any]:
    return _paper_observer_evaluation_file(settings)


@lru_cache(maxsize=1)
def _runtime_build_info() -> dict[str, Any]:
    package_root = Path(__file__).resolve().parent
    source_root = package_root.parent
    digest = hashlib.sha256()
    source_file_count = 0
    latest_mtime = 0
    for path in sorted(package_root.rglob("*.py"), key=lambda item: str(item.relative_to(package_root))):
        if "__pycache__" in path.parts:
            continue
        rel_path = str(path.relative_to(package_root)).replace(os.sep, "/")
        try:
            stat = path.stat()
            content = path.read_bytes()
        except OSError:
            continue
        digest.update(rel_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
        source_file_count += 1
        latest_mtime = max(latest_mtime, int(stat.st_mtime))
    return {
        "package_version": _package_version(),
        "source_fingerprint": digest.hexdigest()[:12] if source_file_count else "unknown",
        "source_file_count": source_file_count,
        "source_root": str(source_root),
        "source_delivery": _source_delivery_label(source_root),
        "latest_source_mtime": latest_mtime,
        "computed_at": int(time.time()),
    }


def _source_delivery_label(source_root: Path) -> str:
    root = source_root.resolve()
    if _is_mountpoint(root):
        return "bind_mount"
    if str(root).startswith("/app/src"):
        return "image_source"
    return "local_source"


def _is_mountpoint(path: Path) -> bool:
    try:
        return path.is_mount()
    except OSError:
        return False


def _package_version() -> str:
    try:
        return importlib_metadata.version("polymarket-copy-robot")
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
            key, sep, value = line.partition("=")
            if sep and key.strip() == "version":
                return value.strip().strip("\"'")
    return "unknown"


def discovery_data(
    settings: RobotSettings,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
    limit: int = 150,
) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return _discovery_data(conn, settings, stage=stage, source=source, query=query, signal=signal, limit=limit)
    finally:
        conn.close()


def wallet_table_rows(
    settings: RobotSettings,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    conn = connect_readonly(settings.db_path)
    try:
        return _wallet_table_rows(conn, stage=stage, source=source, query=query, signal=signal, limit=limit)
    finally:
        conn.close()


def wallet_detail_data(settings: RobotSettings, address: str) -> dict[str, Any]:
    conn = connect_readonly(settings.db_path)
    try:
        return _wallet_detail_data(
            conn,
            address,
            paper_min_score=_paper_candidate_min_score(settings),
            research_only=settings.execution_mode in {"research", "scoring", "research_scoring"},
        )
    finally:
        conn.close()


def _handler_factory(config: WebConsoleConfig) -> type[BaseHTTPRequestHandler]:
    class WebConsoleHandler(BaseHTTPRequestHandler):
        server_version = "PMRobotWeb/0.1"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if not self._authorize(parsed):
                return

            if parsed.path == "/":
                self._send_html(_render_dashboard(config.settings))
                return
            if parsed.path in {"/wallets", "/discovery"}:
                params = parse_qs(parsed.query)
                self._send_html(
                    _render_wallets(
                        config.settings,
                        stage=_first(params, "stage"),
                        source=_first(params, "source"),
                        query=_first(params, "q"),
                        signal=_first(params, "signal"),
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
                    dashboard_data(config.settings)
                    if _first(params, "fresh") == "1"
                    else _dashboard_data_cached(config.settings)
                )
                return
            if parsed.path == "/api/paper-handoff":
                params = parse_qs(parsed.query)
                self._send_json(paper_handoff_data(config.settings, limit=_int_param(params, "limit", 50)))
                return
            if parsed.path == "/api/paper-handoff.csv":
                params = parse_qs(parsed.query)
                self._send_csv(
                    paper_handoff_csv(config.settings, limit=_int_param(params, "limit", 250)),
                    filename="pm_robot_paper_handoff.csv",
                )
                return
            if parsed.path == "/api/paper-observer-preview":
                params = parse_qs(parsed.query)
                self._send_json(
                    paper_observer_preview_data(
                        config.settings,
                        limit=_int_param(params, "limit", 50),
                        max_signal_age_sec=_int_param(params, "max_signal_age_sec", 21_600),
                    )
                )
                return
            if parsed.path == "/api/paper-observer-evaluation":
                self._send_json(paper_observer_evaluation_data(config.settings))
                return
            if parsed.path == "/api/execution-preflight":
                params = parse_qs(parsed.query)
                self._send_json(
                    execution_preflight_data(
                        config.settings,
                        max_signal_age_sec=_int_param(params, "max_signal_age_sec", DEFAULT_MAX_SIGNAL_AGE_SEC),
                    )
                )
                return
            if parsed.path == "/api/paper-realtime-audit":
                params = parse_qs(parsed.query)
                self._send_json(
                    paper_realtime_audit_data(
                        config.settings,
                        max_signal_age_sec=_int_param(params, "max_signal_age_sec", DEFAULT_MAX_SIGNAL_AGE_SEC),
                        lookback_sec=_int_param(params, "lookback_sec", DEFAULT_ACTIVITY_LOOKBACK_SEC),
                        limit=_int_param(params, "limit", 50),
                    )
                )
                return
            if parsed.path == "/api/rtds-watch-audit":
                params = parse_qs(parsed.query)
                self._send_json(
                    rtds_watch_audit_data(
                        config.settings,
                        min_score=_float_param(params, "min_score", DEFAULT_RTDS_WATCH_MIN_SCORE),
                        lookback_sec=_int_param(params, "lookback_sec", DEFAULT_ACTIVITY_LOOKBACK_SEC),
                        limit=_int_param(params, "limit", 50),
                    )
                )
                return
            if parsed.path == "/api/paper-pool-expansion":
                params = parse_qs(parsed.query)
                self._send_json(paper_pool_expansion_data(config.settings, limit=_int_param(params, "limit", 50)))
                return
            if parsed.path == "/api/discovery":
                params = parse_qs(parsed.query)
                self._send_json(
                    discovery_data(
                        config.settings,
                        stage=_first(params, "stage"),
                        source=_first(params, "source"),
                        query=_first(params, "q"),
                        signal=_first(params, "signal"),
                        limit=_int_param(params, "limit", 100),
                    )
                )
                return
            if parsed.path == "/api/wallets":
                params = parse_qs(parsed.query)
                self._send_json(
                    wallet_table_rows(
                        config.settings,
                        stage=_first(params, "stage"),
                        source=_first(params, "source"),
                        query=_first(params, "q"),
                        signal=_first(params, "signal"),
                        limit=_int_param(params, "limit", 100),
                    )
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
            self._send_html(_render_page("Not Found", "<h1>Not Found</h1>"), status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/login":
                self._send_html(_render_page("Not Found", "<h1>Not Found</h1>"), status=HTTPStatus.NOT_FOUND)
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
            self._send_html(_render_login(error="重置密钥/访问令牌不正确"), status=HTTPStatus.UNAUTHORIZED)

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
                encoded = urlencode({k: v[0] for k, v in clean_query.items() if v})
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

        def _send_csv(self, body: str, *, filename: str, status: HTTPStatus = HTTPStatus.OK) -> None:
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
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
                "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'",
            )

    return WebConsoleHandler


def _dashboard_data(
    conn: sqlite3.Connection,
    settings: RobotSettings,
    *,
    include_pair_quality: bool = True,
) -> dict[str, Any]:
    paper_thresholds = _paper_candidate_thresholds(settings)
    paper_min_score = paper_thresholds["min_score"]
    total_candidates = _scalar(conn, "SELECT COUNT(*) FROM candidate_wallets")
    stage_rows = _rows(
        conn,
        """
        SELECT candidate_stage AS name, COUNT(*) AS count
        FROM candidate_wallets
        GROUP BY candidate_stage
        ORDER BY count DESC, name ASC
        """,
    )
    score_rows = _rows(
        conn,
        """
        SELECT review_stage AS name, COUNT(*) AS count, AVG(leader_score) AS avg_score, MAX(leader_score) AS max_score
        FROM leader_latest_scores
        GROUP BY review_stage
        ORDER BY count DESC, name ASC
        """,
    )
    source_rows = _rows(
        conn,
        """
        SELECT source AS name, COUNT(DISTINCT address) AS count, MAX(recorded_at) AS latest_at
        FROM candidate_source_events
        GROUP BY source
        ORDER BY count DESC, latest_at DESC
        LIMIT 12
        """,
    )
    latest_runs = _rows(
        conn,
        """
        SELECT ingest_type AS run_type, status, started_at, finished_at, rows_written AS row_count, error
        FROM ingest_runs
        ORDER BY started_at DESC
        LIMIT 8
        """,
    )
    publish = _rows(
        conn,
        """
        SELECT status AS name, COUNT(*) AS count, MAX(published_at) AS latest_at
        FROM leader_publish
        GROUP BY status
        ORDER BY count DESC, name ASC
        """,
    )
    top_review_candidates = _top_review_candidate_rows(conn, paper_min_score=paper_min_score)
    manual_review_actions = _manual_review_action_rows(conn, paper_min_score=paper_min_score)
    needs_data_reasons = _needs_data_reason_rows(conn)
    score_policy = _score_policy_freshness(conn, settings, paper_min_score=paper_min_score)
    score_policy["threshold_policy_loaded"] = paper_thresholds["policy_loaded"]
    score_policy["threshold_policy_error"] = paper_thresholds["policy_error"]
    paper_handoff = _paper_handoff_summary(conn, settings)
    paper_observer_preview = _paper_observer_preview_summary(conn, limit=8)
    paper_observer_evaluation = paper_observer_evaluation_data(settings)
    paper_observer_evaluation["history"] = _paper_signal_evaluation_history(conn)
    readiness = _production_readiness_summary(
        conn,
        settings=settings,
        stage_rows=stage_rows,
        top_review_candidates=top_review_candidates,
        manual_review_actions=manual_review_actions,
        paper_min_score=paper_min_score,
    )
    return {
        "generated_at": int(time.time()),
        "runtime": _runtime_build_info(),
        "db_path": str(settings.db_path),
        "db_size_bytes": settings.db_path.stat().st_size if settings.db_path.exists() else 0,
        "total_candidates": total_candidates,
        "stage_counts": stage_rows,
        "score_stage_counts": score_rows,
        "source_counts": source_rows,
        "discovery_freshness": _discovery_freshness_summary(conn),
        "source_quality": _source_quality_rows(conn),
        "activity_coverage": _activity_coverage_fast_summary(conn),
        "backfill_queue": evidence_backfill_summary(conn),
        "evidence_pipeline": _evidence_pipeline_summary(conn),
        "storage_maintenance": _storage_maintenance_summary(settings),
        "ops_health": _ops_health_summary(conn, settings),
        "production_readiness": readiness,
        "execution_preflight": execution_preflight_status(
            conn,
            max_signal_age_sec=DEFAULT_MAX_SIGNAL_AGE_SEC,
            execution_services=DEFAULT_EXECUTION_SERVICES,
            compose_error="not_checked_from_web",
        ),
        "paper_realtime_audit": paper_realtime_audit_status(
            conn,
            max_signal_age_sec=DEFAULT_MAX_SIGNAL_AGE_SEC,
            lookback_sec=DEFAULT_ACTIVITY_LOOKBACK_SEC,
            limit=50,
        ),
        "rtds_watch_audit": rtds_watch_audit_status(
            conn,
            min_score=DEFAULT_RTDS_WATCH_MIN_SCORE,
            lookback_sec=DEFAULT_ACTIVITY_LOOKBACK_SEC,
            limit=50,
        ),
        "paper_pool_expansion": _paper_pool_expansion_audit(
            conn,
            paper_min_score=paper_min_score,
            min_copy_events=paper_thresholds["min_copy_events"],
            min_copy_markets=paper_thresholds["min_copy_markets"],
            limit=50,
        ),
        "score_policy": score_policy,
        "paper_handoff": paper_handoff,
        "paper_observer_preview": paper_observer_preview,
        "paper_observer_evaluation": paper_observer_evaluation,
        "copyability_lane": _copyability_lane_summary(
            conn,
            settings=settings,
            include_pair_quality=include_pair_quality,
        ),
        "copyability_no_signal": _copyability_no_signal_summary(conn),
        "manual_review_actions": manual_review_actions,
        "needs_data_reasons": needs_data_reasons,
        "top_review_candidates": top_review_candidates,
        "top_review_blockers": _top_review_blocker_rows(top_review_candidates),
        "published_leaders": publish,
        "recent_runs": latest_runs,
        "paper_quality": _paper_quality_summary(conn),
    }


def _discovery_data(
    conn: sqlite3.Connection,
    settings: RobotSettings,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
    limit: int = 150,
) -> dict[str, Any]:
    rows = _wallet_table_rows(conn, stage=stage, source=source, query=query, signal=signal, limit=limit)
    evidence_depth = _evidence_depth_summary(conn, stage=stage, source=source, query=query)
    wallet_total_count = _wallet_scope_count(conn, stage=stage, source=source, query=query)
    return {
        "generated_at": int(time.time()),
        "db_path": str(settings.db_path),
        "filters": {"stage": stage, "source": source, "query": query, "signal": signal, "limit": limit},
        "funnel": _discovery_funnel(conn, evidence_depth=evidence_depth, stage=stage, source=source, query=query),
        "evidence_depth": evidence_depth,
        "source_focus": _source_focus_data(conn, source, paper_min_score=_paper_candidate_min_score(settings)) if source else {},
        "source_quality": _source_quality_rows(conn),
        "backfill_queue": evidence_backfill_summary(conn),
        "recent_runs": _discovery_run_rows(conn),
        "recent_source_events": _recent_source_events(conn),
        "stage_counts": _discovery_stage_counts(conn, stage=stage, source=source, query=query),
        "signal_counts": _discovery_signal_counts(
            conn,
            paper_min_score=_paper_candidate_min_score(settings),
            stage=stage,
            source=source,
            query=query,
        ),
        "wallets": rows,
        "wallet_count": len(rows),
        "wallet_total_count": wallet_total_count,
    }


def _wallet_filter_where(
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
) -> tuple[list[str], list[Any]]:
    where = []
    params: list[Any] = []
    if stage:
        where.append("cw.candidate_stage = ?")
        params.append(stage)
    if source:
        where.append(_wallet_source_match_sql("cw"))
        params.extend(_wallet_source_match_params(source))
    if query:
        where.append("(cw.address LIKE ? OR cw.sources LIKE ? OR cw.labels LIKE ? OR cw.notes LIKE ?)")
        like = f"%{query.lower()}%"
        params.extend([like, like, like, like])
    depth_expr = "COALESCE(wps.activity_count, eb.current_depth, 0)"
    copy_evidence_expr = (
        "(COALESCE(cls.qualified_follower_count, 0) > 0 "
        "OR COALESCE(cls.copy_event_count, 0) > 0 "
        "OR COALESCE(wf.copy_event_count, 0) > 0 "
        "OR COALESCE(wf.copy_market_count, 0) > 0 "
        "OR COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0) > 0 "
        "OR COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), 0) > 0 "
        "OR COALESCE(clp.backtest_trade_count, 0) > 0)"
    )
    copy_validation_expr = (
        "(COALESCE(cls.qualified_follower_count, 0) > 0 "
        "OR COALESCE(clp.backtest_trade_count, 0) > 0 "
        "OR COALESCE(clp.edge_retention_pct, 0) > 0 "
        "OR COALESCE(clp.walk_forward_consistency_pct, 0) > 0)"
    )
    if signal == "needs_backfill":
        where.append("COALESCE(wps.next_action, eb.stage, '') IN ('light_pending', 'medium_pending', 'deep_pending')")
        where.append("COALESCE(wps.evidence_status, '') != 'summary_ready'")
        where.append("cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')")
    elif signal == "copy_signal":
        where.append("(COALESCE(cls.qualified_follower_count, 0) > 0 OR COALESCE(wf.leader_in_degree, 0) > 0)")
    elif signal == "paper_signal":
        where.append("(COALESCE(pq.orders, 0) > 0 OR cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible'))")
    elif signal == "high_score":
        where.append("COALESCE(ls.leader_score, 0) >= 45")
    elif signal == "thin_evidence":
        where.append(f"{depth_expr} < 200")
    elif signal == "review_copy_pending":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append("COALESCE(cj.status, '') IN ('queued', 'running')")
    elif signal == "review_copy_no_signal":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append(f"{depth_expr} >= 200")
        where.append("COALESCE(cj.status, '') = 'done'")
        where.append("COALESCE(cj.copyability_scan_mode, '') IN ('', 'default', 'deep')")
        where.append(f"NOT {copy_evidence_expr}")
    elif signal == "review_copy_light_no_signal":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append(f"{depth_expr} >= 200")
        where.append("COALESCE(cj.status, '') = 'done'")
        where.append("COALESCE(cj.copyability_scan_mode, '') NOT IN ('', 'default', 'deep')")
        where.append(f"NOT {copy_evidence_expr}")
    elif signal == "review_copy_unvalidated":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append(f"{depth_expr} >= 200")
        where.append(copy_evidence_expr)
        where.append(f"NOT {copy_validation_expr}")
    elif signal == "review_paper_evidence_incomplete":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append("COALESCE(ls.leader_score, 0) >= 70")
        where.append("COALESCE(ls.review_reason, '') = 'paper_evidence_tier_incomplete'")
        where.append(f"NOT {paper_evidence_ready_sql('wps')}")
    elif signal == "review_thin_evidence":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append(f"{depth_expr} < 200")
    elif signal == "review_missing_copyability":
        where.append("cw.candidate_stage = 'needs_manual_review'")
        where.append(f"{depth_expr} >= 200")
        where.append("COALESCE(cj.status, '') NOT IN ('queued', 'running', 'done')")
        where.append(f"NOT {copy_evidence_expr}")
    return where, params


def _wallet_table_rows(
    conn: sqlite3.Connection,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    where, params = _wallet_filter_where(stage=stage, source=source, query=query, signal=signal)
    clause = "WHERE " + " AND ".join(where) if where else ""
    params.append(min(max(limit, 1), MAX_LIST_LIMIT))
    rows = _rows(
        conn,
        f"""
        WITH copy_job AS (
            SELECT
                pj.wallet,
                pj.status,
                pj.priority,
                COALESCE(
                    json_extract(pj.output_json, '$.graph_scan_mode'),
                    json_extract(pj.input_json, '$.graph_scan_mode'),
                    ''
                ) AS copyability_scan_mode
            FROM pipeline_jobs pj
            JOIN (
                SELECT wallet, MAX(job_id) AS job_id
                FROM pipeline_jobs
                WHERE job_type = 'copyability_evidence'
                GROUP BY wallet
            ) latest_job
              ON latest_job.job_id = pj.job_id
        ),
        base AS (
            SELECT
                cw.address,
                cw.sources,
                cw.labels,
                cw.notes,
                cw.links,
                cw.status,
                cw.candidate_stage,
                cw.first_seen_at,
                cw.updated_at,
                COALESCE(ls.leader_score, 0) AS leader_score,
                COALESCE(ls.review_stage, '') AS review_stage,
                COALESCE(ls.review_reason, '') AS review_reason,
                ls.scored_at,
                COALESCE(wf.recent_30d_volume_usdc, 0) AS recent_30d_volume_usdc,
                COALESCE(wf.net_pnl_usdc, 0) AS net_pnl_usdc,
                COALESCE(wf.total_volume_usdc, 0) AS total_volume_usdc,
                COALESCE(wf.leader_in_degree, 0) AS leader_in_degree,
                CASE
                    WHEN COALESCE(wf.copy_event_count, 0) > 0 THEN COALESCE(wf.copy_event_count, 0)
                    ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0)
                END AS feature_copy_event_count,
                CASE
                    WHEN COALESCE(wf.copy_market_count, 0) > 0 THEN COALESCE(wf.copy_market_count, 0)
                    ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), 0)
                END AS feature_copy_market_count,
                COALESCE(wf.copy_stream_roi, 0) AS copy_stream_roi,
                COALESCE(wf.hygiene_status, '') AS hygiene_status,
                COALESCE(wf.primary_category, '') AS primary_category,
                COALESCE(pq.orders, 0) AS paper_orders,
                COALESCE(pq.settled_positions, 0) AS paper_settled_positions,
                COALESCE(pq.total_roi, 0) AS paper_total_roi,
                COALESCE(pq.production_ready, 0) AS paper_ready,
                COALESCE(eb.stage, '') AS backfill_stage,
                COALESCE(eb.priority, 0) AS backfill_priority,
                COALESCE(eb.current_depth, 0) AS current_depth,
                COALESCE(eb.target_depth, 0) AS target_depth,
                COALESCE(eb.error_count, 0) AS backfill_errors,
                COALESCE(eb.stop_reason, '') AS backfill_stop_reason,
                COALESCE(wps.discovery_tier, '') AS evidence_tier,
                COALESCE(wps.evidence_status, '') AS evidence_status,
                COALESCE(wps.current_stage, '') AS evidence_current_stage,
                COALESCE(wps.next_action, '') AS next_action,
                COALESCE(wps.activity_count, eb.current_depth, 0) AS evidence_activity_count,
                COALESCE(wps.distinct_markets, 0) AS distinct_markets,
                COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
                COALESCE(cls.qualified_follower_count, 0) AS qualified_follower_count,
                COALESCE(cls.copy_event_count, 0) AS leader_copy_events,
                COALESCE(cls.copy_market_count, 0) AS leader_copy_markets,
                COALESCE(clp.backtest_trade_count, 0) AS backtest_trade_count,
                COALESCE(clp.net_roi, 0) AS copy_backtest_net_roi,
                COALESCE(clp.edge_retention_pct, 0) AS edge_retention_pct,
                COALESCE(clp.walk_forward_consistency_pct, 0) AS walk_forward_consistency_pct,
                COALESCE(cj.status, '') AS copyability_status,
                COALESCE(cj.priority, 0) AS copyability_priority,
                COALESCE(cj.copyability_scan_mode, '') AS copyability_scan_mode,
                COALESCE(wtre.maker_fraction, wf.maker_fraction, 0) AS maker_fraction,
                COALESCE(wtre.sample_complete, 0) AS trade_role_sample_complete
            FROM candidate_wallets cw
            LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
            LEFT JOIN wallet_features wf ON wf.address = cw.address
            LEFT JOIN paper_wallet_quality pq ON pq.wallet = cw.address
            LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
            LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
            LEFT JOIN copy_leader_stats cls ON cls.leader_wallet = cw.address
            LEFT JOIN copy_leader_performance clp ON clp.leader_wallet = cw.address
            LEFT JOIN copy_job cj ON cj.wallet = cw.address
            LEFT JOIN wallet_trade_role_evidence wtre ON wtre.wallet = cw.address
            {clause}
            ORDER BY
                CASE cw.candidate_stage
                    WHEN 'live_eligible' THEN 0
                    WHEN 'paper_approved' THEN 1
                    WHEN 'paper_candidate' THEN 2
                    WHEN 'needs_manual_review' THEN 3
                    WHEN 'needs_data' THEN 4
                    ELSE 5
                END ASC,
                COALESCE(ls.leader_score, 0) DESC,
                COALESCE(cls.qualified_follower_count, 0) DESC,
                COALESCE(wps.activity_count, eb.current_depth, 0) DESC,
                cw.updated_at DESC
            LIMIT ?
        )
        SELECT
            base.address,
            base.sources,
            base.labels,
            base.notes,
            base.links,
            base.status,
            base.candidate_stage,
            base.first_seen_at,
            base.updated_at,
            base.leader_score,
            base.review_stage,
            base.review_reason,
            base.scored_at,
            CASE
                WHEN COALESCE(base.evidence_activity_count, 0) > 0 THEN COALESCE(base.evidence_activity_count, 0)
                ELSE (SELECT COUNT(*) FROM wallet_activity wa WHERE wa.address = base.address)
            END AS activity_count,
            NULL AS oldest_ts,
            NULL AS newest_ts,
            0 AS source_event_count,
            0 AS source_count,
            base.sources AS latest_source,
            NULL AS latest_source_at,
            base.recent_30d_volume_usdc,
            base.net_pnl_usdc,
            base.total_volume_usdc,
            base.leader_in_degree,
            base.feature_copy_event_count,
            base.feature_copy_market_count,
            base.copy_stream_roi,
            base.hygiene_status,
            base.primary_category,
            base.paper_orders,
            base.paper_settled_positions,
            base.paper_total_roi,
            base.paper_ready,
            base.backfill_stage,
            base.backfill_priority,
            base.current_depth,
            base.target_depth,
            base.backfill_errors,
            base.backfill_stop_reason,
            base.evidence_tier,
            base.evidence_status,
            base.evidence_current_stage,
            base.next_action,
            base.distinct_markets,
            base.non_fast_trade_count,
            base.qualified_follower_count,
            base.leader_copy_events,
            base.leader_copy_markets,
            base.backtest_trade_count,
            base.copy_backtest_net_roi,
            base.edge_retention_pct,
            base.walk_forward_consistency_pct,
            base.copyability_status,
            base.copyability_priority,
            base.copyability_scan_mode,
            base.maker_fraction,
            base.trade_role_sample_complete
        FROM base
        """,
        tuple(params),
    )
    for row in rows:
        row["source_count"] = _text_source_count(row.get("sources"))
        row["latest_source"] = _first_text_source(row.get("latest_source") or row.get("sources"))
        row["discovery_priority"] = _discovery_priority(row)
        row["evidence_depth_label"] = _evidence_depth_label(row.get("activity_count"))
        row["backfill_progress_pct"] = _progress_pct(row.get("current_depth"), row.get("target_depth"))
    return sorted(rows, key=lambda row: (-float(row.get("discovery_priority") or 0), str(row.get("address") or "")))


def _discovery_funnel(
    conn: sqlite3.Connection,
    *,
    evidence_depth: dict[str, Any],
    stage: str = "",
    source: str = "",
    query: str = "",
) -> list[dict[str, Any]]:
    discovered = _wallet_scope_count(conn, stage=stage, source=source, query=query)
    activity_seen = discovered - int(evidence_depth.get("none") or 0)
    evidence_ready = int(evidence_depth.get("ready") or 0) + int(evidence_depth.get("deep") or 0)
    scored = _wallet_scope_count(
        conn,
        stage=stage,
        source=source,
        query=query,
        extra_where=["ls.address IS NOT NULL"],
    )
    copy_signal = _wallet_scope_count(conn, stage=stage, source=source, query=query, signal="copy_signal")
    review_pool = _wallet_scope_count(
        conn,
        stage=stage,
        source=source,
        query=query,
        extra_where=["cw.candidate_stage IN ('needs_manual_review', 'paper_candidate', 'paper_approved', 'live_eligible')"],
    )
    paper_pool = _wallet_scope_count(
        conn,
        stage=stage,
        source=source,
        query=query,
        extra_where=["(cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible') OR COALESCE(pq.orders, 0) > 0)"],
    )
    return [
        {"key": "discovered", "name": "发现池", "count": discovered, "note": "当前范围候选地址"},
        {"key": "activity_seen", "name": "有活动", "count": activity_seen, "note": "至少一条交易活动"},
        {"key": "evidence_ready", "name": "证据足量", "count": evidence_ready, "note": "活动事件 >= 200"},
        {"key": "scored", "name": "已评分", "count": scored, "note": "存在 leader score"},
        {"key": "copy_signal", "name": "Copy 信号", "count": copy_signal, "note": "有合格跟随者"},
        {"key": "review_pool", "name": "复核观察", "count": review_pool, "note": "评分后停靠或 paper 候选"},
        {"key": "paper_pool", "name": "Paper 池", "count": paper_pool, "note": "可进入纸面验证"},
    ]


def _activity_coverage_fast_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        WITH counts AS (
            SELECT
                cw.address,
                COALESCE(wps.activity_count, eb.current_depth, 0) AS activity_count
            FROM candidate_wallets cw
            LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
            LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
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
    return dict(row) if row else {}


def _discovery_freshness_summary(conn: sqlite3.Connection, *, limit: int = 12) -> dict[str, Any]:
    now = int(time.time())
    since_24h = now - 86_400
    since_72h = now - 259_200
    candidate_row = _one(
        conn,
        """
        SELECT
            COUNT(*) AS total_candidates,
            SUM(CASE WHEN first_seen_at >= ? THEN 1 ELSE 0 END) AS candidates_24h,
            SUM(CASE WHEN first_seen_at >= ? THEN 1 ELSE 0 END) AS candidates_72h,
            MAX(first_seen_at) AS latest_candidate_at
        FROM candidate_wallets
        """,
        (since_24h, since_72h),
    )
    observed_row = _one(
        conn,
        """
        SELECT
            COUNT(*) AS observed_wallets,
            SUM(CASE WHEN updated_at >= ? THEN 1 ELSE 0 END) AS observed_seen_24h,
            SUM(CASE WHEN updated_at >= ? THEN 1 ELSE 0 END) AS observed_seen_72h,
            SUM(CASE WHEN promoted_at IS NOT NULL THEN 1 ELSE 0 END) AS promoted_wallets,
            SUM(CASE WHEN promoted_at >= ? THEN 1 ELSE 0 END) AS promoted_24h,
            SUM(CASE WHEN promoted_at >= ? THEN 1 ELSE 0 END) AS promoted_72h,
            MAX(updated_at) AS latest_observed_at,
            MAX(promoted_at) AS latest_promoted_at
        FROM observed_wallets
        """,
        (since_24h, since_72h, since_24h, since_72h),
    )
    source_rows = _rows(
        conn,
        """
        SELECT
            source,
            COUNT(*) AS source_events,
            COUNT(DISTINCT address) AS wallets,
            SUM(CASE WHEN recorded_at >= ? THEN 1 ELSE 0 END) AS events_24h,
            SUM(CASE WHEN recorded_at >= ? THEN 1 ELSE 0 END) AS events_72h,
            COUNT(DISTINCT CASE WHEN recorded_at >= ? THEN address ELSE NULL END) AS wallets_24h,
            COUNT(DISTINCT CASE WHEN recorded_at >= ? THEN address ELSE NULL END) AS wallets_72h,
            MAX(recorded_at) AS latest_at
        FROM candidate_source_events
        WHERE source != ''
        GROUP BY source
        ORDER BY events_24h DESC, events_72h DESC, latest_at DESC, wallets DESC
        LIMIT ?
        """,
        (since_24h, since_72h, since_24h, since_72h, int(limit)),
    )
    observed_sources = _rows(
        conn,
        """
        SELECT
            sources AS source,
            COUNT(*) AS observed_wallets,
            SUM(CASE WHEN updated_at >= ? THEN 1 ELSE 0 END) AS seen_24h,
            SUM(CASE WHEN updated_at >= ? THEN 1 ELSE 0 END) AS seen_72h,
            SUM(CASE WHEN promoted_at IS NOT NULL THEN 1 ELSE 0 END) AS promoted_wallets,
            SUM(CASE WHEN promoted_at >= ? THEN 1 ELSE 0 END) AS promoted_24h,
            SUM(CASE WHEN promoted_at >= ? THEN 1 ELSE 0 END) AS promoted_72h,
            MAX(updated_at) AS latest_seen_at,
            MAX(promoted_at) AS latest_promoted_at
        FROM observed_wallets
        WHERE sources != ''
        GROUP BY sources
        ORDER BY seen_24h DESC, promoted_24h DESC, observed_wallets DESC, latest_seen_at DESC
        LIMIT ?
        """,
        (since_24h, since_72h, since_24h, since_72h, int(limit)),
    )
    recent_stage_rows = _rows(
        conn,
        """
        SELECT
            candidate_stage,
            COUNT(*) AS wallets,
            SUM(CASE WHEN first_seen_at >= ? THEN 1 ELSE 0 END) AS new_24h,
            SUM(CASE WHEN first_seen_at >= ? THEN 1 ELSE 0 END) AS new_72h,
            MAX(first_seen_at) AS latest_at
        FROM candidate_wallets
        GROUP BY candidate_stage
        ORDER BY new_24h DESC, wallets DESC, candidate_stage ASC
        """,
        (since_24h, since_72h),
    )
    events_24h = sum(int(row.get("events_24h") or 0) for row in source_rows)
    candidates_24h = int(candidate_row.get("candidates_24h") or 0)
    observed_seen_24h = int(observed_row.get("observed_seen_24h") or 0)
    promoted_24h = int(observed_row.get("promoted_24h") or 0)
    if events_24h or observed_seen_24h or candidates_24h:
        state = "fresh"
        next_action = "发现源有新鲜写入，继续观察晋级质量。"
    else:
        state = "stale"
        next_action = "最近 24 小时没有发现活水，优先检查发现 loop、代理和 Polymarket 访问。"
    return {
        "state": state,
        "next_action": next_action,
        "source_events_24h": events_24h,
        "source_events_72h": sum(int(row.get("events_72h") or 0) for row in source_rows),
        "candidate_wallets": int(candidate_row.get("total_candidates") or 0),
        "candidates_24h": candidates_24h,
        "candidates_72h": int(candidate_row.get("candidates_72h") or 0),
        "latest_candidate_at": candidate_row.get("latest_candidate_at") or 0,
        "observed_wallets": int(observed_row.get("observed_wallets") or 0),
        "observed_seen_24h": observed_seen_24h,
        "observed_seen_72h": int(observed_row.get("observed_seen_72h") or 0),
        "promoted_wallets": int(observed_row.get("promoted_wallets") or 0),
        "promoted_24h": promoted_24h,
        "promoted_72h": int(observed_row.get("promoted_72h") or 0),
        "latest_observed_at": observed_row.get("latest_observed_at") or 0,
        "latest_promoted_at": observed_row.get("latest_promoted_at") or 0,
        "source_rows": source_rows,
        "observed_sources": observed_sources,
        "stage_rows": recent_stage_rows,
        "paper_activity_pulse": _paper_stage_activity_pulse_summary(conn, now=now),
        "paper_rtds_bridge": _paper_rtds_bridge_summary(conn, now=now),
    }


def _paper_stage_activity_pulse_summary(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    limit: int = 12,
) -> dict[str, Any]:
    """Summarize all paper-stage wallet activity without changing pipeline state."""

    now = int(time.time()) if now is None else int(now)
    since_recent = now - PAPER_RTDS_BRIDGE_RECENT_SEC
    since_actionable = now - PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC
    paper_wallet_rows = _rows(
        conn,
        f"""
        SELECT address, candidate_stage
        FROM candidate_wallets
        WHERE candidate_stage IN ({",".join("?" for _ in PAPER_ELIGIBLE_CANDIDATE_STAGES)})
        ORDER BY
            CASE candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END,
            updated_at DESC,
            address ASC
        """,
        PAPER_ELIGIBLE_CANDIDATE_STAGES,
    )
    paper_wallets = [str(row.get("address") or "") for row in paper_wallet_rows if row.get("address")]
    if not paper_wallets:
        return {
            "state": "no_paper_wallets",
            "paper_stage_wallets": 0,
            "events_24h": 0,
            "buy_events_24h": 0,
            "timely_buy_events": 0,
            "stale_buy_events_24h": 0,
            "non_buy_events_24h": 0,
            "latest_activity_at": 0,
            "latest_buy_at": 0,
            "latest_buy_ingested_at": 0,
            "latest_buy_age_sec": None,
            "latest_buy_ingest_lag_sec": None,
            "actionable_signal_window_sec": PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC,
            "next_action": "还没有 paper-stage 钱包；先等待评分和验证产生 paper_approved。",
            "source_rows": [],
            "wallet_rows": [],
        }

    stage_by_wallet = {str(row.get("address") or ""): str(row.get("candidate_stage") or "") for row in paper_wallet_rows}
    event_count = 0
    buy_count = 0
    timely_buy_count = 0
    stale_buy_count = 0
    non_buy_count = 0
    latest_activity_at = 0
    latest_buy_at = 0
    latest_buy_ingested_at = 0
    max_buy_ingest_lag: int | None = None
    buy_ingest_lag_sum = 0.0
    buy_ingest_lag_samples = 0
    wallet_rows: list[dict[str, Any]] = []
    source_groups: dict[str, dict[str, Any]] = {}

    for chunk in _chunks(paper_wallets, 400):
        placeholders = ",".join("?" for _ in chunk)
        rows = _rows(
            conn,
            f"""
            WITH scoped AS (
                SELECT
                    wa.rowid AS activity_rowid,
                    address,
                    timestamp,
                    ingested_at,
                    type,
                    side,
                    CASE
                        WHEN timestamp > 0 AND ingested_at > 0 AND ingested_at >= timestamp
                            THEN ingested_at - timestamp
                        WHEN timestamp > 0 AND ingested_at > 0
                            THEN 0
                        ELSE NULL
                    END AS ingest_lag_sec,
                    COALESCE(
                        NULLIF(json_extract(CASE WHEN json_valid(raw_json) THEN raw_json ELSE '{{}}' END, '$.source'), ''),
                        'wallet_activity_poll'
                    ) AS activity_source
                FROM wallet_activity AS wa INDEXED BY idx_wallet_activity_address_time
                WHERE address IN ({placeholders})
                  AND (timestamp >= ? OR ingested_at >= ?)
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY address
                        ORDER BY COALESCE(ingested_at, 0) DESC, COALESCE(timestamp, 0) DESC, activity_rowid DESC
                    ) AS latest_rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY address
                        ORDER BY
                            CASE
                                WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                                 AND UPPER(COALESCE(side, '')) = 'BUY'
                                THEN COALESCE(timestamp, 0)
                                ELSE 0
                            END DESC,
                            CASE
                                WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                                 AND UPPER(COALESCE(side, '')) = 'BUY'
                                THEN COALESCE(ingested_at, 0)
                                ELSE 0
                            END DESC,
                            activity_rowid DESC
                    ) AS latest_buy_rank
                FROM scoped
            )
            SELECT
                address,
                COUNT(*) AS events_24h,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN 1 ELSE 0 END) AS buy_events_24h,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY'
                          AND timestamp >= ?
                          AND (ingest_lag_sec IS NULL OR ingest_lag_sec <= ?) THEN 1 ELSE 0 END) AS timely_buy_events,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY'
                          AND (timestamp < ? OR COALESCE(ingest_lag_sec, 0) > ?) THEN 1 ELSE 0 END) AS stale_buy_events_24h,
                SUM(CASE WHEN NOT (UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY') THEN 1 ELSE 0 END) AS non_buy_events_24h,
                AVG(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS avg_buy_ingest_lag_sec,
                COUNT(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS buy_ingest_lag_samples,
                MAX(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingest_lag_sec ELSE NULL END) AS max_buy_ingest_lag_sec,
                MAX(timestamp) AS latest_activity_at,
                MAX(CASE WHEN latest_buy_rank = 1
                          AND UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN timestamp ELSE 0 END) AS latest_buy_at,
                MAX(CASE WHEN latest_buy_rank = 1
                          AND UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN ingested_at ELSE 0 END) AS latest_buy_ingested_at,
                MAX(CASE WHEN latest_rank = 1 THEN COALESCE(type, '') ELSE '' END) AS latest_event_type,
                MAX(CASE WHEN latest_rank = 1 THEN COALESCE(side, '') ELSE '' END) AS latest_side,
                MAX(CASE WHEN latest_rank = 1 THEN activity_source ELSE '' END) AS latest_source
            FROM ranked
            GROUP BY address
            """,
            (
                *chunk,
                since_recent,
                since_recent,
                since_actionable,
                PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC,
                since_actionable,
                PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC,
            ),
        )
        for row in rows:
            events = int(row.get("events_24h") or 0)
            buys = int(row.get("buy_events_24h") or 0)
            timely_buys = int(row.get("timely_buy_events") or 0)
            stale_buys = int(row.get("stale_buy_events_24h") or 0)
            non_buys = int(row.get("non_buy_events_24h") or 0)
            avg_buy_lag = row.get("avg_buy_ingest_lag_sec")
            buy_lag_samples = int(row.get("buy_ingest_lag_samples") or 0)
            row_max_buy_lag = row.get("max_buy_ingest_lag_sec")
            activity_at = int(row.get("latest_activity_at") or 0)
            buy_at = int(row.get("latest_buy_at") or 0)
            buy_ingested_at = int(row.get("latest_buy_ingested_at") or 0)
            event_count += events
            buy_count += buys
            timely_buy_count += timely_buys
            stale_buy_count += stale_buys
            non_buy_count += non_buys
            latest_activity_at = max(latest_activity_at, activity_at)
            if buy_at > latest_buy_at or (buy_at == latest_buy_at and buy_ingested_at > latest_buy_ingested_at):
                latest_buy_at = buy_at
                latest_buy_ingested_at = buy_ingested_at
            if avg_buy_lag is not None and buy_lag_samples > 0:
                buy_ingest_lag_sum += float(avg_buy_lag) * buy_lag_samples
                buy_ingest_lag_samples += buy_lag_samples
            if row_max_buy_lag is not None:
                row_max_buy_lag_int = int(row_max_buy_lag)
                max_buy_ingest_lag = row_max_buy_lag_int if max_buy_ingest_lag is None else max(max_buy_ingest_lag, row_max_buy_lag_int)
            wallet_rows.append(
                {
                    "wallet": row.get("address"),
                    "candidate_stage": stage_by_wallet.get(str(row.get("address") or ""), ""),
                    "events_24h": events,
                    "buy_events_24h": buys,
                    "timely_buy_events": timely_buys,
                    "stale_buy_events_24h": stale_buys,
                    "non_buy_events_24h": non_buys,
                    "avg_buy_ingest_lag_sec": round(float(avg_buy_lag), 2) if avg_buy_lag is not None else None,
                    "max_buy_ingest_lag_sec": int(row_max_buy_lag) if row_max_buy_lag is not None else None,
                    "max_buy_ingest_lag": _duration_label(row_max_buy_lag),
                    "latest_activity_at": activity_at,
                    "latest_buy_at": buy_at,
                    "latest_buy_age": _duration_label(max(0, now - buy_at) if buy_at else None),
                    "latest_buy_ingested_at": buy_ingested_at,
                    "latest_event_type": row.get("latest_event_type") or "",
                    "latest_side": row.get("latest_side") or "",
                    "latest_source": row.get("latest_source") or "",
                }
            )

        source_rows = _rows(
            conn,
            f"""
            SELECT
                COALESCE(
                    NULLIF(json_extract(CASE WHEN json_valid(raw_json) THEN raw_json ELSE '{{}}' END, '$.source'), ''),
                    'wallet_activity_poll'
                ) AS source,
                COUNT(*) AS events_24h,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY' THEN 1 ELSE 0 END) AS buy_events_24h,
                MAX(ingested_at) AS latest_ingested_at,
                MAX(timestamp) AS latest_activity_at
            FROM wallet_activity AS wa INDEXED BY idx_wallet_activity_address_time
            WHERE address IN ({placeholders})
              AND (timestamp >= ? OR ingested_at >= ?)
            GROUP BY source
            """,
            (*chunk, since_recent, since_recent),
        )
        for row in source_rows:
            source = str(row.get("source") or "wallet_activity_poll")
            group = source_groups.setdefault(
                source,
                {"source": source, "events_24h": 0, "buy_events_24h": 0, "latest_ingested_at": 0, "latest_activity_at": 0},
            )
            group["events_24h"] = int(group["events_24h"]) + int(row.get("events_24h") or 0)
            group["buy_events_24h"] = int(group["buy_events_24h"]) + int(row.get("buy_events_24h") or 0)
            group["latest_ingested_at"] = max(int(group["latest_ingested_at"] or 0), int(row.get("latest_ingested_at") or 0))
            group["latest_activity_at"] = max(int(group["latest_activity_at"] or 0), int(row.get("latest_activity_at") or 0))

    latest_buy_age = max(0, now - latest_buy_at) if latest_buy_at else None
    latest_buy_ingest_lag = (
        max(0, latest_buy_ingested_at - latest_buy_at)
        if latest_buy_at and latest_buy_ingested_at
        else None
    )
    avg_buy_ingest_lag = round(buy_ingest_lag_sum / buy_ingest_lag_samples, 2) if buy_ingest_lag_samples else None
    if timely_buy_count > 0:
        state = "timely_buy_activity"
        next_action = "paper-stage 钱包出现可及时跟 BUY；继续看 observer 报价评估和外部 paper 接手。"
    elif buy_count > 0 and stale_buy_count >= buy_count:
        state = "buy_seen_but_stale"
        next_action = "24h 内有 BUY，但入库或发现时已经超过可跟窗口；优先修复实时发现/低延迟入库。"
    elif buy_count > 0:
        state = "buy_seen_not_actionable"
        next_action = "24h 内有 BUY，但当前没有可及时跟信号；继续压低发现延迟并等待新 BUY。"
    elif event_count > 0:
        state = "active_no_buy"
        next_action = "paper-stage 钱包 24h 内有活动，但不是 BUY；等待可复制买入信号。"
    else:
        state = "no_recent_activity"
        next_action = "paper-stage 钱包 24h 内没有活动；等待钱包重新活跃或替换观察对象。"

    wallet_rows.sort(
        key=lambda row: (
            int(row.get("timely_buy_events") or 0),
            int(row.get("latest_buy_at") or 0),
            int(row.get("latest_activity_at") or 0),
        ),
        reverse=True,
    )
    source_rows = sorted(
        source_groups.values(),
        key=lambda row: (int(row.get("latest_ingested_at") or 0), int(row.get("events_24h") or 0)),
        reverse=True,
    )
    return {
        "state": state,
        "paper_stage_wallets": len(paper_wallets),
        "events_24h": event_count,
        "buy_events_24h": buy_count,
        "timely_buy_events": timely_buy_count,
        "stale_buy_events_24h": stale_buy_count,
        "non_buy_events_24h": non_buy_count,
        "avg_buy_ingest_lag_sec": avg_buy_ingest_lag,
        "max_buy_ingest_lag_sec": max_buy_ingest_lag,
        "latest_activity_at": latest_activity_at,
        "latest_buy_at": latest_buy_at,
        "latest_buy_ingested_at": latest_buy_ingested_at,
        "latest_buy_age_sec": latest_buy_age,
        "latest_buy_ingest_lag_sec": latest_buy_ingest_lag,
        "actionable_signal_window_sec": PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC,
        "next_action": next_action,
        "source_rows": source_rows[: int(limit)],
        "wallet_rows": wallet_rows[: int(limit)],
    }


def _paper_rtds_bridge_summary(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    limit: int = 12,
) -> dict[str, Any]:
    now = int(time.time()) if now is None else int(now)
    since_fresh = now - PAPER_RTDS_BRIDGE_FRESH_SEC
    since_recent = now - PAPER_RTDS_BRIDGE_RECENT_SEC
    try:
        paper_min_trade_usdc = float(os.environ.get("PM_ROBOT_RTDS_PAPER_MIN_TRADE_USDC", "0") or 0)
    except ValueError:
        paper_min_trade_usdc = 0.0
    paper_wallet_rows = _rows(
        conn,
        f"""
        SELECT address, candidate_stage
        FROM candidate_wallets
        WHERE candidate_stage IN ({",".join("?" for _ in PAPER_ELIGIBLE_CANDIDATE_STAGES)})
        ORDER BY
            CASE candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END,
            updated_at DESC,
            address ASC
        """,
        PAPER_ELIGIBLE_CANDIDATE_STAGES,
    )
    paper_wallets = [str(row.get("address") or "") for row in paper_wallet_rows if row.get("address")]
    if not paper_wallets:
        return {
            "state": "no_paper_wallets",
            "paper_stage_wallets": 0,
            "rtds_activity_events": 0,
            "rtds_activity_wallets": 0,
            "rtds_activity_events_fresh": 0,
            "rtds_activity_events_24h": 0,
            "rtds_trade_events": 0,
            "rtds_buy_events": 0,
            "rtds_buy_events_fresh": 0,
            "rtds_buy_events_24h": 0,
            "rtds_redeem_events": 0,
            "rtds_non_buy_events": 0,
            "rtds_avg_ingest_lag_sec": None,
            "rtds_max_ingest_lag_sec": None,
            "paper_min_trade_usdc": paper_min_trade_usdc,
            "latest_rtds_activity_at": 0,
            "latest_rtds_ingested_at": 0,
            "latest_rtds_activity_age_sec": None,
            "latest_rtds_ingest_age_sec": None,
            "next_action": "还没有 paper-stage 钱包；先等待评分和验证产生 paper_approved。",
            "wallet_rows": [],
        }

    event_count = 0
    wallet_count = 0
    fresh_count = 0
    recent_count = 0
    trade_count = 0
    buy_count = 0
    buy_fresh_count = 0
    buy_recent_count = 0
    redeem_count = 0
    non_buy_count = 0
    ingest_lag_sum = 0.0
    ingest_lag_samples = 0
    max_ingest_lag: int | None = None
    latest_activity_at = 0
    latest_ingested_at = 0
    wallet_rows: list[dict[str, Any]] = []
    stage_by_wallet = {str(row.get("address") or ""): str(row.get("candidate_stage") or "") for row in paper_wallet_rows}

    # Keep the large wallet_activity table bounded by indexed paper wallet addresses.
    for chunk in _chunks(paper_wallets, 400):
        placeholders = ",".join("?" for _ in chunk)
        rows = _rows(
            conn,
            f"""
            WITH scoped AS (
                SELECT
                    wa.rowid AS activity_rowid,
                    address,
                    timestamp,
                    ingested_at,
                    type,
                    side,
                    CASE
                        WHEN timestamp > 0 AND ingested_at > 0 AND ingested_at >= timestamp
                            THEN ingested_at - timestamp
                        WHEN timestamp > 0 AND ingested_at > 0
                            THEN 0
                        ELSE NULL
                    END AS ingest_lag_sec
                FROM wallet_activity AS wa INDEXED BY idx_wallet_activity_address_time
                WHERE address IN ({placeholders})
                  AND json_extract(CASE WHEN json_valid(raw_json) THEN raw_json ELSE '{{}}' END, '$.source') = 'polymarket_rtds_activity'
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY address
                        ORDER BY COALESCE(ingested_at, 0) DESC, COALESCE(timestamp, 0) DESC, activity_rowid DESC
                    ) AS latest_rank
                FROM scoped
            )
            SELECT
                address,
                COUNT(*) AS rtds_activity_events,
                SUM(CASE WHEN timestamp >= ? OR ingested_at >= ? THEN 1 ELSE 0 END) AS rtds_activity_events_fresh,
                SUM(CASE WHEN timestamp >= ? OR ingested_at >= ? THEN 1 ELSE 0 END) AS rtds_activity_events_24h,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE' THEN 1 ELSE 0 END) AS rtds_trade_events,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY' THEN 1 ELSE 0 END) AS rtds_buy_events,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY'
                          AND (timestamp >= ? OR ingested_at >= ?) THEN 1 ELSE 0 END) AS rtds_buy_events_fresh,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'TRADE'
                          AND UPPER(COALESCE(side, '')) = 'BUY'
                          AND (timestamp >= ? OR ingested_at >= ?) THEN 1 ELSE 0 END) AS rtds_buy_events_24h,
                SUM(CASE WHEN UPPER(COALESCE(type, '')) = 'REDEEM' THEN 1 ELSE 0 END) AS rtds_redeem_events,
                SUM(CASE WHEN NOT (UPPER(COALESCE(type, '')) = 'TRADE' AND UPPER(COALESCE(side, '')) = 'BUY') THEN 1 ELSE 0 END) AS rtds_non_buy_events,
                AVG(ingest_lag_sec) AS avg_ingest_lag_sec,
                COUNT(ingest_lag_sec) AS ingest_lag_samples,
                MAX(ingest_lag_sec) AS max_ingest_lag_sec,
                MAX(timestamp) AS latest_rtds_activity_at,
                MAX(ingested_at) AS latest_rtds_ingested_at,
                MAX(CASE WHEN latest_rank = 1 THEN COALESCE(type, '') ELSE '' END) AS latest_rtds_event_type,
                MAX(CASE WHEN latest_rank = 1 THEN COALESCE(side, '') ELSE '' END) AS latest_rtds_side
            FROM ranked
            GROUP BY address
            """,
            (
                *chunk,
                since_fresh,
                since_fresh,
                since_recent,
                since_recent,
                since_fresh,
                since_fresh,
                since_recent,
                since_recent,
            ),
        )
        for row in rows:
            events = int(row.get("rtds_activity_events") or 0)
            fresh_events = int(row.get("rtds_activity_events_fresh") or 0)
            recent_events = int(row.get("rtds_activity_events_24h") or 0)
            trades = int(row.get("rtds_trade_events") or 0)
            buys = int(row.get("rtds_buy_events") or 0)
            buy_fresh = int(row.get("rtds_buy_events_fresh") or 0)
            buy_recent = int(row.get("rtds_buy_events_24h") or 0)
            redeems = int(row.get("rtds_redeem_events") or 0)
            non_buys = int(row.get("rtds_non_buy_events") or 0)
            avg_lag = row.get("avg_ingest_lag_sec")
            lag_samples = int(row.get("ingest_lag_samples") or 0)
            row_max_lag = row.get("max_ingest_lag_sec")
            activity_at = int(row.get("latest_rtds_activity_at") or 0)
            ingested_at = int(row.get("latest_rtds_ingested_at") or 0)
            event_count += events
            fresh_count += fresh_events
            recent_count += recent_events
            trade_count += trades
            buy_count += buys
            buy_fresh_count += buy_fresh
            buy_recent_count += buy_recent
            redeem_count += redeems
            non_buy_count += non_buys
            if avg_lag is not None and lag_samples > 0:
                ingest_lag_sum += float(avg_lag) * lag_samples
                ingest_lag_samples += lag_samples
            if row_max_lag is not None:
                row_max_lag_int = int(row_max_lag)
                max_ingest_lag = row_max_lag_int if max_ingest_lag is None else max(max_ingest_lag, row_max_lag_int)
            wallet_count += 1
            latest_activity_at = max(latest_activity_at, activity_at)
            latest_ingested_at = max(latest_ingested_at, ingested_at)
            wallet_rows.append(
                {
                    "wallet": row.get("address"),
                    "candidate_stage": stage_by_wallet.get(str(row.get("address") or ""), ""),
                    "rtds_activity_events": events,
                    "rtds_activity_events_fresh": fresh_events,
                    "rtds_activity_events_24h": recent_events,
                    "rtds_trade_events": trades,
                    "rtds_buy_events": buys,
                    "rtds_buy_events_fresh": buy_fresh,
                    "rtds_buy_events_24h": buy_recent,
                    "rtds_redeem_events": redeems,
                    "rtds_non_buy_events": non_buys,
                    "rtds_avg_ingest_lag_sec": round(float(avg_lag), 2) if avg_lag is not None else None,
                    "rtds_max_ingest_lag_sec": int(row_max_lag) if row_max_lag is not None else None,
                    "rtds_max_ingest_lag": _duration_label(row_max_lag),
                    "latest_rtds_event_type": row.get("latest_rtds_event_type") or "",
                    "latest_rtds_side": row.get("latest_rtds_side") or "",
                    "latest_rtds_activity_at": activity_at,
                    "latest_rtds_ingested_at": ingested_at,
                }
            )

    latest_activity_age = max(0, now - latest_activity_at) if latest_activity_at else None
    latest_ingest_age = max(0, now - latest_ingested_at) if latest_ingested_at else None
    avg_ingest_lag = round(ingest_lag_sum / ingest_lag_samples, 2) if ingest_lag_samples else None
    if buy_fresh_count > 0:
        state = "fresh_buy_activity"
        next_action = "RTDS 已接入 paper-stage 钱包的短窗 BUY，继续观察 paper observer 报价和可跟随判断。"
    elif fresh_count > 0:
        state = "rtds_active_no_buy"
        next_action = "RTDS 有短窗 paper-stage 活动，但不是可跟随 BUY；通常是 REDEEM/SELL/空 side，等待新的 BUY 信号。"
    elif buy_recent_count > 0:
        state = "recent_buy_activity"
        next_action = "RTDS 24 小时内接入过 paper-stage BUY，但当前短窗没有及时信号；重点看钱包活跃间隔和 observer 新鲜度。"
    elif recent_count > 0:
        state = "recent_non_buy_activity"
        next_action = "RTDS 24 小时内有 paper-stage 活动，但没有 BUY；等待可复制交易，或继续确认钱包是否以赎回/卖出为主。"
    elif event_count > 0:
        state = "stale"
        next_action = "RTDS 桥接曾接入 paper-stage 钱包交易，但 24 小时内没有新交易，重点看钱包是否暂时沉默。"
    else:
        state = "waiting_for_rtds_activity"
        next_action = "paper-stage 钱包已存在，但 RTDS 还没捕捉到它们的新交易；继续等待实时事件或检查 RTDS loop。"

    wallet_rows.sort(
        key=lambda row: (
            int(row.get("latest_rtds_ingested_at") or 0),
            int(row.get("latest_rtds_activity_at") or 0),
        ),
        reverse=True,
    )
    return {
        "state": state,
        "paper_stage_wallets": len(paper_wallets),
        "rtds_activity_events": event_count,
        "rtds_activity_wallets": wallet_count,
        "rtds_activity_events_fresh": fresh_count,
        "rtds_activity_events_24h": recent_count,
        "rtds_trade_events": trade_count,
        "rtds_buy_events": buy_count,
        "rtds_buy_events_fresh": buy_fresh_count,
        "rtds_buy_events_24h": buy_recent_count,
        "rtds_redeem_events": redeem_count,
        "rtds_non_buy_events": non_buy_count,
        "rtds_avg_ingest_lag_sec": avg_ingest_lag,
        "rtds_max_ingest_lag_sec": max_ingest_lag,
        "paper_min_trade_usdc": paper_min_trade_usdc,
        "latest_rtds_activity_at": latest_activity_at,
        "latest_rtds_ingested_at": latest_ingested_at,
        "latest_rtds_activity_age_sec": latest_activity_age,
        "latest_rtds_ingest_age_sec": latest_ingest_age,
        "next_action": next_action,
        "wallet_rows": wallet_rows[: int(limit)],
    }


def _evidence_depth_summary(
    conn: sqlite3.Connection,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
) -> dict[str, Any]:
    candidate_count = _wallet_scope_count(conn, stage=stage, source=source, query=query)
    where, params = _wallet_filter_where(stage=stage, source=source, query=query)
    clause = "WHERE " + " AND ".join(where) if where else ""
    if _wallet_processing_state_ready(conn) or candidate_count > 5_000:
        row = conn.execute(
            f"""
            WITH counts AS (
                SELECT
                    cw.address,
                    COALESCE(wps.activity_count, eb.current_depth, 0) AS activity_count
                FROM candidate_wallets cw
                LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
                LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
                {clause}
            )
            SELECT
                SUM(CASE WHEN activity_count = 0 THEN 1 ELSE 0 END) AS none,
                SUM(CASE WHEN activity_count BETWEEN 1 AND 99 THEN 1 ELSE 0 END) AS starter,
                SUM(CASE WHEN activity_count BETWEEN 100 AND 199 THEN 1 ELSE 0 END) AS light,
                SUM(CASE WHEN activity_count BETWEEN 200 AND 999 THEN 1 ELSE 0 END) AS ready,
                SUM(CASE WHEN activity_count >= 1000 THEN 1 ELSE 0 END) AS deep,
                AVG(activity_count) AS avg_events,
                MAX(activity_count) AS max_events
            FROM counts
            """,
            tuple(params),
        ).fetchone()
        return dict(row) if row else {}
    row = conn.execute(
        f"""
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
                    eb.current_depth,
                    0
                ) AS activity_count
            FROM candidate_wallets cw
            LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
            LEFT JOIN actual_counts ac ON ac.address = cw.address
            LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
            {clause}
        )
        SELECT
            SUM(CASE WHEN activity_count = 0 THEN 1 ELSE 0 END) AS none,
            SUM(CASE WHEN activity_count BETWEEN 1 AND 99 THEN 1 ELSE 0 END) AS starter,
            SUM(CASE WHEN activity_count BETWEEN 100 AND 199 THEN 1 ELSE 0 END) AS light,
            SUM(CASE WHEN activity_count BETWEEN 200 AND 999 THEN 1 ELSE 0 END) AS ready,
            SUM(CASE WHEN activity_count >= 1000 THEN 1 ELSE 0 END) AS deep,
            AVG(activity_count) AS avg_events,
            MAX(activity_count) AS max_events
        FROM counts
        """,
        tuple(params),
    ).fetchone()
    return dict(row) if row else {}


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


def _source_quality_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        WITH top_sources AS (
            SELECT source, COUNT(*) AS wallets, MAX(latest_recorded_at) AS latest_at
            FROM candidate_source_wallet_latest
            WHERE source != ''
            GROUP BY source
            ORDER BY wallets DESC, latest_at DESC, source ASC
            LIMIT 12
        ),
        source_wallets AS (
            SELECT csl.source, csl.address, csl.latest_recorded_at AS latest_at
            FROM candidate_source_wallet_latest csl
            JOIN top_sources ts ON ts.source = csl.source
        ),
        source_wallet_metrics AS (
            SELECT
                sw.source,
                sw.address,
                sw.latest_at,
                COALESCE(wds.candidate_stage, '') AS candidate_stage,
                COALESCE(wds.activity_count, 0) AS activity_count,
                COALESCE(wds.discovery_tier, '') AS discovery_tier,
                COALESCE(wds.next_action, '') AS next_action,
                COALESCE(wds.leader_score, 0) AS leader_score
            FROM source_wallets sw
            JOIN wallet_dashboard_snapshot wds ON wds.address = sw.address
        )
        SELECT
            source,
            COUNT(*) AS wallets,
            SUM(CASE WHEN activity_count > 0 THEN 1 ELSE 0 END) AS activity_wallets,
            SUM(CASE WHEN activity_count >= 200 THEN 1 ELSE 0 END) AS evidence_ready_wallets,
            SUM(CASE WHEN discovery_tier = 'l3_deep' THEN 1 ELSE 0 END) AS l3_wallets,
            SUM(CASE WHEN next_action = 'score_wallet' THEN 1 ELSE 0 END) AS score_ready_wallets,
            SUM(CASE WHEN candidate_stage = 'needs_manual_review' THEN 1 ELSE 0 END) AS review_wallets,
            SUM(CASE WHEN candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible') THEN 1 ELSE 0 END) AS paper_wallets,
            SUM(CASE WHEN candidate_stage IN ('blocked_hygiene', 'blocked_copyability') THEN 1 ELSE 0 END) AS blocked_wallets,
            SUM(CASE WHEN candidate_stage = 'rejected' THEN 1 ELSE 0 END) AS rejected_wallets,
            AVG(activity_count) AS avg_activity,
            AVG(leader_score) AS avg_score,
            MAX(leader_score) AS max_score,
            MAX(latest_at) AS latest_at
        FROM source_wallet_metrics
        GROUP BY source
        ORDER BY paper_wallets DESC, review_wallets DESC, max_score DESC, evidence_ready_wallets DESC, wallets DESC, latest_at DESC
        LIMIT 12
        """,
    )


def _source_focus_data(conn: sqlite3.Connection, source: str, *, paper_min_score: float) -> dict[str, Any]:
    source = source.strip()
    if not source:
        return {}
    rows = _source_focus_wallet_rows(conn, source=source, paper_min_score=paper_min_score)
    return {
        "source": source,
        "matched_wallets": len(rows),
        "blockers": _source_focus_blocker_rows(rows),
        "top_wallets": rows[:8],
    }


def _source_focus_wallet_rows(
    conn: sqlite3.Connection,
    *,
    source: str,
    paper_min_score: float,
    limit: int = 250,
) -> list[dict[str, Any]]:
    rows = _rows(
        conn,
        """
        WITH matched_wallets AS (
            SELECT DISTINCT cw.address
            FROM candidate_wallets cw
            WHERE {source_match}
            UNION
            SELECT DISTINCT cse.address
            FROM candidate_source_events cse
            WHERE cse.source = ?
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.current_stage, '') AS evidence_current_stage,
            COALESCE(wps.next_action, '') AS next_action,
            COALESCE(wps.activity_count, eb.current_depth, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            COALESCE(cls.qualified_follower_count, 0) AS qualified_follower_count,
            COALESCE(cls.copy_event_count, 0) AS copy_event_count,
            COALESCE(clp.backtest_trade_count, 0) AS backtest_trade_count,
            COALESCE((
                SELECT pj.status
                FROM pipeline_jobs pj
                WHERE pj.wallet = cw.address
                  AND pj.job_type = 'copyability_evidence'
                ORDER BY pj.updated_at DESC, pj.job_id DESC
                LIMIT 1
            ), '') AS copyability_status
            , COALESCE((
                SELECT COALESCE(
                    json_extract(pj.output_json, '$.graph_scan_mode'),
                    json_extract(pj.input_json, '$.graph_scan_mode'),
                    ''
                )
                FROM pipeline_jobs pj
                WHERE pj.wallet = cw.address
                  AND pj.job_type = 'copyability_evidence'
                ORDER BY pj.updated_at DESC, pj.job_id DESC
                LIMIT 1
            ), '') AS copyability_scan_mode
        FROM matched_wallets mw
        JOIN candidate_wallets cw ON cw.address = mw.address
        LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
        LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
        LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
        LEFT JOIN copy_leader_stats cls ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp ON clp.leader_wallet = cw.address
        ORDER BY
            COALESCE(ls.leader_score, 0) DESC,
            CASE cw.candidate_stage
                WHEN 'paper_candidate' THEN 0
                WHEN 'needs_manual_review' THEN 1
                WHEN 'needs_data' THEN 2
                ELSE 3
            END ASC,
            COALESCE(wps.activity_count, eb.current_depth, 0) DESC,
            cw.address ASC
        LIMIT ?
        """.format(source_match=_candidate_source_token_match_sql("cw")),
        (
            source.strip(),
            source.strip(),
            min(max(int(limit), 1), MAX_LIST_LIMIT),
        ),
    )
    for row in rows:
        blocker_key, blocker_label = _source_focus_blocker(row, paper_min_score=paper_min_score)
        row["blocker_key"] = blocker_key
        row["blocker_label"] = blocker_label
        row["review_next_action"] = _review_blocker_next_action(blocker_key)
    return rows


def _candidate_source_token_match_sql(candidate_alias: str) -> str:
    """Match a full pipe-delimited source token, not an arbitrary substring."""
    return f"instr(' | ' || {candidate_alias}.sources || ' | ', ' | ' || ? || ' | ') > 0"


def _wallet_source_match_sql(candidate_alias: str) -> str:
    """Match either candidate source tokens or immutable source-event provenance."""
    return (
        f"({_candidate_source_token_match_sql(candidate_alias)} "
        "OR EXISTS ("
        "SELECT 1 FROM candidate_source_events cse_filter "
        f"WHERE cse_filter.address = {candidate_alias}.address "
        "AND cse_filter.source = ?"
        "))"
    )


def _wallet_source_match_params(source: str) -> list[str]:
    normalized = source.strip()
    return [normalized, normalized]


def _source_focus_blocker(row: dict[str, Any], *, paper_min_score: float) -> tuple[str, str]:
    stage = str(row.get("candidate_stage") or "")
    score = float(row.get("leader_score") or 0)
    activity = int(row.get("activity_count") or 0)
    next_action = str(row.get("next_action") or "")
    copy_status = str(row.get("copyability_status") or "")
    has_copy_signal = _has_copyability_signal(row)
    has_copy_validation = _has_copyability_validation(row)
    if stage in {"paper_candidate", "paper_approved", "live_eligible"}:
        return ("paper_ready", "已进入 paper")
    if stage == "blocked_hygiene":
        return ("hygiene_blocked", "hygiene 阻断")
    if stage == "blocked_copyability":
        return ("copyability_blocked", "copyability 阻断")
    if stage == "rejected":
        return ("rejected", "已拒绝")
    if activity <= 0:
        return ("no_history", "未补历史")
    if activity < 200:
        return ("thin_evidence", "历史证据偏薄")
    if next_action in {"light_pending", "medium_pending", "deep_pending"}:
        return ("history_pending", "历史证据补充中")
    if stage == "needs_data":
        return ("score_needs_data", "评分证据不足")
    if score <= 0:
        return ("not_scored", "待评分")
    if not has_copy_signal:
        if copy_status in {"queued", "running"}:
            return ("copyability_pending", "copyability 补证据中")
        if copy_status == "done":
            if _is_light_copyability_scan(row):
                return ("copyability_light_no_signal", "copyability 轻扫无信号")
            return ("copyability_no_signal", "copyability 无跟随信号")
        return ("missing_copyability", "缺 copyability 证据")
    if not has_copy_validation:
        if copy_status in {"queued", "running"}:
            return ("copyability_pending", "copyability 验证补证据中")
        return ("copyability_unvalidated", "copyability 线索未通过验证")
    if score < paper_min_score:
        return ("score_below_paper", f"分数未达 {paper_min_score:.0f}")
    if _is_paper_evidence_incomplete_review(row, paper_min_score=paper_min_score):
        return ("paper_evidence_incomplete", "L3 证据未完成")
    if stage == "needs_manual_review":
        return ("manual_review", "复核停靠状态")
    return ("unknown", "待确认")


def _source_focus_blocker_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("blocker_key") or "unknown")
        label = str(row.get("blocker_label") or "待确认")
        score = float(row.get("leader_score") or 0)
        group = groups.setdefault(
            key,
            {
                "key": key,
                "blocker": label,
                "count": 0,
                "max_score": score,
                "next_action": _review_blocker_next_action(key),
                "example": row.get("address") or "",
            },
        )
        group["count"] = int(group["count"]) + 1
        if score > float(group.get("max_score") or 0):
            group["max_score"] = score
            group["example"] = row.get("address") or ""
    return sorted(groups.values(), key=lambda item: (-int(item.get("count") or 0), -float(item.get("max_score") or 0), str(item.get("blocker") or "")))


def _copyability_int(row: dict[str, Any], *fields: str) -> int:
    values: list[int] = []
    for field in fields:
        try:
            values.append(int(row.get(field) or 0))
        except (TypeError, ValueError):
            values.append(0)
    return max(values) if values else 0


def _copyability_float(row: dict[str, Any], *fields: str) -> float:
    values: list[float] = []
    for field in fields:
        try:
            values.append(float(row.get(field) or 0))
        except (TypeError, ValueError):
            values.append(0.0)
    return max(values) if values else 0.0


def _has_copyability_signal(row: dict[str, Any]) -> bool:
    """Raw copy links mean there is something to validate, not that it is copyable."""

    copy_events = _copyability_int(row, "copy_event_count", "leader_copy_events", "feature_copy_event_count")
    copy_markets = _copyability_int(row, "copy_market_count", "leader_copy_markets", "feature_copy_market_count")
    followers = _copyability_int(row, "qualified_follower_count")
    backtest_trades = _copyability_int(row, "backtest_trade_count")
    return copy_events > 0 or copy_markets > 0 or followers > 0 or backtest_trades > 0


def _has_copyability_validation(row: dict[str, Any]) -> bool:
    """Validated copyability requires qualified followers or backtest-style evidence."""

    followers = _copyability_int(row, "qualified_follower_count")
    backtest_trades = _copyability_int(row, "backtest_trade_count")
    edge_retention = _copyability_float(row, "edge_retention_pct")
    walk_forward = _copyability_float(row, "walk_forward_consistency_pct")
    return followers > 0 or backtest_trades > 0 or edge_retention > 0 or walk_forward > 0


def _is_light_copyability_scan(row: dict[str, Any]) -> bool:
    scan_mode = str(row.get("copyability_scan_mode") or "").strip()
    return bool(scan_mode and scan_mode not in {"default", "deep"})


def _discovery_run_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        SELECT ingest_type AS run_type, status, started_at, finished_at, rows_written AS row_count, error
        FROM ingest_runs
        WHERE ingest_type IN ('activity_discovery', 'leaderboard_discovery', 'evidence_backfill', 'activity', 'positions', 'gamma_markets')
        ORDER BY started_at DESC
        LIMIT 8
        """,
    )


def _recent_source_events(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        SELECT address, source, status, labels, observed_at, recorded_at
        FROM candidate_source_events
        ORDER BY event_id DESC
        LIMIT 10
        """,
    )


def _discovery_stage_counts(
    conn: sqlite3.Connection,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
) -> list[dict[str, Any]]:
    where, params = _wallet_filter_where(stage=stage, source=source, query=query)
    clause = "WHERE " + " AND ".join(where) if where else ""
    return _rows(
        conn,
        f"""
        SELECT candidate_stage AS stage, COUNT(*) AS count
        FROM candidate_wallets cw
        {clause}
        GROUP BY candidate_stage
        ORDER BY count DESC, stage ASC
        """,
        tuple(params),
    )


def _discovery_signal_counts(
    conn: sqlite3.Connection,
    *,
    paper_min_score: float,
    stage: str = "",
    source: str = "",
    query: str = "",
) -> list[dict[str, Any]]:
    return [
        {"signal": "needs_backfill", "name": "待补历史", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="needs_backfill")},
        {"signal": "thin_evidence", "name": "证据偏薄", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="thin_evidence")},
        {"signal": "high_score", "name": "高分候选", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="high_score")},
        {"signal": "review_copy_pending", "name": "复核: Copy 补证据", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_copy_pending")},
        {"signal": "review_copy_no_signal", "name": "复核: 深扫无信号", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_copy_no_signal")},
        {"signal": "review_copy_light_no_signal", "name": "复核: 轻扫无信号", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_copy_light_no_signal")},
        {"signal": "review_copy_unvalidated", "name": "复核: Copy 未通过", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_copy_unvalidated")},
        {"signal": "review_paper_evidence_incomplete", "name": "复核: L3 未完成", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_paper_evidence_incomplete")},
        {"signal": "review_thin_evidence", "name": "复核: 历史偏薄", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_thin_evidence")},
        {"signal": "review_missing_copyability", "name": "复核: 缺 Copy", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="review_missing_copyability")},
        {"signal": "copy_signal", "name": "Copy 结构", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="copy_signal")},
        {"signal": "paper_signal", "name": "外部验证线索", "count": _wallet_signal_count(conn, stage=stage, source=source, query=query, signal="paper_signal")},
    ]


def _wallet_signal_count(
    conn: sqlite3.Connection,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
) -> int:
    return _wallet_scope_count(conn, stage=stage, source=source, query=query, signal=signal)


def _wallet_scope_count(
    conn: sqlite3.Connection,
    *,
    stage: str = "",
    source: str = "",
    query: str = "",
    signal: str = "",
    extra_where: list[str] | None = None,
) -> int:
    where, params = _wallet_filter_where(stage=stage, source=source, query=query, signal=signal)
    if extra_where:
        where.extend(extra_where)
    clause = "WHERE " + " AND ".join(where) if where else ""
    return _scalar(
        conn,
        f"""
        WITH copy_job AS (
            SELECT
                pj.wallet,
                pj.status,
                COALESCE(
                    json_extract(pj.output_json, '$.graph_scan_mode'),
                    json_extract(pj.input_json, '$.graph_scan_mode'),
                    ''
                ) AS copyability_scan_mode
            FROM pipeline_jobs pj
            JOIN (
                SELECT wallet, MAX(job_id) AS job_id
                FROM pipeline_jobs
                WHERE job_type = 'copyability_evidence'
                GROUP BY wallet
            ) latest_job
              ON latest_job.job_id = pj.job_id
        )
        SELECT COUNT(*)
        FROM candidate_wallets cw
        LEFT JOIN leader_latest_scores ls ON ls.address = cw.address
        LEFT JOIN wallet_features wf ON wf.address = cw.address
        LEFT JOIN paper_wallet_quality pq ON pq.wallet = cw.address
        LEFT JOIN evidence_backfill_budget eb ON eb.wallet = cw.address
        LEFT JOIN wallet_processing_state wps ON wps.wallet = cw.address
        LEFT JOIN copy_leader_stats cls ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp ON clp.leader_wallet = cw.address
        LEFT JOIN copy_job cj ON cj.wallet = cw.address
        {clause}
        """,
        tuple(params),
    )


def _text_source_count(value: Any) -> int:
    return len(_text_source_parts(value))


def _first_text_source(value: Any) -> str:
    parts = _text_source_parts(value)
    return parts[0] if parts else ""


def _text_source_parts(value: Any) -> list[str]:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").replace(";", "|").replace(",", "|").split("|"):
        item = raw.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        parts.append(item)
    return parts


def _discovery_priority(row: dict[str, Any]) -> float:
    score = float(row.get("leader_score") or 0)
    activity = min(float(row.get("activity_count") or 0) / 20.0, 15.0)
    sources = min(float(row.get("source_count") or 0) * 2.0, 8.0)
    copy = min(float(row.get("qualified_follower_count") or 0) * 5.0, 15.0)
    paper = 8.0 if int(row.get("paper_orders") or 0) > 0 else 0.0
    penalty = 12.0 if str(row.get("candidate_stage") or "") in {"rejected", "blocked_hygiene", "blocked_copyability"} else 0.0
    return round(max(score + activity + sources + copy + paper - penalty, 0.0), 2)


def _evidence_depth_label(value: Any) -> str:
    count = int(value or 0)
    if count >= 1000:
        return "deep"
    if count >= 200:
        return "ready"
    if count >= 100:
        return "light"
    if count > 0:
        return "starter"
    return "none"


def _progress_pct(current: Any, target: Any) -> float:
    try:
        target_value = float(target or 0)
        if target_value <= 0:
            return 0.0
        return round(max(0.0, min(100.0, float(current or 0) / target_value * 100.0)), 1)
    except (TypeError, ValueError):
        return 0.0


def _wallet_pipeline_diagnostic(
    candidate: dict[str, Any],
    processing_state: dict[str, Any],
    pipeline_jobs: list[dict[str, Any]],
    *,
    now: int | None = None,
) -> dict[str, Any]:
    """Summarize queue state for operators without mutating pipeline state."""
    current = int(time.time()) if now is None else int(now)
    latest_job = pipeline_jobs[0] if pipeline_jobs else {}
    candidate_stage = str(candidate.get("candidate_stage") or "")
    evidence_status = str(processing_state.get("evidence_status") or "")
    evidence_tier = str(processing_state.get("discovery_tier") or "")
    next_action = str(processing_state.get("next_action") or "")
    next_action_at = int(processing_state.get("next_action_at") or 0)
    job_status = str(latest_job.get("status") or "")
    lease_until = int(latest_job.get("lease_until") or 0)
    next_attempt_at = int(latest_job.get("next_attempt_at") or 0)
    latest_error = str(latest_job.get("last_error") or "")

    if not next_action:
        due_state = "none"
        due_label = "无待处理动作"
    elif next_action_at <= 0 or next_action_at <= current:
        due_state = "due"
        due_label = "已到调度时间"
    else:
        due_state = "scheduled"
        due_label = f"{_duration_label(next_action_at - current)}后可调度"

    state = "attention"
    headline = "等待证据管道继续处理"
    suggested_action = "检查 planner 与对应 worker 的最近心跳。"
    if job_status == "running" and lease_until and lease_until <= current:
        state = "critical"
        headline = "运行租约已经过期"
        suggested_action = "等待队列回收租约；若持续存在，检查对应 worker 心跳。"
    elif job_status == "failed":
        state = "critical"
        headline = "最近一次证据任务失败"
        suggested_action = "展开完整错误，修复上游问题后重试对应任务。"
    elif job_status == "queued" and next_attempt_at > current:
        headline = "任务正在等待重试窗口"
        suggested_action = f"预计 {_duration_label(next_attempt_at - current)}后可再次领取。"
    elif job_status == "queued":
        headline = "任务已排队等待 worker"
        suggested_action = "确认对应 worker 有心跳且存在可用并发。"
    elif job_status == "running":
        state = "ok"
        headline = "证据任务正在执行"
        suggested_action = "无需人工处理；关注租约到期前是否完成或续租。"
    elif job_status == "done" and next_action and due_state == "due":
        headline = "上一任务完成，下一动作等待派发"
        suggested_action = "确认 pipeline planner 正常运行并创建下一层任务。"
    elif evidence_status == EvidenceStatus.SUMMARY_READY.value:
        state = "ok"
        headline = "历史证据摘要已经就绪"
        suggested_action = "等待评分与自动复核循环消费最新证据。"
    elif not processing_state:
        state = "critical"
        headline = "尚未生成钱包处理状态"
        suggested_action = "运行状态物化流程，再由 pipeline planner 创建任务。"
    elif next_action and not latest_job:
        state = "critical" if due_state == "due" else "attention"
        headline = "下一动作尚未进入执行队列"
        suggested_action = "检查 pipeline planner 是否漏派该钱包。"

    if candidate_stage in {
        CandidateStage.BLOCKED_HYGIENE.value,
        CandidateStage.BLOCKED_COPYABILITY.value,
        CandidateStage.REJECTED.value,
    }:
        state = "critical"
        headline = "钱包已被研究风险规则阻断"
        suggested_action = "先查看最新评分原因和阶段变更，再决定是否重新补证据。"
    elif candidate_stage == CandidateStage.NEEDS_REVIEW.value and state == "ok":
        headline = "证据链正常，等待自动复核结论"

    return {
        "state": state,
        "headline": headline,
        "suggested_action": suggested_action,
        "candidate_stage": candidate_stage,
        "candidate_stage_label": _CANDIDATE_STAGE_LABELS.get(candidate_stage, candidate_stage),
        "evidence_status": evidence_status,
        "evidence_status_label": _EVIDENCE_STATUS_LABELS.get(evidence_status, evidence_status),
        "evidence_tier": evidence_tier,
        "evidence_tier_label": _EVIDENCE_TIER_LABELS.get(evidence_tier, evidence_tier),
        "next_action": next_action,
        "next_action_label": _EVIDENCE_ACTION_LABELS.get(next_action, next_action),
        "next_action_at": next_action_at,
        "due_state": due_state,
        "due_label": due_label,
        "latest_job_status": job_status,
        "latest_job_status_label": _JOB_STATUS_LABELS.get(job_status, job_status),
        "latest_job_type": str(latest_job.get("job_type") or ""),
        "latest_error": latest_error,
        "attempts": int(latest_job.get("attempts") or 0),
        "max_attempts": int(latest_job.get("max_attempts") or 0),
    }


def _wallet_history_timeline(
    score_history: list[dict[str, Any]],
    review_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge score and stage history for display without changing stored events."""
    rows = [
        {
            "event_type": "score",
            "event_label": "评分",
            "stage": row.get("review_stage"),
            "score": row.get("leader_score"),
            "reason": row.get("review_reason"),
            "policy_version": row.get("policy_version"),
            "occurred_at": row.get("scored_at"),
        }
        for row in score_history
    ]
    rows.extend(
        {
            "event_type": "stage_change",
            "event_label": "阶段变更",
            "stage": f'{row.get("from_stage") or "-"} -> {row.get("to_stage") or "-"}',
            "score": None,
            "reason": row.get("reason"),
            "policy_version": "",
            "occurred_at": row.get("created_at"),
        }
        for row in review_events
    )
    return sorted(rows, key=lambda row: int(row.get("occurred_at") or 0), reverse=True)


def _wallet_detail_data(
    conn: sqlite3.Connection,
    address: str,
    *,
    paper_min_score: float = 70.0,
    research_only: bool = True,
) -> dict[str, Any]:
    address = address.lower()
    candidate = conn.execute("SELECT * FROM candidate_wallets WHERE address = ?", (address,)).fetchone()
    if not candidate:
        return {"address": address, "found": False}
    feature = conn.execute("SELECT * FROM wallet_features WHERE address = ?", (address,)).fetchone()
    latest_score = conn.execute(
        """
        SELECT * FROM leader_latest_scores
        WHERE address = ?
        LIMIT 1
        """,
        (address,),
    ).fetchone()
    paper_handoff_rows = _paper_handoff_rows(
        conn,
        address=address,
        limit=1,
        paper_min_score=paper_min_score,
        research_only=research_only,
    )
    processing_state = _one(
        conn,
        "SELECT * FROM wallet_processing_state WHERE wallet = ?",
        (address,),
    )
    pipeline_jobs = _rows(
        conn,
        """
        SELECT
            job_id,
            job_type,
            subject_key AS job_action,
            tier AS job_scope,
            status,
            priority,
            shard,
            attempts,
            max_attempts,
            lease_owner,
            lease_until,
            next_attempt_at,
            last_error,
            created_at,
            updated_at,
            completed_at
        FROM pipeline_jobs
        WHERE wallet = ?
        ORDER BY updated_at DESC, job_id DESC
        LIMIT 30
        """,
        (address,),
    )
    score_history = _rows(
        conn,
        """
        SELECT leader_score, review_stage, review_reason, policy_version, scored_at
        FROM leader_scores
        WHERE address = ?
        ORDER BY scored_at DESC, score_id DESC
        LIMIT 20
        """,
        (address,),
    )
    review_events = _rows(
        conn,
        """
        SELECT from_stage, to_stage, reason, created_at
        FROM review_events
        WHERE address = ?
        ORDER BY created_at DESC, event_id DESC
        LIMIT 20
        """,
        (address,),
    )
    candidate_data = dict(candidate)
    return {
        "address": address,
        "found": True,
        "candidate": candidate_data,
        "feature": _json_columns(dict(feature)) if feature else {},
        "latest_score": _json_columns(dict(latest_score)) if latest_score else {},
        "processing_state": processing_state,
        "processing_diagnostic": _wallet_pipeline_diagnostic(candidate_data, processing_state, pipeline_jobs),
        "paper_handoff": paper_handoff_rows[0] if paper_handoff_rows else {},
        "paper_quality": _one(conn, "SELECT * FROM paper_wallet_quality WHERE wallet = ?", (address,)),
        "paper_performance": _one(conn, "SELECT * FROM paper_wallet_performance WHERE wallet = ?", (address,)),
        "publish": _one(conn, "SELECT * FROM leader_publish WHERE wallet = ?", (address,)),
        "backfill": _one(conn, "SELECT * FROM evidence_backfill_budget WHERE wallet = ?", (address,)),
        "pipeline_jobs": pipeline_jobs,
        "score_history": score_history,
        "review_events": review_events,
        "history_timeline": _wallet_history_timeline(score_history, review_events),
        "source_events": _rows(
            conn,
            """
            SELECT source, status, labels, notes, links, observed_at, recorded_at, evidence_json
            FROM candidate_source_events
            WHERE address = ?
            ORDER BY observed_at DESC, event_id DESC
            LIMIT 20
            """,
            (address,),
        ),
        "recent_activity": _rows(
            conn,
            """
            SELECT timestamp, type, side, market_slug, outcome, price, size, usdc_size, transaction_hash
            FROM wallet_activity
            WHERE address = ?
            ORDER BY timestamp DESC, activity_id DESC
            LIMIT 50
            """,
            (address,),
        ),
        "episodes": _rows(
            conn,
            """
            SELECT market_slug, outcome, status, buy_count, sell_count, bought_usdc, sold_usdc,
                   realized_pnl_est, first_ts, last_ts
            FROM wallet_episodes
            WHERE address = ?
            ORDER BY COALESCE(last_ts, 0) DESC, episode_id DESC
            LIMIT 25
            """,
            (address,),
        ),
        "copy_leader": _one(conn, "SELECT * FROM copy_leader_stats WHERE leader_wallet = ?", (address,)),
        "top_copy_pairs": _rows(
            conn,
            """
            SELECT follower_wallet, copy_event_count, copy_market_count, containment_pct,
                   leader_precedes_pct, median_lag_seconds, qualifies, last_copy_ts
            FROM copy_pair_stats
            WHERE leader_wallet = ?
            ORDER BY qualifies DESC, copy_event_count DESC
            LIMIT 20
            """,
            (address,),
        ),
    }


def _render_dashboard(settings: RobotSettings) -> str:
    data = _dashboard_data_cached(settings)
    tiles = [
        ("候选钱包", _fmt_int(data["total_candidates"]), "总候选地址库"),
        ("活动事件", _fmt_int(data["activity_coverage"].get("total_events")), "已落库 wallet_activity"),
        ("200+ 事件钱包", _fmt_int(data["activity_coverage"].get("wallets_ge_200")), "进入轻量证据层"),
        ("数据库", _fmt_bytes(data["db_size_bytes"]), "SQLite 文件大小"),
    ]
    body = [
        _top_nav("dashboard"),
        '<main class="shell">',
        '<section class="toolbar"><div><h1>研究总览</h1><p>NAS research/scoring：发现源、证据厚度、评分阶段和外部交接状态。</p></div>'
        '<a class="button" href="/wallets">候选钱包</a></section>',
        '<section class="metric-grid">',
        "".join(f'<div class="metric"><span>{_e(label)}</span><strong>{value}</strong><small>{_e(note)}</small></div>' for label, value, note in tiles),
        "</section>",
        '<section class="grid two">',
        _panel("候选阶段", _simple_table(data["stage_counts"], ["name", "count"], ["阶段", "数量"])),
        _panel("评分阶段", _simple_table(data["score_stage_counts"], ["name", "count", "avg_score", "max_score"], ["阶段", "数量", "均分", "最高分"])),
        "</section>",
        _panel("评分规则", _dict_table(data["score_policy"])),
        '<section class="grid two">',
        _panel("发现来源", _simple_table(data["source_counts"], ["name", "count", "latest_at"], ["来源", "钱包数", "最近记录"])),
        _panel("补证据队列", _backfill_queue_tables(data["backfill_queue"])),
        "</section>",
        _panel("发现活水", _discovery_freshness_panel(data["discovery_freshness"])),
        _panel("L1/L2/L3 证据流水线", _evidence_pipeline_panel(data["evidence_pipeline"])),
        _panel("生产收敛摘要", _production_readiness_panel(data["production_readiness"])),
        _panel("Execution Preflight 执行前检查", _execution_preflight_panel(data["execution_preflight"])),
        _panel("Paper 实时钱包审计", _paper_realtime_audit_panel(data["paper_realtime_audit"])),
        _panel("RTDS Watch 近 Paper 审计", _rtds_watch_audit_panel(data["rtds_watch_audit"])),
        _panel("Paper 候选扩池审计", _paper_pool_expansion_panel(data["paper_pool_expansion"])),
        _panel("Paper 交接观察", _paper_handoff_panel(data["paper_handoff"])),
        _panel("Paper Observer 预览", _paper_observer_preview_panel(data["paper_observer_preview"])),
        _panel("Paper Observer 报价评估", _paper_observer_evaluation_panel(data["paper_observer_evaluation"])),
        _panel("Copyability 证据通道", _copyability_lane_panel(data["copyability_lane"])),
        _panel("Copyability 无信号高潜池", _copyability_no_signal_panel(data["copyability_no_signal"])),
        _panel("Needs Data 原因", _needs_data_reason_table(data["needs_data_reasons"])),
        _panel("来源质量摘要", _source_quality_table(data["source_quality"])),
        _panel("存储维护", _storage_maintenance_panel(data["storage_maintenance"])),
        _panel("系统健康", _ops_health_panel(data["ops_health"])),
        _panel("高分阻塞分布", _simple_table(data["top_review_blockers"], ["blocker", "count", "max_score", "next_action", "example"], ["主阻塞", "数量", "最高分", "下一步", "例子"])),
        _panel("高分待验证", _top_review_candidates_panel(data["top_review_candidates"])),
        '<section class="grid two">',
        _panel("外部验证质量", _dict_table(data["paper_quality"])),
        _panel("本地发布库", _simple_table(data["published_leaders"], ["name", "count", "latest_at"], ["状态", "数量", "最近发布"])),
        "</section>",
        _panel("最近任务", _simple_table(data["recent_runs"], ["run_type", "status", "row_count", "started_at", "finished_at", "error"], ["任务", "状态", "行数", "开始", "结束", "错误"])),
        "</main>",
    ]
    return _render_page("pm-robot 研究总览", "".join(body))


def _render_wallets(settings: RobotSettings, *, stage: str, source: str, query: str, signal: str) -> str:
    data = discovery_data(settings, stage=stage, source=source, query=query, signal=signal, limit=150)
    rows = data["wallets"]
    body = [
        _top_nav("wallets"),
        '<main class="shell">',
        '<section class="toolbar discovery-hero"><div><p class="eyebrow">Wallet Discovery</p>'
        '<h1>钱包发现工作台</h1><p>从公开活动、排行榜、Copy 结构和研究评分线索里筛出值得继续补历史的钱包。</p></div>'
        f'<div class="hero-actions"><a class="button secondary" href="/api/discovery?{_e(urlencode({"stage": stage, "source": source, "q": query, "signal": signal, "limit": "150"}))}">JSON</a>'
        '<a class="button" href="/">总览</a></div></section>',
        _discovery_status_strip(data),
        _discovery_summary_tiles(data),
        _funnel_steps(data["funnel"]),
        '<section class="grid three">',
        _panel("证据深度", _evidence_depth_card(data["evidence_depth"])),
        _panel("阶段分布", _mini_count_list(data["stage_counts"], "stage")),
        _panel("快速筛选", _signal_filter_bar(data["signal_counts"], stage=stage, source=source, query=query, active=signal)),
        "</section>",
        _source_focus_section(data.get("source_focus") or {}),
        f"""
        <form class="filters" method="get" action="/wallets">
          <label>阶段<input name="stage" value="{_e(stage)}" placeholder="needs_manual_review"></label>
          <label>来源<input name="source" value="{_e(source)}" placeholder="trades_global"></label>
          <label>搜索<input name="q" value="{_e(query)}" placeholder="地址 / 标签 / 备注"></label>
          <input type="hidden" name="signal" value="{_e(signal)}">
          <button type="submit">筛选</button>
          <a class="button secondary" href="/wallets">清空</a>
        </form>
        """,
        '<section class="grid two">',
        _panel("发现来源质量", _source_quality_table(data["source_quality"])),
        _panel("最近发现事件", _simple_table(data["recent_source_events"], ["address", "source", "status", "labels", "recorded_at"], ["钱包", "来源", "状态", "标签", "记录"])),
        "</section>",
        '<section class="grid two">',
        _panel("补历史队列", _backfill_queue_tables(data["backfill_queue"])),
        _panel("发现相关任务", _simple_table(data["recent_runs"], ["run_type", "status", "row_count", "started_at", "finished_at", "error"], ["任务", "状态", "行数", "开始", "结束", "错误"])),
        "</section>",
        '<section class="section-head"><div><h2>候选队列</h2>'
        f'<p>当前筛选返回 {_fmt_int(data["wallet_count"])} 个钱包，按综合发现优先级排序。</p></div></section>',
        _wallets_table(rows),
        "</main>",
    ]
    return _render_page("钱包发现工作台", "".join(body))


def _render_wallet_detail(settings: RobotSettings, address: str) -> str:
    data = wallet_detail_data(settings, address)
    if not data.get("found"):
        return _render_page("钱包不存在", _top_nav("wallets") + f'<main class="shell"><h1>钱包不存在</h1><p>{_e(address)}</p></main>')
    candidate = data["candidate"]
    score = data.get("latest_score") or {}
    processing_state = data.get("processing_state") or {}
    processing_diagnostic = data.get("processing_diagnostic") or {}
    body = [
        _top_nav("wallets"),
        '<main class="shell">',
        '<section class="toolbar"><div>',
        f'<h1 class="address">{_e(address)}</h1>',
        f'<p>{_e(candidate.get("sources", ""))}</p>',
        '</div><a class="button secondary" href="/wallets">返回列表</a></section>',
        _wallet_processing_diagnostic_panel(processing_diagnostic),
        '<section class="metric-grid">',
        _metric("候选阶段", processing_diagnostic.get("candidate_stage_label", "")),
        _metric("Leader Score", _fmt_num(score.get("leader_score"))),
        _metric("Review Stage", score.get("review_stage", "")),
        _metric("证据层级", processing_diagnostic.get("evidence_tier_label", "")),
        _metric("下一动作", processing_diagnostic.get("next_action_label", "")),
        _metric("Paper ROI", _fmt_pct((data.get("paper_quality") or {}).get("total_roi"))),
        "</section>",
        '<section class="grid two">',
        _panel("最新评分", _dict_table(score)),
        _panel("特征字段", _dict_table(data.get("feature", {}))),
        "</section>",
        '<section class="grid two">',
        _panel("Paper 交接审计", _paper_handoff_detail_panel(data.get("paper_handoff") or {})),
        _panel("Paper 质量", _dict_table(data.get("paper_quality") or {})),
        "</section>",
        '<section class="grid two">',
        _panel("证据处理状态", _wallet_processing_state_table(processing_state)),
        _panel("补历史预算", _dict_table(data.get("backfill") or {})),
        "</section>",
        _panel("任务历史", _pipeline_jobs_table(data.get("pipeline_jobs") or [])),
        _panel("评分与阶段时间线", _wallet_history_timeline_table(data.get("history_timeline") or [])),
        '<section class="grid two">',
        _panel("发布状态", _dict_table(data.get("publish") or {})),
        _panel("Copy Leader", _dict_table(data.get("copy_leader") or {})),
        "</section>",
        _panel("来源事件", _simple_table(data["source_events"], ["source", "status", "labels", "observed_at", "recorded_at"], ["来源", "状态", "标签", "观察", "记录"])),
        _panel("最近活动", _simple_table(data["recent_activity"], ["timestamp", "type", "side", "market_slug", "outcome", "price", "usdc_size"], ["时间", "类型", "方向", "市场", "结果", "价格", "USDC"])),
        _panel("交易 Episode", _simple_table(data["episodes"], ["market_slug", "outcome", "status", "buy_count", "sell_count", "bought_usdc", "sold_usdc", "realized_pnl_est"], ["市场", "结果", "状态", "买", "卖", "买入", "卖出", "估算PnL"])),
        _panel("Top Copy Pairs", _simple_table(data["top_copy_pairs"], ["follower_wallet", "copy_event_count", "copy_market_count", "containment_pct", "leader_precedes_pct", "qualifies"], ["Follower", "事件", "市场", "Containment", "Precedes", "通过"])),
        "</main>",
    ]
    return _render_page(address, "".join(body))


def _render_login(error: str) -> str:
    message = f'<p class="error">{_e(error)}</p>' if error else ""
    return _render_page(
        "pm-robot 登录",
        f"""
        <main class="login">
          <form method="post" action="/login" class="login-box">
            <h1>pm-robot</h1>
            <p>输入 VPS 上配置的 PM_ROBOT_UI_TOKEN。</p>
            {message}
            <input name="token" type="password" autocomplete="current-password" autofocus>
            <button type="submit">登录</button>
          </form>
        </main>
        """,
    )


def _render_missing_token() -> str:
    return _render_page(
        "未配置访问令牌",
        """
        <main class="login">
          <section class="login-box">
            <h1>未配置访问令牌</h1>
            <p>请在 /opt/pm-robot/.env 设置 PM_ROBOT_UI_TOKEN，然后重启 pm-robot-web.service。</p>
          </section>
        </main>
        """,
    )


def _render_page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{_e(title)}</title>
  <style>{_CSS}</style>
</head>
<body>{body}</body>
</html>"""


def _top_nav(active: str) -> str:
    dashboard = "active" if active == "dashboard" else ""
    wallets = "active" if active == "wallets" else ""
    return f"""
    <header class="top">
      <a class="brand" href="/">pm-robot</a>
      <nav>
        <a class="{dashboard}" href="/">总览</a>
        <a class="{wallets}" href="/wallets">钱包发现</a>
        <a href="/api/summary">API</a>
        <a href="/logout">退出</a>
      </nav>
    </header>
    """


def _wallets_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<section class="panel"><p class="empty">没有匹配的钱包。</p></section>'
    header = ["优先级", "钱包", "来源/阶段", "证据", "Copy", "Paper", "回填", "操作"]
    body = []
    for row in rows:
        address = row["address"]
        link = row.get("links") or f"https://polymarket.com/profile/{address}"
        body.append(
            "<tr>"
            f'<td class="priority-cell"><strong>{_fmt_num(row.get("discovery_priority"))}</strong>{_score_bar(row.get("discovery_priority"), 100)}</td>'
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<div class="muted-line">{_e(row.get("review_reason", ""))}</div></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<div class="stacked-pills">{_source_pill(row.get("latest_source") or row.get("sources"))}{_badge(row.get("hygiene_status") or "hygiene?")}</div></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))} events</div>{_depth_badge(row.get("evidence_depth_label"))}'
            f'{_progress_bar(row.get("activity_count"), 200, label="200")}</td>'
            f'<td><div class="numline">{_fmt_int(row.get("qualified_follower_count"))} followers</div>'
            f'<small>{_fmt_int(row.get("leader_copy_events"))} links · {_fmt_pct(row.get("copy_backtest_net_roi"))} · {_e(row.get("copyability_status") or "no job")} · {_e(row.get("copyability_scan_mode") or "unknown")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("paper_orders"))} orders</div><small>{_fmt_pct(row.get("paper_total_roi"))} ROI · {_fmt_int(row.get("paper_settled_positions"))} settled</small></td>'
            f'<td><div class="numline">{_e(row.get("backfill_stage", "") or "none")}</div>'
            f'{_progress_bar(row.get("current_depth"), row.get("target_depth"), label=str(row.get("target_depth") or ""))}</td>'
            f'<td class="actions"><a class="icon-button" title="查看详情" href="/wallet/{_e(address)}">↗</a>'
            f'{_external_link(link)}</td>'
            "</tr>"
        )
    return '<section class="panel table-panel">' + _table(header, "".join(body)) + "</section>"


def _discovery_status_strip(data: dict[str, Any]) -> str:
    funnel = {item["key"]: int(item.get("count") or 0) for item in data.get("funnel", [])}
    discovered = max(funnel.get("discovered", 0), 1)
    evidence_ready = funnel.get("evidence_ready", 0)
    scored = funnel.get("scored", 0)
    paper_pool = funnel.get("paper_pool", 0)
    if paper_pool > 0:
        status = "paper-ready"
        message = "已有候选进入 paper 观察池，重点看回测稳定性和外部验证表现。"
    elif scored > 0 and evidence_ready == 0:
        status = "history-needed"
        message = "已发现并评分了一批钱包，但历史活动深度仍偏薄，优先补历史。"
    elif evidence_ready / discovered < 0.2:
        status = "history-needed"
        message = "发现池正在扩张，当前主要瓶颈是活动证据厚度。"
    else:
        status = "review-needed"
        message = "证据层已形成，下一步应集中复核高分与 Copy 信号重合的钱包。"
    return (
        f'<section class="status-strip {status}">'
        f'<strong>{_e(message)}</strong>'
        f'<span>当前筛选 {_fmt_int(data.get("wallet_count"))} 个钱包 · 更新时间 {_fmt_ts(data.get("generated_at"))}</span>'
        "</section>"
    )


def _discovery_summary_tiles(data: dict[str, Any]) -> str:
    funnel = {item["key"]: item for item in data.get("funnel", [])}
    depth = data.get("evidence_depth", {})
    tiles = [
        ("发现池", _fmt_int((funnel.get("discovered") or {}).get("count")), "全部候选钱包"),
        ("证据足量", _fmt_int((funnel.get("evidence_ready") or {}).get("count")), "活动事件 >= 200"),
        ("Copy 信号", _fmt_int((funnel.get("copy_signal") or {}).get("count")), "有合格跟随结构"),
        ("最深活动", _fmt_int(depth.get("max_events")), "单钱包最大事件数"),
    ]
    return (
        '<section class="metric-grid discovery-metrics">'
        + "".join(
            f'<div class="metric"><span>{_e(label)}</span><strong>{value}</strong><small>{_e(note)}</small></div>'
            for label, value, note in tiles
        )
        + "</section>"
    )


def _funnel_steps(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    max_count = max((int(row.get("count") or 0) for row in rows), default=1) or 1
    parts = []
    for row in rows:
        count = int(row.get("count") or 0)
        width = max(4.0, count / max_count * 100.0)
        parts.append(
            f'<div class="funnel-step"><div class="funnel-label"><strong>{_e(row.get("name"))}</strong>'
            f'<span>{_fmt_int(count)}</span></div><div class="funnel-track"><i style="width:{width:.1f}%"></i></div>'
            f'<small>{_e(row.get("note"))}</small></div>'
        )
    return '<section class="panel funnel-panel"><h2>发现漏斗</h2><div class="funnel-grid">' + "".join(parts) + "</div></section>"


def _evidence_depth_card(values: dict[str, Any]) -> str:
    rows = [
        ("none", "无活动", values.get("none", 0)),
        ("starter", "1-99", values.get("starter", 0)),
        ("light", "100-199", values.get("light", 0)),
        ("ready", "200-999", values.get("ready", 0)),
        ("deep", "1000+", values.get("deep", 0)),
    ]
    total = max(sum(int(count or 0) for _, _, count in rows), 1)
    body = []
    for key, label, count in rows:
        pct = int(count or 0) / total * 100.0
        body.append(
            f'<div class="depth-row"><span>{_depth_badge(key)} {_e(label)}</span><strong>{_fmt_int(count)}</strong>'
            f'<div class="mini-track"><i class="{_e(key)}" style="width:{pct:.1f}%"></i></div></div>'
        )
    body.append(f'<p class="panel-note">平均 {_fmt_num(values.get("avg_events"))} 条，最高 {_fmt_int(values.get("max_events"))} 条。</p>')
    return "".join(body)


def _mini_count_list(rows: list[dict[str, Any]], key_name: str) -> str:
    if not rows:
        return '<p class="empty">暂无数据。</p>'
    max_count = max((int(row.get("count") or 0) for row in rows), default=1) or 1
    body = []
    for row in rows:
        count = int(row.get("count") or 0)
        width = max(3.0, count / max_count * 100.0)
        body.append(
            f'<div class="mini-row"><span>{_badge(row.get(key_name))}</span><strong>{_fmt_int(count)}</strong>'
            f'<div class="mini-track"><i style="width:{width:.1f}%"></i></div></div>'
        )
    return "".join(body)


def _signal_filter_bar(rows: list[dict[str, Any]], *, stage: str, source: str, query: str, active: str) -> str:
    links = [
        f'<a class="chip {"active" if not active else ""}" href="{_filter_href(stage=stage, source=source, query=query, signal="")}">全部</a>'
    ]
    for row in rows:
        signal = str(row.get("signal") or "")
        label = f'{row.get("name")} · {_fmt_int(row.get("count"))}'
        links.append(
            f'<a class="chip {"active" if active == signal else ""}" href="{_filter_href(stage=stage, source=source, query=query, signal=signal)}">{_e(label)}</a>'
        )
    return '<div class="chip-group">' + "".join(links) + "</div>"


def _source_quality_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无数据。</p>'
    body = []
    for row in rows:
        wallets = float(row.get("wallets") or 0)
        ready = float(row.get("evidence_ready_wallets") or 0)
        ready_pct = ready / wallets if wallets else 0.0
        source = str(row.get("source") or "")
        source_href = "/wallets?" + urlencode({"source": source})
        review_count = int(row.get("review_wallets") or 0)
        paper_count = int(row.get("paper_wallets") or 0)
        blocked_count = int(row.get("blocked_wallets") or 0) + int(row.get("rejected_wallets") or 0)
        body.append(
            "<tr>"
            f'<td><a class="plain-link" href="{_e(source_href)}">{_source_pill(source)}</a>'
            f'<small>{_format_cell(row.get("latest_at"))}</small></td>'
            f'<td class="num">{_fmt_int(row.get("wallets"))}</td>'
            f'<td>{_progress_bar(ready, wallets, label=_fmt_pct(ready_pct))}</td>'
            f'<td class="num">{_fmt_int(row.get("l3_wallets"))}</td>'
            f'<td class="num">{_fmt_int(row.get("score_ready_wallets"))}</td>'
            f'<td><div class="numline">{_fmt_int(review_count)} / {_fmt_int(paper_count)}</div>'
            f'<small>复核 / Paper</small></td>'
            f'<td class="num">{_fmt_int(blocked_count)}</td>'
            f'<td class="num">{_fmt_num(row.get("avg_score"))}</td>'
            f'<td class="num">{_fmt_num(row.get("max_score"))}</td>'
            "</tr>"
        )
    return _table(["来源", "钱包", "L2+", "L3", "可评分", "观察/Paper", "阻断", "均分", "最高分"], "".join(body))


def _storage_maintenance_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无存储维护数据。</p>'
    state = str(values.get("state") or "ok")
    banner_state = "ok" if state == "ok" else "attention"
    wal_ratio = float(values.get("wal_to_db_ratio") or 0)
    free_ratio = float(values.get("free_disk_ratio") or 0)
    cards = [
        ("数据库", _fmt_bytes(int(values.get("db_bytes") or 0)), "SQLite 主文件", "ok"),
        (
            "WAL",
            _fmt_bytes(int(values.get("wal_bytes") or 0)),
            f"{_fmt_pct(wal_ratio)} of DB",
            "warn" if bool(values.get("needs_wal_window")) else "ok",
        ),
        ("SHM", _fmt_bytes(int(values.get("shm_bytes") or 0)), "共享内存索引", "ok"),
        (
            "可用空间",
            _fmt_bytes(int(values.get("free_disk_bytes") or 0)),
            f"{_fmt_pct(free_ratio)} free",
            "warn" if bool(values.get("low_free_disk")) else "ok",
        ),
        (
            "最近备份",
            _fmt_ts(values.get("latest_backup_at")) if values.get("latest_backup_at") else "无",
            (
                str(values.get("latest_backup_name") or "尚未生成备份")
                if values.get("latest_backup_age_seconds") is None
                else f"{_fmt_duration_hours(float(values.get('latest_backup_age_seconds') or 0) / 3600)} 前"
            ),
            "ok" if bool(values.get("backup_fresh")) else "warn",
        ),
        (
            "备份数量",
            _fmt_int(values.get("backup_count")),
            f"新鲜阈值 {_fmt_duration_hours(float(values.get('backup_max_age_seconds') or 0) / 3600)}",
            "ok" if bool(values.get("backup_fresh")) else "warn",
        ),
        ("WAL 提醒阈值", _fmt_bytes(int(values.get("wal_warn_bytes") or 0)), "超过后安排窗口", "ok"),
        ("WAL 严重阈值", _fmt_bytes(int(values.get("wal_critical_bytes") or 0)), "建议长窗口", "warn" if bool(values.get("critical_wal")) else "ok"),
    ]
    commands = [
        {"command": values.get("backup_now_command"), "when": "立即创建一致性 SQLite 备份"},
        {"command": values.get("read_only_check"), "when": "先看报告，不改数据库"},
        {"command": values.get("idle_window_command"), "when": "推荐：等待队列空闲后自动进入维护窗口"},
        {"command": values.get("safe_command"), "when": "普通维护窗口"},
        {"command": values.get("long_window_command"), "when": "WAL 很大或 NAS 较忙"},
    ]
    return (
        f'<div class="health-banner {banner_state}"><strong>{_e(values.get("next_action"))}</strong>'
        f'<span>{_e(values.get("maintenance_boundary"))}</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {card_state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, card_state in cards
        )
        + "</div>"
        + '<h3 class="subhead">维护命令</h3>'
        + _simple_table(commands, ["command", "when"], ["命令", "使用场景"])
    )


def _discovery_freshness_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无发现活水数据。</p>'
    state = str(values.get("state") or "")
    banner_state = "ok" if state == "fresh" else "attention"
    pulse = values.get("paper_activity_pulse") or {}
    pulse_state = str(pulse.get("state") or "")
    pulse_banner_state = "ok" if pulse_state == "timely_buy_activity" else "attention"
    bridge = values.get("paper_rtds_bridge") or {}
    bridge_state = str(bridge.get("state") or "")
    bridge_banner_state = "ok" if bridge_state == "fresh_buy_activity" else "attention"
    cards = [
        ("来源事件 24h", _fmt_int(values.get("source_events_24h")), "candidate_source_events", "ok" if int(values.get("source_events_24h") or 0) else "warn"),
        ("新候选 24h", _fmt_int(values.get("candidates_24h")), f"总 {_fmt_int(values.get('candidate_wallets'))}", "ok" if int(values.get("candidates_24h") or 0) else "warn"),
        ("观察池活跃 24h", _fmt_int(values.get("observed_seen_24h")), f"总 {_fmt_int(values.get('observed_wallets'))}", "ok" if int(values.get("observed_seen_24h") or 0) else "warn"),
        ("观察池晋级 24h", _fmt_int(values.get("promoted_24h")), f"总 {_fmt_int(values.get('promoted_wallets'))}", "ok" if int(values.get("promoted_24h") or 0) else "warn"),
        ("最近候选", _fmt_ts(values.get("latest_candidate_at")) or "无", "first_seen_at", "ok" if values.get("latest_candidate_at") else "warn"),
        ("最近晋级", _fmt_ts(values.get("latest_promoted_at")) or "无", "promoted_at", "ok" if values.get("latest_promoted_at") else "warn"),
    ]
    bridge_cards = [
        ("Paper 钱包", _fmt_int(bridge.get("paper_stage_wallets")), "paper_candidate+", "ok" if int(bridge.get("paper_stage_wallets") or 0) else "warn"),
        ("RTDS 桥接事件", _fmt_int(bridge.get("rtds_activity_events")), f"{_fmt_int(bridge.get('rtds_activity_wallets'))} 个钱包", "ok" if int(bridge.get("rtds_activity_events") or 0) else "warn"),
        ("RTDS BUY", _fmt_int(bridge.get("rtds_buy_events")), f"24h {_fmt_int(bridge.get('rtds_buy_events_24h'))}", "ok" if int(bridge.get("rtds_buy_events") or 0) else "warn"),
        ("RTDS 非BUY", _fmt_int(bridge.get("rtds_non_buy_events")), f"REDEEM {_fmt_int(bridge.get('rtds_redeem_events'))}", "ok" if int(bridge.get("rtds_non_buy_events") or 0) else "warn"),
        ("短窗 BUY", _fmt_int(bridge.get("rtds_buy_events_fresh")), f"近 {_duration_label(PAPER_RTDS_BRIDGE_FRESH_SEC)}", "ok" if int(bridge.get("rtds_buy_events_fresh") or 0) else "warn"),
        ("24h 新交易", _fmt_int(bridge.get("rtds_activity_events_24h")), "paper-stage RTDS", "ok" if int(bridge.get("rtds_activity_events_24h") or 0) else "warn"),
        ("最大入库延迟", _duration_label(bridge.get("rtds_max_ingest_lag_sec")), "RTDS event -> DB", "ok" if bridge.get("rtds_max_ingest_lag_sec") is not None else "warn"),
        ("Paper RTDS阈值", f"${_fmt_num(bridge.get('paper_min_trade_usdc'))}", "paper-stage activity", "ok"),
        ("最近接入", _duration_label(bridge.get("latest_rtds_ingest_age_sec")), "ingested_at", "ok" if bridge.get("latest_rtds_ingested_at") else "warn"),
        ("最近交易", _duration_label(bridge.get("latest_rtds_activity_age_sec")), "activity timestamp", "ok" if bridge.get("latest_rtds_activity_at") else "warn"),
    ]
    pulse_cards = [
        ("Paper 钱包", _fmt_int(pulse.get("paper_stage_wallets")), "paper_candidate+", "ok" if int(pulse.get("paper_stage_wallets") or 0) else "warn"),
        ("24h 活动", _fmt_int(pulse.get("events_24h")), "all sources", "ok" if int(pulse.get("events_24h") or 0) else "warn"),
        ("24h BUY", _fmt_int(pulse.get("buy_events_24h")), f"非BUY {_fmt_int(pulse.get('non_buy_events_24h'))}", "ok" if int(pulse.get("buy_events_24h") or 0) else "warn"),
        ("可及时 BUY", _fmt_int(pulse.get("timely_buy_events")), f"<= {_duration_label(pulse.get('actionable_signal_window_sec'))}", "ok" if int(pulse.get("timely_buy_events") or 0) else "warn"),
        ("过时 BUY", _fmt_int(pulse.get("stale_buy_events_24h")), "seen after window", "warn" if int(pulse.get("stale_buy_events_24h") or 0) else "ok"),
        ("最大 BUY 延迟", _duration_label(pulse.get("max_buy_ingest_lag_sec")), "event -> DB", "warn" if int(pulse.get("max_buy_ingest_lag_sec") or 0) > PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC else "ok"),
        ("最近 BUY", _duration_label(pulse.get("latest_buy_age_sec")), "activity age", "ok" if pulse.get("latest_buy_at") else "warn"),
        ("最近 BUY 入库延迟", _duration_label(pulse.get("latest_buy_ingest_lag_sec")), "latest buy lag", "warn" if int(pulse.get("latest_buy_ingest_lag_sec") or 0) > PAPER_OBSERVER_ACTIONABLE_SIGNAL_SEC else "ok"),
    ]
    return (
        f'<div class="health-banner {banner_state}"><strong>{_e(values.get("next_action"))}</strong>'
        '<span>用最近 24/72 小时数据判断发现链路是否还有活水。</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {card_state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, card_state in cards
        )
        + "</div>"
        + '<h3 class="subhead">Paper-stage 活动脉冲</h3>'
        + f'<div class="health-banner {pulse_banner_state}"><strong>{_e(pulse.get("next_action") or "")}</strong>'
        '<span>统计 paper-stage 钱包的全部 wallet_activity，区分“看到 BUY”和“及时可跟 BUY”。</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {card_state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, card_state in pulse_cards
        )
        + "</div>"
        + _simple_table(
            pulse.get("wallet_rows") or [],
            [
                "wallet",
                "candidate_stage",
                "events_24h",
                "buy_events_24h",
                "timely_buy_events",
                "stale_buy_events_24h",
                "max_buy_ingest_lag",
                "latest_buy_age",
                "latest_event_type",
                "latest_side",
                "latest_source",
            ],
            ["钱包", "阶段", "24h事件", "24h BUY", "及时BUY", "过时BUY", "最大BUY延迟", "最近BUY", "最新类型", "最新方向", "最新来源"],
        )
        + '<h3 class="subhead">Paper 活动来源</h3>'
        + _simple_table(
            pulse.get("source_rows") or [],
            ["source", "events_24h", "buy_events_24h", "latest_ingested_at", "latest_activity_at"],
            ["来源", "24h事件", "24h BUY", "最近入库", "最近活动"],
        )
        + '<h3 class="subhead">RTDS→Paper 实时桥接</h3>'
        + f'<div class="health-banner {bridge_banner_state}"><strong>{_e(bridge.get("next_action") or "")}</strong>'
        '<span>只统计已经进入 paper-stage 的钱包，确认实时交易是否进入 wallet_activity。</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {card_state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, card_state in bridge_cards
        )
        + "</div>"
        + _simple_table(
            bridge.get("wallet_rows") or [],
            [
                "wallet",
                "candidate_stage",
                "rtds_activity_events",
                "rtds_buy_events",
                "rtds_non_buy_events",
                "rtds_redeem_events",
                "rtds_max_ingest_lag",
                "latest_rtds_event_type",
                "latest_rtds_side",
                "latest_rtds_ingested_at",
                "latest_rtds_activity_at",
            ],
            ["钱包", "阶段", "事件", "BUY", "非BUY", "REDEEM", "最大延迟", "最新类型", "最新方向", "最近接入", "最近交易"],
        )
        + '<h3 class="subhead">来源事件新鲜度</h3>'
        + _simple_table(
            values.get("source_rows") or [],
            ["source", "wallets", "events_24h", "events_72h", "wallets_24h", "wallets_72h", "latest_at"],
            ["来源", "累计钱包", "事件24h", "事件72h", "钱包24h", "钱包72h", "最近记录"],
        )
        + '<h3 class="subhead">观察池晋级</h3>'
        + _simple_table(
            values.get("observed_sources") or [],
            ["source", "observed_wallets", "seen_24h", "seen_72h", "promoted_wallets", "promoted_24h", "promoted_72h", "latest_seen_at"],
            ["观察来源", "观察钱包", "活跃24h", "活跃72h", "累计晋级", "晋级24h", "晋级72h", "最近观察"],
        )
        + '<h3 class="subhead">候选新增阶段</h3>'
        + _simple_table(
            values.get("stage_rows") or [],
            ["candidate_stage", "wallets", "new_24h", "new_72h", "latest_at"],
            ["候选阶段", "累计钱包", "新增24h", "新增72h", "最近新增"],
        )
    )


def _source_focus_section(values: dict[str, Any]) -> str:
    if not values:
        return ""
    source = str(values.get("source") or "")
    matched = _fmt_int(values.get("matched_wallets"))
    return (
        '<section class="section-head"><div>'
        f'<h2>当前来源诊断</h2><p>{_e(source)} · 匹配 {matched} 个钱包</p>'
        "</div></section>"
        '<section class="grid two">'
        + _panel(
            "来源阻塞分布",
            _simple_table(values.get("blockers") or [], ["blocker", "count", "max_score", "example"], ["主阻塞", "数量", "最高分", "例子"]),
        )
        + _panel("来源高分样本", _source_focus_wallets_table(values.get("top_wallets") or []))
        + "</section>"
    )


def _source_focus_wallets_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无匹配钱包。</p>'
    body = []
    for row in rows:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}</td>'
            f'<td>{_badge(row.get("blocker_label") or "待确认")}<small>{_e(row.get("review_next_action") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}</td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))}</div>'
            f'<small>{_e(row.get("evidence_tier") or "")} · {_e(row.get("next_action") or "")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("qualified_follower_count"))}</div>'
            f'<small>{_fmt_int(row.get("copy_event_count"))} links · {_e(row.get("copyability_scan_mode") or "unknown")}</small></td>'
            "</tr>"
        )
    return _table(["钱包", "分数", "主阻塞", "阶段", "历史", "Copy"], "".join(body))


def _filter_href(*, stage: str, source: str, query: str, signal: str) -> str:
    params = {k: v for k, v in {"stage": stage, "source": source, "q": query, "signal": signal}.items() if v}
    encoded = urlencode(params)
    return "/wallets" + (f"?{encoded}" if encoded else "")


def _progress_bar(current: Any, target: Any, *, label: str = "") -> str:
    pct = _progress_pct(current, target)
    label_text = label or f"{pct:.0f}%"
    return f'<div class="progress" title="{_e(label_text)}"><i style="width:{pct:.1f}%"></i><span>{_e(label_text)}</span></div>'


def _score_bar(value: Any, max_value: float) -> str:
    try:
        pct = max(0.0, min(100.0, float(value or 0) / max_value * 100.0))
    except (TypeError, ValueError):
        pct = 0.0
    return f'<div class="score-bar"><i style="width:{pct:.1f}%"></i></div>'


def _source_pill(value: Any) -> str:
    text = str(value or "unknown")
    short = text.replace("polymarket_", "pm_")
    if len(short) > 28:
        short = short[:25] + "..."
    return f'<span class="source-pill" title="{_e(text)}">{_e(short)}</span>'


def _depth_badge(value: Any) -> str:
    text = str(value or "none")
    labels = {"none": "none", "starter": "starter", "light": "light", "ready": "ready", "deep": "deep"}
    return f'<span class="depth-badge {_e(text)}">{_e(labels.get(text, text))}</span>'


def _external_link(value: Any) -> str:
    link = _first_link(str(value or ""))
    if not link:
        return ""
    return f'<a class="icon-button secondary" title="打开外部资料" href="{_e(link)}" target="_blank" rel="noreferrer">⇱</a>'


def _first_link(links: str) -> str:
    for part in links.replace(",", " ").replace("|", " ").split():
        if part.startswith("http://") or part.startswith("https://"):
            return part
    return ""


def _panel(title: str, content: str) -> str:
    return f'<section class="panel"><h2>{_e(title)}</h2>{content}</section>'


def _metric(label: str, value: Any) -> str:
    return f'<div class="metric"><span>{_e(label)}</span><strong>{_e(value)}</strong></div>'


def _simple_table(
    rows: list[dict[str, Any]],
    keys: list[str],
    labels: list[str],
    *,
    table_class: str = "",
) -> str:
    if not rows:
        return '<p class="empty">暂无数据。</p>'
    body = []
    for row in rows:
        cells = "".join(f"<td>{_format_cell(row.get(key))}</td>" for key in keys)
        body.append(f"<tr>{cells}</tr>")
    return _table(labels, "".join(body), table_class=table_class)


def _operator_code(value: Any, labels: dict[str, str]) -> str:
    code = str(value or "")
    if not code:
        return "-"
    label = labels.get(code, code)
    raw = f'<small class="mono">{_e(code)}</small>' if label != code else ""
    return f'<span class="operator-label">{_e(label)}</span>{raw}'


def _expandable_error(error: Any) -> str:
    text = str(error or "")
    if not text:
        return ""
    return (
        '<details class="job-error-details">'
        f'<summary>{_e(_short_error(text))}</summary>'
        f'<div>{_e(text)}</div>'
        "</details>"
    )


def _wallet_processing_diagnostic_panel(values: dict[str, Any]) -> str:
    if not values:
        return ""
    state = str(values.get("state") or "attention")
    latest_job = values.get("latest_job_status_label") or "暂无任务"
    attempts = int(values.get("attempts") or 0)
    max_attempts = int(values.get("max_attempts") or 0)
    cards = [
        (
            "证据状态",
            values.get("evidence_status_label") or "未生成",
            values.get("evidence_tier_label") or "未分层",
        ),
        (
            "候选阶段",
            values.get("candidate_stage_label") or "未设置",
            values.get("candidate_stage") or "-",
        ),
        (
            "下一动作",
            values.get("next_action_label") or "无",
            values.get("due_label") or "无调度时间",
        ),
        (
            "最近任务",
            latest_job,
            f"尝试 {attempts}/{max_attempts}" if max_attempts else f"尝试 {attempts}",
        ),
    ]
    error = _expandable_error(values.get("latest_error"))
    return (
        '<section class="wallet-diagnostic">'
        f'<div class="health-banner {state}"><strong>{_e(values.get("headline") or "")}</strong>'
        f'<span>{_e(values.get("suggested_action") or "")}</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note in cards
        )
        + "</div>"
        + (f'<div class="diagnostic-error"><strong>最新错误</strong>{error}</div>' if error else "")
        + "</section>"
    )


def _wallet_processing_state_table(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">尚未生成钱包处理状态。</p>'
    rows = [
        ("证据层级", _operator_code(values.get("discovery_tier"), _EVIDENCE_TIER_LABELS)),
        ("证据状态", _operator_code(values.get("evidence_status"), _EVIDENCE_STATUS_LABELS)),
        ("当前任务阶段", _operator_code(values.get("current_stage"), _EVIDENCE_ACTION_LABELS)),
        ("下一动作", _operator_code(values.get("next_action"), _EVIDENCE_ACTION_LABELS)),
        ("下一调度时间", _format_cell(values.get("next_action_at"))),
        ("证据深度", _format_cell(values.get("evidence_depth"))),
        ("证据置信度", _format_cell(values.get("evidence_confidence"))),
        (
            "历史覆盖",
            f'{_fmt_int(values.get("activity_count"))} 条活动 · '
            f'{_fmt_int(values.get("distinct_markets"))} 个市场 · '
            f'{_fmt_int(values.get("non_fast_trade_count"))} 条非快盘',
        ),
        ("处理优先级", _format_cell(values.get("priority"))),
        ("最近轻量回填", _format_cell(values.get("last_light_backfill_at"))),
        ("最近中量回填", _format_cell(values.get("last_medium_backfill_at"))),
        ("最近深度回填", _format_cell(values.get("last_deep_backfill_at"))),
        ("状态更新时间", _format_cell(values.get("updated_at"))),
    ]
    body = "".join(f"<tr><th>{_e(label)}</th><td>{value}</td></tr>" for label, value in rows)
    return f'<div class="table-wrap"><table class="kv"><tbody>{body}</tbody></table></div>'


def _stage_history_label(value: Any) -> str:
    text = str(value or "")
    if " -> " not in text:
        return _operator_code(text, _CANDIDATE_STAGE_LABELS)
    from_stage, to_stage = text.split(" -> ", 1)
    from_label = _CANDIDATE_STAGE_LABELS.get(from_stage, from_stage)
    to_label = _CANDIDATE_STAGE_LABELS.get(to_stage, to_stage)
    return (
        f'<span class="operator-label">{_e(from_label)} -> {_e(to_label)}</span>'
        f'<small class="mono">{_e(text)}</small>'
    )


def _wallet_history_timeline_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无评分或阶段变更记录。</p>'
    body = []
    for row in rows:
        score = row.get("score")
        stage = _stage_history_label(row.get("stage"))
        stage_score = stage
        if score is not None:
            stage_score = f'<strong class="timeline-score">{_e(_fmt_num(score))}</strong>{stage}'
        body.append(
            "<tr>"
            f'<td>{_badge(row.get("event_label"))}</td>'
            f"<td>{stage_score}</td>"
            f'<td>{_e(row.get("reason") or "")}</td>'
            f'<td class="mono">{_e(row.get("policy_version") or "-")}</td>'
            f'<td>{_fmt_ts(row.get("occurred_at"))}</td>'
            "</tr>"
        )
    return _table(["事件", "阶段 / 分数", "原因", "策略版本", "时间"], "".join(body))


def _pipeline_jobs_table(
    rows: list[dict[str, Any]],
    *,
    include_wallet: bool = False,
) -> str:
    if not rows:
        return '<p class="empty">暂无数据。</p>'
    body = []
    now = int(time.time())
    for row in rows:
        wallet = str(row.get("wallet") or "")
        wallet_cell = ""
        if include_wallet:
            wallet_cell = (
                f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a></td>'
                if wallet
                else "<td></td>"
            )
        error = str(row.get("last_error") or "")
        status_cell = _operator_code(row.get("status"), _JOB_STATUS_LABELS) + _expandable_error(error)
        queue_cell = (
            _operator_code(row.get("job_type"), _PIPELINE_JOB_TYPE_LABELS)
            + _operator_code(row.get("job_action"), _EVIDENCE_ACTION_LABELS)
            + f'<small>scope <span class="mono">{_e(row.get("job_scope") or "-")}</span></small>'
        )
        attempts_cell = (
            f'{_fmt_int(row.get("attempts"))}/{_fmt_int(row.get("max_attempts"))}'
            f'<small>priority {_fmt_int(row.get("priority"))} · shard {_fmt_int(row.get("shard"))}</small>'
        )
        next_attempt_at = int(row.get("next_attempt_at") or 0)
        lease_until = int(row.get("lease_until") or 0)
        lease_owner = str(row.get("lease_owner") or "")
        if next_attempt_at:
            retry_note = (
                f'{_duration_label(next_attempt_at - now)}后重试'
                if next_attempt_at > now
                else "已到重试时间"
            )
            retry_line = f'<div>{_fmt_ts(next_attempt_at)}<small>{_e(retry_note)}</small></div>'
        else:
            retry_line = '<div class="muted-line">无重试计划</div>'
        if lease_owner:
            lease_state = "租约已过期" if lease_until and lease_until <= now else "租约有效"
            lease_line = (
                f'<div class="lease-line"><span class="mono">{_e(lease_owner)}</span>'
                f'<small>{_e(lease_state)} · {_fmt_ts(lease_until) or "无到期时间"}</small></div>'
            )
        else:
            lease_line = '<div class="muted-line">无活动租约</div>'
        schedule_cell = retry_line + lease_line
        created_at = int(row.get("created_at") or 0)
        age = f'{_duration_label(max(0, now - created_at))}前' if created_at else "未知"
        timing_cell = (
            f'创建 {_fmt_ts(created_at) or "-"}<small>任务年龄 {age}</small>'
            f'<small>更新 {_fmt_ts(row.get("updated_at")) or "-"}</small>'
            f'<small>完成 {_fmt_ts(row.get("completed_at")) if row.get("completed_at") else "-"}</small>'
        )
        body.append(
            "<tr>"
            + wallet_cell
            + f"<td>{queue_cell}</td><td>{status_cell}</td>"
            + f"<td>{schedule_cell}</td><td>{attempts_cell}</td><td>{timing_cell}</td></tr>"
        )
    labels = (["钱包"] if include_wallet else []) + ["队列 / 动作", "状态 / 错误", "重试 / 租约", "尝试", "时间"]
    return _table(labels, "".join(body))


def _dict_table(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无数据。</p>'
    body = []
    for key, value in values.items():
        if key.endswith("_json") and isinstance(value, str):
            value = _json_load(value)
        if isinstance(value, dict):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        body.append(f"<tr><th>{_e(key)}</th><td>{_format_cell(value)}</td></tr>")
    return '<table class="kv"><tbody>' + "".join(body) + "</tbody></table>"


def _backfill_queue_tables(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无数据。</p>'
    stages = values.get("stages") or []
    sources = values.get("sources") or []
    return (
        '<h3 class="subhead">阶段</h3>'
        + _simple_table(stages, ["stage", "count"], ["阶段", "数量"])
        + '<h3 class="subhead">来源</h3>'
        + _simple_table(sources, ["source", "count"], ["来源", "数量"])
    )


def _ops_health_summary(conn: sqlite3.Connection, settings: RobotSettings) -> dict[str, Any]:
    now = int(time.time())
    storage = _storage_health(settings)
    address_quality = _address_quality_fast_report(conn)
    pipeline_backlog = _pending_evidence_backlog_summary(conn)
    runtime_loops = _runtime_loop_summary(conn, now=now)
    research_control_steps = _research_control_step_summary(conn, now=now)
    upstream_request_budget = (
        api_rate_limit_summary(conn, now=now)
        if _table_exists(conn, "api_rate_limit_state")
        else {"scope_count": 0, "active_cooldowns": 0, "total_cooldowns": 0, "rows": []}
    )
    job_status = _rows(
        conn,
        """
        SELECT job_type, status, COUNT(*) AS count
        FROM pipeline_jobs
        GROUP BY job_type, status
        ORDER BY job_type ASC, status ASC
        """,
    )
    active_jobs = _rows(
        conn,
        """
        SELECT job_type, subject_key AS job_action, status, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE status IN ('queued', 'running')
        GROUP BY job_type, subject_key, status
        ORDER BY job_type ASC, subject_key ASC, status ASC
        """,
    )
    stale_samples = _rows(
        conn,
        """
        SELECT job_type, wallet, subject_key AS job_action, attempts, lease_until
        FROM pipeline_jobs
        WHERE status = 'running'
          AND lease_until <= ?
        ORDER BY lease_until ASC, priority ASC, updated_at ASC
        LIMIT 6
        """,
        (now,),
    )
    stale_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM pipeline_jobs WHERE status = 'running' AND lease_until <= ?",
            (now,),
        ).fetchone()[0]
    )
    failed_samples = _rows(
        conn,
        """
        SELECT
            job_id,
            job_type,
            wallet,
            subject_key AS job_action,
            tier AS job_scope,
            status,
            priority,
            shard,
            attempts,
            max_attempts,
            lease_owner,
            lease_until,
            next_attempt_at,
            last_error,
            created_at,
            updated_at,
            completed_at
        FROM pipeline_jobs
        WHERE status = 'failed'
        ORDER BY updated_at DESC, priority ASC, job_id DESC
        LIMIT 10
        """,
    )
    status_totals = {str(row["status"]): int(row["count"] or 0) for row in job_status}
    invalid_address_rows = int(address_quality.get("invalid_address_rows") or 0)
    high_priority_backlog = int(pipeline_backlog.get("high_priority_pending_without_active_job") or 0)
    if invalid_address_rows:
        health = "attention"
        note = "候选或队列表存在非标准钱包地址，需要先隔离再补证据。"
    elif stale_count:
        health = "attention"
        note = "有过期 running 队列，等待维护 loop 回收。"
    elif high_priority_backlog:
        health = "attention"
        note = "高优先级钱包等待补证据但未进入活动队列，需要重新规划或检查 worker。"
    elif int(research_control_steps.get("attention_count") or 0):
        health = "attention"
        note = "研究控制阶段最近有失败、部分完成或超时，请查看具体阶段。"
    elif int(runtime_loops.get("attention_count") or 0):
        health = "attention"
        note = "常驻循环有失败、部分完成或超时，需要查看运行循环新鲜度。"
    elif int(status_totals.get("failed", 0)):
        health = "attention"
        note = "存在 failed 队列，需要查看 last_error。"
    elif int(storage.get("wal_bytes") or 0) >= 1_000_000_000:
        health = "attention"
        note = "WAL 已超过 1GB，建议安排 ./pmrobot-nas.sh wal-truncate-window。"
    else:
        health = "ok"
        note = "队列和 WAL 处于常规范围。"
    return {
        "health": health,
        "note": note,
        "address_quality": address_quality,
        "runtime": _runtime_build_info(),
        "runtime_loops": runtime_loops,
        "research_control_steps": research_control_steps,
        "upstream_request_budget": upstream_request_budget,
        "storage": storage,
        "pipeline_backlog": pipeline_backlog,
        "job_status": job_status,
        "active_jobs": active_jobs,
        "failed_job_samples": failed_samples,
        "queue_progress": _queue_progress_rows(conn, now=now),
        "stale_running_count": stale_count,
        "stale_running_samples": stale_samples,
        "generated_at": now,
    }


def _runtime_loop_summary(conn: sqlite3.Connection, *, now: int) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    attention_states = {"error", "partial", "interrupted", "stale", "stale_running"}
    for spec in _RUNTIME_LOOP_SPECS:
        latest = _latest_runtime_loop_run(conn, spec)
        if not latest:
            rows.append(
                {
                    "loop_key": spec.key,
                    "loop": spec.label,
                    "state": "no_data",
                    "state_label": "无记录",
                    "last_status": "",
                    "age_label": "",
                    "last_at": 0,
                    "rows_written": 0,
                    "error": "",
                }
            )
            continue
        last_status = str(latest.get("status") or "")
        last_at = int(latest.get("finished_at") or latest.get("started_at") or 0)
        age_seconds = max(0, now - last_at) if last_at else 0
        state = _runtime_loop_state(last_status, age_seconds=age_seconds, max_age_seconds=spec.max_age_seconds)
        rows.append(
            {
                "loop_key": spec.key,
                "loop": spec.label,
                "state": state,
                "state_label": _runtime_loop_state_label(state),
                "last_status": last_status,
                "age_label": _age_label(age_seconds) if last_at else "",
                "last_at": last_at,
                "rows_written": int(latest.get("rows_written") or 0),
                "error": _short_error(str(latest.get("error") or "")),
            }
        )
    attention_count = sum(1 for row in rows if row["state"] in attention_states)
    no_data_count = sum(1 for row in rows if row["state"] == "no_data")
    ok_count = sum(1 for row in rows if row["state"] in {"ok", "running"})
    return {
        "state": "attention" if attention_count else "ok",
        "ok_count": ok_count,
        "attention_count": attention_count,
        "no_data_count": no_data_count,
        "total": len(rows),
        "rows": rows,
    }


def _latest_runtime_loop_run(conn: sqlite3.Connection, spec: RuntimeLoopSpec) -> dict[str, Any] | None:
    operator = "LIKE" if "%" in spec.ingest_pattern else "="
    rows = _rows(
        conn,
        f"""
        SELECT ingest_type, status, started_at, finished_at, rows_written, error
        FROM ingest_runs
        WHERE ingest_type {operator} ?
        ORDER BY COALESCE(finished_at, started_at) DESC, run_id DESC
        LIMIT 1
        """,
        (spec.ingest_pattern,),
    )
    return rows[0] if rows else None


def _research_control_step_summary(conn: sqlite3.Connection, *, now: int) -> dict[str, Any]:
    expected = {f"{_RESEARCH_CONTROL_STEP_PREFIX}{key}": (key, label) for key, label in _RESEARCH_CONTROL_STEP_SPECS}
    placeholders = ",".join("?" for _ in expected)
    latest_rows = _rows(
        conn,
        f"""
        WITH ranked AS (
            SELECT
                ingest_type,
                status,
                started_at,
                finished_at,
                rows_written,
                error,
                ROW_NUMBER() OVER (
                    PARTITION BY ingest_type
                    ORDER BY COALESCE(finished_at, started_at) DESC, run_id DESC
                ) AS row_num
            FROM ingest_runs
            WHERE ingest_type IN ({placeholders})
        )
        SELECT ingest_type, status, started_at, finished_at, rows_written, error
        FROM ranked
        WHERE row_num = 1
        """,
        tuple(expected),
    )
    latest_by_type = {str(row["ingest_type"]): row for row in latest_rows}
    rows: list[dict[str, Any]] = []
    attention_states = {"error", "partial", "interrupted", "stale", "stale_running"}
    for step_key, label in _RESEARCH_CONTROL_STEP_SPECS:
        ingest_type = f"{_RESEARCH_CONTROL_STEP_PREFIX}{step_key}"
        latest = latest_by_type.get(ingest_type)
        if not latest:
            rows.append(
                {
                    "step_key": step_key,
                    "step": label,
                    "state": "no_data",
                    "state_label": "无记录",
                    "last_status": "",
                    "duration_label": "",
                    "age_label": "",
                    "last_at": 0,
                    "rows_written": 0,
                    "error": "",
                }
            )
            continue
        status = str(latest.get("status") or "")
        started_at = int(latest.get("started_at") or 0)
        finished_at = int(latest.get("finished_at") or started_at)
        last_at = finished_at or started_at
        age_seconds = max(0, now - last_at) if last_at else 0
        state = _runtime_loop_state(status, age_seconds=age_seconds, max_age_seconds=1_800)
        rows.append(
            {
                "step_key": step_key,
                "step": label,
                "state": state,
                "state_label": _runtime_loop_state_label(state),
                "last_status": status,
                "duration_label": _short_duration_label(max(0, finished_at - started_at)),
                "age_label": _age_label(age_seconds) if last_at else "",
                "last_at": last_at,
                "rows_written": int(latest.get("rows_written") or 0),
                "error": _short_error(str(latest.get("error") or "")),
            }
        )
    attention_count = sum(1 for row in rows if row["state"] in attention_states)
    has_data = any(row["state"] != "no_data" for row in rows)
    return {
        "state": "attention" if attention_count else "ok",
        "has_data": has_data,
        "attention_count": attention_count,
        "rows": rows,
    }


def _runtime_loop_state(last_status: str, *, age_seconds: int, max_age_seconds: int) -> str:
    if last_status == "running":
        return "stale_running" if age_seconds > max_age_seconds else "running"
    if last_status == "failed":
        return "error"
    if age_seconds > max_age_seconds:
        return "stale"
    if last_status == "partial":
        return "partial"
    if last_status == "interrupted":
        return "interrupted"
    return "ok"


def _runtime_loop_state_label(state: str) -> str:
    return {
        "ok": "正常",
        "running": "运行中",
        "partial": "部分完成",
        "interrupted": "被新任务接替",
        "stale": "较久未更新",
        "stale_running": "running 超时",
        "error": "最近失败",
        "no_data": "无记录",
    }.get(state, state)


def _age_label(seconds: int) -> str:
    if seconds < 60:
        return "刚刚"
    return f"{_duration_label(seconds)}前"


def _short_error(value: str) -> str:
    value = value.strip()
    if len(value) <= 120:
        return value
    return value[:117] + "..."


def _address_quality_fast_report(conn: sqlite3.Connection) -> dict[str, Any]:
    table_specs = [
        ("candidate_wallets", "address", "candidate_wallets.address"),
        ("candidate_source_wallet_latest", "address", "candidate_source_events.address"),
        ("observed_wallets", "wallet", "observed_wallets.wallet"),
        ("wallet_processing_state", "wallet", "wallet_processing_state.wallet"),
        ("pipeline_jobs", "wallet", "pipeline_jobs.wallet"),
    ]
    by_table: dict[str, int] = {}
    total = 0
    for table, column, label in table_specs:
        if not _table_exists(conn, table):
            if table == "candidate_source_wallet_latest" and _table_exists(conn, "candidate_source_events"):
                table = "candidate_source_events"
            else:
                continue
        count = _invalid_evm_address_count(conn, table, column)
        by_table[label] = count
        total += count
    return {
        "available": bool(by_table),
        "invalid_address_rows": total,
        "by_table": by_table,
    }


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    )


def _invalid_evm_address_count(conn: sqlite3.Connection, table: str, column: str) -> int:
    valid = (
        f"length(COALESCE({column}, '')) = 42 "
        f"AND substr(COALESCE({column}, ''), 1, 2) = '0x' "
        f"AND lower(substr(COALESCE({column}, ''), 3)) NOT GLOB '*[^0-9a-f]*'"
    )
    return _scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE COALESCE({column}, '') != ''
          AND NOT ({valid})
        """,
    )


def _pending_evidence_backlog_summary(conn: sqlite3.Connection, *, now: int | None = None) -> dict[str, Any]:
    """Summarize evidence states that should have active wallet backfill jobs."""

    now_ts = int(now or time.time())
    by_action = _pending_evidence_backlog_rows(conn, now=now_ts)
    total = sum(int(row.get("count") or 0) for row in by_action)
    high_priority = sum(int(row.get("high_priority_count") or 0) for row in by_action)
    return {
        "pending_without_active_job": total,
        "high_priority_pending_without_active_job": high_priority,
        "high_priority_threshold": HIGH_PRIORITY_PENDING_JOB_PRIORITY,
        "by_action": by_action,
        "high_priority_samples": _pending_evidence_backlog_samples(
            conn,
            priority_ceiling=HIGH_PRIORITY_PENDING_JOB_PRIORITY,
            now=now_ts,
        ),
    }


def _pending_evidence_backlog_rows(conn: sqlite3.Connection, *, now: int) -> list[dict[str, Any]]:
    action_placeholders = ",".join("?" for _ in PENDING_EVIDENCE_ACTIONS)
    active_placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    params: tuple[Any, ...] = (
        HIGH_PRIORITY_PENDING_JOB_PRIORITY,
        now,
        *PENDING_EVIDENCE_ACTIONS,
        *BLOCKING_CANDIDATE_STAGES,
        PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
        *ACTIVE_JOB_STATUSES,
    )
    return _rows(
        conn,
        f"""
        SELECT
            wps.next_action,
            COUNT(*) AS count,
            SUM(CASE WHEN COALESCE(wps.priority, 100) <= ? THEN 1 ELSE 0 END) AS high_priority_count,
            MIN(COALESCE(wps.priority, 100)) AS min_priority,
            MAX(COALESCE(wps.updated_at, 0)) AS latest_updated_at
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        WHERE wps.next_action_at <= ?
          AND wps.next_action IN ({action_placeholders})
          AND wps.evidence_status NOT IN ('paused', 'summary_ready')
          AND cw.candidate_stage NOT IN ({blocking_placeholders})
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs pj
              WHERE pj.job_type = ?
                AND pj.wallet = wps.wallet
                AND pj.subject_key = wps.next_action
                AND pj.status IN ({active_placeholders})
          )
        GROUP BY wps.next_action
        ORDER BY high_priority_count DESC, count DESC, wps.next_action ASC
        """,
        params,
    )


def _pending_evidence_backlog_samples(
    conn: sqlite3.Connection,
    *,
    priority_ceiling: int,
    now: int,
    limit: int = 6,
) -> list[dict[str, Any]]:
    action_placeholders = ",".join("?" for _ in PENDING_EVIDENCE_ACTIONS)
    active_placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
    blocking_placeholders = ",".join("?" for _ in BLOCKING_CANDIDATE_STAGES)
    params: tuple[Any, ...] = (
        *PENDING_EVIDENCE_ACTIONS,
        *BLOCKING_CANDIDATE_STAGES,
        priority_ceiling,
        now,
        PipelineJobType.WALLET_EVIDENCE_BACKFILL.value,
        *ACTIVE_JOB_STATUSES,
        limit,
    )
    return _rows(
        conn,
        f"""
        SELECT
            wps.wallet,
            wps.discovery_tier AS evidence_tier,
            wps.evidence_status,
            wps.next_action,
            COALESCE(wps.priority, 100) AS priority,
            wps.updated_at
        FROM wallet_processing_state wps
        JOIN candidate_wallets cw
          ON cw.address = wps.wallet
        WHERE wps.next_action IN ({action_placeholders})
          AND wps.evidence_status NOT IN ('paused', 'summary_ready')
          AND cw.candidate_stage NOT IN ({blocking_placeholders})
          AND COALESCE(wps.priority, 100) <= ?
          AND wps.next_action_at <= ?
          AND NOT EXISTS (
              SELECT 1
              FROM pipeline_jobs pj
              WHERE pj.job_type = ?
                AND pj.wallet = wps.wallet
                AND pj.subject_key = wps.next_action
                AND pj.status IN ({active_placeholders})
          )
        ORDER BY priority ASC, wps.updated_at ASC, wps.wallet ASC
        LIMIT ?
        """,
        params,
    )


def _top_review_candidate_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 12,
    paper_min_score: float = 70.0,
) -> list[dict[str, Any]]:
    rows = _rows(
        conn,
        """
        WITH copy_job AS (
            SELECT
                pj.wallet,
                pj.status,
                pj.priority,
                COALESCE(
                    json_extract(pj.output_json, '$.graph_scan_mode'),
                    json_extract(pj.input_json, '$.graph_scan_mode'),
                    ''
                ) AS copyability_scan_mode
            FROM pipeline_jobs pj
            JOIN (
                SELECT wallet, MAX(job_id) AS job_id
                FROM pipeline_jobs
                WHERE job_type = 'copyability_evidence'
                GROUP BY wallet
            ) latest_job
              ON latest_job.job_id = pj.job_id
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.current_stage, '') AS evidence_current_stage,
            COALESCE(wps.next_action, '') AS next_action,
            COALESCE(cls.qualified_follower_count, 0) AS qualified_follower_count,
            COALESCE(cls.copy_event_count, 0) AS copy_event_count,
            COALESCE(cls.copy_market_count, 0) AS copy_market_count,
            CASE
                WHEN COALESCE(wf.copy_event_count, 0) > 0 THEN COALESCE(wf.copy_event_count, 0)
                ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0)
            END AS feature_copy_event_count,
            CASE
                WHEN COALESCE(wf.copy_market_count, 0) > 0 THEN COALESCE(wf.copy_market_count, 0)
                ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), 0)
            END AS feature_copy_market_count,
            COALESCE(clp.backtest_trade_count, 0) AS backtest_trade_count,
            COALESCE(clp.net_roi, 0) AS copy_backtest_roi,
            COALESCE(clp.edge_retention_pct, 0) AS edge_retention_pct,
            COALESCE(clp.walk_forward_consistency_pct, 0) AS walk_forward_consistency_pct,
            COALESCE(cj.status, '') AS copyability_status,
            COALESCE(cj.priority, 0) AS copyability_priority,
            COALESCE(cj.copyability_scan_mode, '') AS copyability_scan_mode,
            ls.scored_at
        FROM leader_latest_scores ls
        JOIN candidate_wallets cw
          ON cw.address = ls.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN copy_leader_stats cls
          ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        LEFT JOIN copy_job cj
          ON cj.wallet = cw.address
        WHERE COALESCE(ls.leader_score, 0) > 0
          AND cw.candidate_stage NOT IN ('rejected', 'blocked_hygiene', 'blocked_copyability')
        ORDER BY
            COALESCE(ls.leader_score, 0) DESC,
            COALESCE(cls.qualified_follower_count, 0) DESC,
            COALESCE(wps.activity_count, 0) DESC,
            cw.address ASC
        LIMIT ?
        """,
        (int(limit),),
    )
    for row in rows:
        blocker_key, blocker_label = _top_review_blocker(row, paper_min_score=paper_min_score)
        row["blocker_key"] = blocker_key
        row["blocker_label"] = blocker_label
        row["review_next_action"] = _review_blocker_next_action(blocker_key)
    return rows


def _manual_review_action_rows(conn: sqlite3.Connection, *, paper_min_score: float = 70.0) -> list[dict[str, Any]]:
    rows = _rows(
        conn,
        """
        WITH copy_job AS (
            SELECT
                pj.wallet,
                pj.status,
                pj.priority,
                COALESCE(
                    json_extract(pj.output_json, '$.graph_scan_mode'),
                    json_extract(pj.input_json, '$.graph_scan_mode'),
                    ''
                ) AS copyability_scan_mode
            FROM pipeline_jobs pj
            JOIN (
                SELECT wallet, MAX(job_id) AS job_id
                FROM pipeline_jobs
                WHERE job_type = 'copyability_evidence'
                GROUP BY wallet
            ) latest_job
              ON latest_job.job_id = pj.job_id
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.current_stage, '') AS evidence_current_stage,
            COALESCE(wps.next_action, '') AS next_action,
            COALESCE(cls.qualified_follower_count, 0) AS qualified_follower_count,
            COALESCE(cls.copy_event_count, 0) AS copy_event_count,
            COALESCE(cls.copy_market_count, 0) AS copy_market_count,
            CASE
                WHEN COALESCE(wf.copy_event_count, 0) > 0 THEN COALESCE(wf.copy_event_count, 0)
                ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0)
            END AS feature_copy_event_count,
            CASE
                WHEN COALESCE(wf.copy_market_count, 0) > 0 THEN COALESCE(wf.copy_market_count, 0)
                ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), 0)
            END AS feature_copy_market_count,
            COALESCE(clp.backtest_trade_count, 0) AS backtest_trade_count,
            COALESCE(clp.edge_retention_pct, 0) AS edge_retention_pct,
            COALESCE(clp.walk_forward_consistency_pct, 0) AS walk_forward_consistency_pct,
            COALESCE(cj.status, '') AS copyability_status,
            COALESCE(cj.priority, 0) AS copyability_priority,
            COALESCE(cj.copyability_scan_mode, '') AS copyability_scan_mode
        FROM candidate_wallets cw
        LEFT JOIN leader_latest_scores ls
          ON ls.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN copy_leader_stats cls
          ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        LEFT JOIN copy_job cj
          ON cj.wallet = cw.address
        WHERE cw.candidate_stage = 'needs_manual_review'
        ORDER BY COALESCE(ls.leader_score, 0) DESC, COALESCE(wps.activity_count, 0) DESC, cw.address ASC
        """,
    )
    for row in rows:
        blocker_key, blocker_label = _top_review_blocker(row, paper_min_score=paper_min_score)
        row["blocker_key"] = blocker_key
        row["blocker_label"] = blocker_label
        row["review_next_action"] = _review_blocker_next_action(blocker_key)
    return _top_review_blocker_rows(rows)


def _paper_pool_expansion_audit(
    conn: sqlite3.Connection,
    *,
    paper_min_score: float = 70.0,
    watch_min_score: float = DEFAULT_RTDS_WATCH_MIN_SCORE,
    min_copy_events: int = 5,
    min_copy_markets: int = 5,
    limit: int = 50,
) -> dict[str, Any]:
    rows = _paper_pool_expansion_rows(
        conn,
        paper_min_score=paper_min_score,
        watch_min_score=watch_min_score,
        min_copy_events=min_copy_events,
        min_copy_markets=min_copy_markets,
        limit=limit,
    )
    state_counts: dict[str, int] = {}
    for row in rows:
        state = str(row.get("expansion_state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
    near_paper = [row for row in rows if str(row.get("expansion_state") or "").startswith("near_paper")]
    needs_copy = [
        row for row in rows
        if str(row.get("expansion_state") or "") in {
            "near_paper_waiting_copy_events",
            "near_paper_waiting_copy_markets",
            "watchlist_needs_copyability",
            "missing_copyability_signal",
        }
    ]
    return {
        "schema_version": "paper_pool_expansion_v1",
        "generated_at": int(time.time()),
        "scope": {
            "candidate_stage": "needs_manual_review",
            "paper_min_score": float(paper_min_score),
            "watch_min_score": float(watch_min_score),
            "min_copy_events": int(min_copy_events),
            "min_copy_markets": int(min_copy_markets),
            "limit": int(limit),
        },
        "wallet_count": len(rows),
        "near_paper_count": len(near_paper),
        "copyability_needed_count": len(needs_copy),
        "best_score": _round_value(max((float(row.get("leader_score") or 0) for row in rows), default=0)),
        "state_counts": [
            {"state": key, "count": state_counts[key]}
            for key in sorted(state_counts, key=lambda item: (-state_counts[item], item))
        ],
        "wallets": rows,
        "write_boundary": "paper pool expansion audit is read-only; it does not promote wallets or start execution",
    }


def _paper_pool_expansion_rows(
    conn: sqlite3.Connection,
    *,
    paper_min_score: float,
    watch_min_score: float,
    min_copy_events: int,
    min_copy_markets: int,
    limit: int,
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "candidate_wallets") or not _table_exists(conn, "leader_latest_scores"):
        return []
    rows = _rows(
        conn,
        """
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.current_stage, '') AS evidence_current_stage,
            COALESCE(wps.next_action, '') AS evidence_next_action,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            COALESCE(wf.hygiene_status, '') AS hygiene_status,
            ROUND(COALESCE(wf.net_pnl_usdc, 0), 2) AS net_pnl_usdc,
            ROUND(COALESCE(wf.total_volume_usdc, 0), 2) AS total_volume_usdc,
            CASE
                WHEN COALESCE(wf.copy_event_count, 0) > 0 THEN COALESCE(wf.copy_event_count, 0)
                ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_event_count'), 0)
            END AS feature_copy_event_count,
            CASE
                WHEN COALESCE(wf.copy_market_count, 0) > 0 THEN COALESCE(wf.copy_market_count, 0)
                ELSE COALESCE(json_extract(wf.extra_json, '$.copy_candidate_market_count'), 0)
            END AS feature_copy_market_count,
            COALESCE(cls.qualified_follower_count, 0) AS qualified_follower_count,
            COALESCE(cls.copy_event_count, 0) AS copy_event_count,
            COALESCE(cls.copy_market_count, 0) AS copy_market_count,
            COALESCE(clp.backtest_trade_count, 0) AS backtest_trade_count,
            ROUND(COALESCE(clp.net_roi, 0), 4) AS copy_backtest_roi,
            ROUND(COALESCE(clp.edge_retention_pct, 0), 2) AS edge_retention_pct,
            ls.scored_at
        FROM candidate_wallets cw
        JOIN leader_latest_scores ls
          ON ls.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN copy_leader_stats cls
          ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        WHERE cw.candidate_stage = 'needs_manual_review'
          AND COALESCE(ls.leader_score, 0) > 0
        ORDER BY COALESCE(ls.leader_score, 0) DESC, COALESCE(wps.activity_count, 0) DESC, cw.address ASC
        LIMIT ?
        """,
        (max(1, min(int(limit), 250)),),
    )
    audited = []
    for row in rows:
        item = dict(row)
        score = float(item.get("leader_score") or 0)
        feature_copy_events = _int_value(item.get("feature_copy_event_count"))
        feature_copy_markets = _int_value(item.get("feature_copy_market_count"))
        graph_copy_events = _int_value(item.get("copy_event_count"))
        graph_copy_markets = _int_value(item.get("copy_market_count"))
        copy_events = max(feature_copy_events, graph_copy_events)
        copy_markets = max(feature_copy_markets, graph_copy_markets)
        item["leader_score"] = _round_value(score)
        item["score_gap_to_paper"] = _round_value(max(0.0, float(paper_min_score) - score))
        item["score_gap_to_watch"] = _round_value(max(0.0, float(watch_min_score) - score))
        item["copy_signal_events"] = copy_events
        item["copy_signal_markets"] = copy_markets
        item["copy_event_gap"] = max(0, int(min_copy_events) - copy_events)
        item["copy_market_gap"] = max(0, int(min_copy_markets) - copy_markets)
        for key in (
            "activity_count",
            "distinct_markets",
            "non_fast_trade_count",
            "qualified_follower_count",
            "backtest_trade_count",
        ):
            item[key] = _int_value(item.get(key))
        item["copy_backtest_roi"] = _round_value(item.get("copy_backtest_roi"), digits=4)
        item["edge_retention_pct"] = _round_value(item.get("edge_retention_pct"))
        item["net_pnl_usdc"] = _round_value(item.get("net_pnl_usdc"))
        item["total_volume_usdc"] = _round_value(item.get("total_volume_usdc"))
        state, action = _paper_pool_expansion_state(
            item,
            paper_min_score=paper_min_score,
            watch_min_score=watch_min_score,
            min_copy_events=min_copy_events,
            min_copy_markets=min_copy_markets,
        )
        item["expansion_state"] = state
        item["next_action"] = action
        audited.append(item)
    return audited


def _paper_pool_expansion_state(
    row: dict[str, Any],
    *,
    paper_min_score: float,
    watch_min_score: float,
    min_copy_events: int,
    min_copy_markets: int,
) -> tuple[str, str]:
    score = float(row.get("leader_score") or 0)
    copy_events = _int_value(row.get("copy_signal_events"))
    copy_markets = _int_value(row.get("copy_signal_markets"))
    activity = _int_value(row.get("activity_count"))
    evidence_status = str(row.get("evidence_status") or "")
    if activity < 200 or evidence_status != "summary_ready":
        return "needs_more_evidence", "历史证据还不够厚；先让 L1/L2/L3 管道补完再判断。"
    if score >= float(watch_min_score):
        if copy_events < int(min_copy_events):
            return "near_paper_waiting_copy_events", "离 paper 最近；继续补足可验证的 copyability 事件。"
        if copy_markets < int(min_copy_markets):
            return "near_paper_waiting_copy_markets", "离 paper 最近；继续补足跨市场 copyability 证据。"
        if score < float(paper_min_score):
            return "near_paper_score_gap", "分数只差一点；等待更多实时可跟证据或下一轮评分。"
        return "near_paper_manual_gate", "证据接近 paper；需要复核阻塞原因后再考虑升级。"
    if score >= 50:
        if copy_events < int(min_copy_events) or copy_markets < int(min_copy_markets):
            return "watchlist_needs_copyability", "分数可观察但缺 copyability；优先做 copyability 深扫或等待实时跟随证据。"
        return "watchlist_score_gap", "已有部分证据但分数离 paper 仍有距离；继续观察近期表现。"
    if copy_events < int(min_copy_events) or copy_markets < int(min_copy_markets):
        return "missing_copyability_signal", "历史证据已够但缺可跟随信号；放入 copyability/RTDS 观察。"
    return "score_gap_large", "分数离 paper 较远；暂不扩池，保留观察。"


def _paper_candidate_thresholds(settings: RobotSettings) -> dict[str, Any]:
    try:
        policy = load_policy(settings.policy_path)
        return {
            "min_score": float((policy.get("review_bands") or {}).get("paper_candidate", 70)),
            "min_copy_events": int(threshold(policy, "min_copy_events", 5)),
            "min_copy_markets": int(threshold(policy, "min_copy_markets", 5)),
            "policy_loaded": True,
            "policy_error": "",
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return {
            "min_score": 70.0,
            "min_copy_events": 5,
            "min_copy_markets": 5,
            "policy_loaded": False,
            "policy_error": f"{type(exc).__name__}: {str(exc)[:160]}",
        }


def _paper_candidate_min_score(settings: RobotSettings) -> float:
    return float(_paper_candidate_thresholds(settings)["min_score"])


def _top_review_blocker(row: dict[str, Any], *, paper_min_score: float) -> tuple[str, str]:
    stage = str(row.get("candidate_stage") or "")
    score = float(row.get("leader_score") or 0)
    activity = int(row.get("activity_count") or 0)
    copy_status = str(row.get("copyability_status") or "")
    has_copy_signal = _has_copyability_signal(row)
    has_copy_validation = _has_copyability_validation(row)
    if stage in {"paper_candidate", "paper_approved", "live_eligible"}:
        return ("paper_ready", "已进入 paper")
    if stage == "blocked_hygiene":
        return ("hygiene_blocked", "hygiene 阻断")
    if stage == "blocked_copyability":
        return ("copyability_blocked", "copyability 阻断")
    if activity < 200:
        return ("thin_evidence", "历史证据偏薄")
    if not has_copy_signal:
        if copy_status in {"queued", "running"}:
            return ("copyability_pending", "copyability 补证据中")
        if copy_status == "done":
            if _is_light_copyability_scan(row):
                return ("copyability_light_no_signal", "copyability 轻扫无信号")
            return ("copyability_no_signal", "copyability 无跟随信号")
        return ("missing_copyability", "缺 copyability 证据")
    if not has_copy_validation:
        if copy_status in {"queued", "running"}:
            return ("copyability_pending", "copyability 验证补证据中")
        return ("copyability_unvalidated", "copyability 线索未通过验证")
    if score < paper_min_score:
        return ("score_below_paper", f"分数未达 {paper_min_score:.0f}")
    if _is_paper_evidence_incomplete_review(row, paper_min_score=paper_min_score):
        return ("paper_evidence_incomplete", "L3 证据未完成")
    if stage == "needs_manual_review":
        return ("manual_review", "复核停靠状态")
    return ("unknown", "待确认")


def _top_review_blocker_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("blocker_key") or "unknown")
        label = str(row.get("blocker_label") or "待确认")
        score = float(row.get("leader_score") or 0)
        group = groups.setdefault(
            key,
            {
                "key": key,
                "blocker": label,
                "count": 0,
                "max_score": score,
                "next_action": _review_blocker_next_action(key),
                "example": row.get("address") or "",
            },
        )
        group["count"] = int(group["count"]) + 1
        if score > float(group.get("max_score") or 0):
            group["max_score"] = score
            group["example"] = row.get("address") or ""
    return sorted(groups.values(), key=lambda item: (-int(item.get("count") or 0), -float(item.get("max_score") or 0), str(item.get("blocker") or "")))


def _is_paper_evidence_incomplete_review(row: dict[str, Any], *, paper_min_score: float) -> bool:
    """Classify high-score review wallets whose research evidence is not L3-ready."""

    if str(row.get("candidate_stage") or "") != "needs_manual_review":
        return False
    if float(row.get("leader_score") or 0) < paper_min_score:
        return False
    if paper_evidence_ready(row):
        return False
    return str(row.get("review_reason") or "") == "paper_evidence_tier_incomplete" or bool(row.get("evidence_tier") or row.get("evidence_status"))


def _review_blocker_next_action(key: str) -> str:
    actions = {
        "paper_ready": "进入外部 paper 验证或发布前复核",
        "paper_evidence_incomplete": "保持复核；L3 未达 summary_ready，不进入 paper",
        "hygiene_blocked": "保持阻断；只在风险证据修正后复核",
        "copyability_blocked": "保持阻断；只在新增 copyability 证据后复核",
        "rejected": "保持拒绝；不进入自动队列",
        "copyability_pending": "等待 copyability 队列完成后重评",
        "copyability_no_signal": "深扫仍无信号，可自动降级或保持阻断",
        "copyability_light_no_signal": "仅轻扫无信号；高分再排 deep，低分保留观察",
        "copyability_unvalidated": "复核 follower/backtest 证据质量",
        "missing_copyability": "加入 copyability 证据队列",
        "score_below_paper": "保留观察；等待新证据提高评分",
        "thin_evidence": "继续补历史，样本不足不放行",
        "history_pending": "等待 L1/L2/L3 补证据任务完成",
        "manual_review": "检查 review_reason，改写为明确阻断原因",
    }
    return actions.get(key, "继续观察或人工复核")


def _paper_handoff_summary(conn: sqlite3.Connection, settings: RobotSettings, *, limit: int = 10) -> dict[str, Any]:
    paper_min_score = _paper_candidate_min_score(settings)
    observer_current_cutoff = _paper_observer_current_cutoff()
    research_only = settings.execution_mode in {"research", "scoring", "research_scoring"}
    rows = _paper_handoff_rows(
        conn,
        limit=limit,
        paper_min_score=paper_min_score,
        observer_current_cutoff=observer_current_cutoff,
        research_only=research_only,
    )
    stage_counts = _paper_handoff_stage_counts(conn)
    state_counts = _paper_handoff_state_counts(conn, observer_current_cutoff=observer_current_cutoff)
    candidate_count = sum(int(row.get("count") or 0) for row in stage_counts)
    visible_research_ready = sum(1 for row in rows if row.get("research_ready"))
    incomplete_research_wallets = [
        {
            "address": row.get("address") or "",
            "candidate_stage": row.get("candidate_stage") or "",
            "leader_score": row.get("leader_score") or 0,
            "missing": row.get("research_check_summary") or "",
        }
        for row in rows
        if not row.get("research_ready")
    ]
    if research_only:
        boundary = "NAS research/scoring：这里只是研究批准和交接清单，paper 标签不代表 NAS 已自动跟单。"
        next_action = "保留在交接清单；只有外部或独立 paper runner 接手后，才会产生纸面成交记录。"
        execution_boundary = "research_handoff_only"
        paper_loop_status = "not_started_in_nas_research_stack"
    else:
        boundary = f"当前模式 {settings.execution_mode}：本面板仍只表达交接状态，是否运行 paper loop 以服务进程为准。"
        next_action = "检查 paper-run / settle 常驻循环，再用 paper 质量决定是否发布。"
        execution_boundary = "runtime_service_dependent"
        paper_loop_status = "not_verified_by_web_console"
    return {
        "schema_version": PAPER_HANDOFF_SCHEMA_VERSION,
        "generated_at": int(time.time()),
        "runtime_mode": settings.execution_mode,
        "research_only": research_only,
        "execution_boundary": execution_boundary,
        "nas_paper_loop_enabled": False if research_only else None,
        "paper_loop_status": paper_loop_status,
        "boundary": boundary,
        "next_action": next_action,
        "candidate_count": candidate_count,
        "visible_wallet_count": len(rows),
        "paper_min_score": paper_min_score,
        "observer_current_window_sec": PAPER_OBSERVER_CURRENT_EVALUATION_MAX_AGE_SEC,
        "visible_research_ready": visible_research_ready,
        "visible_research_incomplete": len(incomplete_research_wallets),
        "incomplete_research_wallets": incomplete_research_wallets,
        "stage_counts": stage_counts,
        "state_counts": state_counts,
        "wallets": rows,
    }


def _paper_handoff_csv(summary: dict[str, Any]) -> str:
    """Flatten paper handoff wallets into a spreadsheet-friendly CSV."""

    fields = [
        "generated_at",
        "runtime_mode",
        "execution_boundary",
        "paper_min_score",
        "address",
        "candidate_stage",
        "handoff_state",
        "leader_score",
        "review_reason",
        "research_ready",
        "research_check_summary",
        "next_action",
        "evidence_tier",
        "evidence_status",
        "evidence_current_stage",
        "activity_count",
        "distinct_markets",
        "non_fast_trade_count",
        "hygiene_status",
        "qualified_follower_count",
        "copy_event_count",
        "copy_market_count",
        "backtest_trade_count",
        "copy_backtest_roi",
        "edge_retention_pct",
        "walk_forward_consistency_pct",
        "observer_evaluations",
        "observer_accepted_signals",
        "observer_actionable_signals",
        "observer_stale_signals",
        "observer_quote_errors",
        "observer_latest_evaluated_at",
        "observer_quality_state",
        "observer_quality_window_sec",
        "observer_quality_evaluations",
        "observer_quality_accepted",
        "observer_quality_actionable",
        "observer_quality_accepted_rate_pct",
        "observer_quality_actionable_rate_pct",
        "observer_quality_quote_errors",
        "observer_quality_stale",
        "observer_quality_avg_signal_age_sec",
        "observer_quality_max_signal_age_sec",
        "observer_quality_avg_slippage_bps",
        "observer_quality_latest_at",
        "observer_quality_next_action",
        "paper_orders",
        "paper_execution_state",
        "publish_status",
        "formal_blockers",
        "formal_next_action",
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    base = {
        "generated_at": summary.get("generated_at") or "",
        "runtime_mode": summary.get("runtime_mode") or "",
        "execution_boundary": summary.get("execution_boundary") or "",
        "paper_min_score": summary.get("paper_min_score") or "",
    }
    for wallet in summary.get("wallets") or []:
        writer.writerow({**base, **wallet})
    return out.getvalue()


def _paper_handoff_stage_counts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        SELECT candidate_stage AS stage, COUNT(*) AS count
        FROM candidate_wallets
        WHERE candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        GROUP BY candidate_stage
        ORDER BY
            CASE candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END
        """,
    )


def _paper_observer_current_cutoff(now: int | None = None) -> int:
    """Return the minimum evaluation time that may drive current handoff state."""

    current = int(time.time()) if now is None else int(now)
    return max(0, current - PAPER_OBSERVER_CURRENT_EVALUATION_MAX_AGE_SEC)


def _paper_handoff_state_counts(conn: sqlite3.Connection, *, observer_current_cutoff: int) -> list[dict[str, Any]]:
    return _rows(
        conn,
        """
        WITH observer AS (
            SELECT
                wallet,
                SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS actionable_signals
            FROM paper_signal_evaluations
            WHERE evaluated_at >= ?
            GROUP BY wallet
        ),
        handoff AS (
            SELECT
                CASE
                    WHEN cw.candidate_stage = 'live_eligible' AND COALESCE(lp.status, '') = 'active' THEN 'published'
                    WHEN COALESCE(pq.production_ready, 0) > 0 THEN 'paper_passed'
                    WHEN COALESCE(pq.orders, 0) > 0 THEN 'paper_observing'
                    WHEN cw.candidate_stage = 'paper_approved' AND COALESCE(observer.actionable_signals, 0) = 0 THEN 'awaiting_actionable_signal'
                    WHEN cw.candidate_stage = 'paper_approved' THEN 'awaiting_external_paper'
                    WHEN cw.candidate_stage = 'paper_candidate' THEN 'awaiting_pre_paper_review'
                    ELSE 'research_ready'
                END AS handoff_state
            FROM candidate_wallets cw
            LEFT JOIN paper_wallet_quality pq
              ON pq.wallet = cw.address
            LEFT JOIN leader_publish lp
              ON lp.wallet = cw.address
            LEFT JOIN observer
              ON observer.wallet = cw.address
            WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        )
        SELECT handoff_state AS state, COUNT(*) AS count
        FROM handoff
        GROUP BY handoff_state
        ORDER BY count DESC, state ASC
        """,
        (observer_current_cutoff,),
    )


def _paper_handoff_rows(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
    paper_min_score: float = 70.0,
    address: str = "",
    observer_current_cutoff: int | None = None,
    research_only: bool = True,
) -> list[dict[str, Any]]:
    address = address.lower()
    current_cutoff = _paper_observer_current_cutoff() if observer_current_cutoff is None else int(observer_current_cutoff)
    quality_cutoff = max(0, int(time.time()) - PAPER_OBSERVER_QUALITY_LOOKBACK_SEC)
    address_clause = "AND cw.address = ?" if address else ""
    params: tuple[Any, ...] = (
        (current_cutoff, quality_cutoff, address, int(limit))
        if address
        else (current_cutoff, quality_cutoff, int(limit))
    )
    rows = _rows(
        conn,
        f"""
        WITH observer AS (
            SELECT
                wallet,
                COUNT(*) AS observer_evaluations,
                SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS observer_accepted_signals,
                SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS observer_actionable_signals,
                SUM(CASE WHEN actionability_reason = 'signal_too_old' THEN 1 ELSE 0 END) AS observer_stale_signals,
                SUM(CASE WHEN quote_error != '' THEN 1 ELSE 0 END) AS observer_quote_errors,
                MAX(evaluated_at) AS observer_latest_evaluated_at
            FROM paper_signal_evaluations
            WHERE evaluated_at >= ?
            GROUP BY wallet
        ),
        observer_quality AS (
            SELECT
                wallet,
                COUNT(*) AS observer_quality_evaluations,
                SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS observer_quality_accepted,
                SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS observer_quality_actionable,
                SUM(CASE WHEN actionability_reason = 'signal_too_old' THEN 1 ELSE 0 END) AS observer_quality_stale,
                SUM(CASE WHEN COALESCE(quote_error, '') != '' THEN 1 ELSE 0 END) AS observer_quality_quote_errors,
                AVG(signal_age_sec) AS observer_quality_avg_signal_age_sec,
                MAX(signal_age_sec) AS observer_quality_max_signal_age_sec,
                AVG(CASE WHEN accepted = 1 THEN slippage_bps ELSE NULL END) AS observer_quality_avg_slippage_bps,
                MAX(evaluated_at) AS observer_quality_latest_at
            FROM paper_signal_evaluations
            WHERE evaluated_at >= ?
            GROUP BY wallet
        )
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.current_stage, '') AS evidence_current_stage,
            COALESCE(wf.net_pnl_usdc, 0) AS net_pnl_usdc,
            COALESCE(wf.total_volume_usdc, 0) AS total_volume_usdc,
            COALESCE(wf.hygiene_status, '') AS hygiene_status,
            COALESCE(cls.qualified_follower_count, 0) AS qualified_follower_count,
            COALESCE(cls.copy_event_count, 0) AS copy_event_count,
            COALESCE(cls.copy_market_count, 0) AS copy_market_count,
            COALESCE(clp.backtest_trade_count, 0) AS backtest_trade_count,
            COALESCE(clp.net_roi, 0) AS copy_backtest_roi,
            COALESCE(clp.edge_retention_pct, 0) AS edge_retention_pct,
            COALESCE(clp.walk_forward_consistency_pct, 0) AS walk_forward_consistency_pct,
            COALESCE(observer.observer_evaluations, 0) AS observer_evaluations,
            COALESCE(observer.observer_accepted_signals, 0) AS observer_accepted_signals,
            COALESCE(observer.observer_actionable_signals, 0) AS observer_actionable_signals,
            COALESCE(observer.observer_stale_signals, 0) AS observer_stale_signals,
            COALESCE(observer.observer_quote_errors, 0) AS observer_quote_errors,
            COALESCE(observer.observer_latest_evaluated_at, 0) AS observer_latest_evaluated_at,
            COALESCE(oq.observer_quality_evaluations, 0) AS observer_quality_evaluations,
            COALESCE(oq.observer_quality_accepted, 0) AS observer_quality_accepted,
            COALESCE(oq.observer_quality_actionable, 0) AS observer_quality_actionable,
            COALESCE(oq.observer_quality_stale, 0) AS observer_quality_stale,
            COALESCE(oq.observer_quality_quote_errors, 0) AS observer_quality_quote_errors,
            COALESCE(oq.observer_quality_avg_signal_age_sec, 0) AS observer_quality_avg_signal_age_sec,
            COALESCE(oq.observer_quality_max_signal_age_sec, 0) AS observer_quality_max_signal_age_sec,
            COALESCE(oq.observer_quality_avg_slippage_bps, 0) AS observer_quality_avg_slippage_bps,
            COALESCE(oq.observer_quality_latest_at, 0) AS observer_quality_latest_at,
            CASE WHEN pq.wallet IS NULL THEN 0 ELSE 1 END AS paper_quality_present,
            COALESCE(pq.orders, 0) AS paper_orders,
            COALESCE(pq.settled_positions, 0) AS paper_settled_positions,
            COALESCE(pq.total_roi, 0) AS paper_total_roi,
            COALESCE(pq.production_ready, 0) AS paper_ready,
            COALESCE(lp.status, '') AS publish_status,
            ls.scored_at
        FROM candidate_wallets cw
        LEFT JOIN leader_latest_scores ls
          ON ls.address = cw.address
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = cw.address
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN copy_leader_stats cls
          ON cls.leader_wallet = cw.address
        LEFT JOIN copy_leader_performance clp
          ON clp.leader_wallet = cw.address
        LEFT JOIN paper_wallet_quality pq
          ON pq.wallet = cw.address
        LEFT JOIN leader_publish lp
          ON lp.wallet = cw.address
        LEFT JOIN observer
          ON observer.wallet = cw.address
        LEFT JOIN observer_quality oq
          ON oq.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
          {address_clause}
        ORDER BY
            CASE cw.candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END,
            COALESCE(pq.production_ready, 0) DESC,
            COALESCE(ls.leader_score, 0) DESC,
            cw.updated_at DESC
        LIMIT ?
        """,
        params,
    )
    for row in rows:
        state, action = _paper_handoff_state(row)
        row["handoff_state"] = state
        row["next_action"] = action
        checks = _paper_handoff_research_checks(row, paper_min_score=paper_min_score)
        row["research_checks"] = checks
        row["research_check_passed"] = sum(1 for check in checks if check["passed"])
        row["research_check_total"] = len(checks)
        row["research_ready"] = all(check["passed"] for check in checks)
        row["research_check_summary"] = _paper_handoff_check_summary(checks)
        row["paper_execution_state"] = "paper_started" if int(row.get("paper_orders") or 0) > 0 else "not_started_on_nas"
        blockers, formal_action = _paper_handoff_formal_blockers(row, research_only=research_only)
        row["formal_blocker_list"] = blockers
        row["formal_blockers"] = " | ".join(blockers)
        row["formal_next_action"] = formal_action
        _paper_handoff_observer_quality(row)
    return rows


def _paper_handoff_observer_quality(row: dict[str, Any]) -> None:
    """Summarize read-only observer history without changing handoff state."""

    total = int(row.get("observer_quality_evaluations") or 0)
    accepted = int(row.get("observer_quality_accepted") or 0)
    actionable = int(row.get("observer_quality_actionable") or 0)
    stale = int(row.get("observer_quality_stale") or 0)
    quote_errors = int(row.get("observer_quality_quote_errors") or 0)
    row["observer_quality_window_sec"] = PAPER_OBSERVER_QUALITY_LOOKBACK_SEC
    row["observer_quality_accepted_rate_pct"] = round(100.0 * accepted / total, 2) if total else 0.0
    row["observer_quality_actionable_rate_pct"] = round(100.0 * actionable / total, 2) if total else 0.0
    row["observer_quality_avg_signal_age_sec"] = round(float(row.get("observer_quality_avg_signal_age_sec") or 0), 2)
    row["observer_quality_max_signal_age_sec"] = int(row.get("observer_quality_max_signal_age_sec") or 0)
    if total <= 0:
        state = "no_observer_quality"
        action = "等待 paper observer 捕捉这个钱包的新 BUY 信号。"
    elif actionable > 0:
        state = "actionable_seen"
        action = "已出现可及时跟信号；下一步需要独立 paper runner 生成纸面订单和结算质量。"
    elif accepted > 0 and stale >= accepted:
        state = "accepted_but_stale"
        action = "报价可成交但信号过时；需要更及时的发现/观察链路。"
    elif quote_errors >= total:
        state = "quote_unavailable"
        action = "评估都卡在盘口报价；先检查该市场流动性或 CLOB 报价通道。"
    elif quote_errors > 0:
        state = "quote_fragile"
        action = "部分信号报价失败；继续观察并优先修复报价稳定性。"
    else:
        state = "no_actionable_quote"
        action = "已有观察评估，但暂未形成可及时跟的盘口信号。"
    row["observer_quality_state"] = state
    row["observer_quality_next_action"] = action


def _paper_handoff_research_checks(row: dict[str, Any], *, paper_min_score: float) -> list[dict[str, Any]]:
    score = float(row.get("leader_score") or 0)
    evidence_tier = str(row.get("evidence_tier") or "")
    evidence_status = str(row.get("evidence_status") or "")
    evidence_current_stage = str(row.get("evidence_current_stage") or "")
    hygiene_status = str(row.get("hygiene_status") or "")
    followers = int(row.get("qualified_follower_count") or 0)
    backtest_trades = int(row.get("backtest_trade_count") or 0)
    edge_retention = float(row.get("edge_retention_pct") or 0)
    walk_forward = float(row.get("walk_forward_consistency_pct") or 0)
    return [
        {
            "key": "score_ready",
            "label": "分数达标",
            "passed": score >= paper_min_score,
            "value": f"{score:.2f}/{paper_min_score:.0f}",
        },
        {
            "key": "l3_summary",
            "label": "深证据完成",
            "passed": paper_evidence_ready(row),
            "value": f"{evidence_tier or 'none'}:{evidence_status or 'none'}:{evidence_current_stage or 'none'}",
        },
        {
            "key": "hygiene_clean",
            "label": "Hygiene 低风险",
            "passed": hygiene_status in {"ok", "clean", "low_risk"},
            "value": hygiene_status or "unknown",
        },
        {
            "key": "copyability_validated",
            "label": "Copyability 已验证",
            "passed": followers > 0 or backtest_trades > 0 or edge_retention > 0 or walk_forward > 0,
            "value": f"{followers} followers / {backtest_trades} backtest",
        },
    ]


def _paper_handoff_check_summary(checks: list[dict[str, Any]]) -> str:
    missing = [str(check.get("label") or check.get("key") or "") for check in checks if not check.get("passed")]
    if not missing:
        return "研究证据完整"
    return "缺 " + "、".join(missing)


def _paper_handoff_formal_blockers(row: dict[str, Any], *, research_only: bool) -> tuple[list[str], str]:
    blockers: list[str] = []
    stage = str(row.get("candidate_stage") or "")
    publish_status = str(row.get("publish_status") or "")
    paper_quality_present = int(row.get("paper_quality_present") or 0) > 0
    paper_orders = int(row.get("paper_orders") or 0)
    settled_positions = int(row.get("paper_settled_positions") or 0)
    paper_ready = int(row.get("paper_ready") or 0) > 0

    if research_only:
        blockers.append("runtime_research_only")
    if stage != "live_eligible":
        blockers.append(f"stage_not_live_eligible:{stage or 'missing'}")
    if not paper_evidence_ready(row):
        blockers.append("paper_evidence_tier_incomplete")
    if not paper_quality_present:
        blockers.append("missing_paper_wallet_quality")
    elif not paper_ready:
        blockers.append("paper_quality_not_production_ready")
    if paper_orders <= 0:
        blockers.append("no_paper_orders")
    if settled_positions <= 0:
        blockers.append("no_settled_positions")
    if publish_status != "active":
        blockers.append(f"publish_not_active:{publish_status or 'missing'}")
    if not blockers:
        return [], "已满足当前正式发布观察条件，继续监控撤销和过期。"

    priority_actions = {
        "runtime_research_only": "当前 NAS 只做 research/scoring；正式化需要独立 paper/settle/publish 运行面。",
        "paper_evidence_tier_incomplete": "先完成 L3 深度证据，再进入 paper 和正式发布链路。",
        "missing_paper_wallet_quality": "等待及时信号后生成 paper_orders，并通过 settle 形成 paper_wallet_quality。",
        "paper_quality_not_production_ready": "继续累计纸面订单、结算和风险观察，直到 production_ready。",
        "no_paper_orders": "先让 paper observer 捕捉可及时跟的 BUY 信号，再进入纸面订单验证。",
        "no_settled_positions": "等待纸面仓位结算，形成 ROI、回撤和市场集中度证据。",
    }
    for blocker in blockers:
        if blocker in priority_actions:
            return blockers, priority_actions[blocker]
    if any(blocker.startswith("stage_not_live_eligible") for blocker in blockers):
        return blockers, "先通过稳定纸面质量把候选升级到 live_eligible。"
    if any(blocker.startswith("publish_not_active") for blocker in blockers):
        return blockers, "满足前置门槛后再运行 publish-leaders。"
    return blockers, "继续补正式发布前置证据。"


def _paper_handoff_state(row: dict[str, Any]) -> tuple[str, str]:
    stage = str(row.get("candidate_stage") or "")
    publish_status = str(row.get("publish_status") or "")
    paper_orders = int(row.get("paper_orders") or 0)
    paper_ready = int(row.get("paper_ready") or 0) > 0
    observer_evaluations = int(row.get("observer_evaluations") or 0)
    observer_actionable = int(row.get("observer_actionable_signals") or 0)
    if stage == "live_eligible" and publish_status == "active":
        return ("published", "已发布给外部执行系统，继续监控撤销条件。")
    if paper_ready:
        return ("paper_passed", "paper 质量已达发布前条件，进入发布前复核。")
    if paper_orders > 0:
        return ("paper_observing", "已有 paper 成交，继续等待结算和质量指标。")
    if stage == "paper_approved" and observer_actionable <= 0:
        if observer_evaluations > 0:
            return ("awaiting_actionable_signal", "最近信号缺盘口或已过可跟窗口；继续等待新的及时 BUY 信号，不进入发布。")
        return ("awaiting_actionable_signal", "研究侧已批准；等待 paper observer 捕捉可及时跟的 BUY 信号。")
    if stage == "paper_approved":
        return ("awaiting_external_paper", "研究侧已批准；NAS research/scoring 不会自动下 paper 单，等待外部观察接手。")
    if stage == "paper_candidate":
        return ("awaiting_pre_paper_review", "候选已过评分线；NAS 只展示交接，不自动 paper。")
    return ("research_ready", "研究侧已给出候选，等待下游验证。")


def _paper_observer_evaluation_file(settings: RobotSettings) -> dict[str, Any]:
    path = _report_file_path(settings, "paper_observer_evaluation.json")
    base = {
        "schema_version": PAPER_OBSERVER_EVALUATION_SCHEMA_VERSION,
        "state": "missing",
        "json_path": str(path),
        "json_exists": path.exists(),
        "read_only": True,
        "write_scope": "no_orders",
        "boundary": "只读盘口评估：查 CLOB 深度、滑点和延迟，不写 paper_orders。",
        "next_action": "等待 paper-observer-loop 生成 paper_observer_evaluation.json。",
    }
    if not path.exists():
        return base
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            **base,
            "json_exists": True,
            "state": "invalid",
            "error": f"{type(exc).__name__}: {str(exc)[:180]}",
            "next_action": "评估文件无法读取，重启 paper-observer-loop 重新生成。",
        }
    generated_at = int(payload.get("generated_at") or 0)
    mtime = int(path.stat().st_mtime)
    age_seconds = max(0, int(time.time()) - (generated_at or mtime))
    state = "stale" if age_seconds > 3600 else "current"
    next_action = (
        "报价评估已生成；actionable 才代表在时间窗内可跟，accepted 只代表盘口可模拟成交。"
        if state == "current"
        else "报价评估已过期，等待下一轮 paper-observer-loop 刷新。"
    )
    return {
        **payload,
        "schema_version": str(payload.get("schema_version") or PAPER_OBSERVER_EVALUATION_SCHEMA_VERSION),
        "state": state,
        "json_path": str(path),
        "json_exists": True,
        "age_seconds": age_seconds,
        "read_only": True,
        "write_scope": "no_orders",
        "boundary": base["boundary"],
        "next_action": next_action,
    }


def _paper_signal_evaluation_history(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, "paper_signal_evaluations"):
        return {"available": False}
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_evaluations,
            COUNT(DISTINCT wallet) AS wallets,
            SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS accepted,
            SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS actionable,
            SUM(CASE WHEN actionability_reason = 'signal_too_old' THEN 1 ELSE 0 END) AS stale_signals,
            SUM(CASE WHEN quote_error != '' THEN 1 ELSE 0 END) AS quote_errors,
            AVG(CASE WHEN accepted = 1 THEN slippage_bps ELSE NULL END) AS avg_slippage_bps,
            AVG(quote_latency_ms) AS avg_latency_ms,
            MAX(evaluated_at) AS latest_evaluated_at
        FROM paper_signal_evaluations
        """
    ).fetchone()
    total = int(row["total_evaluations"] or 0) if row else 0
    accepted = int(row["accepted"] or 0) if row else 0
    actionable = int(row["actionable"] or 0) if row else 0
    return {
        "available": True,
        "total_evaluations": total,
        "wallets": int(row["wallets"] or 0) if row else 0,
        "accepted": accepted,
        "accepted_rate_pct": round((accepted / total) * 100, 2) if total else 0.0,
        "actionable": actionable,
        "actionable_rate_pct": round((actionable / total) * 100, 2) if total else 0.0,
        "stale_signals": int(row["stale_signals"] or 0) if row else 0,
        "quote_errors": int(row["quote_errors"] or 0) if row else 0,
        "avg_slippage_bps": row["avg_slippage_bps"] if row else None,
        "avg_latency_ms": row["avg_latency_ms"] if row else None,
        "latest_evaluated_at": row["latest_evaluated_at"] if row else None,
        "reason_counts": _rows(
            conn,
            """
            SELECT decision_reason AS reason, COUNT(*) AS count
            FROM paper_signal_evaluations
            GROUP BY decision_reason
            ORDER BY count DESC, reason ASC
            LIMIT 8
            """,
        ),
        "actionability_reason_counts": _rows(
            conn,
            """
            SELECT actionability_reason AS reason, COUNT(*) AS count
            FROM paper_signal_evaluations
            GROUP BY actionability_reason
            ORDER BY count DESC, reason ASC
            LIMIT 8
            """,
        ),
        "wallets_summary": _rows(
            conn,
            """
            SELECT
                wallet,
                COUNT(*) AS signals,
                SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS accepted,
                SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS actionable,
                ROUND(100.0 * SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS accepted_rate_pct,
                ROUND(100.0 * SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS actionable_rate_pct,
                AVG(CASE WHEN accepted = 1 THEN slippage_bps ELSE NULL END) AS avg_slippage_bps,
                SUM(CASE WHEN quote_error != '' THEN 1 ELSE 0 END) AS quote_errors,
                SUM(CASE WHEN actionability_reason = 'signal_too_old' THEN 1 ELSE 0 END) AS stale_signals,
                MAX(evaluated_at) AS latest_evaluated_at
            FROM paper_signal_evaluations
            GROUP BY wallet
            ORDER BY accepted_rate_pct DESC, signals DESC, latest_evaluated_at DESC
            LIMIT 10
            """,
        ),
    }


def _report_file_path(settings: RobotSettings, filename: str) -> Path:
    reports_dir = settings.candidate_review_path.parent
    if reports_dir.is_absolute():
        return reports_dir / filename
    if settings.db_path.is_absolute():
        if settings.db_path.parent.name == "data":
            return settings.db_path.parent.parent / reports_dir / filename
        return settings.db_path.parent / reports_dir / filename
    return reports_dir / filename


def _paper_observer_preview_summary(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    max_signal_age_sec: int = 21_600,
) -> dict[str, Any]:
    preview = preview_paper_observer(conn, limit=limit, max_signal_age_sec=max_signal_age_sec)
    payload = dict(preview.__dict__)
    for row in payload.get("window_diagnostics") or []:
        row["avg_ingest_lag"] = _duration_label(row.get("avg_ingest_lag_sec"))
        row["max_ingest_lag"] = _duration_label(row.get("max_ingest_lag_sec"))
    payload["read_only"] = True
    payload["write_scope"] = "no_writes"
    payload["boundary"] = "只读 paper observer 预览：列出合格近期信号，不查盘口、不写 paper_orders。"
    payload["suggested_window"] = _paper_observer_suggested_window(payload)
    payload["next_action"] = (
        "有信号时交给外部 paper observer 做报价、滑点和结算质量记录。"
        if preview.signals_seen
        else _paper_observer_no_signal_action(payload)
    )
    return payload


def _paper_observer_suggested_window(values: dict[str, Any]) -> dict[str, Any]:
    for row in values.get("window_diagnostics") or []:
        if int(row.get("eligible_signals") or 0) <= 0:
            continue
        seconds = int(row.get("max_signal_age_sec") or 0)
        label = str(row.get("window_label") or "")
        mode = "live_window" if seconds <= int(values.get("max_signal_age_sec") or 0) else "historical_review_window"
        return {
            "window_label": label,
            "max_signal_age_sec": seconds,
            "eligible_signals": int(row.get("eligible_signals") or 0),
            "mode": mode,
            "api_path": f"/api/paper-observer-preview?max_signal_age_sec={seconds}",
        }
    return {}


def _paper_observer_no_signal_action(values: dict[str, Any]) -> str:
    reason = str(values.get("no_signal_reason") or "")
    suggested = values.get("suggested_window") or {}
    if suggested:
        return (
            f"默认窗口暂无信号；最短可复盘窗口是 {suggested.get('window_label')}，"
            "仅用于历史 paper 观察复盘，不代表实时跟单。"
        )
    if reason == "no_paper_stage_wallets":
        return "当前没有 paper-stage 钱包，先等待研究侧批准候选。"
    if reason == "no_buy_activity":
        return "paper-stage 钱包已有记录，但还没有可观察 BUY 活动。"
    if reason == "latest_buy_outside_window":
        return "最近 BUY 已超出当前观察窗口，继续等待新交易或临时放宽观察窗口。"
    if reason == "recent_buys_failed_eligibility_or_deduped":
        return "观察窗口内有 BUY，但被 eligibility、价格、去重或 paper 阻断规则过滤。"
    return "当前时间窗内没有合格新信号，继续等待 paper_approved 钱包出现近期 BUY 活动。"


def _production_readiness_summary(
    conn: sqlite3.Connection,
    *,
    settings: RobotSettings,
    stage_rows: list[dict[str, Any]],
    top_review_candidates: list[dict[str, Any]],
    manual_review_actions: list[dict[str, Any]],
    paper_min_score: float,
) -> dict[str, Any]:
    stage_counts = {str(row.get("name") or ""): int(row.get("count") or 0) for row in stage_rows}
    manual = conn.execute(
        """
        SELECT
            COUNT(*) AS wallets,
            MAX(COALESCE(ls.leader_score, 0)) AS max_score,
            AVG(COALESCE(ls.leader_score, 0)) AS avg_score,
            SUM(CASE WHEN COALESCE(ls.leader_score, 0) >= ? THEN 1 ELSE 0 END) AS at_threshold,
            SUM(CASE WHEN COALESCE(ls.leader_score, 0) >= ? AND COALESCE(ls.leader_score, 0) < ? THEN 1 ELSE 0 END) AS near_threshold
        FROM candidate_wallets cw
        LEFT JOIN leader_latest_scores ls
          ON ls.address = cw.address
        WHERE cw.candidate_stage = 'needs_manual_review'
        """,
        (paper_min_score, max(0.0, paper_min_score - 5.0), paper_min_score),
    ).fetchone()
    queue_rows = _rows(
        conn,
        """
        SELECT job_type, status, COUNT(*) AS count
        FROM pipeline_jobs
        WHERE status IN ('queued', 'running')
        GROUP BY job_type, status
        """,
    )
    queue_counts = {
        (str(row.get("job_type") or ""), str(row.get("status") or "")): int(row.get("count") or 0)
        for row in queue_rows
    }
    paper_stage_wallets = sum(stage_counts.get(stage, 0) for stage in ("paper_candidate", "paper_approved", "live_eligible"))
    manual_wallets = int(manual["wallets"] or 0) if manual else stage_counts.get("needs_manual_review", 0)
    max_manual_score = float(manual["max_score"] or 0) if manual else 0.0
    at_threshold = int(manual["at_threshold"] or 0) if manual else 0
    near_threshold = int(manual["near_threshold"] or 0) if manual else 0
    score_gap = max(0.0, paper_min_score - max_manual_score) if manual_wallets else paper_min_score
    evidence_active_pending = int(queue_counts.get(("wallet_evidence_backfill", "queued"), 0)) + int(
        queue_counts.get(("wallet_evidence_backfill", "running"), 0)
    )
    evidence_state_pending = int(
        _pending_evidence_backlog_summary(conn).get("pending_without_active_job") or 0
    )
    evidence_pending = evidence_active_pending + evidence_state_pending
    copyability_pending = int(queue_counts.get(("copyability_evidence", "queued"), 0)) + int(queue_counts.get(("copyability_evidence", "running"), 0))
    observer_current_cutoff = _paper_observer_current_cutoff()
    observer = conn.execute(
        """
        WITH observer AS (
            SELECT
                wallet,
                COUNT(*) AS evaluations,
                SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS accepted_signals,
                SUM(CASE WHEN actionable = 1 THEN 1 ELSE 0 END) AS actionable_signals,
                SUM(CASE WHEN actionability_reason = 'signal_too_old' THEN 1 ELSE 0 END) AS stale_signals,
                SUM(CASE WHEN quote_error != '' THEN 1 ELSE 0 END) AS quote_errors,
                MAX(evaluated_at) AS latest_evaluated_at
            FROM paper_signal_evaluations
            WHERE evaluated_at >= ?
            GROUP BY wallet
        )
        SELECT
            SUM(COALESCE(observer.evaluations, 0)) AS evaluations,
            SUM(COALESCE(observer.accepted_signals, 0)) AS accepted_signals,
            SUM(COALESCE(observer.actionable_signals, 0)) AS actionable_signals,
            SUM(COALESCE(observer.stale_signals, 0)) AS stale_signals,
            SUM(COALESCE(observer.quote_errors, 0)) AS quote_errors,
            SUM(CASE WHEN COALESCE(observer.actionable_signals, 0) > 0 THEN 1 ELSE 0 END) AS actionable_wallets,
            MAX(observer.latest_evaluated_at) AS latest_evaluated_at
        FROM candidate_wallets cw
        LEFT JOIN observer
          ON observer.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        """,
        (observer_current_cutoff,),
    ).fetchone()
    observer_evaluations = int(observer["evaluations"] or 0) if observer else 0
    observer_accepted_signals = int(observer["accepted_signals"] or 0) if observer else 0
    observer_actionable_signals = int(observer["actionable_signals"] or 0) if observer else 0
    observer_stale_signals = int(observer["stale_signals"] or 0) if observer else 0
    observer_quote_errors = int(observer["quote_errors"] or 0) if observer else 0
    observer_actionable_wallets = int(observer["actionable_wallets"] or 0) if observer else 0
    observer_latest_evaluated_at = int(observer["latest_evaluated_at"] or 0) if observer else 0
    formal_publish_gate = _formal_publish_gate_summary(conn, settings=settings, stage_counts=stage_counts)
    # Operator-facing blocker should point at the active manual-review queue;
    # archived blockers remain visible in the stage counts.
    active_blocker = manual_review_actions[0] if manual_review_actions else {}
    top_blocker_key = (
        active_blocker.get("key")
        or (top_review_candidates[0].get("blocker_key") if top_review_candidates else "")
        or ""
    )
    top_blocker = (
        active_blocker.get("blocker")
        or (top_review_candidates[0].get("blocker_label") if top_review_candidates else "")
        or ""
    )
    if paper_stage_wallets and observer_actionable_signals <= 0:
        state = "paper_candidates_waiting_actionable_signals"
        next_action = "已有 paper 候选，但当前没有可及时跟信号；继续 observer 收集，不发布。"
    elif paper_stage_wallets:
        state = "paper_candidates_present"
        next_action = "已有 paper 候选且出现可及时跟信号；外部 paper 验证应记录结果，仍不直接发布。"
    elif at_threshold:
        state = "manual_gate"
        next_action = "已有达到分数阈值的钱包，但仍停在复核观察状态，需要补齐风险或 copyability 证据。"
    elif near_threshold and top_blocker_key == "copyability_no_signal":
        state = "near_threshold_no_copy_signal"
        next_action = "近阈值钱包已补 copyability 但没有跟随信号，优先复核策略可复制性或降低候选优先级。"
    elif near_threshold and top_blocker_key == "copyability_unvalidated":
        state = "near_threshold_copyability_unvalidated"
        next_action = "近阈值钱包有原始 copyability 线索，但没有合格 follower 或回测成交证据，应降级观察而不是进入 paper。"
    elif near_threshold:
        state = "near_threshold"
        next_action = "有接近 paper 阈值的钱包，优先补 copyability/hygiene 证据并复核评分。"
    elif copyability_pending:
        state = "copyability_pending"
        next_action = "copyability 证据队列仍在消化，先看这些任务能否解除高分钱包阻塞。"
    elif evidence_pending:
        state = "evidence_pending"
        next_action = "历史证据队列仍在消化，等待 L1/L2/L3 证据补齐后再评分。"
    else:
        state = "needs_better_sources"
        next_action = "暂无接近阈值的钱包，需要继续发现更高质量来源或收紧初筛。"
    return {
        "state": state,
        "next_action": next_action,
        "paper_min_score": paper_min_score,
        "near_score_floor": max(0.0, paper_min_score - 5.0),
        "paper_stage_wallets": paper_stage_wallets,
        "needs_manual_review": manual_wallets,
        "manual_at_threshold": at_threshold,
        "manual_near_threshold": near_threshold,
        "max_manual_score": round(max_manual_score, 2),
        "score_gap_to_paper": round(score_gap, 2),
        "blocked_hygiene": stage_counts.get("blocked_hygiene", 0),
        "blocked_copyability": stage_counts.get("blocked_copyability", 0),
        "needs_data": stage_counts.get("needs_data", 0),
        "evidence_pending": evidence_pending,
        "evidence_active_pending": evidence_active_pending,
        "evidence_state_pending": evidence_state_pending,
        "copyability_pending": copyability_pending,
        "observer_evaluations": observer_evaluations,
        "observer_current_window_sec": PAPER_OBSERVER_CURRENT_EVALUATION_MAX_AGE_SEC,
        "observer_accepted_signals": observer_accepted_signals,
        "observer_actionable_signals": observer_actionable_signals,
        "observer_stale_signals": observer_stale_signals,
        "observer_quote_errors": observer_quote_errors,
        "observer_actionable_wallets": observer_actionable_wallets,
        "observer_latest_evaluated_at": observer_latest_evaluated_at,
        "formal_publish_gate": formal_publish_gate,
        "top_blocker_key": top_blocker_key,
        "top_blocker": top_blocker,
        "manual_review_actions": manual_review_actions,
    }


def _formal_publish_gate_summary(
    conn: sqlite3.Connection,
    *,
    settings: RobotSettings,
    stage_counts: dict[str, int],
) -> dict[str, Any]:
    now = int(time.time())
    research_only = settings.execution_mode in {"research", "scoring", "research_scoring"}
    paper_stage_wallets = sum(stage_counts.get(stage, 0) for stage in ("paper_candidate", "paper_approved", "live_eligible"))
    live_eligible_wallets = stage_counts.get("live_eligible", 0)
    quality_row = conn.execute(
        """
        SELECT
            COUNT(*) AS quality_wallets,
            SUM(CASE WHEN production_ready = 1 THEN 1 ELSE 0 END) AS production_ready_wallets
        FROM paper_wallet_quality
        """
    ).fetchone()
    paper_stage_quality = conn.execute(
        """
        SELECT
            SUM(CASE WHEN pwq.wallet IS NOT NULL THEN 1 ELSE 0 END) AS paper_stage_with_quality,
            SUM(CASE WHEN pwq.wallet IS NULL THEN 1 ELSE 0 END) AS paper_stage_missing_quality,
            SUM(CASE WHEN COALESCE(pwq.production_ready, 0) = 1 THEN 1 ELSE 0 END) AS paper_stage_production_ready
        FROM candidate_wallets cw
        LEFT JOIN paper_wallet_quality pwq
          ON pwq.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        """
    ).fetchone()
    publish_row = conn.execute(
        """
        SELECT
            SUM(CASE
                    WHEN status = 'active'
                     AND revoked_at IS NULL
                     AND (expires_at = 0 OR expires_at > ?)
                    THEN 1 ELSE 0
                END) AS active_published_leaders,
            SUM(CASE WHEN status = 'revoked' THEN 1 ELSE 0 END) AS revoked_published_leaders,
            MAX(published_at) AS latest_published_at
        FROM leader_publish
        """,
        (now,),
    ).fetchone()
    quality_wallets = int(quality_row["quality_wallets"] or 0) if quality_row else 0
    production_ready_wallets = int(quality_row["production_ready_wallets"] or 0) if quality_row else 0
    paper_stage_with_quality = int(paper_stage_quality["paper_stage_with_quality"] or 0) if paper_stage_quality else 0
    paper_stage_missing_quality = int(paper_stage_quality["paper_stage_missing_quality"] or 0) if paper_stage_quality else 0
    paper_stage_production_ready = int(paper_stage_quality["paper_stage_production_ready"] or 0) if paper_stage_quality else 0
    active_published_leaders = int(publish_row["active_published_leaders"] or 0) if publish_row else 0
    revoked_published_leaders = int(publish_row["revoked_published_leaders"] or 0) if publish_row else 0
    latest_published_at = int(publish_row["latest_published_at"] or 0) if publish_row else 0
    formal_blocker_rows = _formal_publish_blocker_rows(conn, research_only=research_only)
    top_formal_blocker = formal_blocker_rows[0] if formal_blocker_rows else {}
    active_formal_wallets = _active_formal_wallet_rows(conn, now=now)
    paper_stage_gap_wallets = _paper_stage_formal_gap_rows(conn, research_only=research_only)

    if active_published_leaders:
        state = "published_active"
        next_action = "已有 active published leader；继续监控过期、撤销和纸面质量回撤。"
        root_formal_blocker = ""
        root_formal_next_action = next_action
    elif research_only:
        state = "research_mode_publish_disabled"
        next_action = "NAS 当前是 research/scoring：不会自动写 paper_orders、结算或发布；正式化需要独立 paper/settle/publish 运行面。"
        root_formal_blocker = "runtime_research_only"
        root_formal_next_action = next_action
    elif live_eligible_wallets <= 0:
        state = "waiting_live_eligible"
        next_action = "还没有 live_eligible 钱包；先让 paper 质量证据稳定后再开放发布。"
        root_formal_blocker = "stage_not_live_eligible"
        root_formal_next_action = next_action
    elif paper_stage_production_ready <= 0:
        state = "waiting_paper_quality"
        next_action = "已有候选但没有 production_ready 纸面质量；需要 paper orders、settlement 和稳定观察证据。"
        root_formal_blocker = "paper_quality_not_production_ready"
        root_formal_next_action = next_action
    else:
        state = "publish_loop_needed"
        next_action = "存在 production_ready 钱包但没有 active 发布；检查 publish-leaders 运行面和 publish eligibility。"
        root_formal_blocker = "publish_not_active"
        root_formal_next_action = next_action

    gate_rows = [
        {
            "gate": "运行边界",
            "status": "研究模式" if research_only else "可检查发布运行面",
            "count": settings.execution_mode,
            "next_action": "NAS 不自动 paper/publish" if research_only else "确认 paper-run、settle、publish 服务",
        },
        {
            "gate": "Paper 阶段钱包",
            "status": "已进入观察" if paper_stage_wallets else "暂无",
            "count": paper_stage_wallets,
            "next_action": "等待及时信号和纸面验证" if paper_stage_wallets else "先产生 paper_approved",
        },
        {
            "gate": "纸面质量证据",
            "status": "缺失" if paper_stage_missing_quality else "已有",
            "count": f"{paper_stage_with_quality}/{paper_stage_wallets}",
            "next_action": "需要 paper_orders/settle 生成 paper_wallet_quality" if paper_stage_missing_quality else "继续看 production_ready",
        },
        {
            "gate": "Production ready",
            "status": "通过" if paper_stage_production_ready else "未通过",
            "count": paper_stage_production_ready,
            "next_action": "可进入 live_eligible 检查" if paper_stage_production_ready else "等待稳定纸面收益和风险指标",
        },
        {
            "gate": "Live eligible",
            "status": "已有" if live_eligible_wallets else "暂无",
            "count": live_eligible_wallets,
            "next_action": "可执行 publish-leaders" if live_eligible_wallets else "不会进入 leader_publish active",
        },
        {
            "gate": "Active publish",
            "status": "已有" if active_published_leaders else "暂无",
            "count": active_published_leaders,
            "next_action": "持续监控过期和撤销" if active_published_leaders else "当前正式钱包为 0",
        },
        {
            "gate": "Revoked publish",
            "status": "历史撤销" if revoked_published_leaders else "无",
            "count": revoked_published_leaders,
            "next_action": "只作历史记录，不算正式钱包" if revoked_published_leaders else "无历史撤销发布",
        },
    ]
    return {
        "state": state,
        "next_action": next_action,
        "runtime_mode": settings.execution_mode,
        "research_only": research_only,
        "publish_loop_enabled": False if research_only else None,
        "paper_stage_wallets": paper_stage_wallets,
        "paper_candidate_wallets": stage_counts.get("paper_candidate", 0),
        "paper_approved_wallets": stage_counts.get("paper_approved", 0),
        "live_eligible_wallets": live_eligible_wallets,
        "quality_wallets": quality_wallets,
        "production_ready_wallets": production_ready_wallets,
        "paper_stage_with_quality": paper_stage_with_quality,
        "paper_stage_missing_quality": paper_stage_missing_quality,
        "paper_stage_production_ready": paper_stage_production_ready,
        "active_published_leaders": active_published_leaders,
        "revoked_published_leaders": revoked_published_leaders,
        "latest_published_at": latest_published_at,
        "current_formal_status": f"当前正式钱包为 {active_published_leaders}",
        "root_formal_blocker": root_formal_blocker,
        "root_formal_next_action": root_formal_next_action,
        "active_formal_wallets": active_formal_wallets,
        "paper_stage_gap_wallets": paper_stage_gap_wallets,
        "top_formal_blocker": top_formal_blocker.get("blocker") or "",
        "formal_blocker_rows": formal_blocker_rows,
        "gate_rows": gate_rows,
    }


def _active_formal_wallet_rows(
    conn: sqlite3.Connection,
    *,
    now: int,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return wallets that are currently active in the formal publish library."""

    return _rows(
        conn,
        """
        SELECT
            wallet AS address,
            publish_stage,
            status,
            ROUND(COALESCE(leader_score, 0), 2) AS leader_score,
            COALESCE(review_reason, '') AS review_reason,
            published_at,
            expires_at
        FROM leader_publish
        WHERE status = 'active'
          AND revoked_at IS NULL
          AND (expires_at = 0 OR expires_at > ?)
        ORDER BY COALESCE(leader_score, 0) DESC, published_at DESC, wallet ASC
        LIMIT ?
        """,
        (now, min(max(int(limit), 1), MAX_LIST_LIMIT)),
    )


def _paper_stage_formal_gap_rows(
    conn: sqlite3.Connection,
    *,
    research_only: bool,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Explain why paper-stage wallets are not formal publish wallets."""

    rows = _rows(
        conn,
        """
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_reason, '') AS review_reason,
            CASE WHEN pwq.wallet IS NULL THEN 0 ELSE 1 END AS paper_quality_present,
            COALESCE(pwq.orders, 0) AS paper_orders,
            COALESCE(pwq.settled_positions, 0) AS paper_settled_positions,
            COALESCE(pwq.production_ready, 0) AS paper_ready,
            COALESCE(lp.status, '') AS publish_status
        FROM candidate_wallets cw
        LEFT JOIN leader_latest_scores ls
          ON ls.address = cw.address
        LEFT JOIN paper_wallet_quality pwq
          ON pwq.wallet = cw.address
        LEFT JOIN leader_publish lp
          ON lp.wallet = cw.address
        WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        ORDER BY
            CASE cw.candidate_stage
                WHEN 'live_eligible' THEN 0
                WHEN 'paper_approved' THEN 1
                WHEN 'paper_candidate' THEN 2
                ELSE 3
            END,
            COALESCE(pwq.production_ready, 0) DESC,
            COALESCE(ls.leader_score, 0) DESC,
            cw.updated_at DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), MAX_LIST_LIMIT),),
    )
    for row in rows:
        blockers, formal_action = _paper_handoff_formal_blockers(row, research_only=research_only)
        row["formal_blocker_list"] = blockers
        row["formal_blockers"] = " | ".join(blockers)
        row["formal_next_action"] = formal_action
    return rows


def _formal_publish_blocker_rows(conn: sqlite3.Connection, *, research_only: bool) -> list[dict[str, Any]]:
    """Aggregate why paper-stage wallets are not formal publish wallets."""

    runtime_select = (
        "SELECT cw.address, 'runtime_research_only' AS blocker "
        "FROM candidate_wallets cw "
        "WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')"
        if research_only
        else "SELECT cw.address, '' AS blocker FROM candidate_wallets cw WHERE 0"
    )
    return _rows(
        conn,
        f"""
        WITH scoped AS (
            SELECT
                cw.address,
                cw.candidate_stage,
                COALESCE(pwq.orders, 0) AS paper_orders,
                COALESCE(pwq.settled_positions, 0) AS settled_positions,
                CASE WHEN pwq.wallet IS NULL THEN 0 ELSE 1 END AS paper_quality_present,
                COALESCE(pwq.production_ready, 0) AS production_ready,
                COALESCE(lp.status, '') AS publish_status,
                COALESCE(lp.revoked_at, 0) AS revoked_at,
                COALESCE(lp.expires_at, 0) AS expires_at
            FROM candidate_wallets cw
            LEFT JOIN paper_wallet_quality pwq
              ON pwq.wallet = cw.address
            LEFT JOIN leader_publish lp
              ON lp.wallet = cw.address
            WHERE cw.candidate_stage IN ('paper_candidate', 'paper_approved', 'live_eligible')
        ),
        blockers AS (
            {runtime_select}
            UNION ALL
            SELECT address, 'stage_not_live_eligible' FROM scoped WHERE candidate_stage != 'live_eligible'
            UNION ALL
            SELECT address, 'missing_paper_wallet_quality' FROM scoped WHERE paper_quality_present = 0
            UNION ALL
            SELECT address, 'paper_quality_not_production_ready' FROM scoped WHERE paper_quality_present = 1 AND production_ready = 0
            UNION ALL
            SELECT address, 'no_paper_orders' FROM scoped WHERE paper_orders <= 0
            UNION ALL
            SELECT address, 'no_settled_positions' FROM scoped WHERE settled_positions <= 0
            UNION ALL
            SELECT address, 'publish_not_active' FROM scoped WHERE publish_status != 'active' OR revoked_at > 0
        )
        SELECT
            blocker,
            COUNT(*) AS count,
            MIN(address) AS example,
            CASE blocker
                WHEN 'runtime_research_only' THEN '当前 NAS 是 research/scoring，不会自动 paper/settle/publish。'
                WHEN 'stage_not_live_eligible' THEN '先通过稳定 paper 质量升级到 live_eligible。'
                WHEN 'missing_paper_wallet_quality' THEN '等待及时 BUY 信号后生成 paper_orders 和 paper_wallet_quality。'
                WHEN 'paper_quality_not_production_ready' THEN '继续累计纸面订单、结算、ROI 和风险观察。'
                WHEN 'no_paper_orders' THEN '先捕捉可及时跟的 BUY，再进入纸面订单验证。'
                WHEN 'no_settled_positions' THEN '等待纸面仓位结算，形成收益和回撤证据。'
                WHEN 'publish_not_active' THEN '满足前置门槛后才允许 active publish。'
                ELSE '继续补正式发布前置证据。'
            END AS next_action
        FROM blockers
        WHERE blocker != ''
        GROUP BY blocker
        ORDER BY count DESC, blocker ASC
        """,
    )


def _score_policy_freshness(
    conn: sqlite3.Connection,
    settings: RobotSettings,
    *,
    paper_min_score: float,
) -> dict[str, Any]:
    try:
        current_version = str(load_policy(settings.policy_path).get("version", ""))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "state": "policy_load_error",
            "policy_path": str(settings.policy_path),
            "current_policy_version": "",
            "error": type(exc).__name__,
            "message": str(exc)[:160],
        }
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS latest_scores,
            SUM(CASE WHEN policy_version = ? THEN 1 ELSE 0 END) AS current_policy_scores,
            SUM(CASE WHEN policy_version != ? THEN 1 ELSE 0 END) AS stale_policy_scores,
            SUM(CASE WHEN policy_version != ? AND leader_score >= ? THEN 1 ELSE 0 END) AS stale_near_threshold_scores,
            SUM(CASE WHEN policy_version != ? AND leader_score >= ? THEN 1 ELSE 0 END) AS stale_paper_threshold_scores,
            MAX(CASE WHEN policy_version != ? THEN leader_score ELSE NULL END) AS max_stale_score
        FROM leader_latest_scores
        """,
        (
            current_version,
            current_version,
            current_version,
            max(0.0, paper_min_score - 5.0),
            current_version,
            paper_min_score,
            current_version,
        ),
    ).fetchone()
    latest_scores = int(row["latest_scores"] or 0) if row else 0
    stale_scores = int(row["stale_policy_scores"] or 0) if row else 0
    stale_near_threshold_scores = int(row["stale_near_threshold_scores"] or 0) if row else 0
    stale_paper_threshold_scores = int(row["stale_paper_threshold_scores"] or 0) if row else 0
    if stale_scores == 0:
        state = "current"
        next_action = "评分记录已使用当前规则版本。"
    elif stale_near_threshold_scores or stale_paper_threshold_scores:
        state = "stale_scores_need_rescore"
        next_action = "评分规则已升级，先运行新版增量重评分。"
    else:
        state = "stale_low_scores_deferred"
        next_action = "剩余旧规则评分低于观察阈值，可延后重评；优先处理证据和 copyability 队列。"
    return {
        "state": state,
        "policy_path": str(settings.policy_path),
        "current_policy_version": current_version,
        "latest_scores": latest_scores,
        "current_policy_scores": int(row["current_policy_scores"] or 0) if row else 0,
        "stale_policy_scores": stale_scores,
        "stale_near_threshold_scores": stale_near_threshold_scores,
        "stale_paper_threshold_scores": stale_paper_threshold_scores,
        "max_stale_score": round(float(row["max_stale_score"] or 0), 2) if row else 0.0,
        "next_action": next_action,
    }


def _evidence_pipeline_summary(conn: sqlite3.Connection, *, limit: int = 10) -> dict[str, Any]:
    now = int(time.time())
    job_type = PipelineJobType.WALLET_EVIDENCE_BACKFILL.value
    schedule_status = wallet_pipeline_schedule_status(
        conn,
        now=now,
        priority_aging_seconds=_web_env_int(
            "PM_ROBOT_PIPELINE_PRIORITY_AGING_SECONDS",
            DEFAULT_PIPELINE_PRIORITY_AGING_SECONDS,
        ),
        stage_weights={
            EvidenceJobStage.LIGHT_PENDING.value: _web_env_int(
                "PM_ROBOT_PIPELINE_PLANNER_LIGHT_LIMIT",
                DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.LIGHT_PENDING.value],
            ),
            EvidenceJobStage.MEDIUM_PENDING.value: _web_env_int(
                "PM_ROBOT_PIPELINE_PLANNER_MEDIUM_LIMIT",
                DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.MEDIUM_PENDING.value],
            ),
            EvidenceJobStage.DEEP_PENDING.value: _web_env_int(
                "PM_ROBOT_PIPELINE_PLANNER_DEEP_LIMIT",
                DEFAULT_PIPELINE_STAGE_WEIGHTS[EvidenceJobStage.DEEP_PENDING.value],
            ),
        },
    )
    stage_schedule = []
    for raw_row in schedule_status.get("stage_schedule") or []:
        row = dict(raw_row)
        row["stage_label"] = _EVIDENCE_ACTION_LABELS.get(
            str(row.get("job_action") or ""),
            str(row.get("job_action") or ""),
        )
        row["oldest_wait_label"] = (
            _duration_label(row.get("oldest_claimable_wait_seconds"))
            if int(row.get("oldest_claimable_wait_seconds") or 0)
            else "-"
        )
        stage_schedule.append(row)
    queue_progress = _queue_progress_for_job_type(conn, job_type, now=now)
    pending_state_backlog = _pending_evidence_backlog_summary(conn, now=now)
    status_counts = _rows(
        conn,
        """
        SELECT status, COUNT(*) AS count, MIN(priority) AS min_priority, MAX(updated_at) AS latest_updated_at
        FROM pipeline_jobs
        WHERE job_type = ?
        GROUP BY status
        ORDER BY
            CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'done' THEN 3 ELSE 4 END,
            status ASC
        """,
        (job_type,),
    )
    totals = {str(row.get("status") or ""): int(row.get("count") or 0) for row in status_counts}
    active_count = totals.get("queued", 0) + totals.get("running", 0)
    pending_state_count = int(pending_state_backlog.get("pending_without_active_job") or 0)
    due_queued_count = int(schedule_status.get("due_queued_count") or 0)
    deferred_queued_count = int(schedule_status.get("deferred_queued_count") or 0)
    exhausted_queued_count = int(schedule_status.get("exhausted_queued_count") or 0)
    total_due_backlog = due_queued_count + totals.get("running", 0) + pending_state_count
    recent_rate_per_hour = float(queue_progress.get("recent_rate_per_hour") or 0)
    total_eta_label = (
        _fmt_duration_hours(total_due_backlog / recent_rate_per_hour)
        if total_due_backlog and recent_rate_per_hour > 0
        else ""
    )
    state_by_tier = _rows(
        conn,
        """
        SELECT
            discovery_tier AS evidence_tier,
            evidence_status,
            next_action,
            COUNT(*) AS count,
            MIN(priority) AS min_priority,
            ROUND(AVG(activity_count), 1) AS avg_activity_count,
            MAX(activity_count) AS max_activity_count,
            MAX(updated_at) AS latest_updated_at
        FROM wallet_processing_state
        GROUP BY discovery_tier, evidence_status, next_action
        ORDER BY
            CASE discovery_tier
                WHEN 'l0_discovered' THEN 0
                WHEN 'l1_light' THEN 1
                WHEN 'l2_medium' THEN 2
                WHEN 'l3_deep' THEN 3
                ELSE 4
            END,
            count DESC,
            evidence_status ASC,
            next_action ASC
        LIMIT 16
        """,
    )
    active_by_action = _rows(
        conn,
        """
        SELECT
            subject_key AS job_action,
            tier AS job_scope,
            status,
            COUNT(*) AS count,
            MIN(priority) AS min_priority,
            MIN(created_at) AS oldest_created_at,
            MAX(updated_at) AS latest_updated_at
        FROM pipeline_jobs
        WHERE job_type = ?
          AND status IN ('queued', 'running')
        GROUP BY subject_key, tier, status
        ORDER BY min_priority ASC,
                 CASE status WHEN 'running' THEN 0 ELSE 1 END,
                 count DESC,
                 oldest_created_at ASC
        LIMIT 16
        """,
        (job_type,),
    )
    top_active_jobs = _rows(
        conn,
        """
        SELECT
            pj.wallet,
            pj.status,
            pj.subject_key AS job_action,
            pj.tier AS job_scope,
            pj.priority,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(wps.discovery_tier, '') AS evidence_tier,
            COALESCE(wps.evidence_status, '') AS evidence_status,
            COALESCE(wps.next_action, '') AS next_action,
            COALESCE(wps.activity_count, 0) AS activity_count,
            COALESCE(wps.distinct_markets, 0) AS distinct_markets,
            COALESCE(wps.non_fast_trade_count, 0) AS non_fast_trade_count,
            COALESCE(json_extract(pj.input_json, '$.target_depth'), 0) AS target_depth,
            COALESCE(json_extract(pj.input_json, '$.stage'), pj.subject_key) AS input_stage,
            pj.attempts,
            pj.updated_at,
            pj.last_error
        FROM pipeline_jobs pj
        LEFT JOIN candidate_wallets cw
          ON cw.address = pj.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = pj.wallet
        LEFT JOIN wallet_processing_state wps
          ON wps.wallet = pj.wallet
        WHERE pj.job_type = ?
          AND pj.status IN ('queued', 'running')
        ORDER BY
            pj.priority ASC,
            CASE pj.status WHEN 'running' THEN 0 ELSE 1 END,
            pj.updated_at ASC,
            pj.job_id ASC
        LIMIT ?
        """,
        (job_type, int(limit)),
    )
    recent_completed_jobs = _rows(
        conn,
        """
        SELECT
            pj.wallet,
            pj.priority,
            pj.completed_at,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(json_extract(pj.output_json, '$.stage'), pj.subject_key) AS completed_stage,
            COALESCE(json_extract(pj.output_json, '$.next_stage'), '') AS next_stage,
            COALESCE(json_extract(pj.output_json, '$.target_depth'), 0) AS target_depth,
            COALESCE(json_extract(pj.output_json, '$.activity_count'), 0) AS activity_count,
            COALESCE(json_extract(pj.output_json, '$.state.discovery_tier'), '') AS evidence_tier,
            COALESCE(json_extract(pj.output_json, '$.state.evidence_status'), '') AS evidence_status,
            COALESCE(json_extract(pj.output_json, '$.state.next_action'), '') AS next_action
        FROM pipeline_jobs pj
        LEFT JOIN candidate_wallets cw
          ON cw.address = pj.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = pj.wallet
        WHERE pj.job_type = ?
          AND pj.status = 'done'
        ORDER BY COALESCE(pj.completed_at, 0) DESC, pj.updated_at DESC, pj.job_id DESC
        LIMIT ?
        """,
        (job_type, int(limit)),
    )
    state_total = _scalar(conn, "SELECT COUNT(*) FROM wallet_processing_state")
    l3_wallets = _scalar(conn, "SELECT COUNT(*) FROM wallet_processing_state WHERE discovery_tier = 'l3_deep'")
    summary_ready = _scalar(conn, "SELECT COUNT(*) FROM wallet_processing_state WHERE evidence_status = 'summary_ready'")
    return {
        "job_type": job_type,
        "state_wallets": state_total,
        "l3_wallets": l3_wallets,
        "summary_ready_wallets": summary_ready,
        "status_counts": status_counts,
        "queued": totals.get("queued", 0),
        "running": totals.get("running", 0),
        "done": totals.get("done", 0),
        "failed": totals.get("failed", 0),
        "active": active_count,
        "completed_1h": queue_progress.get("completed_1h", 0),
        "completed_6h": queue_progress.get("completed_6h", 0),
        "completed_24h": queue_progress.get("completed_24h", 0),
        "recent_rate_per_hour": recent_rate_per_hour,
        "eta_label": queue_progress.get("eta_label", ""),
        "total_due_backlog": total_due_backlog,
        "total_eta_label": total_eta_label,
        "latest_completed_at": queue_progress.get("latest_completed_at", 0),
        "priority_aging_seconds": schedule_status.get("priority_aging_seconds", 0),
        "priority_aging_label": _duration_label(schedule_status.get("priority_aging_seconds")),
        "due_queued_jobs": due_queued_count,
        "deferred_queued_jobs": deferred_queued_count,
        "exhausted_queued_jobs": exhausted_queued_count,
        "aged_queued_jobs": schedule_status.get("aged_queued_count", 0),
        "oldest_claimable_wait_seconds": schedule_status.get("oldest_claimable_wait_seconds", 0),
        "oldest_claimable_wait_label": (
            _duration_label(schedule_status.get("oldest_claimable_wait_seconds"))
            if int(schedule_status.get("oldest_claimable_wait_seconds") or 0)
            else "-"
        ),
        "stage_schedule": stage_schedule,
        "state_by_tier": state_by_tier,
        "active_by_action": active_by_action,
        "pending_state_without_active_job": pending_state_count,
        "high_priority_pending_state_without_active_job": pending_state_backlog.get(
            "high_priority_pending_without_active_job",
            0,
        ),
        "pending_state_by_action": pending_state_backlog.get("by_action", []),
        "pending_state_high_priority_samples": pending_state_backlog.get("high_priority_samples", []),
        "top_active_jobs": top_active_jobs,
        "recent_completed_jobs": recent_completed_jobs,
    }


def _copyability_no_signal_summary(conn: sqlite3.Connection, *, limit: int = 12) -> dict[str, Any]:
    score_floor = 50.0
    pnl_floor = 5_000.0
    base_where = """
        cw.candidate_stage = 'blocked_copyability'
        AND ls.review_reason = 'copyability_scan_no_signal'
    """
    summary = _one(
        conn,
        f"""
        SELECT
            COUNT(*) AS wallet_count,
            SUM(CASE WHEN COALESCE(ls.leader_score, 0) >= ? THEN 1 ELSE 0 END) AS high_score_wallets,
            SUM(CASE WHEN COALESCE(wf.net_pnl_usdc, 0) >= ? THEN 1 ELSE 0 END) AS high_pnl_wallets,
            SUM(
                CASE
                    WHEN COALESCE(wf.hygiene_status, '') IN ('ok', 'clean', 'low_risk')
                    THEN 1 ELSE 0
                END
            ) AS clean_wallets,
            ROUND(MAX(COALESCE(ls.leader_score, 0)), 2) AS max_score,
            ROUND(MAX(COALESCE(wf.net_pnl_usdc, 0)), 2) AS max_net_pnl_usdc
        FROM leader_latest_scores ls
        JOIN candidate_wallets cw
          ON cw.address = ls.address
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        WHERE {base_where}
        """,
        (score_floor, pnl_floor),
    )
    rows = _rows(
        conn,
        f"""
        SELECT
            cw.address,
            cw.candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_stage, '') AS review_stage,
            COALESCE(ls.review_reason, '') AS review_reason,
            ROUND(COALESCE(wf.net_pnl_usdc, 0), 2) AS net_pnl_usdc,
            ROUND(COALESCE(wf.total_volume_usdc, 0), 2) AS total_volume_usdc,
            ROUND(COALESCE(wf.recent_30d_volume_usdc, 0), 2) AS recent_30d_volume_usdc,
            ROUND(COALESCE(wf.leader_in_degree, 0), 2) AS leader_in_degree,
            ROUND(COALESCE(wf.copy_event_count, 0), 2) AS copy_event_count,
            ROUND(COALESCE(wf.copy_market_count, 0), 2) AS copy_market_count,
            COALESCE(wf.hygiene_status, '') AS hygiene_status,
            COALESCE(wf.primary_category, '') AS primary_category,
            ROUND(COALESCE(wf.single_market_pnl_share, 0), 2) AS single_market_pnl_share,
            ROUND(COALESCE(wf.net_to_gross_exposure, 0), 2) AS net_to_gross_exposure,
            ls.scored_at
        FROM leader_latest_scores ls
        JOIN candidate_wallets cw
          ON cw.address = ls.address
        LEFT JOIN wallet_features wf
          ON wf.address = cw.address
        WHERE {base_where}
        ORDER BY
            COALESCE(ls.leader_score, 0) DESC,
            COALESCE(wf.net_pnl_usdc, 0) DESC,
            COALESCE(wf.total_volume_usdc, 0) DESC,
            cw.address ASC
        LIMIT ?
        """,
        (int(limit),),
    )
    return {
        "wallet_count": int(summary.get("wallet_count") or 0),
        "high_score_wallets": int(summary.get("high_score_wallets") or 0),
        "high_pnl_wallets": int(summary.get("high_pnl_wallets") or 0),
        "clean_wallets": int(summary.get("clean_wallets") or 0),
        "max_score": summary.get("max_score") or 0,
        "max_net_pnl_usdc": summary.get("max_net_pnl_usdc") or 0,
        "score_floor": score_floor,
        "pnl_floor": pnl_floor,
        "rows": rows,
        "next_action": "这些钱包收益/分数可能不错，但缺少可跟随信号；先保留观察，不自动进入 paper。",
    }


def _copyability_lane_summary(
    conn: sqlite3.Connection,
    *,
    settings: RobotSettings | None = None,
    include_pair_quality: bool = True,
) -> dict[str, Any]:
    now = int(time.time())
    policy = _load_policy_for_dashboard(settings)
    queue_progress = _queue_progress_for_job_type(conn, "copyability_evidence", now=now)
    status_counts = _rows(
        conn,
        """
        SELECT status, COUNT(*) AS count, MIN(priority) AS min_priority, MAX(updated_at) AS latest_updated_at
        FROM pipeline_jobs
        WHERE job_type = 'copyability_evidence'
        GROUP BY status
        ORDER BY
            CASE status WHEN 'running' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'done' THEN 3 ELSE 4 END,
            status ASC
        """,
    )
    totals = {str(row.get("status") or ""): int(row.get("count") or 0) for row in status_counts}
    active = totals.get("queued", 0) + totals.get("running", 0)
    max_active_jobs = max(0, _web_env_int("PM_ROBOT_COPYABILITY_PLANNER_MAX_ACTIVE_JOBS", 50))
    available_slots = max(0, max_active_jobs - active) if max_active_jobs else 0
    utilization_pct = round((active / max_active_jobs) * 100, 1) if max_active_jobs else 0.0
    active_by_priority = _rows(
        conn,
        """
        SELECT
            pj.priority,
            pj.status,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            COALESCE(ls.review_stage, '') AS review_stage,
            COUNT(*) AS count,
            ROUND(MAX(COALESCE(ls.leader_score, 0)), 2) AS max_score,
            MIN(pj.created_at) AS oldest_created_at,
            MAX(pj.updated_at) AS latest_updated_at
        FROM pipeline_jobs pj
        LEFT JOIN candidate_wallets cw
          ON cw.address = pj.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = pj.wallet
        WHERE pj.job_type = 'copyability_evidence'
          AND pj.status IN ('queued', 'running')
        GROUP BY pj.priority, pj.status, cw.candidate_stage, ls.review_stage
        ORDER BY pj.priority ASC, pj.status DESC, count DESC
        LIMIT 16
        """,
    )
    top_active_jobs = _rows(
        conn,
        """
        SELECT
            pj.wallet,
            pj.status,
            pj.priority,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(json_extract(pj.input_json, '$.activity_count'), 0) AS activity_count,
            COALESCE(json_extract(pj.input_json, '$.max_pair_events'), 0) AS max_pair_events,
            COALESCE(json_extract(pj.input_json, '$.max_pair_markets'), 0) AS max_pair_markets,
            COALESCE(json_extract(pj.input_json, '$.graph_scan_mode'), 'deep') AS graph_scan_mode,
            pj.updated_at
        FROM pipeline_jobs pj
        LEFT JOIN candidate_wallets cw
          ON cw.address = pj.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = pj.wallet
        WHERE pj.job_type = 'copyability_evidence'
          AND pj.status IN ('queued', 'running')
        ORDER BY pj.priority ASC,
                 CASE pj.status WHEN 'running' THEN 0 ELSE 1 END,
                 pj.updated_at ASC,
                 pj.job_id ASC
        LIMIT 10
        """,
    )
    recent_runs = _rows(
        conn,
        """
        SELECT
            ingest_type AS run_type,
            status,
            wallets_attempted,
            wallets_succeeded,
            rows_written,
            CASE
                WHEN finished_at IS NOT NULL AND finished_at > 0
                THEN finished_at - started_at
                ELSE 0
            END AS duration_seconds,
            started_at,
            finished_at,
            error
        FROM ingest_runs
        WHERE ingest_type LIKE 'copyability_evidence_worker%'
        ORDER BY started_at DESC
        LIMIT 8
        """,
    )
    recent_completed_jobs = _rows(
        conn,
        """
        SELECT
            pj.wallet,
            pj.priority,
            pj.completed_at,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            ROUND(COALESCE(ls.leader_score, 0), 2) AS leader_score,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(json_extract(pj.output_json, '$.graph.links_written'), 0) AS links_written,
            COALESCE(json_extract(pj.output_json, '$.graph.pair_stats_written'), 0) AS pair_stats_written,
            COALESCE(json_extract(pj.output_json, '$.graph.qualified_pairs'), 0) AS qualified_pairs,
            COALESCE(json_extract(pj.output_json, '$.backtest.trades_written'), 0) AS backtest_trades_written,
            CASE
                WHEN json_extract(pj.output_json, '$.score_written') = 1 THEN 'yes'
                WHEN json_extract(pj.output_json, '$.score_written') = 0 THEN 'no'
                ELSE ''
            END AS score_written
        FROM pipeline_jobs pj
        LEFT JOIN candidate_wallets cw
          ON cw.address = pj.wallet
        LEFT JOIN leader_latest_scores ls
          ON ls.address = pj.wallet
        WHERE pj.job_type = 'copyability_evidence'
          AND pj.status = 'done'
        ORDER BY COALESCE(pj.completed_at, 0) DESC, pj.updated_at DESC, pj.job_id DESC
        LIMIT 8
        """,
    )
    completed_24h = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM pipeline_jobs
        WHERE job_type = 'copyability_evidence'
          AND status = 'done'
          AND COALESCE(completed_at, 0) >= ?
        """,
        (now - 86_400,),
    )
    high_priority_active = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM pipeline_jobs
        WHERE job_type = 'copyability_evidence'
          AND status IN ('queued', 'running')
          AND priority <= 10
        """,
    )
    return {
        "status_counts": status_counts,
        "queued": totals.get("queued", 0),
        "running": totals.get("running", 0),
        "done": totals.get("done", 0),
        "failed": totals.get("failed", 0),
        "active": active,
        "max_active_jobs": max_active_jobs,
        "available_slots": available_slots,
        "queue_utilization_pct": utilization_pct,
        "queue_waterline_reached": bool(max_active_jobs and active >= max_active_jobs),
        "completed_24h": completed_24h,
        "completed_1h": queue_progress.get("completed_1h", 0),
        "completed_6h": queue_progress.get("completed_6h", 0),
        "recent_rate_per_hour": queue_progress.get("recent_rate_per_hour", 0),
        "eta_label": queue_progress.get("eta_label", ""),
        "latest_completed_at": queue_progress.get("latest_completed_at", 0),
        "high_priority_active": high_priority_active,
        "active_by_priority": active_by_priority,
        "pair_quality": _copyability_pair_quality_summary(conn, policy) if include_pair_quality else {},
        "top_active_jobs": top_active_jobs,
        "recent_runs": recent_runs,
        "recent_completed_jobs": recent_completed_jobs,
    }


def _load_policy_for_dashboard(settings: RobotSettings | None) -> dict[str, Any]:
    try:
        return load_policy(settings.policy_path) if settings else load_policy()
    except (OSError, json.JSONDecodeError):
        return {}


def _copyability_pair_quality_summary(conn: sqlite3.Connection, policy: dict[str, Any]) -> dict[str, Any]:
    min_events = int(threshold(policy, "min_copy_events", 5))
    min_markets = int(threshold(policy, "min_copy_markets", 5))
    min_containment = float(threshold(policy, "min_containment_pct", 0.9))
    min_precedes = float(threshold(policy, "min_leader_precedes_pct", 0.9))
    bucket_case = """
        CASE
            WHEN copy_event_count >= ? AND copy_market_count >= ? AND containment_pct >= ? AND leader_precedes_pct >= ?
                THEN 'qualified'
            WHEN copy_event_count < ?
                THEN 'thin_events'
            WHEN copy_market_count < ?
                THEN 'thin_markets'
            WHEN containment_pct < ? AND leader_precedes_pct < ?
                THEN 'miss_containment_and_precedes'
            WHEN containment_pct < ?
                THEN 'miss_containment'
            WHEN leader_precedes_pct < ?
                THEN 'miss_precedes'
            ELSE 'other'
        END
    """
    bucket_params = (
        min_events,
        min_markets,
        min_containment,
        min_precedes,
        min_events,
        min_markets,
        min_containment,
        min_precedes,
        min_containment,
        min_precedes,
    )
    bucket_rows = _rows(
        conn,
        f"""
        SELECT
            {bucket_case} AS bucket,
            COUNT(*) AS count,
            SUM(copy_event_count) AS copy_events,
            SUM(copy_market_count) AS copy_markets,
            MAX(copy_event_count) AS max_copy_events,
            MAX(copy_market_count) AS max_copy_markets,
            ROUND(AVG(containment_pct), 2) AS avg_containment,
            ROUND(AVG(leader_precedes_pct), 2) AS avg_precedes
        FROM copy_pair_stats
        GROUP BY bucket
        ORDER BY
            CASE bucket
                WHEN 'qualified' THEN 0
                WHEN 'miss_containment' THEN 1
                WHEN 'miss_precedes' THEN 2
                WHEN 'miss_containment_and_precedes' THEN 3
                WHEN 'thin_markets' THEN 4
                WHEN 'thin_events' THEN 5
                ELSE 6
            END,
            count DESC
        """,
        bucket_params,
    )
    near_miss_leaders = _rows(
        conn,
        """
        SELECT
            cps.leader_wallet,
            COALESCE(cw.candidate_stage, '') AS candidate_stage,
            COUNT(*) AS pair_count,
            SUM(cps.copy_event_count) AS copy_events,
            SUM(cps.copy_market_count) AS copy_markets,
            SUM(CASE WHEN cps.copy_event_count >= ? AND cps.copy_market_count >= ? THEN 1 ELSE 0 END) AS repeated_market_pairs,
            MAX(cps.copy_event_count) AS max_pair_events,
            MAX(cps.copy_market_count) AS max_pair_markets,
            ROUND(AVG(cps.containment_pct), 2) AS avg_containment,
            ROUND(AVG(cps.leader_precedes_pct), 2) AS avg_precedes,
            ROUND(MAX(cps.containment_pct), 2) AS max_containment,
            ROUND(MAX(cps.leader_precedes_pct), 2) AS max_precedes
        FROM copy_pair_stats cps
        LEFT JOIN candidate_wallets cw
          ON cw.address = cps.leader_wallet
        WHERE cps.qualifies = 0
        GROUP BY cps.leader_wallet, cw.candidate_stage
        HAVING pair_count >= 5 OR copy_events >= 20
        ORDER BY repeated_market_pairs DESC, copy_events DESC
        LIMIT 12
        """,
        (min_events, min_markets),
    )
    return {
        "thresholds": {
            "min_events": min_events,
            "min_markets": min_markets,
            "min_containment": min_containment,
            "min_leader_precedes": min_precedes,
        },
        "bucket_rows": bucket_rows,
        "near_miss_leaders": near_miss_leaders,
    }


def _queue_progress_for_job_type(conn: sqlite3.Connection, job_type: str, *, now: int) -> dict[str, Any]:
    for row in _queue_progress_rows(conn, now=now):
        if row.get("job_type") == job_type:
            return row
    return {}


def _needs_data_reason_rows(conn: sqlite3.Connection, *, limit: int = 8) -> list[dict[str, Any]]:
    rows = _rows(
        conn,
        """
        WITH base AS (
            SELECT
                wds.address,
                COALESCE(wds.leader_score, 0) AS leader_score,
                COALESCE(ls.review_reason, '') AS review_reason,
                COALESCE(ls.scored_at, wds.updated_at, 0) AS sort_at,
                COALESCE(wds.activity_count, 0) AS activity_count,
                COALESCE(wds.next_action, '') AS next_action
            FROM wallet_dashboard_snapshot wds
            LEFT JOIN leader_latest_scores ls ON ls.address = wds.address
            WHERE wds.candidate_stage = 'needs_data'
        ),
        bucketed AS (
            SELECT
                *,
                CASE
                    WHEN review_reason = ''
                         AND (activity_count < 200 OR next_action IN ('light_pending', 'medium_pending', 'deep_pending'))
                        THEN 'history_pending'
                    WHEN review_reason = ''
                        THEN 'not_scored'
                    WHEN review_reason = 'no_wallet_metrics_attached'
                        THEN 'missing_wallet_features'
                    WHEN review_reason LIKE 'missing_required_score_components:%'
                         AND (
                             (
                                 review_reason LIKE '%leader_in_degree%'
                                 OR review_reason LIKE '%copy_event_count%'
                                 OR review_reason LIKE '%copy_market_count%'
                                 OR review_reason LIKE '%copy_stream_roi%'
                             )
                             AND (
                                 review_reason LIKE '%bot_score%'
                                 OR review_reason LIKE '%single_market_pnl_share%'
                                 OR review_reason LIKE '%net_to_gross_exposure%'
                                 OR review_reason LIKE '%maker_fraction%'
                                 OR review_reason LIKE '%recent_30d_volume_usdc%'
                                 OR review_reason LIKE '%total_volume_usdc%'
                                 OR review_reason LIKE '%net_pnl_usdc%'
                             )
                         )
                        THEN 'missing_core_score_components'
                    WHEN review_reason LIKE 'missing_required_score_components:%'
                         AND (
                             review_reason LIKE '%bot_score%'
                             OR review_reason LIKE '%single_market_pnl_share%'
                             OR review_reason LIKE '%net_to_gross_exposure%'
                             OR review_reason LIKE '%maker_fraction%'
                         )
                         AND (
                             review_reason LIKE '%recent_30d_volume_usdc%'
                             OR review_reason LIKE '%total_volume_usdc%'
                             OR review_reason LIKE '%net_pnl_usdc%'
                         )
                        THEN 'missing_core_score_components'
                    WHEN review_reason LIKE 'missing_required_score_components:%'
                         AND (
                             review_reason LIKE '%leader_in_degree%'
                             OR review_reason LIKE '%copy_event_count%'
                             OR review_reason LIKE '%copy_market_count%'
                             OR review_reason LIKE '%copy_stream_roi%'
                         )
                        THEN 'missing_copyability_components'
                    WHEN review_reason LIKE 'missing_required_score_components:%'
                         AND (
                             review_reason LIKE '%bot_score%'
                             OR review_reason LIKE '%single_market_pnl_share%'
                             OR review_reason LIKE '%net_to_gross_exposure%'
                             OR review_reason LIKE '%maker_fraction%'
                         )
                        THEN 'missing_hygiene_components'
                    WHEN review_reason LIKE 'missing_required_score_components:%'
                         AND (
                             review_reason LIKE '%recent_30d_volume_usdc%'
                             OR review_reason LIKE '%total_volume_usdc%'
                             OR review_reason LIKE '%net_pnl_usdc%'
                         )
                        THEN 'missing_economic_components'
                    WHEN review_reason LIKE 'missing_required_score_components:%'
                        THEN 'missing_score_components'
                    WHEN review_reason LIKE 'insufficient_net_pnl_usdc:%'
                        THEN 'low_net_pnl'
                    WHEN review_reason LIKE 'insufficient_total_volume_usdc:%'
                        THEN 'low_total_volume'
                    WHEN review_reason LIKE 'insufficient_recent_30d_volume_usdc:%'
                        THEN 'low_recent_volume'
                    WHEN review_reason LIKE 'missing_economic_materiality:%'
                        THEN 'missing_economic_materiality'
                    WHEN review_reason LIKE 'insufficient_copy_backtest_net_pnl_usdc:%'
                        THEN 'copy_backtest_weak'
                    WHEN review_reason = 'hygiene_evidence_incomplete'
                        THEN 'hygiene_incomplete'
                    ELSE 'other_needs_data'
                END AS key
            FROM base
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY key
                    ORDER BY leader_score DESC, sort_at DESC, address ASC
                ) AS rn
            FROM bucketed
        )
        SELECT
            key,
            COUNT(*) AS count,
            ROUND(MAX(leader_score), 2) AS max_score,
            MAX(CASE WHEN rn = 1 THEN address ELSE '' END) AS example,
            MAX(CASE WHEN rn = 1 THEN review_reason ELSE '' END) AS example_reason
        FROM ranked
        GROUP BY key
        ORDER BY count DESC, max_score DESC, key ASC
        LIMIT ?
        """,
        (max(1, int(limit)),),
    )
    for row in rows:
        label, action = _needs_data_reason_metadata(str(row.get("key") or ""))
        row["reason"] = label
        row["next_action"] = action
    return rows


def _needs_data_reason_metadata(key: str) -> tuple[str, str]:
    return {
        "history_pending": ("历史证据不足", "继续 L1/L2/L3 补历史"),
        "not_scored": ("未评分", "触发评分常驻服务或检查评分输入"),
        "missing_wallet_features": ("缺钱包特征", "物化 wallet_features 后重评"),
        "missing_core_score_components": ("缺基础评分组件", "先物化 wallet_features，再分流补 copyability/hygiene"),
        "missing_copyability_components": ("缺 copyability 组件", "补 copyability 证据并重评"),
        "missing_hygiene_components": ("缺 hygiene/风险组件", "补风险特征并重评"),
        "missing_economic_components": ("缺经济指标", "补交易历史并物化特征"),
        "missing_score_components": ("缺评分组件", "补齐评分特征"),
        "low_net_pnl": ("净收益不足", "降低优先级或收紧发现来源"),
        "low_total_volume": ("累计交易量不足", "降低优先级或提高初筛成交额"),
        "low_recent_volume": ("近期交易量不足", "观察但不优先补深证据"),
        "missing_economic_materiality": ("缺经济门槛数据", "补历史并物化经济指标"),
        "copy_backtest_weak": ("copy 回测收益不足", "降低 copyability 优先级"),
        "hygiene_incomplete": ("hygiene 证据不足", "补保守风险特征"),
    }.get(key, ("其他 needs_data", "抽样检查评分原因"))


def _storage_health(settings: RobotSettings) -> dict[str, Any]:
    db_path = settings.db_path
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")
    storage: dict[str, Any] = {
        "db_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "wal_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
        "shm_bytes": shm_path.stat().st_size if shm_path.exists() else 0,
    }
    try:
        stat = os.statvfs(db_path.parent)
        storage["free_disk_bytes"] = stat.f_bavail * stat.f_frsize
        storage["total_disk_bytes"] = stat.f_blocks * stat.f_frsize
    except OSError:
        storage["free_disk_bytes"] = 0
        storage["total_disk_bytes"] = 0
    return storage


def _storage_maintenance_summary(
    settings: RobotSettings,
    *,
    wal_warn_bytes: int = WAL_WARN_BYTES,
    wal_critical_bytes: int = WAL_CRITICAL_BYTES,
    low_free_disk_bytes: int = LOW_FREE_DISK_BYTES,
    backup_max_age_seconds: int = BACKUP_MAX_AGE_SECONDS,
    now: int | None = None,
) -> dict[str, Any]:
    now = int(time.time()) if now is None else int(now)
    storage = _storage_health(settings)
    db_bytes = int(storage.get("db_bytes") or 0)
    wal_bytes = int(storage.get("wal_bytes") or 0)
    free_disk_bytes = int(storage.get("free_disk_bytes") or 0)
    total_disk_bytes = int(storage.get("total_disk_bytes") or 0)
    wal_to_db_ratio = round(wal_bytes / db_bytes, 4) if db_bytes > 0 else 0.0
    free_disk_ratio = round(free_disk_bytes / total_disk_bytes, 4) if total_disk_bytes > 0 else 0.0
    needs_wal_window = wal_bytes >= int(wal_warn_bytes)
    critical_wal = wal_bytes >= int(wal_critical_bytes)
    low_free_disk = bool(free_disk_bytes and free_disk_bytes < int(low_free_disk_bytes))
    backup_files = sorted(
        (
            path
            for path in settings.backup_dir.glob("pm_robot-*.sqlite")
            if path.name != "pm_robot-latest.sqlite"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    latest_backup = backup_files[0] if backup_files else None
    latest_backup_at = int(latest_backup.stat().st_mtime) if latest_backup else 0
    latest_backup_age_seconds = max(0, now - latest_backup_at) if latest_backup else None
    backup_fresh = bool(
        latest_backup
        and latest_backup_age_seconds is not None
        and latest_backup_age_seconds <= int(backup_max_age_seconds)
    )
    if critical_wal:
        state = "wal_critical"
        next_action = "WAL 已明显偏大，安排维护窗口执行 ./pmrobot-nas.sh wal-truncate-window 900。"
    elif needs_wal_window:
        state = "wal_attention"
        next_action = "WAL 超过常规阈值，等队列低峰时执行 ./pmrobot-nas.sh wal-truncate-window。"
    elif low_free_disk:
        state = "low_free_disk"
        next_action = "NAS 数据卷可用空间偏低，先清理备份或归档旧原始证据。"
    elif not latest_backup:
        state = "backup_missing"
        next_action = "尚无可验证数据库备份，立即执行 ./pmrobot-nas.sh backup-now。"
    elif not backup_fresh:
        state = "backup_stale"
        next_action = "最近数据库备份已过期，检查 backup-loop 并执行 ./pmrobot-nas.sh backup-now。"
    else:
        state = "ok"
        next_action = "存储处于常规范围，继续由 maintenance-loop 做轻量维护。"
    return {
        "state": state,
        "next_action": next_action,
        "storage": storage,
        "db_bytes": db_bytes,
        "wal_bytes": wal_bytes,
        "shm_bytes": int(storage.get("shm_bytes") or 0),
        "free_disk_bytes": free_disk_bytes,
        "total_disk_bytes": total_disk_bytes,
        "wal_to_db_ratio": wal_to_db_ratio,
        "free_disk_ratio": free_disk_ratio,
        "wal_warn_bytes": int(wal_warn_bytes),
        "wal_critical_bytes": int(wal_critical_bytes),
        "low_free_disk_bytes": int(low_free_disk_bytes),
        "needs_wal_window": needs_wal_window,
        "critical_wal": critical_wal,
        "low_free_disk": low_free_disk,
        "backup_count": len(backup_files),
        "latest_backup_name": latest_backup.name if latest_backup else "",
        "latest_backup_at": latest_backup_at,
        "latest_backup_age_seconds": latest_backup_age_seconds,
        "backup_max_age_seconds": int(backup_max_age_seconds),
        "backup_fresh": backup_fresh,
        "backup_now_command": "./pmrobot-nas.sh backup-now",
        "safe_command": "./pmrobot-nas.sh wal-truncate-window",
        "long_window_command": "./pmrobot-nas.sh wal-truncate-window 900",
        "idle_window_command": "./pmrobot-nas.sh wal-truncate-when-idle 7200 900 30",
        "read_only_check": "./pmrobot-nas.sh maintenance --dry-run",
        "maintenance_boundary": "WAL truncate 会临时停止 research/scoring 服务；不要在 worker 高峰时执行。",
    }


def _paper_pool_expansion_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 paper 扩池审计数据。</p>'
    wallets = values.get("wallets") or []
    scope = values.get("scope") or {}
    policy_warning = ""
    if values.get("policy_loaded") is False:
        policy_warning = (
            '<p class="empty">评分策略加载失败；当前阈值仅为故障回退值：'
            + _e(values.get("policy_error") or "unknown")
            + "</p>"
        )
    cards = [
        ("待审钱包", _fmt_int(values.get("wallet_count")), "needs_manual_review", "ok" if wallets else "warn"),
        ("近 Paper", _fmt_int(values.get("near_paper_count")), f">= {_fmt_num(scope.get('watch_min_score'))}", "ok" if int(values.get("near_paper_count") or 0) else "warn"),
        ("需 Copy 证据", _fmt_int(values.get("copyability_needed_count")), "市场/跟随缺口", "warn" if int(values.get("copyability_needed_count") or 0) else "ok"),
        ("最高分", _fmt_num(values.get("best_score")), f"paper {_fmt_num(scope.get('paper_min_score'))}", "ok" if float(values.get("best_score") or 0) >= float(scope.get("watch_min_score") or 0) else "warn"),
    ]
    if not wallets:
        return (
            '<div class="health-grid">'
            + "".join(
                f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
                for label, value, note, state in cards
            )
            + "</div>"
            + policy_warning
            + '<p class="empty">暂无 needs_manual_review 钱包。</p>'
        )
    body = []
    for row in wallets[:12]:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}<small>gap {_fmt_num(row.get("score_gap_to_paper"))}</small></td>'
            f'<td>{_badge(row.get("expansion_state"))}<small>{_e(row.get("next_action") or "")}</small></td>'
            f'<td class="num">{_fmt_int(row.get("copy_signal_events"))}<small>event gap {_fmt_int(row.get("copy_event_gap"))} · markets {_fmt_int(row.get("copy_signal_markets"))} · market gap {_fmt_int(row.get("copy_market_gap"))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))} events</div>'
            f'<small>{_e(row.get("evidence_tier") or "")} · {_e(row.get("evidence_status") or "")}</small></td>'
            f'<td>{_badge(row.get("hygiene_status") or "unknown")}<small>PnL ${_fmt_num(row.get("net_pnl_usdc"))} · vol ${_fmt_num(row.get("total_volume_usdc"))}</small></td>'
            "</tr>"
        )
    return (
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + policy_warning
        + _simple_table(values.get("state_counts") or [], ["state", "count"], ["扩池状态", "数量"])
        + _table(["钱包", "分数", "扩池判断", "Copy 证据", "历史证据", "Hygiene/PnL"], "".join(body))
        + '<p class="muted">JSON: /api/paper-pool-expansion · read-only，只做扩池审计，不自动升级 paper。</p>'
    )


def _top_review_candidates_panel(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无高分待验证钱包。</p>'
    body = []
    for row in rows:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason", ""))}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}</td>'
            f'<td>{_badge(row.get("blocker_label") or "待确认")}<small>{_e(row.get("review_next_action") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>{_e(row.get("review_stage", ""))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))} events</div>'
            f'<small>{_e(row.get("evidence_tier", ""))} · {_e(row.get("next_action", ""))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("qualified_follower_count"))} followers</div>'
            f'<small>{_fmt_int(row.get("copy_event_count"))} links · {_fmt_pct(row.get("copy_backtest_roi"))}</small></td>'
            f'<td>{_badge(row.get("copyability_status") or "none")}<small>{_e(row.get("copyability_scan_mode") or "unknown")} · p{_fmt_int(row.get("copyability_priority"))}</small></td>'
            "</tr>"
        )
    return _table(["钱包", "分数", "主阻塞", "阶段", "历史证据", "Copy 证据", "Copy 队列"], "".join(body))


def _needs_data_reason_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无 needs_data 钱包。</p>'
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f'<td>{_badge(row.get("reason"))}<small>{_e(row.get("key") or "")}</small></td>'
            f'<td class="num">{_fmt_int(row.get("count"))}</td>'
            f'<td>{_e(row.get("next_action") or "")}</td>'
            f'<td><a class="mono strong-link" href="/wallet/{_e(row.get("example") or "")}">{_short(row.get("example") or "")}</a>'
            f'<small>{_e(row.get("example_reason") or "")}</small></td>'
            "</tr>"
        )
    return _table(["原因", "数量", "建议动作", "样本"], "".join(body))


def _execution_preflight_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 execution-preflight 数据。</p>'
    wallets = values.get("wallets") or {}
    recent_buy = values.get("recent_paper_stage_buy") or {}
    observer = values.get("observer") or {}
    orders = values.get("paper_orders") or {}
    publish = values.get("publish") or {}
    realtime = values.get("paper_realtime_coverage") or {}
    rtds_runtime = values.get("rtds_runtime_diagnostics") or {}
    profile = values.get("execution_profile") or {}
    window = values.get("signal_window") or {}
    ready = bool(values.get("ready_to_start_execution"))
    running_services = profile.get("running_services") or []
    cards = [
        (
            "Paper 钱包",
            _fmt_int(wallets.get("paper_stage_wallets")),
            f"approved {_fmt_int(wallets.get('paper_approved'))}",
            "ok" if int(wallets.get("paper_stage_wallets") or 0) else "warn",
        ),
        (
            "当前 BUY",
            _fmt_int(recent_buy.get("events")),
            f"窗口 {_fmt_int(window.get('max_signal_age_sec'))} 秒",
            "ok" if int(recent_buy.get("events") or 0) else "warn",
        ),
        (
            "Actionable",
            _fmt_int(observer.get("actionable")),
            f"{_fmt_int(observer.get('evaluations'))} 次评估",
            "ok" if int(observer.get("actionable") or 0) else "warn",
        ),
        (
            "RTDS BUY",
            _fmt_int(realtime.get("rtds_buy_events_24h")),
            f"当前 {_fmt_int(realtime.get('current_rtds_buy_events'))}",
            "ok" if int(realtime.get("current_rtds_buy_events") or 0) else "warn",
        ),
        (
            "RTDS Paper 匹配",
            _fmt_int(rtds_runtime.get("paper_matches")),
            f"rows {_fmt_int(rtds_runtime.get('paper_rows'))}",
            "ok" if int(rtds_runtime.get("paper_matches") or 0) else "warn",
        ),
        (
            "RTDS 流进度",
            _fmt_int(rtds_runtime.get("message_delta")),
            f"{_duration_label(rtds_runtime.get('progress_window_sec'))}",
            "ok" if int(rtds_runtime.get("message_delta") or 0) > 0 else "warn",
        ),
        (
            "RTDS Watch 匹配",
            _fmt_int(rtds_runtime.get("watch_matches")),
            f"eligible {_fmt_int(rtds_runtime.get('watch_eligible'))}",
            "ok" if int(rtds_runtime.get("watch_matches") or 0) else "warn",
        ),
        (
            "及时 BUY",
            _fmt_int(realtime.get("timely_buy_events")),
            f"延迟 {_fmt_int(realtime.get('delayed_current_buy_events'))}",
            "ok" if int(realtime.get("timely_buy_events") or 0) else "warn",
        ),
        (
            "Paper Orders",
            _fmt_int(orders.get("paper_stage_orders")),
            f"近期 {_fmt_int(orders.get('paper_stage_recent_orders'))}",
            "ok" if int(orders.get("paper_stage_orders") or 0) else "warn",
        ),
        (
            "正式发布",
            _fmt_int(publish.get("active")),
            f"revoked {_fmt_int(publish.get('revoked'))}",
            "ok" if int(publish.get("active") or 0) else "warn",
        ),
        (
            "Execution 服务",
            _fmt_int(len(running_services)),
            ", ".join(running_services) if running_services else "未运行",
            "warn" if running_services else "ok",
        ),
    ]
    detail_rows = [
        {"key": "state", "value": values.get("state") or ""},
        {"key": "ready_to_start_execution", "value": str(ready)},
        {"key": "recommended_action", "value": values.get("recommended_action") or ""},
        {"key": "paper_realtime_state", "value": realtime.get("state") or ""},
        {"key": "paper_realtime_next_action", "value": realtime.get("next_action") or ""},
        {"key": "rtds_runtime_state", "value": rtds_runtime.get("state") or ""},
        {"key": "rtds_runtime_next_action", "value": rtds_runtime.get("next_action") or ""},
        {"key": "rtds_stream_state", "value": rtds_runtime.get("stream_state") or ""},
        {"key": "rtds_stream_next_action", "value": rtds_runtime.get("stream_next_action") or ""},
        {"key": "rtds_heartbeat_age", "value": _duration_label(rtds_runtime.get("heartbeat_age_sec"))},
        {"key": "rtds_progress_samples", "value": _fmt_int(rtds_runtime.get("progress_samples"))},
        {"key": "rtds_message_delta", "value": _fmt_int(rtds_runtime.get("message_delta"))},
        {"key": "rtds_selected_delta", "value": _fmt_int(rtds_runtime.get("selected_delta"))},
        {"key": "rtds_wallet_keys", "value": rtds_runtime.get("paper_wallet_keys") or ""},
        {"key": "rtds_paper_rows", "value": _fmt_int(rtds_runtime.get("paper_rows"))},
        {"key": "rtds_paper_wallet_rows", "value": _fmt_int(rtds_runtime.get("paper_wallet_rows"))},
        {"key": "rtds_paper_matches", "value": _fmt_int(rtds_runtime.get("paper_matches"))},
        {"key": "rtds_watch_eligible", "value": _fmt_int(rtds_runtime.get("watch_eligible"))},
        {"key": "rtds_watch_matches", "value": _fmt_int(rtds_runtime.get("watch_matches"))},
        {"key": "rtds_watch_events", "value": _fmt_int(rtds_runtime.get("watch_events"))},
        {"key": "write_boundary", "value": values.get("write_boundary") or ""},
        {"key": "compose_check", "value": profile.get("compose_error") or "ok"},
        {"key": "latest_buy", "value": _fmt_ts(recent_buy.get("latest_ts"))},
        {"key": "latest_rtds", "value": _fmt_ts(realtime.get("latest_rtds_ts"))},
        {"key": "max_buy_ingest_lag", "value": _duration_label(realtime.get("max_buy_ingest_lag_sec"))},
        {"key": "latest_observer", "value": _fmt_ts(observer.get("latest_evaluated_at"))},
        {"key": "latest_paper_order", "value": _fmt_ts(orders.get("paper_stage_latest_order_at"))},
    ]
    heartbeat_rows = []
    for row in values.get("heartbeats") or []:
        heartbeat_rows.append(
            {
                "name": row.get("name"),
                "status": row.get("status"),
                "finished_at": _fmt_ts(row.get("finished_at")),
                "rows_written": row.get("rows_written"),
                "error": row.get("error") or "",
            }
        )
    return (
        f'<div class="health-banner {"ok" if ready else "attention"}"><strong>{_e(values.get("recommended_action") or "")}</strong>'
        f'<span>execution-preflight · {_e(values.get("state") or "")}</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + _simple_table(detail_rows, ["key", "value"], ["检查项", "值"])
        + '<h3 class="subhead">运行心跳</h3>'
        + _simple_table(heartbeat_rows, ["name", "status", "finished_at", "rows_written", "error"], ["任务", "状态", "最近完成", "行数", "错误"])
    )


def _paper_realtime_audit_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 paper realtime audit 数据。</p>'
    wallets = values.get("wallets") or []
    blocker_counts = values.get("blocker_counts") or []
    window = values.get("signal_window") or {}
    if not wallets:
        return (
            '<p class="empty">暂无 paper-stage 钱包。</p>'
            + _simple_table(blocker_counts, ["blocker", "count"], ["实时阻塞", "数量"])
        )
    cards = [
        ("审计钱包", _fmt_int(values.get("wallet_count")), f"lookback {_duration_label(window.get('lookback_sec'))}", "ok"),
        ("可跟窗口", _fmt_int(window.get("max_signal_age_sec")), "秒", "ok"),
        (
            "RTDS 当前命中",
            _fmt_int(sum(int(row.get("current_rtds_buy_events") or 0) for row in wallets)),
            "paper BUY",
            "ok" if any(int(row.get("current_rtds_buy_events") or 0) for row in wallets) else "warn",
        ),
        (
            "Actionable",
            _fmt_int(sum(int(row.get("observer_actionable") or 0) for row in wallets)),
            "observer",
            "ok" if any(int(row.get("observer_actionable") or 0) for row in wallets) else "warn",
        ),
        (
            "延迟补进",
            _fmt_int(sum(int(row.get("delayed_buy_events_24h") or 0) for row in wallets)),
            "24h BUY",
            "warn" if any(int(row.get("delayed_buy_events_24h") or 0) for row in wallets) else "ok",
        ),
    ]
    body = []
    for row in wallets:
        address = row.get("address") or ""
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("sources") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>score {_fmt_num(row.get("leader_score"))}</small></td>'
            f'<td>{_badge(row.get("realtime_blocker"))}<small>{_e(row.get("next_action") or "")}</small></td>'
            f'<td class="num">{_fmt_int(row.get("buy_events_24h"))}<small>current {_fmt_int(row.get("current_buy_events"))}</small></td>'
            f'<td class="num">{_fmt_int(row.get("rtds_buy_events_24h"))}<small>current {_fmt_int(row.get("current_rtds_buy_events"))}</small></td>'
            f'<td class="num">{_fmt_int(row.get("timely_buy_events_24h"))}<small>current {_fmt_int(row.get("timely_buy_events"))}</small></td>'
            f'<td class="num">{_fmt_int(row.get("delayed_buy_events_24h"))}<small>current {_fmt_int(row.get("delayed_current_buy_events"))}</small></td>'
            f'<td>{_fmt_ts(row.get("latest_buy_ts"))}<small>max {_duration_label(row.get("max_buy_ingest_lag_sec"))} · avg {_duration_label(row.get("avg_buy_ingest_lag_sec"))}</small></td>'
            f'<td>{_fmt_ts(row.get("latest_rtds_ts"))}<small>observer {_fmt_int(row.get("observer_actionable"))}/{_fmt_int(row.get("observer_evaluations"))}</small></td>'
            "</tr>"
        )
    return (
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + _simple_table(blocker_counts, ["blocker", "count"], ["实时阻塞", "数量"])
        + '<h3 class="subhead">Paper 钱包逐个卡点</h3>'
        + _table(["钱包", "阶段/分数", "当前卡点", "BUY 24h", "RTDS BUY", "及时 BUY", "延迟补进", "最近 BUY", "最近 RTDS/报价"], "".join(body))
        + '<p class="muted">JSON: /api/paper-realtime-audit · read-only，不会写 paper_orders 或发布库。</p>'
    )


def _rtds_watch_audit_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 RTDS watch 数据。</p>'
    wallets = values.get("wallets") or []
    scope = values.get("scope") or {}
    state_counts = values.get("state_counts") or []
    if not wallets:
        return (
            '<p class="empty">暂无近 paper watch 钱包。</p>'
            + _simple_table(state_counts, ["state", "count"], ["状态", "数量"])
            + '<p class="muted">JSON: /api/rtds-watch-audit · research-only，不会升级 paper 或发布。</p>'
        )
    cards = [
        ("Watch 钱包", _fmt_int(values.get("wallet_count")), f"score >= {_fmt_num(scope.get('min_score'))}", "ok"),
        (
            "Watch 命中",
            _fmt_int(sum(int(row.get("watch_events_24h") or 0) for row in wallets)),
            "24h RTDS",
            "ok" if any(int(row.get("watch_events_24h") or 0) for row in wallets) else "warn",
        ),
        (
            "当前命中",
            _fmt_int(sum(int(row.get("current_watch_events") or 0) for row in wallets)),
            "5m",
            "ok" if any(int(row.get("current_watch_events") or 0) for row in wallets) else "warn",
        ),
        (
            "Copy 市场",
            _fmt_int(max((int(row.get("copy_market_count") or 0) for row in wallets), default=0)),
            "max",
            "ok" if any(int(row.get("copy_market_count") or 0) > 1 for row in wallets) else "warn",
        ),
    ]
    body = []
    for row in wallets:
        address = row.get("address") or ""
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}<small>{_badge(row.get("candidate_stage"))}</small></td>'
            f'<td>{_badge(row.get("watch_state"))}<small>{_e(row.get("next_action") or "")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))} events</div>'
            f'<small>{_e(row.get("discovery_tier") or "")} · {_fmt_int(row.get("distinct_markets"))} markets</small></td>'
            f'<td class="num">{_fmt_int(row.get("copy_event_count"))}<small>{_fmt_int(row.get("copy_market_count"))} markets · {_fmt_pct(row.get("copy_stream_roi"))}</small></td>'
            f'<td>{_badge(row.get("hygiene_status") or "unknown")}<small>pnl {_fmt_num(row.get("net_pnl_usdc"))} · vol {_fmt_num(row.get("total_volume_usdc"))}</small></td>'
            f'<td class="num">{_fmt_int(row.get("watch_events_24h"))}<small>buy {_fmt_int(row.get("watch_buy_events_24h"))}</small></td>'
            f'<td>{_fmt_ts(row.get("latest_watch_ts"))}<small>lag {_duration_label(row.get("max_watch_ingest_lag_sec"))}</small></td>'
            "</tr>"
        )
    return (
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + _simple_table(state_counts, ["state", "count"], ["Watch 状态", "数量"])
        + '<h3 class="subhead">近 Paper 钱包实时观察</h3>'
        + _table(["钱包", "分数/阶段", "Watch 状态", "历史证据", "Copy", "Hygiene/PnL", "RTDS 24h", "最近 RTDS"], "".join(body))
        + '<p class="muted">JSON: /api/rtds-watch-audit · research-only，不会升级 paper 或发布。</p>'
    )


def _production_readiness_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无收敛数据。</p>'
    has_paper = int(values.get("paper_stage_wallets") or 0) > 0
    banner_state = "ok" if has_paper else "attention"
    publish_gate = values.get("formal_publish_gate") or {}
    cards = [
        ("Paper 候选", _fmt_int(values.get("paper_stage_wallets")), "可进入外部验证", "ok" if has_paper else "warn"),
        (
            "正式钱包",
            _fmt_int(publish_gate.get("active_published_leaders")),
            publish_gate.get("state") or "active publish",
            "ok" if int(publish_gate.get("active_published_leaders") or 0) else "warn",
        ),
        (
            "撤销发布",
            _fmt_int(publish_gate.get("revoked_published_leaders")),
            "历史记录，不计正式",
            "warn" if int(publish_gate.get("revoked_published_leaders") or 0) else "ok",
        ),
        (
            "纸面质量",
            f"{_fmt_int(publish_gate.get('paper_stage_with_quality'))}/{_fmt_int(publish_gate.get('paper_stage_wallets'))}",
            "paper_stage with quality",
            "ok" if int(publish_gate.get("paper_stage_missing_quality") or 0) == 0 and has_paper else "warn",
        ),
        (
            "Live eligible",
            _fmt_int(publish_gate.get("live_eligible_wallets")),
            "publish 前置阶段",
            "ok" if int(publish_gate.get("live_eligible_wallets") or 0) else "warn",
        ),
        ("观察最高分", _fmt_num(values.get("max_manual_score")), f"阈值 {_fmt_num(values.get('paper_min_score'))}", "warn" if float(values.get("score_gap_to_paper") or 0) > 0 else "ok"),
        ("近阈值观察", _fmt_int(values.get("manual_near_threshold")), f">= {_fmt_num(values.get('near_score_floor'))}", "ok" if int(values.get("manual_near_threshold") or 0) else "warn"),
        (
            "历史待补",
            _fmt_int(values.get("evidence_pending")),
            f"活动 {_fmt_int(values.get('evidence_active_pending'))} · 待派 {_fmt_int(values.get('evidence_state_pending'))}",
            "warn" if int(values.get("evidence_pending") or 0) else "ok",
        ),
        ("Copy 待补", _fmt_int(values.get("copyability_pending")), "copyability_evidence", "warn" if int(values.get("copyability_pending") or 0) else "ok"),
        (
            "及时可跟",
            _fmt_int(values.get("observer_actionable_signals")),
            f"{_fmt_int(values.get('observer_actionable_wallets'))} 钱包 · {_fmt_int(values.get('observer_evaluations'))} 评估",
            "ok" if int(values.get("observer_actionable_signals") or 0) else "warn",
        ),
        (
            "过时/缺盘口",
            f"{_fmt_int(values.get('observer_stale_signals'))}/{_fmt_int(values.get('observer_quote_errors'))}",
            "stale / quote errors",
            "warn" if int(values.get("observer_stale_signals") or 0) or int(values.get("observer_quote_errors") or 0) else "ok",
        ),
    ]
    rows = [
        {"key": "needs_manual_review", "count": values.get("needs_manual_review")},
        {"key": "blocked_hygiene", "count": values.get("blocked_hygiene")},
        {"key": "blocked_copyability", "count": values.get("blocked_copyability")},
        {"key": "needs_data", "count": values.get("needs_data")},
        {"key": "top_blocker", "count": values.get("top_blocker") or ""},
    ]
    action_rows = values.get("manual_review_actions") or []
    return (
        f'<div class="health-banner {banner_state}"><strong>{_e(values.get("next_action"))}</strong>'
        f'<span>{_e(values.get("state"))} · 距阈值 {_fmt_num(values.get("score_gap_to_paper"))}</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + _simple_table(rows, ["key", "count"], ["指标", "值"])
        + '<h3 class="subhead">正式发布门槛</h3>'
        + f'<div class="health-banner {"ok" if int(publish_gate.get("active_published_leaders") or 0) else "attention"}">'
        f'<strong>{_e(publish_gate.get("current_formal_status") or publish_gate.get("next_action") or "")}</strong>'
        f'<span>{_e(publish_gate.get("state") or "")} · {_e(publish_gate.get("root_formal_blocker") or "formal_ok")}</span></div>'
        + _simple_table(publish_gate.get("gate_rows") or [], ["gate", "status", "count", "next_action"], ["门槛", "状态", "数量/值", "下一步"])
        + '<h3 class="subhead">当前正式钱包</h3>'
        + _formal_wallet_rows_panel(publish_gate.get("active_formal_wallets") or [])
        + '<h3 class="subhead">Paper 到正式缺口</h3>'
        + _paper_stage_gap_rows_panel(publish_gate.get("paper_stage_gap_wallets") or [])
        + '<h3 class="subhead">正式阻塞分布</h3>'
        + _simple_table(publish_gate.get("formal_blocker_rows") or [], ["blocker", "count", "next_action", "example"], ["阻塞", "钱包数", "下一步", "样本"])
        + '<h3 class="subhead">复核停靠分布</h3>'
        + _simple_table(action_rows, ["blocker", "count", "max_score", "next_action", "example"], ["动作", "数量", "最高分", "下一步", "例子"])
    )


def _formal_wallet_rows_panel(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">当前正式钱包为 0；历史 revoked 发布不计入正式钱包。</p>'
    body = []
    for row in rows:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td>{_badge(row.get("publish_stage") or "")}<small>{_badge(row.get("status") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}</td>'
            f'<td>{_fmt_ts(row.get("published_at"))}</td>'
            f'<td>{_fmt_ts(row.get("expires_at"))}</td>'
            "</tr>"
        )
    return _table(["钱包", "发布阶段", "分数", "发布时间", "过期时间"], "".join(body))


def _paper_stage_gap_rows_panel(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无 paper-stage 钱包，因此也没有 paper 到正式的缺口。</p>'
    body = []
    for row in rows:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage") or "")}<small>{_e(row.get("formal_blockers") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}</td>'
            f'<td><div class="numline">{_fmt_int(row.get("paper_orders"))} orders</div>'
            f'<small>{_fmt_int(row.get("paper_settled_positions"))} settled · ready {_fmt_int(row.get("paper_ready"))}</small></td>'
            f'<td>{_e(row.get("formal_next_action") or "")}</td>'
            "</tr>"
        )
    return _table(["钱包", "阶段/阻塞", "分数", "纸面质量", "下一步"], "".join(body))


def _paper_handoff_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 paper 交接数据。</p>'
    wallets = values.get("wallets") or []
    state_counts = values.get("state_counts") or []
    waiting = sum(int(row.get("count") or 0) for row in state_counts if row.get("state") == "awaiting_external_paper")
    waiting_actionable = sum(int(row.get("count") or 0) for row in state_counts if row.get("state") == "awaiting_actionable_signal")
    observing = sum(int(row.get("count") or 0) for row in state_counts if row.get("state") == "paper_observing")
    passed = sum(int(row.get("count") or 0) for row in state_counts if row.get("state") in {"paper_passed", "published"})
    paper_loop_label = "未启用" if values.get("nas_paper_loop_enabled") is False else "未验证"
    cards = [
        ("交接钱包", _fmt_int(values.get("candidate_count")), "paper_candidate+", "ok" if wallets else "warn"),
        ("当前显示", _fmt_int(values.get("visible_wallet_count")), "页面样本", "ok"),
        ("研究证据完整", _fmt_int(values.get("visible_research_ready")), f"当前显示 / 阈值 {_fmt_num(values.get('paper_min_score'))}", "ok" if int(values.get("visible_research_ready") or 0) else "warn"),
        ("证据不完整", _fmt_int(values.get("visible_research_incomplete")), "paper-stage 但未过 4/4", "warn" if int(values.get("visible_research_incomplete") or 0) else "ok"),
        ("NAS Paper Loop", paper_loop_label, values.get("paper_loop_status") or "", "ok" if values.get("nas_paper_loop_enabled") is False else "warn"),
        ("等待新信号", _fmt_int(waiting_actionable), "actionable BUY", "warn" if waiting_actionable else "ok"),
        ("等待外部 paper", _fmt_int(waiting), "研究已批准但未观察", "warn" if waiting else "ok"),
        ("paper 观察中", _fmt_int(observing), "已有纸面成交", "ok" if observing else "warn"),
        ("发布前通过", _fmt_int(passed), "paper_passed/published", "ok" if passed else "warn"),
    ]
    return (
        f'<div class="health-banner {"attention" if values.get("research_only") else "ok"}">'
        f'<strong>{_e(values.get("boundary"))}</strong>'
        f'<span>{_e(values.get("next_action"))}</span></div>'
        '<p class="muted"><a class="button secondary" href="/api/paper-handoff">JSON 交接出口</a> '
        '<a class="button secondary" href="/api/paper-handoff.csv">CSV 表格出口</a></p>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + '<h3 class="subhead">候选阶段分布</h3>'
        + _simple_table(values.get("stage_counts") or [], ["stage", "count"], ["阶段", "数量"])
        + '<h3 class="subhead">证据不完整告警</h3>'
        + _simple_table(values.get("incomplete_research_wallets") or [], ["address", "candidate_stage", "leader_score", "missing"], ["钱包", "阶段", "分数", "缺口"])
        + '<h3 class="subhead">交接状态分布</h3>'
        + _simple_table(state_counts, ["state", "count"], ["状态", "数量"])
        + '<h3 class="subhead">待观察钱包</h3>'
        + _paper_handoff_wallets_table(wallets)
    )


def _paper_handoff_detail_panel(row: dict[str, Any]) -> str:
    if not row:
        return '<p class="empty">该钱包还不在 paper 交接池。</p>'
    checks = row.get("research_checks") or []
    ready = bool(row.get("research_ready"))
    observer_quality_actionable = int(row.get("observer_quality_actionable") or 0)
    cards = [
        ("研究检查", f"{_fmt_int(row.get('research_check_passed'))}/{_fmt_int(row.get('research_check_total'))}", row.get("research_check_summary") or "", "ok" if ready else "warn"),
        ("NAS Paper 状态", row.get("paper_execution_state") or "unknown", "research/scoring 不自动下单", "ok" if row.get("paper_execution_state") == "not_started_on_nas" else "warn"),
        ("Paper 成交", _fmt_int(row.get("paper_orders")), f"{_fmt_int(row.get('paper_settled_positions'))} settled", "ok" if int(row.get("paper_orders") or 0) else "warn"),
        ("及时可跟", _fmt_int(row.get("observer_actionable_signals")), f"{_fmt_int(row.get('observer_evaluations'))} observer evals", "ok" if int(row.get("observer_actionable_signals") or 0) else "warn"),
        (
            "只读质量",
            row.get("observer_quality_state") or "none",
            f"{_fmt_int(row.get('observer_quality_evaluations'))} eval · {_fmt_num(row.get('observer_quality_actionable_rate_pct'))}% actionable",
            "ok" if observer_quality_actionable > 0 else "warn",
        ),
        ("正式阻塞", _fmt_int(len(row.get("formal_blocker_list") or [])), row.get("formal_next_action") or "", "warn" if row.get("formal_blocker_list") else "ok"),
        ("Copy 验证", f"{_fmt_int(row.get('qualified_follower_count'))} followers", f"{_fmt_int(row.get('backtest_trade_count'))} backtest", "ok" if bool(row.get("research_ready")) else "warn"),
    ]
    return (
        f'<div class="health-banner {"ok" if ready else "attention"}"><strong>{_e(row.get("research_check_summary") or "")}</strong>'
        f'<span>{_e(row.get("next_action") or "")}</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + _simple_table(checks, ["label", "passed", "value"], ["检查", "通过", "值"])
        + _simple_table(
            [
                {"key": "formal_blockers", "value": row.get("formal_blockers") or ""},
                {"key": "formal_next_action", "value": row.get("formal_next_action") or ""},
            ],
            ["key", "value"],
            ["正式化诊断", "值"],
        )
        + _simple_table(
            [
                {"key": "observer_quality_state", "value": row.get("observer_quality_state") or ""},
                {"key": "observer_quality_window", "value": _duration_label(row.get("observer_quality_window_sec"))},
                {"key": "observer_quality_evaluations", "value": row.get("observer_quality_evaluations") or 0},
                {"key": "observer_quality_actionable_rate", "value": f"{_fmt_num(row.get('observer_quality_actionable_rate_pct'))}%"},
                {"key": "observer_quality_avg_signal_age", "value": _duration_label(row.get("observer_quality_avg_signal_age_sec"))},
                {"key": "observer_quality_max_signal_age", "value": _duration_label(row.get("observer_quality_max_signal_age_sec"))},
                {"key": "observer_quality_quote_errors", "value": row.get("observer_quality_quote_errors") or 0},
                {"key": "observer_quality_stale", "value": row.get("observer_quality_stale") or 0},
                {"key": "observer_quality_next_action", "value": row.get("observer_quality_next_action") or ""},
            ],
            ["key", "value"],
            ["只读 observer 质量", "值"],
        )
    )


def _paper_handoff_wallets_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无已批准 paper 钱包。</p>'
    body = []
    for row in rows:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}</td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>{_badge(row.get("handoff_state"))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))} events</div>'
            f'<small>{_e(row.get("evidence_tier") or "")} · {_e(row.get("hygiene_status") or "hygiene?")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("copy_event_count"))} links</div>'
            f'<small>{_fmt_int(row.get("qualified_follower_count"))} followers · {_fmt_pct(row.get("copy_backtest_roi"))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("research_check_passed"))}/{_fmt_int(row.get("research_check_total"))}</div>'
            f'<small>{_e(row.get("research_check_summary") or "")} · {_badge(row.get("paper_execution_state") or "")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("observer_actionable_signals"))} actionable</div>'
            f'<small>{_fmt_int(row.get("observer_accepted_signals"))} accepted · {_fmt_int(row.get("observer_stale_signals"))} stale · {_fmt_int(row.get("observer_quote_errors"))} errors</small>'
            f'<small>{_badge(row.get("observer_quality_state") or "")} quality {_fmt_int(row.get("observer_quality_evaluations"))} eval · {_fmt_num(row.get("observer_quality_actionable_rate_pct"))}% actionable</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("paper_orders"))} orders</div>'
            f'<small>{_fmt_pct(row.get("paper_total_roi"))} ROI · ready {_fmt_int(row.get("paper_ready"))}</small></td>'
            f'<td>{_e(row.get("formal_next_action") or row.get("next_action") or "")}'
            f'<small>{_e(row.get("formal_blockers") or "")}</small></td>'
            "</tr>"
        )
    return _table(["钱包", "分数", "交接状态", "历史/hygiene", "Copy 证据", "研究检查", "Observer", "Paper 观察", "下一步"], "".join(body))


def _paper_observer_preview_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 paper observer 预览数据。</p>'
    signal_count = int(values.get("signals_seen") or 0)
    max_age_hours = round(float(values.get("max_signal_age_sec") or 0) / 3600, 2)
    latest_buy_age = _duration_label(values.get("latest_buy_age_sec"))
    max_ingest_lag = values.get("recent_buy_max_ingest_lag_sec")
    suggested = values.get("suggested_window") or {}
    cards = [
        ("可观察信号", _fmt_int(signal_count), "eligible recent BUY", "ok" if signal_count else "warn"),
        ("交接钱包", _fmt_int(values.get("paper_stage_wallets")), "paper-stage wallets", "ok" if int(values.get("paper_stage_wallets") or 0) else "warn"),
        ("窗口内 BUY", _fmt_int(values.get("recent_buy_events")), "before eligibility/dedupe", "ok" if int(values.get("recent_buy_events") or 0) else "warn"),
        ("最近 BUY", latest_buy_age, "latest paper-stage BUY", "ok" if signal_count else "warn"),
        (
            "入库延迟",
            _duration_label(max_ingest_lag) if max_ingest_lag is not None else "无",
            "窗口内 BUY max ingested_at - timestamp",
            "warn" if int(max_ingest_lag or 0) > 300 else "ok",
        ),
        ("建议窗口", suggested.get("window_label") or "无", suggested.get("mode") or "no signals", "ok" if suggested else "warn"),
        ("观察窗口", f"{max_age_hours:g} 小时", "max_signal_age_sec", "ok"),
        ("写入范围", values.get("write_scope") or "unknown", "preview only", "ok" if values.get("read_only") else "warn"),
    ]
    suggested_link = ""
    if suggested.get("api_path"):
        suggested_link = f'<a class="button secondary" href="{_e(suggested.get("api_path"))}">打开 {_e(suggested.get("window_label"))} 复盘</a>'
    return (
        f'<div class="health-banner {"ok" if signal_count else "attention"}"><strong>{_e(values.get("boundary"))}</strong>'
        f'<span>{_e(values.get("next_action"))}</span></div>'
        '<p class="muted"><a class="button secondary" href="/api/paper-observer-preview">JSON 观察预览</a>'
        f'{suggested_link}</p>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        )
        + "</div>"
        + _simple_table(
            [
                {"key": "no_signal_reason", "value": values.get("no_signal_reason") or ""},
                {"key": "latest_buy_at", "value": _fmt_ts(values.get("latest_buy_ts"))},
                {"key": "latest_buy_ingested_at", "value": _fmt_ts(values.get("latest_buy_ingested_at"))},
                {"key": "recent_buy_avg_ingest_lag", "value": _duration_label(values.get("recent_buy_avg_ingest_lag_sec"))},
                {"key": "recent_buy_max_ingest_lag", "value": _duration_label(values.get("recent_buy_max_ingest_lag_sec"))},
                {"key": "latest_activity_at", "value": _fmt_ts(values.get("latest_activity_ts"))},
                {"key": "latest_activity_ingested_at", "value": _fmt_ts(values.get("latest_activity_ingested_at"))},
            ],
            ["key", "value"],
            ["诊断", "值"],
        )
        + '<h3 class="subhead">观察窗口对比</h3>'
        + _simple_table(
            values.get("window_diagnostics") or [],
            ["window_label", "recent_buy_events", "eligible_signals", "avg_ingest_lag", "max_ingest_lag", "no_signal_reason"],
            ["窗口", "BUY 数", "合格信号", "均入库延迟", "最大入库延迟", "无信号原因"],
        )
        + _paper_observer_signals_table(values.get("signals") or [])
    )


def _paper_observer_evaluation_panel(values: dict[str, Any]) -> str:
    if not values or values.get("state") == "missing":
        return '<p class="empty">暂无 paper observer 报价评估文件。</p>'
    state = str(values.get("state") or "")
    accepted = int(values.get("accepted_signals") or 0)
    actionable = int(values.get("actionable_signals") or 0)
    attempted = int(values.get("quotes_attempted") or 0)
    history = values.get("history") or {}
    cards = [
        ("状态", state, "paper_observer_evaluation.json", "ok" if state == "current" else "warn"),
        ("信号", _fmt_int(values.get("signals_seen")), "eligible signals", "ok" if attempted else "warn"),
        ("报价成功", f"{_fmt_int(values.get('quotes_succeeded'))}/{_fmt_int(attempted)}", "CLOB book", "ok" if int(values.get("quotes_succeeded") or 0) else "warn"),
        ("可模拟成交", _fmt_int(accepted), "accepted by paper broker", "ok" if accepted else "warn"),
        ("可及时跟", _fmt_int(actionable), f"<= {_fmt_int(values.get('max_actionable_signal_age_sec'))} 秒", "ok" if actionable else "warn"),
        ("过时信号", _fmt_int(values.get("stale_signal_rejections")), "quoteable but too old", "warn" if int(values.get("stale_signal_rejections") or 0) else "ok"),
        ("拒绝", _fmt_int(values.get("rejected_signals")), "quote/risk/depth", "warn" if int(values.get("rejected_signals") or 0) else "ok"),
        ("平均滑点", _fmt_num(values.get("average_slippage_bps")), "bps, accepted only", "ok" if accepted else "warn"),
        ("平均延迟", _fmt_num(values.get("average_latency_ms")), "ms", "ok"),
        ("历史可跟率", f"{_fmt_num(history.get('actionable_rate_pct'))}%", f"{_fmt_int(history.get('actionable'))}/{_fmt_int(history.get('total_evaluations'))}", "ok" if int(history.get("actionable") or 0) else "warn"),
        ("写入范围", values.get("write_scope") or "unknown", "evaluation only", "ok" if values.get("read_only") else "warn"),
    ]
    return (
        f'<div class="health-banner {"ok" if actionable else "attention"}"><strong>{_e(values.get("boundary"))}</strong>'
        f'<span>{_e(values.get("next_action"))}</span></div>'
        '<p class="muted"><a class="button secondary" href="/api/paper-observer-evaluation">JSON 报价评估</a></p>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {card_state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, card_state in cards
        )
        + "</div>"
        + '<h3 class="subhead">长期报价证据</h3>'
        + _simple_table(
            history.get("wallets_summary") or [],
            ["wallet", "signals", "accepted", "actionable", "actionable_rate_pct", "avg_slippage_bps", "quote_errors", "stale_signals", "latest_evaluated_at"],
            ["钱包", "评估", "盘口通过", "及时可跟", "可跟率 %", "均滑点", "报价错误", "过时", "最近评估"],
        )
        + '<h3 class="subhead">可行动判断</h3>'
        + _simple_table(history.get("actionability_reason_counts") or [], ["reason", "count"], ["原因", "数量"])
        + '<h3 class="subhead">报价拒绝原因</h3>'
        + _simple_table(history.get("reason_counts") or [], ["reason", "count"], ["原因", "数量"])
        + '<h3 class="subhead">当前报价样本</h3>'
        + _paper_observer_evaluation_table(values.get("evaluations") or [])
    )


def _paper_observer_evaluation_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">当前没有可展示的报价评估信号。</p>'
    body = []
    for row in rows[:25]:
        wallet = str(row.get("wallet") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a>'
            f'<small>{_e(row.get("signal_id") or "")}</small></td>'
            f'<td><div class="numline">{_e(row.get("market_slug") or "")}</div>'
            f'<small>{_e(row.get("outcome") or "")} · {_e(row.get("side") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_price"))}</td>'
            f'<td class="num">{_fmt_num(row.get("best_ask"))}</td>'
            f'<td class="num">{_fmt_num(row.get("executable_price"))}</td>'
            f'<td class="num">{_fmt_num(row.get("slippage_bps"))}</td>'
            f'<td>{_badge("actionable" if row.get("actionable") else ("accepted" if row.get("accepted") else "rejected"))}'
            f'<small>{_e(row.get("actionability_reason") or row.get("decision_reason") or row.get("quote_error") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("quote_latency_ms"))}</td>'
            "</tr>"
        )
    return _table(["钱包", "市场", "领头价", "Ask", "模拟成交", "滑点 bps", "判断", "延迟 ms"], "".join(body))


def _paper_observer_signals_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">当前观察窗口没有合格新信号。</p>'
    body = []
    for row in rows:
        wallet = str(row.get("wallet") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a>'
            f'<small>{_e(row.get("signal_id") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>{_e(row.get("validation_cohort") or "")}</small></td>'
            f'<td><div class="numline">{_e(row.get("market_slug") or "")}</div>'
            f'<small>{_e(row.get("outcome") or "")} · {_e(row.get("side") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_price"))}</td>'
            f'<td><div class="numline">{_fmt_num(row.get("leader_score"))}</div>'
            f'<small>{_fmt_int(row.get("copy_event_count"))} copy · {_e(row.get("hygiene_status") or "")}</small></td>'
            f'<td>{_fmt_ts(row.get("detected_at"))}'
            f'<small>ingest lag {_duration_label(row.get("ingest_lag_sec"))} · {_e(row.get("observer_action") or "")}</small></td>'
            "</tr>"
        )
    return _table(["钱包", "阶段", "市场", "价格", "证据", "观察动作"], "".join(body))


def _copyability_lane_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 copyability 队列数据。</p>'
    running = int(values.get("running") or 0)
    queued = int(values.get("queued") or 0)
    active = int(values.get("active") or 0)
    max_active_jobs = int(values.get("max_active_jobs") or 0)
    waterline_reached = bool(values.get("queue_waterline_reached"))
    if waterline_reached:
        banner_state = "attention"
        note = "copyability 队列已达到活动水位，规划器会暂停新增任务。"
    elif running:
        banner_state = "ok"
        note = "copyability worker 正在处理候选钱包。"
    elif queued:
        banner_state = "attention"
        note = "copyability 队列有待处理钱包，但当前没有 running worker。"
    else:
        banner_state = "ok"
        note = "copyability 队列当前为空。"
    pair_quality = values.get("pair_quality") or {}
    thresholds = pair_quality.get("thresholds") or {}
    threshold_note = (
        f"合格阈值: events >= {_fmt_int(thresholds.get('min_events'))}, "
        f"markets >= {_fmt_int(thresholds.get('min_markets'))}, "
        f"containment >= {_fmt_num(thresholds.get('min_containment'))}, "
        f"precedes >= {_fmt_num(thresholds.get('min_leader_precedes'))}"
    )
    cards = [
        (
            "队列水位",
            f"{_fmt_int(active)}/{_fmt_int(max_active_jobs)}" if max_active_jobs else "未限制",
            f"剩余 {_fmt_int(values.get('available_slots'))} 个槽位" if max_active_jobs else "active limit disabled",
            "warn" if float(values.get("queue_utilization_pct") or 0) >= 80 else "ok",
        ),
        ("排队", _fmt_int(queued), "queued jobs", "warn" if queued else "ok"),
        ("运行", _fmt_int(running), "running jobs", "ok" if running else "warn" if queued else "ok"),
        ("近1h完成", _fmt_int(values.get("completed_1h")), "done jobs", "ok"),
        ("24h 完成", _fmt_int(values.get("completed_24h")), "done jobs", "ok"),
        ("估算/小时", _fmt_num(values.get("recent_rate_per_hour")), "recent throughput", "ok"),
        ("粗略剩余", values.get("eta_label") or "未知", "按近期吞吐估算", "warn" if queued and not values.get("eta_label") else "ok"),
        ("高优先级", _fmt_int(values.get("high_priority_active")), "priority <= 10", "warn" if int(values.get("high_priority_active") or 0) else "ok"),
        ("总完成", _fmt_int(values.get("done")), "historical done", "ok"),
    ]
    return (
        f'<div class="health-banner {banner_state}"><strong>{_e(note)}</strong>'
        f'<span>{_fmt_int(active)} 个活动 copyability 任务</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(desc)}</small></div>'
            for label, value, desc, state in cards
        )
        + "</div>"
        + '<h3 class="subhead">活动优先级分布</h3>'
        + _simple_table(
            values.get("active_by_priority") or [],
            ["priority", "status", "candidate_stage", "review_stage", "count", "max_score", "oldest_created_at"],
            ["优先级", "状态", "候选阶段", "评分阶段", "数量", "最高分", "最早入队"],
        )
        + f'<h3 class="subhead">Pair 质量诊断</h3><p class="muted">{_e(threshold_note)}</p>'
        + _simple_table(
            pair_quality.get("bucket_rows") or [],
            [
                "bucket",
                "count",
                "copy_events",
                "copy_markets",
                "max_copy_events",
                "max_copy_markets",
                "avg_containment",
                "avg_precedes",
            ],
            ["分组", "Pairs", "事件", "市场", "最大事件", "最大市场", "平均 containment", "平均 precedes"],
        )
        + '<h3 class="subhead">弱信号钱包</h3>'
        + _copyability_near_miss_leaders_table(pair_quality.get("near_miss_leaders") or [])
        + '<h3 class="subhead">队列前排</h3>'
        + _copyability_active_jobs_table(values.get("top_active_jobs") or [])
        + '<h3 class="subhead">最近 Worker</h3>'
        + _simple_table(
            values.get("recent_runs") or [],
            [
                "run_type",
                "status",
                "wallets_attempted",
                "wallets_succeeded",
                "rows_written",
                "duration_seconds",
                "started_at",
                "error",
            ],
            ["任务", "状态", "尝试", "成功", "写入", "耗时秒", "开始", "错误"],
        )
        + '<h3 class="subhead">最近完成</h3>'
        + _simple_table(
            values.get("recent_completed_jobs") or [],
            [
                "wallet",
                "priority",
                "candidate_stage",
                "leader_score",
                "links_written",
                "pair_stats_written",
                "qualified_pairs",
                "backtest_trades_written",
                "score_written",
                "completed_at",
            ],
            ["钱包", "优先级", "阶段", "分数", "Links", "Pairs", "合格对", "回测", "Score", "完成"],
        )
    )


def _evidence_pipeline_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 L1/L2/L3 证据流水线数据。</p>'
    running = int(values.get("running") or 0)
    queued = int(values.get("queued") or 0)
    active = int(values.get("active") or 0)
    pending_state = int(values.get("pending_state_without_active_job") or 0)
    high_priority_pending_state = int(values.get("high_priority_pending_state_without_active_job") or 0)
    due_queued = int(values.get("due_queued_jobs") or 0)
    deferred_queued = int(values.get("deferred_queued_jobs") or 0)
    exhausted_queued = int(values.get("exhausted_queued_jobs") or 0)
    aged_queued = int(values.get("aged_queued_jobs") or 0)
    if exhausted_queued:
        banner_state = "attention"
        note = "有排队任务已经耗尽尝试次数，worker 不会再领取；维护循环将标记失败并释放水位。"
    elif aged_queued and not running:
        banner_state = "attention"
        note = "有久候任务已达到优先级老化阈值，但当前没有 running worker。"
    elif running:
        banner_state = "ok"
        note = (
            "历史证据 worker 正在补 L1/L2/L3；久候任务会被自动提升。"
            if aged_queued
            else "历史证据 worker 正在补 L1/L2/L3。"
        )
    elif queued:
        banner_state = "attention"
        note = "历史证据队列有积压，但当前没有 running 任务。"
    elif high_priority_pending_state:
        banner_state = "attention"
        note = "高优先级钱包状态待补证据，但没有对应活动任务。"
    elif pending_state:
        banner_state = "attention"
        note = "钱包状态仍有待补证据项，但当前执行队列为空。"
    else:
        banner_state = "ok"
        note = "历史证据队列当前为空。"
    cards = [
        ("状态钱包", _fmt_int(values.get("state_wallets")), "wallet_processing_state", "ok"),
        ("L3 深证据", _fmt_int(values.get("l3_wallets")), "l3_deep", "ok"),
        ("摘要就绪", _fmt_int(values.get("summary_ready_wallets")), "summary_ready", "ok"),
        ("排队", _fmt_int(queued), "queued jobs", "warn" if queued else "ok"),
        ("到期排队", _fmt_int(due_queued), "当前可领取", "warn" if due_queued and not running else "ok"),
        ("退避排队", _fmt_int(deferred_queued), "等待 next_attempt_at", "ok"),
        ("尝试耗尽", _fmt_int(exhausted_queued), "不会被 worker 领取", "warn" if exhausted_queued else "ok"),
        ("运行", _fmt_int(running), "running jobs", "ok" if running else "warn" if queued else "ok"),
        ("状态待派", _fmt_int(pending_state), "状态表有下一动作但无活动任务", "warn" if pending_state else "ok"),
        ("高优待派", _fmt_int(high_priority_pending_state), "优先级待派断点", "warn" if high_priority_pending_state else "ok"),
        (
            "总到期待补",
            _fmt_int(values.get("total_due_backlog")),
            "queued/running + 到期待派",
            "warn" if int(values.get("total_due_backlog") or 0) else "ok",
        ),
        ("近1h完成", _fmt_int(values.get("completed_1h")), "done jobs", "ok"),
        ("24h完成", _fmt_int(values.get("completed_24h")), "done jobs", "ok"),
        (
            "老化排队",
            _fmt_int(aged_queued),
            f">= {values.get('priority_aging_label') or '阈值'}",
            "warn" if aged_queued else "ok",
        ),
        (
            "最久等待",
            values.get("oldest_claimable_wait_label") or "-",
            "按 worker priority aging 口径",
            "warn" if aged_queued else "ok",
        ),
        ("估算/小时", _fmt_num(values.get("recent_rate_per_hour")), "recent throughput", "ok"),
        (
            "总 ETA",
            values.get("total_eta_label") or "未知",
            "按到期总 backlog 估算",
            "warn" if int(values.get("total_due_backlog") or 0) and not values.get("total_eta_label") else "ok",
        ),
        ("队列 ETA", values.get("eta_label") or "未知", "仅 queued/running", "warn" if queued and not values.get("eta_label") else "ok"),
    ]
    return (
        f'<div class="health-banner {banner_state}"><strong>{_e(note)}</strong>'
        f'<span>{_fmt_int(active)} 个活动历史证据任务；wallet_processing_state 是证据层级真相。</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(desc)}</small></div>'
            for label, value, desc, state in cards
        )
        + "</div>"
        + '<h3 class="subhead">钱包证据层级</h3>'
        + _simple_table(
            values.get("state_by_tier") or [],
            [
                "evidence_tier",
                "evidence_status",
                "next_action",
                "count",
                "min_priority",
                "avg_activity_count",
                "max_activity_count",
                "latest_updated_at",
            ],
            ["证据层级", "证据状态", "下一动作", "钱包数", "最高优先级", "平均事件", "最大事件", "最近更新"],
        )
        + '<h3 class="subhead">分层调度状态</h3>'
        + _simple_table(
            values.get("stage_schedule") or [],
            [
                "stage_label",
                "configured_weight",
                "queued_count",
                "due_queued_count",
                "deferred_queued_count",
                "exhausted_queued_count",
                "running_count",
                "active_per_weight",
                "aged_queued_count",
                "oldest_wait_label",
                "current_weight",
                "last_selected_at",
            ],
            ["证据动作", "调度权重", "排队", "到期", "退避", "耗尽", "运行", "活跃/权重", "老化排队", "最久等待", "游标权重", "最近选择"],
            table_class="scheduler-table",
        )
        + '<h3 class="subhead">执行队列分布</h3>'
        + _simple_table(
            values.get("active_by_action") or [],
            ["job_action", "job_scope", "status", "count", "min_priority", "oldest_created_at", "latest_updated_at"],
            ["任务动作", "队列范围", "状态", "数量", "最高优先级", "最早创建", "最近更新"],
        )
        + '<h3 class="subhead">状态待派断点</h3>'
        + _simple_table(
            values.get("pending_state_by_action") or [],
            ["next_action", "count", "high_priority_count", "min_priority", "latest_updated_at"],
            ["下一动作", "钱包数", "高优先级", "最高优先级", "最近更新"],
        )
        + '<h3 class="subhead">执行队列前排</h3>'
        + _evidence_active_jobs_table(values.get("top_active_jobs") or [])
        + '<h3 class="subhead">最近完成证据任务</h3>'
        + _evidence_completed_jobs_table(values.get("recent_completed_jobs") or [])
    )


def _evidence_active_jobs_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无活动历史证据任务。</p>'
    body = []
    for row in rows:
        wallet = str(row.get("wallet") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a>'
            f'<small>{_e(row.get("last_error") or "")}</small></td>'
            f'<td>{_badge(row.get("status"))}<small>p{_fmt_int(row.get("priority"))} · try {_fmt_int(row.get("attempts"))}</small></td>'
            f'<td>{_badge(row.get("job_action"))}<small>{_e(row.get("job_scope") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>{_fmt_num(row.get("leader_score"))}</small></td>'
            f'<td><div class="numline">{_e(row.get("evidence_tier") or "")}</div>'
            f'<small>{_e(row.get("evidence_status") or "")} · {_e(row.get("next_action") or "")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))}/{_fmt_int(row.get("target_depth"))} events</div>'
            f'<small>{_fmt_int(row.get("distinct_markets"))} markets · {_fmt_int(row.get("non_fast_trade_count"))} non-fast</small></td>'
            f'<td>{_fmt_ts(row.get("updated_at"))}</td>'
            "</tr>"
        )
    return _table(["钱包", "队列", "动作", "候选/分数", "证据状态", "历史规模", "更新"], "".join(body))


def _evidence_completed_jobs_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无完成的历史证据任务。</p>'
    body = []
    for row in rows:
        wallet = str(row.get("wallet") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a></td>'
            f'<td><div class="numline">{_e(row.get("completed_stage") or "")}</div>'
            f'<small>next {_e(row.get("next_stage") or "")}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>{_fmt_num(row.get("leader_score"))}</small></td>'
            f'<td><div class="numline">{_e(row.get("evidence_tier") or "")}</div>'
            f'<small>{_e(row.get("evidence_status") or "")} · {_e(row.get("next_action") or "")}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))}/{_fmt_int(row.get("target_depth"))} events</div>'
            f'<small>p{_fmt_int(row.get("priority"))}</small></td>'
            f'<td>{_fmt_ts(row.get("completed_at"))}</td>'
            "</tr>"
        )
    return _table(["钱包", "完成阶段", "候选/分数", "新证据状态", "历史规模", "完成"], "".join(body))


def _copyability_no_signal_panel(values: dict[str, Any]) -> str:
    if not values:
        return '<p class="empty">暂无 copyability 无信号数据。</p>'
    rows = values.get("rows") or []
    wallet_count = int(values.get("wallet_count") or 0)
    banner_state = "attention" if wallet_count else "ok"
    cards = [
        ("无信号阻断", _fmt_int(wallet_count), "blocked_copyability", "warn" if wallet_count else "ok"),
        ("高分样本", _fmt_int(values.get("high_score_wallets")), f">= {_fmt_num(values.get('score_floor'))}", "warn" if int(values.get("high_score_wallets") or 0) else "ok"),
        ("高收益样本", _fmt_int(values.get("high_pnl_wallets")), f">= ${_fmt_int(values.get('pnl_floor'))}", "warn" if int(values.get("high_pnl_wallets") or 0) else "ok"),
        ("Hygiene 干净", _fmt_int(values.get("clean_wallets")), "ok/clean/low_risk", "ok"),
        ("最高分", _fmt_num(values.get("max_score")), "leader_score", "warn" if float(values.get("max_score") or 0) else "ok"),
        ("最高净收益", f"${_fmt_int(values.get('max_net_pnl_usdc'))}", "net_pnl_usdc", "ok"),
    ]
    return (
        f'<div class="health-banner {banner_state}"><strong>{_e(values.get("next_action"))}</strong>'
        '<span>这不是放行队列，只是把高潜但不可跟的阻塞地址集中展示。</span></div>'
        '<div class="health-grid">'
        + "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(desc)}</small></div>'
            for label, value, desc, state in cards
        )
        + "</div>"
        + _copyability_no_signal_wallets_table(rows)
    )


def _copyability_no_signal_wallets_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无因 copyability 无信号被阻断的钱包。</p>'
    body = []
    for row in rows:
        address = str(row.get("address") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(address)}">{_short(address)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td class="num">{_fmt_num(row.get("leader_score"))}</td>'
            f'<td><div class="numline">${_fmt_int(row.get("net_pnl_usdc"))}</div>'
            f'<small>vol ${_fmt_int(row.get("total_volume_usdc"))}</small></td>'
            f'<td><div class="numline">${_fmt_int(row.get("recent_30d_volume_usdc"))}</div>'
            f'<small>{_e(row.get("primary_category") or "")}</small></td>'
            f'<td>{_badge(row.get("hygiene_status") or "unknown")}<small>{_badge(row.get("candidate_stage"))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("copy_event_count"))} events</div>'
            f'<small>{_fmt_int(row.get("copy_market_count"))} markets · in {_fmt_int(row.get("leader_in_degree"))}</small></td>'
            f'<td><div class="numline">single {_fmt_pct(row.get("single_market_pnl_share"))}</div>'
            f'<small>exposure {_fmt_pct(row.get("net_to_gross_exposure"))}</small></td>'
            "</tr>"
        )
    return _table(["钱包", "分数", "收益/总量", "30日量/分类", "阶段", "Copy 字段", "风险字段"], "".join(body))


def _copyability_near_miss_leaders_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无未通过但有重复 pair 的钱包。</p>'
    body = []
    for row in rows:
        wallet = str(row.get("leader_wallet") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a></td>'
            f'<td>{_badge(row.get("candidate_stage"))}</td>'
            f'<td><div class="numline">{_fmt_int(row.get("pair_count"))} pairs</div>'
            f'<small>{_fmt_int(row.get("copy_events"))} events · {_fmt_int(row.get("copy_markets"))} markets</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("repeated_market_pairs"))}</div>'
            f'<small>max {_fmt_int(row.get("max_pair_events"))}/{_fmt_int(row.get("max_pair_markets"))}</small></td>'
            f'<td><div class="numline">{_fmt_num(row.get("avg_containment"))}</div>'
            f'<small>max {_fmt_num(row.get("max_containment"))}</small></td>'
            f'<td><div class="numline">{_fmt_num(row.get("avg_precedes"))}</div>'
            f'<small>max {_fmt_num(row.get("max_precedes"))}</small></td>'
            "</tr>"
        )
    return _table(["钱包", "阶段", "弱信号规模", "重复市场 pair", "Containment", "Precedes"], "".join(body))


def _copyability_active_jobs_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">暂无活动 copyability 任务。</p>'
    body = []
    for row in rows:
        wallet = str(row.get("wallet") or "")
        body.append(
            "<tr>"
            f'<td><a class="mono strong-link" href="/wallet/{_e(wallet)}">{_short(wallet)}</a>'
            f'<small>{_e(row.get("review_reason") or "")}</small></td>'
            f'<td>{_badge(row.get("status"))}<small>p{_fmt_int(row.get("priority"))}</small></td>'
            f'<td>{_badge(row.get("candidate_stage"))}<small>{_fmt_num(row.get("leader_score"))}</small></td>'
            f'<td><div class="numline">{_fmt_int(row.get("activity_count"))} events</div>'
            f'<small>{_fmt_int(row.get("max_pair_events"))} pair events · {_fmt_int(row.get("max_pair_markets"))} markets</small></td>'
            f'<td>{_e(row.get("graph_scan_mode") or "")}</td>'
            f'<td>{_fmt_ts(row.get("updated_at"))}</td>'
            "</tr>"
        )
    return _table(["钱包", "队列", "阶段/分数", "输入线索", "扫描", "更新"], "".join(body))


def _queue_progress_rows(conn: sqlite3.Connection, *, now: int) -> list[dict[str, Any]]:
    rows = _rows(
        conn,
        """
        WITH job_types AS (
            SELECT DISTINCT job_type
            FROM pipeline_jobs
        ),
        active AS (
            SELECT
                job_type,
                SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_count,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
            FROM pipeline_jobs
            GROUP BY job_type
        ),
        recent AS (
            SELECT
                job_type,
                SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) AS completed_1h,
                SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) AS completed_6h,
                SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) AS completed_24h,
                MAX(completed_at) AS latest_completed_at
            FROM pipeline_jobs
            WHERE status = 'done'
            GROUP BY job_type
        )
        SELECT
            jt.job_type,
            COALESCE(active.queued_count, 0) AS queued_count,
            COALESCE(active.running_count, 0) AS running_count,
            COALESCE(active.failed_count, 0) AS failed_count,
            COALESCE(recent.completed_1h, 0) AS completed_1h,
            COALESCE(recent.completed_6h, 0) AS completed_6h,
            COALESCE(recent.completed_24h, 0) AS completed_24h,
            COALESCE(recent.latest_completed_at, 0) AS latest_completed_at
        FROM job_types jt
        LEFT JOIN active ON active.job_type = jt.job_type
        LEFT JOIN recent ON recent.job_type = jt.job_type
        ORDER BY queued_count DESC, running_count DESC, jt.job_type ASC
        """,
        (now - 3_600, now - 21_600, now - 86_400),
    )
    for row in rows:
        queued = int(row.get("queued_count") or 0)
        running = int(row.get("running_count") or 0)
        completed_6h = int(row.get("completed_6h") or 0)
        completed_24h = int(row.get("completed_24h") or 0)
        rate_per_hour = max(completed_6h / 6.0, completed_24h / 24.0)
        pending = queued + running
        row["recent_rate_per_hour"] = round(rate_per_hour, 2)
        row["eta_label"] = _fmt_duration_hours(pending / rate_per_hour) if pending and rate_per_hour > 0 else ""
    return rows


def _ops_health_panel(values: dict[str, Any]) -> str:
    storage = values.get("storage") or {}
    address_quality = values.get("address_quality") or {}
    runtime = values.get("runtime") or {}
    runtime_loops = values.get("runtime_loops") or {}
    research_control_steps = values.get("research_control_steps") or {}
    upstream_request_budget = values.get("upstream_request_budget") or {}
    pipeline_backlog = values.get("pipeline_backlog") or {}
    health = str(values.get("health") or "ok")
    invalid_address_rows = int(address_quality.get("invalid_address_rows") or 0)
    high_priority_backlog = int(pipeline_backlog.get("high_priority_pending_without_active_job") or 0)
    pending_backlog = int(pipeline_backlog.get("pending_without_active_job") or 0)
    priority_threshold = int(pipeline_backlog.get("high_priority_threshold") or HIGH_PRIORITY_PENDING_JOB_PRIORITY)
    package_version = str(runtime.get("package_version") or "unknown")
    source_fingerprint = str(runtime.get("source_fingerprint") or "unknown")
    source_delivery = _source_delivery_text(str(runtime.get("source_delivery") or ""))
    source_root = str(runtime.get("source_root") or "")
    runtime_label = f"v{package_version} / {source_fingerprint}" if package_version != "unknown" else source_fingerprint
    cards = [
        ("运行版本", runtime_label, f"{_fmt_int(runtime.get('source_file_count'))} 个源码文件", "ok"),
        ("源码装载", source_delivery, _short_path(source_root), "ok"),
        (
            "运行循环",
            f"{_fmt_int(runtime_loops.get('ok_count'))}/{_fmt_int(runtime_loops.get('total'))}",
            "最近心跳正常",
            "warn" if int(runtime_loops.get("attention_count") or 0) else "ok",
        ),
        (
            "异常循环",
            _fmt_int(runtime_loops.get("attention_count")),
            f"{_fmt_int(runtime_loops.get('no_data_count'))} 个暂无心跳",
            "warn" if int(runtime_loops.get("attention_count") or 0) else "ok",
        ),
        ("数据库", _fmt_bytes(int(storage.get("db_bytes") or 0)), "SQLite 主文件", "ok"),
        ("WAL", _fmt_bytes(int(storage.get("wal_bytes") or 0)), "写入日志", "warn" if int(storage.get("wal_bytes") or 0) >= 1_000_000_000 else "ok"),
        ("地址质量", _fmt_int(invalid_address_rows), "非标准钱包地址", "warn" if invalid_address_rows else "ok"),
        ("过期队列", _fmt_int(values.get("stale_running_count")), "running lease 已过期", "warn" if int(values.get("stale_running_count") or 0) else "ok"),
        (
            "上游冷却",
            _fmt_int(upstream_request_budget.get("active_cooldowns")),
            f"{_fmt_int(upstream_request_budget.get('scope_count'))} 个共享预算",
            "warn" if int(upstream_request_budget.get("active_cooldowns") or 0) else "ok",
        ),
        ("高优先级漏派", _fmt_int(high_priority_backlog), f"priority <= {priority_threshold}", "warn" if high_priority_backlog else "ok"),
        ("待派 backlog", _fmt_int(pending_backlog), "未进入活动队列", "ok"),
        ("可用空间", _fmt_bytes(int(storage.get("free_disk_bytes") or 0)), "NAS 数据卷", "ok"),
    ]
    body = [
        f'<div class="health-banner {health}"><strong>{_e(values.get("note"))}</strong>'
        f'<span>更新时间 {_fmt_ts(values.get("generated_at"))}</span></div>',
        '<div class="health-grid">',
        "".join(
            f'<div class="health-card {state}"><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></div>'
            for label, value, note, state in cards
        ),
        "</div>",
        '<h3 class="subhead">常驻循环新鲜度</h3>'
        + _simple_table(
            runtime_loops.get("rows") or [],
            ["loop", "state_label", "last_status", "age_label", "last_at", "rows_written", "error"],
            ["循环", "状态", "最近结果", "多久前", "最近时间", "写入", "摘要/错误"],
        ),
        '<div class="ops-columns">',
        '<section><h3 class="subhead">执行队列</h3>'
        + _simple_table(values.get("job_status") or [], ["job_type", "status", "count"], ["队列", "状态", "数量"])
        + "</section>",
        '<section><h3 class="subhead">活动任务</h3>'
        + _simple_table(values.get("active_jobs") or [], ["job_type", "job_action", "status", "count"], ["队列", "动作", "状态", "数量"])
        + "</section>",
        "</div>",
        '<h3 class="subhead">待派证据状态</h3>'
        + _simple_table(
            pipeline_backlog.get("by_action") or [],
            ["next_action", "count", "high_priority_count", "min_priority", "latest_updated_at"],
            ["动作", "待派", "高优先级", "最高优先级", "最近更新"],
        ),
        '<h3 class="subhead">队列吞吐</h3>'
        + _simple_table(
            values.get("queue_progress") or [],
            [
                "job_type",
                "queued_count",
                "running_count",
                "completed_1h",
                "completed_24h",
                "recent_rate_per_hour",
                "eta_label",
                "latest_completed_at",
            ],
            ["队列", "排队", "运行", "近1h完成", "近24h完成", "估算/小时", "粗略剩余", "最近完成"],
        ),
        '<h3 class="subhead">上游 API 调度</h3>'
        + _simple_table(
            upstream_request_budget.get("rows") or [],
            [
                "scope",
                "state",
                "rate",
                "cooldown_remaining_seconds",
                "total_permits",
                "total_cooldowns",
                "last_status_code",
                "updated_at",
            ],
            ["预算", "状态", "速率", "冷却秒", "许可", "冷却次数", "最近状态码", "更新"],
        ),
    ]
    if research_control_steps.get("has_data"):
        body.insert(
            5,
            '<h3 class="subhead">研究控制阶段</h3>'
            + _simple_table(
                research_control_steps.get("rows") or [],
                [
                    "step",
                    "state_label",
                    "last_status",
                    "duration_label",
                    "age_label",
                    "last_at",
                    "rows_written",
                    "error",
                ],
                ["阶段", "状态", "最近结果", "耗时", "多久前", "最近时间", "处理", "摘要/错误"],
            ),
        )
    if values.get("stale_running_samples"):
        body.append(
            '<h3 class="subhead">过期 running 样本</h3>'
            + _simple_table(
                values["stale_running_samples"],
                ["job_type", "wallet", "job_action", "attempts", "lease_until"],
                ["队列", "钱包", "动作", "尝试", "lease 到期"],
            )
        )
    if values.get("failed_job_samples"):
        body.append(
            '<h3 class="subhead">失败任务样本</h3>'
            + _pipeline_jobs_table(
                values["failed_job_samples"],
                include_wallet=True,
            )
        )
    if pipeline_backlog.get("high_priority_samples"):
        body.append(
            '<h3 class="subhead">高优先级漏派样本</h3>'
            + _simple_table(
                pipeline_backlog["high_priority_samples"],
                ["wallet", "evidence_tier", "evidence_status", "next_action", "priority", "updated_at"],
                ["钱包", "证据层", "证据状态", "动作", "优先级", "更新"],
            )
        )
    return "".join(body)


def _source_delivery_text(value: str) -> str:
    labels = {
        "bind_mount": "挂载源码",
        "image_source": "镜像源码",
        "local_source": "本地源码",
    }
    return labels.get(value, "未知")


def _short_path(value: str, *, max_len: int = 36) -> str:
    if not value:
        return ""
    if len(value) <= max_len:
        return value
    return "..." + value[-(max_len - 3) :]


def _table(headers: list[str], body: str, *, table_class: str = "") -> str:
    head = "".join(f"<th>{_e(item)}</th>" for item in headers)
    class_attr = f' class="{_e(table_class)}"' if table_class else ""
    return f'<div class="table-wrap"><table{class_attr}><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def _paper_quality_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS wallets,
            SUM(CASE WHEN production_ready = 1 THEN 1 ELSE 0 END) AS production_ready,
            AVG(total_roi) AS avg_total_roi,
            MAX(total_roi) AS max_total_roi,
            SUM(orders) AS paper_orders
        FROM paper_wallet_quality
        """
    ).fetchone()
    return dict(row) if row else {}


def _rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [_json_columns(dict(row)) for row in conn.execute(sql, params).fetchall()]


def _one(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    row = conn.execute(sql, params).fetchone()
    return _json_columns(dict(row)) if row else {}


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row else 0


def _int_value(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _round_value(value: Any, *, digits: int = 2) -> float:
    try:
        return round(float(value or 0), digits)
    except (TypeError, ValueError):
        return 0.0


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _json_columns(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key.endswith("_json") and isinstance(value, str):
            out[key.removesuffix("_json")] = _json_load(value)
        else:
            out[key] = value
    return out


def _json_load(value: str) -> Any:
    try:
        return json.loads(value or "{}")
    except json.JSONDecodeError:
        return value


def _first(params: dict[str, list[str]], key: str) -> str:
    return (params.get(key, [""])[0] or "").strip()


def _int_param(params: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int(params.get(key, [str(default)])[0])
    except ValueError:
        return default


def _float_param(params: dict[str, list[str]], key: str, default: float) -> float:
    try:
        return float(params.get(key, [str(default)])[0])
    except ValueError:
        return default


def _auth_cookie(token: str) -> str:
    secure = "; Secure" if os.environ.get("PM_ROBOT_UI_SECURE_COOKIE", "0") == "1" else ""
    return f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax{secure}"


def _cookie_value(header: str, name: str) -> str:
    for part in header.split(";"):
        if "=" not in part:
            continue
        key, value = part.strip().split("=", 1)
        if key == name:
            return value
    return ""


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return _e(_fmt_num(value))
    if isinstance(value, int) and value > 1_500_000_000 and value < 4_000_000_000:
        return _e(_fmt_ts(value))
    text = str(value)
    if text.startswith("0x") and len(text) > 20:
        return f'<span class="mono">{_short(text)}</span>'
    return _e(text)


def _fmt_ts(value: Any) -> str:
    if not value:
        return ""
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(int(value)))


def _fmt_num(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def _fmt_duration_hours(hours: float) -> str:
    if hours <= 0:
        return ""
    if hours < 1:
        return f"{max(1, int(round(hours * 60)))} 分钟"
    if hours < 48:
        return f"{hours:.1f} 小时"
    return f"{hours / 24:.1f} 天"


def _duration_label(seconds: Any) -> str:
    if seconds is None or seconds == "":
        return "无"
    try:
        return _fmt_duration_hours(float(seconds) / 3600) or "刚刚"
    except (TypeError, ValueError):
        return str(seconds)


def _short_duration_label(seconds: Any) -> str:
    try:
        value = max(0.0, float(seconds))
    except (TypeError, ValueError):
        return str(seconds or "")
    if value < 1:
        return "<1 秒"
    if value < 60:
        return f"{int(round(value))} 秒"
    return _duration_label(value)


def _fmt_pct(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def _fmt_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} GB"


def _short(address: str) -> str:
    if len(address) <= 16:
        return _e(address)
    return f"{_e(address[:8])}...{_e(address[-6:])}"


def _badge(value: Any) -> str:
    text = str(value or "")
    return f'<span class="badge">{_e(text)}</span>'


def _e(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


_CSS = """
:root {
  color-scheme: light;
  --bg: #f3f5f7;
  --surface: #ffffff;
  --surface-soft: #f8fafc;
  --line: #d7dee8;
  --line-soft: #e9edf3;
  --text: #17202a;
  --muted: #657386;
  --accent: #0f766e;
  --accent-strong: #115e59;
  --accent-soft: #dff4f1;
  --blue: #2563eb;
  --blue-soft: #dbeafe;
  --amber: #b45309;
  --amber-soft: #fef3c7;
  --rose: #be123c;
  --rose-soft: #ffe4e6;
  --ok: #15803d;
  --ok-soft: #dcfce7;
  --shadow: 0 1px 2px rgba(16, 24, 40, 0.06), 0 8px 24px rgba(16, 24, 40, 0.05);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.top {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  align-items: center;
  justify-content: space-between;
  min-height: 56px;
  padding: 0 28px;
  border-bottom: 1px solid var(--line);
  background: rgba(255, 255, 255, 0.96);
  backdrop-filter: blur(10px);
}
.brand { font-weight: 800; color: var(--text); letter-spacing: 0; }
nav { display: flex; gap: 8px; align-items: center; }
nav a, .button, button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 34px;
  padding: 0 12px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--text);
  font-weight: 600;
  white-space: nowrap;
}
nav a.active, .button, button {
  border-color: var(--accent);
  background: var(--accent);
  color: #fff;
}
.button.secondary { border-color: var(--line); background: #fff; color: var(--text); }
.shell { max-width: 1440px; margin: 0 auto; padding: 26px 28px 48px; }
.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 20px;
  margin-bottom: 18px;
}
.discovery-hero {
  padding: 4px 0 2px;
}
.hero-actions { display: flex; gap: 8px; align-items: center; }
.eyebrow {
  margin: 0 0 6px;
  color: var(--accent-strong);
  font-size: 12px;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
h1 { margin: 0; font-size: 26px; line-height: 1.2; letter-spacing: 0; }
h2 { margin: 0 0 12px; font-size: 16px; letter-spacing: 0; }
h3.subhead { margin: 14px 0 8px; font-size: 13px; color: var(--muted); letter-spacing: 0; }
p { margin: 6px 0 0; color: var(--muted); }
.address { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 20px; overflow-wrap: anywhere; }
.status-strip {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  min-height: 48px;
  margin-bottom: 14px;
  padding: 12px 14px;
  border: 1px solid var(--line);
  border-left-width: 4px;
  border-radius: 8px;
  background: var(--surface);
  box-shadow: var(--shadow);
}
.status-strip strong { font-size: 14px; }
.status-strip span { color: var(--muted); white-space: nowrap; }
.status-strip.history-needed { border-left-color: var(--amber); }
.status-strip.paper-ready { border-left-color: var(--ok); }
.status-strip.review-needed { border-left-color: var(--blue); }
.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}
.metric, .panel {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
.metric { padding: 15px 16px; min-height: 92px; }
.metric span, .metric small { display: block; color: var(--muted); }
.metric strong { display: block; margin: 7px 0 3px; font-size: 25px; line-height: 1.1; overflow-wrap: anywhere; font-variant-numeric: tabular-nums; }
.health-banner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-height: 42px;
  margin-bottom: 12px;
  padding: 10px 12px;
  border: 1px solid var(--line-soft);
  border-left: 4px solid var(--ok);
  border-radius: 8px;
  background: var(--surface-soft);
}
.health-banner.attention { border-left-color: var(--amber); }
.health-banner.critical { border-left-color: var(--rose); background: #fff7f8; }
.health-banner span { color: var(--muted); white-space: nowrap; }
.wallet-diagnostic { margin-bottom: 16px; }
.diagnostic-error {
  margin-top: -2px;
  margin-bottom: 12px;
  padding: 12px;
  border: 1px solid #fecdd3;
  border-radius: 8px;
  background: #fff7f8;
}
.diagnostic-error > strong { display: block; margin-bottom: 6px; color: var(--rose); }
.health-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}
.health-card {
  min-height: 82px;
  padding: 12px;
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  background: var(--surface-soft);
}
.health-card.warn { border-color: var(--amber-soft); background: #fffbeb; }
.health-card span, .health-card small { display: block; color: var(--muted); }
.health-card strong { display: block; margin: 6px 0 2px; font-size: 21px; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.ops-columns {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;
}
.grid { display: grid; gap: 16px; margin-bottom: 16px; }
.grid.two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.grid.three { grid-template-columns: 1.1fr 0.9fr 1fr; }
.panel { padding: 16px; overflow: hidden; }
.table-panel { padding: 0; }
.funnel-panel { margin-bottom: 16px; }
.funnel-grid {
  display: grid;
  grid-template-columns: repeat(7, minmax(120px, 1fr));
  gap: 10px;
  overflow-x: auto;
  padding-bottom: 2px;
}
.funnel-step {
  min-width: 120px;
  padding: 10px;
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  background: var(--surface-soft);
}
.funnel-label { display: flex; justify-content: space-between; gap: 10px; align-items: baseline; }
.funnel-label strong { font-size: 13px; }
.funnel-label span { font-weight: 800; font-variant-numeric: tabular-nums; }
.funnel-track, .mini-track, .score-bar, .progress {
  position: relative;
  overflow: hidden;
  background: #eef2f7;
}
.funnel-track { height: 8px; margin: 9px 0 7px; border-radius: 999px; }
.funnel-track i, .mini-track i, .score-bar i, .progress i {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: var(--accent);
}
.funnel-step small { color: var(--muted); }
.depth-row, .mini-row {
  display: grid;
  grid-template-columns: minmax(96px, 1fr) auto;
  gap: 8px;
  align-items: center;
  padding: 8px 0;
  border-bottom: 1px solid var(--line-soft);
}
.depth-row:last-child, .mini-row:last-child { border-bottom: 0; }
.depth-row strong, .mini-row strong { font-variant-numeric: tabular-nums; }
.mini-track { grid-column: 1 / -1; height: 7px; border-radius: 999px; }
.mini-track i.ready, .mini-track i.deep { background: var(--ok); }
.mini-track i.light { background: var(--blue); }
.mini-track i.starter { background: var(--amber); }
.mini-track i.none { background: var(--rose); }
.panel-note { margin-top: 10px; font-size: 12px; }
.section-head {
  display: flex;
  justify-content: space-between;
  align-items: end;
  margin: 6px 0 10px;
}
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
.scheduler-table { min-width: 920px; }
.scheduler-table td { white-space: nowrap; }
th, td { border-bottom: 1px solid #edf0ec; padding: 9px 10px; text-align: left; vertical-align: top; }
th { color: #4d5751; font-size: 12px; font-weight: 700; background: #fafbf9; white-space: nowrap; }
td { max-width: 520px; overflow-wrap: anywhere; }
tbody tr:hover { background: #fbfcfb; }
.kv th { width: 210px; color: var(--muted); background: transparent; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
.numline { font-weight: 800; font-variant-numeric: tabular-nums; white-space: nowrap; }
.muted-line, td small { display: block; margin-top: 4px; color: var(--muted); font-size: 12px; }
.operator-label { display: block; font-weight: 700; }
.timeline-score { display: block; margin-bottom: 3px; font-size: 18px; font-variant-numeric: tabular-nums; }
.job-error-details { margin-top: 6px; max-width: 520px; }
.job-error-details summary { color: var(--rose); cursor: pointer; font-size: 12px; font-weight: 700; }
.job-error-details div {
  margin-top: 6px;
  padding: 8px;
  border: 1px solid #fecdd3;
  border-radius: 6px;
  background: #fff7f8;
  color: #881337;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
.lease-line { margin-top: 7px; }
.priority-cell { min-width: 92px; }
.priority-cell strong { display: block; margin-bottom: 7px; font-size: 18px; font-variant-numeric: tabular-nums; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.strong-link { color: var(--text); font-weight: 800; }
.plain-link { color: inherit; text-decoration: none; }
.plain-link:hover { text-decoration: underline; text-decoration-thickness: 2px; text-underline-offset: 3px; }
.badge {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 0 8px;
  border-radius: 999px;
  background: var(--accent-soft);
  color: #134e4a;
  font-size: 12px;
  font-weight: 700;
  white-space: nowrap;
}
.source-pill, .depth-badge {
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  padding: 0 7px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 800;
  white-space: nowrap;
}
.source-pill { background: var(--blue-soft); color: #1e3a8a; }
.depth-badge.none { background: var(--rose-soft); color: var(--rose); }
.depth-badge.starter { background: var(--amber-soft); color: var(--amber); }
.depth-badge.light { background: var(--blue-soft); color: var(--blue); }
.depth-badge.ready, .depth-badge.deep { background: var(--ok-soft); color: var(--ok); }
.stacked-pills {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 7px;
}
.chip-group { display: flex; flex-wrap: wrap; gap: 8px; }
.chip {
  display: inline-flex;
  align-items: center;
  min-height: 30px;
  padding: 0 10px;
  border: 1px solid var(--line);
  border-radius: 999px;
  color: var(--text);
  background: #fff;
  font-weight: 700;
}
.chip.active { color: #fff; background: var(--accent); border-color: var(--accent); }
.progress {
  width: 100%;
  min-width: 104px;
  height: 20px;
  margin-top: 7px;
  border-radius: 999px;
}
.progress i { background: var(--accent); }
.progress span {
  position: absolute;
  inset: 0;
  display: grid;
  place-items: center;
  color: #23313f;
  font-size: 11px;
  font-weight: 800;
}
.score-bar { height: 7px; border-radius: 999px; }
.actions { min-width: 92px; white-space: nowrap; }
.icon-button {
  display: inline-grid;
  place-items: center;
  width: 34px;
  height: 34px;
  margin-right: 6px;
  border: 1px solid var(--accent);
  border-radius: 6px;
  background: var(--accent);
  color: #fff;
  font-weight: 900;
}
.icon-button.secondary { border-color: var(--line); background: #fff; color: var(--text); }
.filters {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr)) auto auto;
  gap: 10px;
  align-items: end;
  margin-bottom: 16px;
  padding: 14px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; font-weight: 700; }
input {
  width: 100%;
  min-height: 36px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 0 10px;
  font: inherit;
  color: var(--text);
  background: #fff;
}
.empty { color: var(--muted); }
.login {
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 24px;
}
.login-box {
  width: min(420px, 100%);
  padding: 22px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}
.login-box input, .login-box button { margin-top: 14px; width: 100%; }
.error { color: var(--warn); font-weight: 700; }
@media (max-width: 900px) {
  .top { padding: 0 14px; align-items: flex-start; flex-direction: column; gap: 8px; padding-top: 10px; padding-bottom: 10px; }
  nav { flex-wrap: wrap; }
  .shell { padding: 20px 14px 36px; }
  .toolbar { flex-direction: column; }
  .hero-actions { width: 100%; flex-wrap: wrap; }
  .metric-grid, .health-grid, .ops-columns, .grid.two, .grid.three, .filters { grid-template-columns: 1fr; }
  .status-strip { align-items: flex-start; flex-direction: column; }
  .status-strip span, .health-banner span { white-space: normal; }
  .funnel-grid { grid-template-columns: repeat(2, minmax(140px, 1fr)); }
  .metric { min-height: 80px; }
}
"""
