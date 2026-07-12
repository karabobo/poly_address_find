"""Explain how provisional review wallets are handled operationally.

`candidate_stage` remains the persisted scoring stage.  This module derives a
more precise, read-only disposition for operators and diagnostics without
changing queue ownership or promotion rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from pm_robot.orchestration.evidence_readiness import paper_evidence_ready
from pm_robot.pipeline_terms import COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON


HANDLING_AUTOMATIC = "automatic"
HANDLING_WATCH = "watch"
HANDLING_MANUAL = "manual"
HANDLING_BLOCKED = "blocked"
HANDLING_READY = "ready"


@dataclass(frozen=True)
class ReviewDisposition:
    key: str
    label: str
    handling: str
    handling_label: str
    next_action: str
    operator_required: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "handling": self.handling,
            "handling_label": self.handling_label,
            "next_action": self.next_action,
            "operator_required": self.operator_required,
        }


def review_disposition(
    row: Mapping[str, Any],
    *,
    paper_min_score: float = 70.0,
) -> ReviewDisposition:
    """Derive operator handling from evidence facts; never mutate persisted stage."""

    stage = _text(row.get("candidate_stage"))
    score = _float(row.get("leader_score"))
    activity = _int(row.get("activity_count") or row.get("trade_events"))
    next_action = _text(row.get("next_action") or row.get("evidence_next_action"))
    copy_status = _text(row.get("copyability_status"))
    review_reason = _text(row.get("review_reason"))

    if stage in {"paper_candidate", "paper_approved", "live_eligible"}:
        return _result(
            "paper_ready",
            "已进入 Paper",
            HANDLING_READY,
            "已放行",
            "进入外部 paper 验证或发布前复核。",
        )
    if stage == "blocked_hygiene":
        return _result(
            "hygiene_blocked",
            "Hygiene 阻断",
            HANDLING_BLOCKED,
            "风险阻断",
            "只在风险证据修正后重新评分。",
        )
    if stage == "blocked_copyability":
        return _result(
            "copyability_blocked",
            "Copyability 阻断",
            HANDLING_BLOCKED,
            "证据阻断",
            "等待新增 copyability 证据后再开放复核。",
        )
    if stage == "rejected":
        return _result(
            "rejected",
            "已拒绝",
            HANDLING_BLOCKED,
            "停止处理",
            "不进入自动证据队列。",
        )
    if activity < 200:
        return _automatic("thin_evidence", "历史证据偏薄", "继续补历史，样本不足不放行。")
    if next_action in {"light_pending", "medium_pending", "deep_pending"}:
        return _automatic("history_pending", "历史证据补充中", "等待 L1/L2/L3 任务完成。")
    if stage == "needs_data" and review_reason != COPYABILITY_DEEP_SCAN_UNVALIDATED_REASON:
        return _automatic("score_needs_data", "评分证据不足", "补齐评分所需证据后自动重评。")

    has_signal = has_copyability_signal(row)
    has_validation = has_copyability_validation(row)
    if not has_signal:
        if copy_status in {"queued", "running"}:
            return _automatic("copyability_pending", "Copyability 补证据中", "等待当前证据任务完成后自动重评。")
        if copy_status == "done" and is_light_copyability_scan(row):
            return _automatic("copyability_light_no_signal", "copyability 轻扫无信号", "高分钱包自动进入深扫，其余继续观察。")
        if copy_status == "done" and is_deep_copyability_scan(row):
            return _automatic("copyability_no_signal", "copyability 无跟随信号", "自动收敛为 copyability 阻断，不需要人工处理。")
        return _automatic("missing_copyability", "尚未补 Copyability", "加入 copyability 证据队列。")

    if not has_validation:
        if copy_status in {"queued", "running"}:
            return _automatic("copyability_pending", "Copyability 验证中", "等待当前验证任务完成后自动重评。")
        if copy_status == "done" and is_deep_copyability_scan(row):
            return _watch(
                "copyability_near_miss",
                "深扫近失，暂未达标",
                "已有跟随线索但未达到验证门槛；等待新增实时事件后再扫描。",
            )
        return _automatic("copyability_unvalidated", "Copyability 线索待验证", "补 follower/backtest 证据后自动重评。")

    if score < float(paper_min_score):
        return _watch(
            "score_below_paper",
            f"分数未达 {paper_min_score:.0f}",
            "保留自动观察，等待新证据触发下一轮评分。",
        )

    if not _paper_evidence_ready(row):
        return _automatic(
            "paper_evidence_incomplete",
            "Paper 证据门槛未完成",
            "继续补深度证据；达到 L3 或有限历史深度门槛前不进入 paper",
        )
    if stage == "needs_manual_review":
        return _result(
            "manual_review",
            "需要人工判断",
            HANDLING_MANUAL,
            "人工复核",
            "证据与分数均已达线；检查 review_reason 后决定是否升级。",
            operator_required=True,
        )
    return _result(
        "unknown",
        "处置状态待确认",
        HANDLING_MANUAL,
        "人工复核",
        "检查评分阶段与证据状态是否同步。",
        operator_required=True,
    )


def has_copyability_signal(row: Mapping[str, Any]) -> bool:
    copy_events = _max_int(row, "copy_event_count", "leader_copy_events", "feature_copy_event_count")
    copy_markets = _max_int(row, "copy_market_count", "leader_copy_markets", "feature_copy_market_count")
    followers = _max_int(row, "qualified_follower_count")
    backtest_trades = _max_int(row, "backtest_trade_count")
    return copy_events > 0 or copy_markets > 0 or followers > 0 or backtest_trades > 0


def has_copyability_validation(row: Mapping[str, Any]) -> bool:
    followers = _max_int(row, "qualified_follower_count")
    backtest_trades = _max_int(row, "backtest_trade_count")
    edge_retention = _max_float(row, "edge_retention_pct")
    walk_forward = _max_float(row, "walk_forward_consistency_pct")
    return followers > 0 or backtest_trades > 0 or edge_retention > 0 or walk_forward > 0


def is_light_copyability_scan(row: Mapping[str, Any]) -> bool:
    scan_mode = _text(row.get("copyability_scan_mode"))
    return bool(scan_mode and scan_mode not in {"default", "deep"})


def is_deep_copyability_scan(row: Mapping[str, Any]) -> bool:
    return _text(row.get("copyability_scan_mode")) in {"", "default", "deep"}


def _paper_evidence_ready(row: Mapping[str, Any]) -> bool:
    explicit = row.get("paper_evidence_ready")
    if explicit is not None:
        return bool(explicit)
    return paper_evidence_ready(row)


def _automatic(key: str, label: str, next_action: str) -> ReviewDisposition:
    return _result(key, label, HANDLING_AUTOMATIC, "系统自动处理", next_action)


def _watch(key: str, label: str, next_action: str) -> ReviewDisposition:
    return _result(key, label, HANDLING_WATCH, "自动观察", next_action)


def _result(
    key: str,
    label: str,
    handling: str,
    handling_label: str,
    next_action: str,
    *,
    operator_required: bool = False,
) -> ReviewDisposition:
    return ReviewDisposition(
        key=key,
        label=label,
        handling=handling,
        handling_label=handling_label,
        next_action=next_action,
        operator_required=operator_required,
    )


def _max_int(row: Mapping[str, Any], *fields: str) -> int:
    return max((_int(row.get(field)) for field in fields), default=0)


def _max_float(row: Mapping[str, Any], *fields: str) -> float:
    return max((_float(row.get(field)) for field in fields), default=0.0)


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
