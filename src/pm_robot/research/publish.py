"""Publish research-approved wallets for a separate execution system."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pm_robot.risk.eligibility import publish_eligibility_status
from pm_robot.risk.gates import stable_readiness_status

PUBLISH_STAGE = "live_eligible"
DEFAULT_TTL_SECONDS = 86_400


@dataclass(frozen=True)
class PublishSummary:
    active: int
    revoked: int
    expires_at: int
    output_path: str = ""


def publish_leaders(
    conn: sqlite3.Connection,
    *,
    now: int | None = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    output_path: Path | None = None,
) -> PublishSummary:
    published_at = now or int(time.time())
    expires_at = published_at + ttl_seconds
    rows = publishable_leader_rows(conn, published_at=published_at, expires_at=expires_at)
    active_wallets = {row["wallet"] for row in rows}

    for row in rows:
        conn.execute(
            """
            INSERT INTO leader_publish(
                wallet, publish_stage, status, leader_score, review_reason,
                paper_quality_json, readiness_json, evidence_json, blockers_json,
                published_at, expires_at, revoked_at, revoke_reason
            ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, NULL, '')
            ON CONFLICT(wallet) DO UPDATE SET
                publish_stage = excluded.publish_stage,
                status = 'active',
                leader_score = excluded.leader_score,
                review_reason = excluded.review_reason,
                paper_quality_json = excluded.paper_quality_json,
                readiness_json = excluded.readiness_json,
                evidence_json = excluded.evidence_json,
                blockers_json = excluded.blockers_json,
                published_at = excluded.published_at,
                expires_at = excluded.expires_at,
                revoked_at = NULL,
                revoke_reason = ''
            """,
            (
                row["wallet"],
                row["publish_stage"],
                row["leader_score"],
                row["review_reason"],
                json.dumps(row["paper_quality"], ensure_ascii=False, sort_keys=True),
                json.dumps(row["readiness"], ensure_ascii=False, sort_keys=True),
                json.dumps(row["evidence"], ensure_ascii=False, sort_keys=True),
                json.dumps(row["blockers"], ensure_ascii=False, sort_keys=True),
                published_at,
                expires_at,
            ),
        )

    revoked = _revoke_missing(conn, active_wallets, revoked_at=published_at)
    conn.commit()

    if output_path is not None:
        revoked_rows = revoked_published_leaders(conn)
        _write_publish_json(
            output_path,
            rows,
            revoked_rows=revoked_rows,
            published_at=published_at,
            expires_at=expires_at,
        )

    return PublishSummary(
        active=len(rows),
        revoked=revoked,
        expires_at=expires_at,
        output_path=str(output_path or ""),
    )


def publishable_leader_rows(
    conn: sqlite3.Connection,
    *,
    published_at: int | None = None,
    expires_at: int | None = None,
) -> list[dict[str, Any]]:
    published = published_at or int(time.time())
    expires = expires_at or published + DEFAULT_TTL_SECONDS
    rows = conn.execute(
        """
        SELECT
            cw.address AS wallet,
            cw.sources,
            cw.labels,
            cw.notes,
            cw.links,
            cw.status AS candidate_status,
            COALESCE(ls.leader_score, 0) AS leader_score,
            COALESCE(ls.review_reason, '') AS review_reason,
            COALESCE(ls.components_json, '{}') AS components_json,
            COALESCE(ls.penalties_json, '{}') AS penalties_json,
            pwq.*,
            wf.hygiene_status,
            wf.maker_fraction,
            wf.edge_retention_pct,
            wf.walk_forward_consistency_pct,
            wf.copy_event_count,
            wf.extra_json AS feature_extra_json
        FROM candidate_wallets cw
        JOIN paper_wallet_quality pwq
          ON pwq.wallet = cw.address
        JOIN wallet_features wf
          ON wf.address = cw.address
        LEFT JOIN leader_scores ls
          ON ls.score_id = (
              SELECT score_id FROM leader_scores
              WHERE address = cw.address
              ORDER BY scored_at DESC, score_id DESC
              LIMIT 1
          )
        WHERE cw.candidate_stage = 'live_eligible'
          AND pwq.production_ready = 1
          AND lower(COALESCE(wf.hygiene_status, '')) IN ('clean', 'screened')
          AND wf.maker_fraction IS NOT NULL
          AND COALESCE(json_extract(wf.extra_json, '$.maker_fraction_source'), '')
              != 'public_activity_no_maker_flags_observed'
          AND COALESCE(wf.edge_retention_pct, 0) >= 60
          AND COALESCE(wf.walk_forward_consistency_pct, 0) >= 55
          AND COALESCE(wf.copy_event_count, 0) >= 5
        ORDER BY COALESCE(ls.leader_score, 0) DESC, pwq.total_roi DESC, cw.address ASC
        """
    ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if not publish_eligibility_status(conn, item["wallet"]).eligible:
            continue
        blockers = _json_list(item.get("blockers_json"))
        readiness = stable_readiness_status(conn, item["wallet"])
        if blockers or int(readiness["stable_production_ready"]) != 1:
            continue
        paper_quality = {
            "orders": item["orders"],
            "open_positions": item["open_positions"],
            "settled_positions": item["settled_positions"],
            "gamma_marked_positions": item["gamma_marked_positions"],
            "fallback_marked_positions": item["fallback_marked_positions"],
            "mark_coverage": item["mark_coverage"],
            "settled_cost_usd": item["settled_cost_usd"],
            "settled_pnl_usd": item["settled_pnl_usd"],
            "settled_roi": item["settled_roi"],
            "total_pnl_usd": item["total_pnl_usd"],
            "total_roi": item["total_roi"],
            "production_ready": item["production_ready"],
            "max_drawdown_pct": item["max_drawdown_pct"],
            "max_market_exposure_share": item["max_market_exposure_share"],
            "validation_days": item["validation_days"],
            "updated_at": item["updated_at"],
        }
        evidence = {
            "sources": item["sources"],
            "labels": item["labels"],
            "notes": item["notes"],
            "links": item["links"],
            "candidate_status": item["candidate_status"],
            "source_provenance": source_provenance(conn, item["wallet"]),
            "score_components": _json_object(item["components_json"]),
            "score_penalties": _json_object(item["penalties_json"]),
        }
        publish_quality = publish_quality_review(
            evidence=evidence,
            paper_quality=paper_quality,
            readiness=readiness,
            leader_score=float(item["leader_score"] or 0),
        )
        evidence["publish_quality"] = publish_quality
        out.append(
            {
                "wallet": item["wallet"],
                "publish_stage": PUBLISH_STAGE,
                "status": "active",
                "leader_score": float(item["leader_score"] or 0),
                "review_reason": item["review_reason"],
                "paper_quality": paper_quality,
                "readiness": readiness,
                "evidence": evidence,
                "blockers": blockers,
                "published_at": published,
                "expires_at": expires,
            }
        )
    return out


def source_provenance(conn: sqlite3.Connection, wallet: str) -> dict[str, Any]:
    table = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'candidate_source_events'
        """
    ).fetchone()
    if not table:
        return {"events": [], "source_count": 0, "first_source": ""}

    rows = conn.execute(
        """
        SELECT source, status, labels, notes, links, evidence_json, observed_at, recorded_at
        FROM candidate_source_events
        WHERE address = ?
        ORDER BY observed_at ASC, event_id ASC
        """,
        (wallet,),
    ).fetchall()
    events = []
    sources: set[str] = set()
    for row in rows:
        source = row["source"] or ""
        if source:
            sources.add(source)
        events.append(
            {
                "source": source,
                "status": row["status"] or "",
                "labels": row["labels"] or "",
                "notes": row["notes"] or "",
                "links": row["links"] or "",
                "evidence": _json_object(row["evidence_json"]),
                "observed_at": row["observed_at"],
                "recorded_at": row["recorded_at"],
            }
        )
    return {
        "events": events,
        "source_count": len(sources),
        "first_source": events[0]["source"] if events else "",
        "latest_source": events[-1]["source"] if events else "",
    }


def publish_quality_review(
    *,
    evidence: dict[str, Any],
    paper_quality: dict[str, Any],
    readiness: dict[str, Any],
    leader_score: float,
) -> dict[str, Any]:
    warnings: list[str] = []
    provenance = evidence.get("source_provenance") if isinstance(evidence.get("source_provenance"), dict) else {}
    if not evidence.get("sources"):
        warnings.append("missing_current_source")
    if not provenance.get("events"):
        warnings.append("missing_source_provenance")
    if not evidence.get("links"):
        warnings.append("missing_external_link")
    if leader_score < 50:
        warnings.append("borderline_leader_score")
    if int(readiness.get("stable_observation_count") or 0) < 6:
        warnings.append("thin_stable_readiness_history")
    if int(paper_quality.get("settled_positions") or 0) < 50:
        warnings.append("thin_settled_sample")
    if float(paper_quality.get("mark_coverage") or 0) < 0.95:
        warnings.append("incomplete_mark_coverage")

    grade = "pass"
    if "missing_source_provenance" in warnings or "missing_current_source" in warnings:
        grade = "review"
    elif warnings:
        grade = "warn"
    return {
        "grade": grade,
        "warnings": warnings,
        "source_count": int(provenance.get("source_count") or 0),
    }


def active_published_leaders(conn: sqlite3.Connection, *, now: int | None = None) -> list[dict[str, Any]]:
    checked_at = now or int(time.time())
    rows = conn.execute(
        """
        SELECT *
        FROM leader_publish
        WHERE status = 'active'
          AND expires_at > ?
        ORDER BY leader_score DESC, wallet ASC
        """,
        (checked_at,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["paper_quality"] = _json_object(item.pop("paper_quality_json"))
        item["readiness"] = _json_object(item.pop("readiness_json"))
        item["evidence"] = _json_object(item.pop("evidence_json"))
        item["blockers"] = _json_list(item.pop("blockers_json"))
        out.append(item)
    return out


def revoked_published_leaders(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM leader_publish
        WHERE status != 'active'
        ORDER BY COALESCE(revoked_at, published_at) DESC, wallet ASC
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["paper_quality"] = _json_object(item.pop("paper_quality_json"))
        item["readiness"] = _json_object(item.pop("readiness_json"))
        item["evidence"] = _json_object(item.pop("evidence_json"))
        item["blockers"] = _json_list(item.pop("blockers_json"))
        out.append(item)
    return out


def _revoke_missing(conn: sqlite3.Connection, active_wallets: set[str], *, revoked_at: int) -> int:
    current = conn.execute(
        "SELECT wallet FROM leader_publish WHERE status = 'active'"
    ).fetchall()
    revoked = 0
    for row in current:
        wallet = row["wallet"]
        if wallet in active_wallets:
            continue
        conn.execute(
            """
            UPDATE leader_publish
            SET status = 'revoked', revoked_at = ?, revoke_reason = 'no_longer_publishable'
            WHERE wallet = ? AND status = 'active'
            """,
            (revoked_at, wallet),
        )
        revoked += 1
    return revoked


def _write_publish_json(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    revoked_rows: list[dict[str, Any]],
    published_at: int,
    expires_at: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "published_at": published_at,
        "expires_at": expires_at,
        "count": len(rows),
        "revoked_count": len(revoked_rows),
        "leaders": rows,
        "revoked_leaders": revoked_rows,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _json_list(value: Any) -> list[Any]:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, list) else []


def _json_object(value: Any) -> dict[str, Any]:
    parsed = _json_value(value)
    return parsed if isinstance(parsed, dict) else {}


def _json_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return None
