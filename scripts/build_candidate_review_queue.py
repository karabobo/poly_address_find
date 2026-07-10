#!/usr/bin/env python3
"""Build a paper-driven review queue for Polymarket candidate wallets.

This is intentionally conservative: imported/manual addresses become
`needs_data` until transaction-level and paper-trading metrics are attached.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


ADDRESS_RE = re.compile(r"0x[a-f0-9]{40}")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def load_policy(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def metric(row: dict[str, Any], name: str) -> float:
    for key in (name, name.lower(), name.upper()):
        if key in row:
            return as_float(row[key])
    return 0.0


def decide_stage(row: dict[str, Any], policy: dict[str, Any]) -> tuple[str, str]:
    thresholds = policy["thresholds"]
    address = str(row.get("address", "")).lower()
    if not ADDRESS_RE.fullmatch(address):
        return "rejected", "invalid_address"

    if str(row.get("hygiene_status", "")).lower() in {"routing_operator", "wash", "market_maker_taker"}:
        return "blocked_hygiene", str(row.get("hygiene_status"))

    has_required_data = any(
        metric(row, field) > 0
        for field in (
            "cumulative_win_rate",
            "recent_30d_volume_usdc",
            "event_win_rate",
            "copy_event_count",
            "paper_roi_after_slippage",
        )
    )
    if not has_required_data:
        return "needs_data", "no_wallet_metrics_attached"

    score = metric(row, "leader_score")
    if score <= 0:
        return "needs_manual_review", "metrics_present_but_no_leader_score"

    if metric(row, "maker_fraction") > thresholds["max_maker_fraction_for_directional_leader"]:
        return "blocked_hygiene", "maker_fraction_above_directional_threshold"

    if metric(row, "copy_event_count") and metric(row, "copy_event_count") < thresholds["min_copy_events"]:
        return "needs_manual_review", "copy_event_sample_too_small"

    if metric(row, "net_to_gross_exposure") and metric(row, "net_to_gross_exposure") < thresholds["min_net_to_gross_exposure"]:
        return "blocked_copyability", "hedge_or_arbitrage_exposure_too_low"

    bands = policy["review_bands"]
    if score >= bands["paper_candidate"]:
        return "paper_candidate", "score_above_paper_threshold"
    if score >= bands["watchlist"]:
        return "needs_manual_review", "watchlist_score"
    if score < bands["reject_below"]:
        return "rejected", "score_below_reject_band"
    return "needs_manual_review", "borderline_score"


def build_queue(address_rows: list[dict[str, str]], policy: dict[str, Any]) -> list[dict[str, Any]]:
    queue = []
    for row in address_rows:
        normalized = {**row, "address": row.get("address", "").lower()}
        stage, reason = decide_stage(normalized, policy)
        queue.append(
            {
                "address": normalized["address"],
                "review_stage": stage,
                "review_reason": reason,
                "leader_score": normalized.get("leader_score", ""),
                "sources": normalized.get("sources", ""),
                "labels": normalized.get("labels", ""),
                "notes": normalized.get("notes", ""),
                "links": normalized.get("links", ""),
                "status": normalized.get("status", ""),
                "required_next_data": required_next_data(stage, reason),
            }
        )
    return queue


def required_next_data(stage: str, reason: str) -> str:
    if stage == "needs_data":
        return "attach directional trades, recent volume, win rates, DCA, sell rate, bot/MM/wash flags"
    if stage == "needs_manual_review":
        return "review score components, single-market concentration, hedge exposure, and copyability"
    if stage == "paper_candidate":
        return "route to paper trading only; require out-of-sample ROI after slippage before live"
    if stage.startswith("blocked"):
        return "do not trade until blocking flag is independently resolved"
    if stage == "rejected":
        return "archive unless new evidence changes the score"
    return ""


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build candidate wallet review queue")
    parser.add_argument("--addresses", default="data/candidate_addresses.csv")
    parser.add_argument("--policy", default="config/leader_scoring_policy.json")
    parser.add_argument("--out", default="reports/candidate_review_queue.csv")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    policy = load_policy(Path(args.policy))
    rows = load_csv(Path(args.addresses))
    queue = build_queue(rows, policy)
    write_csv(Path(args.out), queue)
    counts: dict[str, int] = {}
    for row in queue:
        counts[row["review_stage"]] = counts.get(row["review_stage"], 0) + 1
    print(f"wrote {len(queue)} review rows to {args.out}")
    for stage, count in sorted(counts.items()):
        print(f"{stage}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
