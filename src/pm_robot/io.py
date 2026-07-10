"""CSV/JSON helpers for candidate and feature files."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from pm_robot.models import CandidateAddress, WalletFeatures


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_candidate_addresses(path: Path) -> list[CandidateAddress]:
    with path.open(encoding="utf-8") as handle:
        rows = csv.DictReader(handle)
        return [
            CandidateAddress(
                address=(row.get("address") or "").strip().lower(),
                sources=row.get("sources", ""),
                labels=row.get("labels", ""),
                notes=row.get("notes", ""),
                links=row.get("links", ""),
                status=row.get("status", ""),
            )
            for row in rows
            if row.get("address")
        ]


def load_wallet_features(path: Path) -> dict[str, WalletFeatures]:
    """Load optional wallet metrics CSV.

    Column names intentionally match `leader_scoring_policy.required_score_components`.
    Unknown columns are preserved in `extra`.
    """
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        out: dict[str, WalletFeatures] = {}
        for row in csv.DictReader(handle):
            address = (row.get("address") or row.get("wallet") or "").strip().lower()
            if not address:
                continue
            known = {
                "address",
                "wallet",
                "cumulative_win_rate",
                "recent_30d_volume_usdc",
                "net_pnl_usdc",
                "total_volume_usdc",
                "event_win_rate",
                "trade_win_rate",
                "avg_dca_entries",
                "sell_pct",
                "bot_score",
                "trades_per_day",
                "median_gap_sec",
                "maker_fraction",
                "leader_in_degree",
                "copy_event_count",
                "copy_market_count",
                "containment_pct_median",
                "copy_stream_roi",
                "edge_retention_pct",
                "walk_forward_consistency_pct",
                "survival_score",
                "single_market_pnl_share",
                "net_to_gross_exposure",
                "hygiene_status",
                "primary_category",
                "last_active_days_ago",
            }
            out[address] = WalletFeatures(
                address=address,
                cumulative_win_rate=_float_or_none(row.get("cumulative_win_rate")),
                recent_30d_volume_usdc=_float_or_none(row.get("recent_30d_volume_usdc")),
                net_pnl_usdc=_float_or_none(row.get("net_pnl_usdc")),
                total_volume_usdc=_float_or_none(row.get("total_volume_usdc")),
                event_win_rate=_float_or_none(row.get("event_win_rate")),
                trade_win_rate=_float_or_none(row.get("trade_win_rate")),
                avg_dca_entries=_float_or_none(row.get("avg_dca_entries")),
                sell_pct=_float_or_none(row.get("sell_pct")),
                bot_score=_float_or_none(row.get("bot_score")),
                trades_per_day=_float_or_none(row.get("trades_per_day")),
                median_gap_sec=_float_or_none(row.get("median_gap_sec")),
                maker_fraction=_float_or_none(row.get("maker_fraction")),
                leader_in_degree=_float_or_none(row.get("leader_in_degree")),
                copy_event_count=_float_or_none(row.get("copy_event_count")),
                copy_market_count=_float_or_none(row.get("copy_market_count")),
                containment_pct_median=_float_or_none(row.get("containment_pct_median")),
                copy_stream_roi=_float_or_none(row.get("copy_stream_roi")),
                edge_retention_pct=_float_or_none(row.get("edge_retention_pct")),
                walk_forward_consistency_pct=_float_or_none(row.get("walk_forward_consistency_pct")),
                survival_score=_float_or_none(row.get("survival_score")),
                single_market_pnl_share=_float_or_none(row.get("single_market_pnl_share")),
                net_to_gross_exposure=_float_or_none(row.get("net_to_gross_exposure")),
                hygiene_status=row.get("hygiene_status", ""),
                primary_category=row.get("primary_category", ""),
                last_active_days_ago=_float_or_none(row.get("last_active_days_ago")),
                extra={k: v for k, v in row.items() if k not in known},
            )
    return out


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
