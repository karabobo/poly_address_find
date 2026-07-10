#!/usr/bin/env python3
"""Rank Polydata wallets using the paper's profitable-wallet signals."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def nested(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = row
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def category_signal(row: dict[str, Any]) -> tuple[str, float]:
    categories = row.get("categories") or nested(row, "detail", "leaderboard", "categories", default={}) or {}
    if not isinstance(categories, dict) or not categories:
        return "", 0.0
    best_name = ""
    best_volume = -1.0
    for name, payload in categories.items():
        volume = as_float(payload.get("volume") if isinstance(payload, dict) else 0)
        if volume > best_volume:
            best_name = str(name).upper()
            best_volume = volume
    if best_name == "POLITICS":
        return best_name, 1.0
    if best_name in {"ECONOMICS", "CRYPTO", "GEOPOLITICS"}:
        return best_name, 0.45
    if best_name:
        return best_name, 0.2
    return "", 0.0


def bot_class_signal(verdict: str, bot_score: float) -> float:
    verdict_upper = verdict.upper()
    if "LIKELY HUMAN" in verdict_upper:
        return 1.0
    if "SEMI-BOT" in verdict_upper:
        return 0.85
    if "HUMAN" in verdict_upper:
        return 0.65
    if "BOT" in verdict_upper:
        return 0.15
    return 1.0 - clamp(bot_score / 100.0)


def score_wallet(row: dict[str, Any]) -> dict[str, Any]:
    net_pnl = as_float(row.get("net_pnl", nested(row, "overview", "net_pnl", default=0)))
    volume = as_float(row.get("volume", nested(row, "overview", "total_buy", default=0)))
    n_trades = as_float(row.get("n_trades", nested(row, "overview", "n_trades", default=0)))
    smart_score = as_float(row.get("smart_score", 0))

    trade_win_rate = as_float(row.get("win_rate", nested(row, "pnl", "win_rate", default=0)))
    if trade_win_rate <= 1.0:
        trade_win_rate *= 100.0
    event_win_rate = as_float(nested(row, "pnl", "pos_event_win_rate", default=0))
    if event_win_rate <= 1.0:
        event_win_rate *= 100.0

    avg_dca = as_float(nested(row, "dca", "avg_dca", default=0))
    sell_pct = as_float(nested(row, "profile", "sell_pct", default=0))
    profit_factor = as_float(nested(row, "pnl", "profit_factor", default=0))
    sharpe = as_float(nested(row, "risk", "sharpe", default=0))
    hhi = as_float(nested(row, "risk", "hhi", default=0))
    bot_score = as_float(row.get("bot_score", nested(row, "bot", "score", default=0)))
    bot_verdict = str(row.get("bot_verdict", nested(row, "bot", "verdict", default="")))
    med_gap = as_float(nested(row, "bot", "med_gap", default=0))
    trades_per_day = as_float(nested(row, "bot", "trades_per_day", default=0))
    category, category_score = category_signal(row)

    profit_score = clamp(math.log10(max(net_pnl, 0) + 1) / 7.0)
    volume_return = net_pnl / volume if volume > 0 else 0.0
    efficiency_score = clamp((volume_return + 0.02) / 0.12)
    smart_score_norm = clamp(smart_score / 100.0)
    event_score = clamp((event_win_rate - 55.0) / 40.0) if event_win_rate else 0.35
    dca_score = clamp(math.log1p(avg_dca) / math.log(51)) if avg_dca else 0.25
    hold_score = clamp(1.0 - sell_pct / 35.0) if sell_pct or "profile" in row else 0.5
    profit_factor_score = clamp(math.log1p(max(profit_factor, 0)) / math.log(11)) if profit_factor else 0.35
    sharpe_score = clamp((sharpe + 0.25) / 1.75) if sharpe else 0.4
    concentration_score = 0.7
    if hhi:
        concentration_score = 1.0 - abs(clamp(hhi / 10000.0) - 0.55) / 0.55
        concentration_score = clamp(concentration_score)
    bot_execution_score = bot_class_signal(bot_verdict, bot_score)

    hft_penalty = 0.0
    if med_gap and med_gap < 5:
        hft_penalty += 8.0
    if trades_per_day and trades_per_day >= 500:
        hft_penalty += 10.0
    if "BOT" in bot_verdict.upper() and "SEMI" not in bot_verdict.upper():
        hft_penalty += 10.0
    if net_pnl < 0:
        hft_penalty += 25.0
    if n_trades and n_trades < 100:
        hft_penalty += 8.0

    score = (
        18 * profit_score
        + 10 * efficiency_score
        + 12 * smart_score_norm
        + 14 * event_score
        + 12 * dca_score
        + 10 * hold_score
        + 8 * profit_factor_score
        + 6 * sharpe_score
        + 4 * concentration_score
        + 4 * category_score
        + 8 * bot_execution_score
        - hft_penalty
    )
    score = clamp(score, 0.0, 100.0)

    if score >= 72 and net_pnl > 0:
        action = "copy_candidate"
    elif score >= 55 and net_pnl > 0:
        action = "watch_only"
    else:
        action = "avoid"

    return {
        "copy_score": round(score, 2),
        "action": action,
        "nickname": row.get("nickname", ""),
        "wallet": row.get("wallet", ""),
        "rank": row.get("rank", ""),
        "net_pnl": round(net_pnl, 2),
        "volume": round(volume, 2),
        "volume_return": round(volume_return, 6),
        "n_trades": int(n_trades) if n_trades else "",
        "trade_win_rate": round(trade_win_rate, 2),
        "event_win_rate": round(event_win_rate, 2) if event_win_rate else "",
        "avg_dca": round(avg_dca, 2) if avg_dca else "",
        "sell_pct": round(sell_pct, 2) if sell_pct else 0,
        "profit_factor": round(profit_factor, 2) if profit_factor else "",
        "sharpe": round(sharpe, 4) if sharpe else "",
        "hhi": round(hhi, 2) if hhi else "",
        "bot_score": round(bot_score, 2),
        "bot_verdict": bot_verdict,
        "med_gap_sec": round(med_gap, 2) if med_gap else "",
        "trades_per_day": round(trades_per_day, 2) if trades_per_day else "",
        "primary_category": category,
        "smart_score": round(smart_score, 2),
    }


def load_traders(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("traders"), list):
        return payload["traders"]
    if isinstance(payload, list):
        return payload
    raise ValueError("input must be a list or an object with a traders list")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank wallets for copy-trading research")
    parser.add_argument("input", help="JSON from scrape_polydata.py")
    parser.add_argument("--out", default="", help="optional CSV output path")
    parser.add_argument("--top", type=int, default=0, help="print top N rows")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = [score_wallet(row) for row in load_traders(Path(args.input))]
    rows.sort(key=lambda item: item["copy_score"], reverse=True)
    if args.out:
        write_csv(Path(args.out), rows)
        print(f"wrote {len(rows)} scored wallets to {args.out}")
    if args.top:
        for row in rows[: args.top]:
            print(
                f'{row["copy_score"]:>6.2f} {row["action"]:<15} '
                f'{row["net_pnl"]:>12,.2f} {row["nickname"]:<28} {row["wallet"]}'
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
