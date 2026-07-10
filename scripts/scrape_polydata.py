#!/usr/bin/env python3
"""Scrape Polydata trader data from server-rendered Next.js pages.

The site embeds useful JSON in React Server Component payloads. This script
extracts the initial leaderboard wallets and can optionally hydrate each wallet
with its detail page metrics.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "https://polydata.pro"
USER_AGENT = "polymarket-wallet-research/0.1"


@dataclass
class FetchConfig:
    timeout: int = 30
    retries: int = 3
    sleep_seconds: float = 0.4


def fetch_text(url: str, cfg: FetchConfig) -> str:
    last_error: Exception | None = None
    for attempt in range(1, cfg.retries + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=cfg.timeout) as response:
                return response.read().decode("utf-8", errors="ignore")
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < cfg.retries:
                time.sleep(cfg.sleep_seconds * attempt)
    try:
        completed = subprocess.run(
            ["curl", "-L", "--fail", "--silent", "--show-error", "--max-time", str(cfg.timeout), url],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"failed to fetch {url}: {last_error}; curl fallback: {exc}") from exc


def normalize_rsc_html(html: str) -> str:
    """Unescape enough of the RSC payload for JSONDecoder.raw_decode."""
    return html.replace('\\"', '"').replace("\\\\/", "/")


def extract_json_after_key(html: str, key: str) -> Any:
    normalized = normalize_rsc_html(html)
    needle = f'"{key}":'
    index = normalized.find(needle)
    if index == -1:
        raise ValueError(f"key not found in page payload: {key}")
    start = index + len(needle)
    return json.JSONDecoder().raw_decode(normalized[start:])[0]


def scrape_leaderboard(cfg: FetchConfig) -> list[dict[str, Any]]:
    html = fetch_text(f"{BASE_URL}/traders", cfg)
    traders = extract_json_after_key(html, "initialTraders")
    if not isinstance(traders, list):
        raise TypeError("initialTraders payload was not a list")
    return traders


def scrape_trader_detail(wallet: str, cfg: FetchConfig) -> dict[str, Any]:
    html = fetch_text(f"{BASE_URL}/traders/{wallet}", cfg)
    data = extract_json_after_key(html, "initialData")
    if not isinstance(data, dict):
        raise TypeError(f"initialData payload for {wallet} was not an object")
    return data


def merge_detail(summary: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    merged = dict(summary)
    merged["detail"] = detail
    for key in ("last_updated", "overview", "pnl", "timing", "dca", "bot", "risk", "profile"):
        if key in detail:
            merged[key] = detail[key]
    if "leaderboard" in detail:
        leaderboard = detail["leaderboard"]
        merged["rank"] = leaderboard.get("rank", merged.get("rank"))
        merged["net_pnl"] = leaderboard.get("pnl", merged.get("net_pnl"))
        merged["volume"] = leaderboard.get("volume", merged.get("volume"))
        merged["categories"] = leaderboard.get("categories", {})
    merged["smart_score"] = detail.get("smart_score", merged.get("smart_score"))
    merged["smart_level"] = detail.get("smart_level", merged.get("smart_level"))
    return merged


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Polydata trader analytics")
    parser.add_argument("--out", default="data/polydata_traders.json", help="output JSON path")
    parser.add_argument("--hydrate-details", action="store_true", help="fetch each trader detail page")
    parser.add_argument("--limit", type=int, default=0, help="limit wallets, useful for testing")
    parser.add_argument("--sleep", type=float, default=0.4, help="seconds between detail requests")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = FetchConfig(sleep_seconds=args.sleep)
    traders = scrape_leaderboard(cfg)
    if args.limit:
        traders = traders[: args.limit]

    if args.hydrate_details:
        hydrated: list[dict[str, Any]] = []
        for idx, trader in enumerate(traders, start=1):
            wallet = trader.get("wallet")
            if not wallet:
                continue
            print(f"[{idx}/{len(traders)}] hydrate {wallet}", file=sys.stderr)
            try:
                detail = scrape_trader_detail(wallet, cfg)
                hydrated.append(merge_detail(trader, detail))
            except Exception as exc:
                print(f"warning: failed detail for {wallet}: {exc}", file=sys.stderr)
                hydrated.append(trader)
            time.sleep(args.sleep)
        traders = hydrated

    payload = {
        "source": BASE_URL,
        "fetched_at_unix": int(time.time()),
        "count": len(traders),
        "traders": traders,
    }
    write_json(Path(args.out), payload)
    print(f"wrote {len(traders)} traders to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
