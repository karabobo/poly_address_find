from pathlib import Path

from pm_robot.io import load_candidate_addresses
from pm_robot.orchestration.review_pipeline import import_candidates_from_csv
from pm_robot.storage.db import connect, run_migrations


BITGET_SOURCE = "bitget_smart_money_20260407"
BITGET_ARTICLE_URL = "https://www.bitget.com/zh-CN/news/detail/12560605342191"
BITGET_CANDIDATES_PATH = Path("config/curated_candidates/bitget_smart_money_20260407.csv")


def test_bitget_curated_candidate_file_has_complete_source_list():
    candidates = load_candidate_addresses(BITGET_CANDIDATES_PATH)
    addresses = [candidate.address for candidate in candidates]

    assert len(candidates) == 26
    assert len(set(addresses)) == 26
    assert all(address == address.lower() for address in addresses)
    assert all(address.startswith("0x") and len(address) == 42 for address in addresses)
    assert all(candidate.sources == BITGET_SOURCE for candidate in candidates)
    assert all(candidate.status == "manual_research_seed" for candidate in candidates)
    assert all(BITGET_ARTICLE_URL in candidate.links for candidate in candidates)


def test_bitget_curated_candidates_import_as_single_source_event_per_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)

        first_count = import_candidates_from_csv(
            conn,
            addresses_path=BITGET_CANDIDATES_PATH,
            source_event_mode="upsert_source",
        )
        second_count = import_candidates_from_csv(
            conn,
            addresses_path=BITGET_CANDIDATES_PATH,
            source_event_mode="upsert_source",
        )

        candidate_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_wallets WHERE sources = ?",
            (BITGET_SOURCE,),
        ).fetchone()[0]
        event_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_source_events WHERE source = ?",
            (BITGET_SOURCE,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert first_count == 26
    assert second_count == 26
    assert candidate_count == 26
    assert event_count == 26
