import sqlite3

from pm_robot.orchestration.evidence_readiness import (
    BOUNDED_DEEP_MIN_ACTIVITY_COUNT,
    BOUNDED_DEEP_MIN_DISTINCT_MARKETS,
    BOUNDED_DEEP_MIN_NON_FAST_TRADE_COUNT,
    PaperEvidenceMode,
    paper_evidence_mode,
    paper_evidence_ready,
    paper_evidence_ready_sql,
)


def test_paper_evidence_mode_distinguishes_full_bounded_and_incomplete() -> None:
    full_l3 = {
        "discovery_tier": "l3_deep",
        "evidence_status": "summary_ready",
    }
    bounded_deep = {
        "evidence_tier": "l2_medium",
        "evidence_status": "summary_ready",
        "evidence_current_stage": "deep_done",
        "evidence_activity_count": BOUNDED_DEEP_MIN_ACTIVITY_COUNT,
        "distinct_markets": BOUNDED_DEEP_MIN_DISTINCT_MARKETS,
        "non_fast_trade_count": BOUNDED_DEEP_MIN_NON_FAST_TRADE_COUNT,
    }
    incomplete = {
        **bounded_deep,
        "non_fast_trade_count": BOUNDED_DEEP_MIN_NON_FAST_TRADE_COUNT - 1,
    }

    assert paper_evidence_mode(full_l3) is PaperEvidenceMode.FULL_L3
    assert paper_evidence_mode(bounded_deep) is PaperEvidenceMode.BOUNDED_DEEP
    assert paper_evidence_mode(incomplete) is PaperEvidenceMode.INCOMPLETE
    assert paper_evidence_mode({**bounded_deep, "evidence_tier": "l1_light"}) is PaperEvidenceMode.INCOMPLETE
    assert paper_evidence_mode({**bounded_deep, "evidence_tier": ""}) is PaperEvidenceMode.INCOMPLETE
    assert paper_evidence_ready(full_l3) is True
    assert paper_evidence_ready(bounded_deep) is True
    assert paper_evidence_ready(incomplete) is False


def test_paper_evidence_ready_sql_matches_python_classifier() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            """
            CREATE TABLE wallet_processing_state(
                wallet TEXT PRIMARY KEY,
                discovery_tier TEXT,
                evidence_status TEXT,
                current_stage TEXT,
                activity_count INTEGER,
                distinct_markets INTEGER,
                non_fast_trade_count INTEGER
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO wallet_processing_state VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("full", "l3_deep", "summary_ready", "deep_done", 1, 1, 1),
                ("bounded", "l2_medium", "summary_ready", "deep_done", 500, 20, 100),
                ("l1", "l1_light", "summary_ready", "deep_done", 500, 20, 100),
                ("missing-tier", "", "summary_ready", "deep_done", 500, 20, 100),
                ("thin", "l2_medium", "summary_ready", "deep_done", 499, 20, 100),
                ("pending", "l3_deep", "needs_deep", "deep_pending", 3000, 200, 2000),
            ],
        )
        rows = conn.execute(
            f"""
            SELECT wps.*, {paper_evidence_ready_sql('wps')} AS sql_ready
            FROM wallet_processing_state wps
            ORDER BY wallet
            """
        ).fetchall()

        for row in rows:
            assert bool(row["sql_ready"]) is paper_evidence_ready(row)
    finally:
        conn.close()
