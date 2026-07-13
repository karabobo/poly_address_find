import time
import json
import sys

import pytest

from pm_robot.clients.http import HttpClientError
from pm_robot.cli import main
from pm_robot.models import CandidateAddress, CandidateStage, WalletFeatures
from pm_robot.orchestration.paper_runner import evaluate_paper_observer, preview_paper_observer, run_paper
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import persist_wallet_activity, upsert_candidate, upsert_wallet_feature


class StaticBookClient:
    def book(self, token_id: str) -> dict:
        return {
            "bids": [{"price": "0.53", "size": "1000"}],
            "asks": [{"price": "0.55", "size": "1000"}],
        }


BOOK_CLIENT = StaticBookClient()


class RateLimitedBookClient:
    def __init__(self):
        self.calls = 0

    def book(self, token_id: str) -> dict:
        self.calls += 1
        raise HttpClientError(
            "shared cooldown",
            status_code=429,
            error_type="upstream_cooldown",
            retry_after_seconds=60.0,
        )


class MissingBookClient:
    def book(self, token_id: str) -> dict:
        return {"bids": [], "asks": []}


def _candidate_address(suffix: str) -> str:
    return "0x" + suffix * 40


def _activity(asset: str = "asset-1", *, timestamp: int = 1_000, idx: int = 1) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "conditionId": "condition-1",
        "eventSlug": "event-1",
        "slug": "market-1",
        "asset": asset,
        "outcome": "YES",
        "type": "TRADE",
        "side": "BUY",
        "price": 0.54,
        "size": 10,
        "usdcSize": 5.4,
        "transactionHash": f"0xhash{idx}",
    }


def _seed_paper_eligibility(
    conn,
    address: str,
    *,
    stage: CandidateStage = CandidateStage.PAPER_CANDIDATE,
    score: float = 55.0,
    review_reason: str = "paper_candidate_test",
) -> None:
    conn.execute(
        "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
        (stage.value, address),
    )
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=address,
            hygiene_status="clean",
            copy_event_count=3,
            edge_retention_pct=75,
            walk_forward_consistency_pct=80,
        ),
    )
    conn.execute(
        """
        INSERT INTO leader_scores(
            address, leader_score, review_stage, review_reason,
            components_json, penalties_json, policy_version, scored_at
        ) VALUES (?, ?, ?, ?, '{}', '{}', 'test', ?)
        """,
        (address, score, stage.value, review_reason, int(time.time())),
    )
    old_rows = [
        _activity(asset=f"asset-old-{idx}", timestamp=1_000 + idx, idx=idx)
        for idx in range(1, 100)
    ]
    recent = _activity(timestamp=int(time.time()), idx=10_000)
    persist_wallet_activity(conn, address, [*old_rows, recent], ingested_at=int(time.time()))
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                  'score_wallet', 0, 1000, 20, 200, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            discovery_tier = excluded.discovery_tier,
            evidence_status = excluded.evidence_status,
            current_stage = excluded.current_stage,
            activity_count = excluded.activity_count,
            distinct_markets = excluded.distinct_markets,
            non_fast_trade_count = excluded.non_fast_trade_count,
            updated_at = excluded.updated_at
        """,
        (address, int(time.time())),
    )
    conn.commit()


def _seed_exploratory_copyability_observer(
    conn,
    address: str,
    *,
    score: float = 60.0,
    hygiene_status: str = "clean",
    evidence_ready: bool = True,
) -> None:
    upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
    conn.execute(
        "UPDATE candidate_wallets SET candidate_stage = 'blocked_copyability' WHERE address = ?",
        (address,),
    )
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=address,
            hygiene_status=hygiene_status,
            copy_event_count=0,
        ),
    )
    conn.execute(
        """
        INSERT INTO leader_scores(
            address, leader_score, review_stage, review_reason,
            components_json, penalties_json, policy_version, scored_at
        ) VALUES (?, ?, 'blocked_copyability', 'copyability_scan_no_signal',
                  '{}', '{}', 'test', ?)
        """,
        (address, score, int(time.time())),
    )
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, ?, 'summary_ready', 1000, 1.0, 10, 'deep_done',
                  'score_wallet', 0, 1000, 30, 200, ?)
        """,
        (address, "l3_deep" if evidence_ready else "l1_light", int(time.time())),
    )
    persist_wallet_activity(
        conn,
        address,
        [_activity(timestamp=int(time.time()), idx=20_000)],
        ingested_at=int(time.time()),
    )
    conn.commit()


def test_paper_runner_ignores_unapproved_candidate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("8")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        row = _activity()
        row["timestamp"] = int(time.time())
        persist_wallet_activity(conn, address, [row], ingested_at=2_000)

        summary = run_paper(conn, ledger_path=None, limit=10, client=BOOK_CLIENT)
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]

        assert summary.signals_seen == 0
        assert summary.orders_recorded == 0
        assert count == 0
    finally:
        conn.close()


def test_paper_runner_propagates_upstream_cooldown_without_writing_order(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    client = RateLimitedBookClient()
    try:
        run_migrations(conn)
        address = _candidate_address("6")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address)

        with pytest.raises(HttpClientError) as captured:
            run_paper(conn, ledger_path=None, limit=10, client=client)

        assert captured.value.status_code == 429
        assert client.calls == 1
        assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 0
    finally:
        conn.close()


def test_paper_observer_propagates_upstream_cooldown_without_persisting(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    client = RateLimitedBookClient()
    try:
        run_migrations(conn)
        address = _candidate_address("7")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED)

        with pytest.raises(HttpClientError) as captured:
            evaluate_paper_observer(conn, limit=10, persist=True, client=client)

        assert captured.value.status_code == 429
        assert client.calls == 1
        assert conn.execute("SELECT COUNT(*) FROM paper_signal_evaluations").fetchone()[0] == 0
    finally:
        conn.close()


def test_paper_runner_does_not_include_high_score_watchlist_without_paper_stage(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("b")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(
            conn,
            address,
            stage=CandidateStage.NEEDS_REVIEW,
            score=52.0,
            review_reason="watchlist_score",
        )

        blocked = run_paper(conn, ledger_path=None, limit=10, client=BOOK_CLIENT)
        still_blocked = run_paper(
            conn,
            ledger_path=None,
            limit=10,
            include_watchlist_min_score=50,
            client=BOOK_CLIENT,
        )
        order = conn.execute("SELECT * FROM paper_orders").fetchone()

        assert blocked.signals_seen == 0
        assert still_blocked.signals_seen == 0
        assert order is None
    finally:
        conn.close()


def test_paper_runner_does_not_include_high_score_manual_review_without_paper_stage(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("d")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(
            conn,
            address,
            stage=CandidateStage.NEEDS_REVIEW,
            score=46.0,
            review_reason="borderline_score",
        )

        blocked = run_paper(
            conn,
            ledger_path=None,
            limit=10,
            include_watchlist_min_score=50,
            client=BOOK_CLIENT,
        )
        still_blocked = run_paper(
            conn,
            ledger_path=None,
            limit=10,
            include_review_min_score=45,
            client=BOOK_CLIENT,
        )
        order = conn.execute("SELECT * FROM paper_orders").fetchone()

        assert blocked.signals_seen == 0
        assert still_blocked.signals_seen == 0
        assert order is None
    finally:
        conn.close()


def test_paper_runner_records_approved_buy_once(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("9")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address)

        first = run_paper(conn, ledger_path=None, limit=10, max_stake_usd=25, client=BOOK_CLIENT)
        second = run_paper(conn, ledger_path=None, limit=10, max_stake_usd=25, client=BOOK_CLIENT)
        rows = conn.execute("SELECT * FROM paper_orders").fetchall()

        assert first.signals_seen == 1
        assert first.orders_recorded == 1
        assert second.signals_seen == 0
        assert len(rows) == 1
        assert rows[0]["signal_id"].startswith("activity-")
        assert rows[0]["wallet"] == address
        assert rows[0]["stake_usd"] == 25
    finally:
        conn.close()


def test_paper_observer_preview_lists_signals_without_writing_orders(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("e")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)

        preview = preview_paper_observer(conn, limit=10)
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]

        assert preview.schema_version == "paper_observer_preview_v1"
        assert preview.signals_seen == 1
        assert preview.paper_stage_wallets == 1
        assert preview.exploratory_copyability_wallets == 0
        assert preview.observer_wallets == 1
        assert preview.recent_buy_events >= 1
        assert preview.paper_stage_recent_buy_events >= 1
        assert preview.exploratory_copyability_recent_buy_events == 0
        assert preview.latest_buy_ts is not None
        assert preview.latest_buy_ingested_at is not None
        assert preview.latest_buy_age_sec is not None
        assert preview.recent_buy_max_ingest_lag_sec is not None
        assert preview.no_signal_reason == ""
        windows = {row["window_label"]: row for row in preview.window_diagnostics}
        assert windows["6h"]["recent_buy_events"] >= 1
        assert windows["6h"]["paper_stage_recent_buy_events"] >= 1
        assert windows["6h"]["exploratory_copyability_recent_buy_events"] == 0
        assert windows["6h"]["eligible_signals"] == 1
        assert windows["6h"]["max_ingest_lag_sec"] is not None
        assert windows["6h"]["no_signal_reason"] == ""
        assert preview.signals[0]["wallet"] == address
        assert preview.signals[0]["candidate_stage"] == CandidateStage.PAPER_APPROVED.value
        assert preview.signals[0]["validation_cohort"] == "validation"
        assert "ingested_at" in preview.signals[0]
        assert "ingest_lag_sec" in preview.signals[0]
        assert preview.signals[0]["observer_action"] == "external_paper_quote_and_evaluate"
        assert count == 0
    finally:
        conn.close()


def test_copyability_observer_is_isolated_from_formal_paper_execution(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("c")
        _seed_exploratory_copyability_observer(conn, address)

        formal_preview = preview_paper_observer(conn, limit=10)
        exploratory_preview = preview_paper_observer(
            conn,
            limit=10,
            exploratory_copyability_min_score=55,
        )
        evaluation = evaluate_paper_observer(
            conn,
            limit=10,
            exploratory_copyability_min_score=55,
            persist=True,
            client=BOOK_CLIENT,
        )
        paper_run = run_paper(conn, ledger_path=None, limit=10, client=BOOK_CLIENT)

        assert formal_preview.signals_seen == 0
        assert formal_preview.observer_wallets == 0
        assert exploratory_preview.signals_seen == 1
        assert exploratory_preview.paper_stage_wallets == 0
        assert exploratory_preview.exploratory_copyability_wallets == 1
        assert exploratory_preview.observer_wallets == 1
        assert exploratory_preview.recent_buy_events == 1
        assert exploratory_preview.paper_stage_recent_buy_events == 0
        assert exploratory_preview.exploratory_copyability_recent_buy_events == 1
        exploratory_windows = {
            row["window_label"]: row for row in exploratory_preview.window_diagnostics
        }
        assert exploratory_windows["6h"]["recent_buy_events"] == 1
        assert exploratory_windows["6h"]["paper_stage_recent_buy_events"] == 0
        assert (
            exploratory_windows["6h"]["exploratory_copyability_recent_buy_events"]
            == 1
        )
        assert exploratory_windows["6h"]["eligible_signals"] == 1
        assert exploratory_preview.signals[0]["validation_cohort"] == "exploratory_copyability"
        assert exploratory_preview.signals[0]["source"] == "copyability_observer_activity"
        assert evaluation.validation_signals == 0
        assert evaluation.exploratory_copyability_signals == 1
        assert paper_run.signals_seen == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 0
        assert conn.execute(
            "SELECT validation_cohort FROM paper_signal_evaluations"
        ).fetchone()[0] == "exploratory_copyability"
        assert conn.execute(
            "SELECT validation_cohort FROM paper_observer_trials"
        ).fetchone()[0] == "exploratory_copyability"
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("score", "hygiene_status", "evidence_ready"),
    [
        (54.9, "clean", True),
        (60.0, "unknown", True),
        (60.0, "clean", False),
    ],
)
def test_copyability_observer_fails_closed_on_research_gates(
    tmp_path,
    score,
    hygiene_status,
    evidence_ready,
):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("4")
        _seed_exploratory_copyability_observer(
            conn,
            address,
            score=score,
            hygiene_status=hygiene_status,
            evidence_ready=evidence_ready,
        )

        preview = preview_paper_observer(
            conn,
            limit=10,
            exploratory_copyability_min_score=55,
        )

        assert preview.signals_seen == 0
        assert preview.exploratory_copyability_wallets == 0
        assert preview.observer_wallets == 0
        assert preview.no_signal_reason == "no_observer_wallets"
    finally:
        conn.close()


def test_copyability_observer_diagnostics_use_exploratory_activity_scope(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("5")
        _seed_exploratory_copyability_observer(conn, address)
        conn.execute(
            "UPDATE wallet_activity SET price = 0.01 WHERE address = ?",
            (address,),
        )
        conn.commit()

        preview = preview_paper_observer(
            conn,
            limit=10,
            exploratory_copyability_min_score=55,
        )

        assert preview.signals_seen == 0
        assert preview.paper_stage_wallets == 0
        assert preview.exploratory_copyability_wallets == 1
        assert preview.observer_wallets == 1
        assert preview.recent_buy_events == 1
        assert preview.paper_stage_recent_buy_events == 0
        assert preview.exploratory_copyability_recent_buy_events == 1
        assert preview.no_signal_reason == "recent_buys_failed_eligibility_or_deduped"
        windows = {row["window_label"]: row for row in preview.window_diagnostics}
        assert windows["6h"]["recent_buy_events"] == 1
        assert windows["6h"]["exploratory_copyability_recent_buy_events"] == 1
        assert windows["6h"]["eligible_signals"] == 0
        assert windows["6h"]["no_signal_reason"] == (
            "recent_buys_failed_eligibility_or_deduped"
        )
    finally:
        conn.close()


def test_paper_observer_preview_prioritizes_newest_signals(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("0")
        now = int(time.time())
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
        conn.execute("DELETE FROM wallet_activity WHERE address = ?", (address,))
        history = [
            _activity(asset=f"asset-history-{idx}", timestamp=now - 2_000 - idx, idx=2_000 + idx)
            for idx in range(98)
        ]
        older = _activity(asset="asset-older", timestamp=now - 240, idx=240)
        newer = _activity(asset="asset-newer", timestamp=now - 30, idx=30)
        persist_wallet_activity(conn, address, [*history, older, newer], ingested_at=now)

        preview = preview_paper_observer(conn, limit=1, now=now)

        assert preview.signals_seen == 1
        assert preview.signals[0]["asset_id"] == "asset-newer"
        assert preview.signals[0]["ingest_lag_sec"] == 30
    finally:
        conn.close()


def test_paper_observer_preview_explains_stale_buy_window(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("1")
        now = int(time.time())
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
        conn.execute("DELETE FROM wallet_activity WHERE address = ?", (address,))
        old_rows = [
            _activity(asset=f"asset-stale-{idx}", timestamp=now - 90_000 - idx, idx=idx)
            for idx in range(1, 101)
        ]
        persist_wallet_activity(conn, address, old_rows, ingested_at=now)

        preview = preview_paper_observer(conn, limit=10, max_signal_age_sec=3600, now=now)
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]

        assert preview.signals_seen == 0
        assert preview.paper_stage_wallets == 1
        assert preview.observer_wallets == 1
        assert preview.recent_buy_events == 0
        assert preview.paper_stage_recent_buy_events == 0
        assert preview.exploratory_copyability_recent_buy_events == 0
        assert preview.latest_buy_ts is not None
        assert preview.latest_buy_ingested_at is not None
        assert preview.latest_buy_age_sec is not None
        assert preview.latest_buy_age_sec > 3600
        assert preview.no_signal_reason == "latest_buy_outside_window"
        windows = {row["window_label"]: row for row in preview.window_diagnostics}
        assert windows["6h"]["eligible_signals"] == 0
        assert windows["6h"]["no_signal_reason"] == "latest_buy_outside_window"
        assert windows["24h"]["eligible_signals"] == 0
        assert windows["72h"]["eligible_signals"] > 0
        assert windows["72h"]["max_ingest_lag_sec"] > 86_000
        assert windows["168h"]["eligible_signals"] > 0
        assert count == 0
    finally:
        conn.close()


def test_paper_observer_preview_cli_exports_json_without_writing_orders(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        address = _candidate_address("f")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
    finally:
        conn.close()

    out = tmp_path / "paper_observer_preview.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "paper-observer-preview",
            "--out",
            str(out),
        ],
    )

    assert main() == 0
    captured = json.loads(capsys.readouterr().out)
    payload = json.loads(out.read_text(encoding="utf-8"))
    conn = connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
    finally:
        conn.close()

    assert payload["schema_version"] == "paper_observer_preview_v1"
    assert payload["signals_seen"] == 1
    assert payload["signals"][0]["wallet"] == address
    assert captured["schema_version"] == payload["schema_version"]
    assert count == 0


def test_paper_observer_evaluate_quotes_without_writing_orders(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("2")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)

        evaluation = evaluate_paper_observer(
            conn,
            limit=10,
            max_stake_usd=25,
            client=BOOK_CLIENT,
        )
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]

        assert evaluation.schema_version == "paper_observer_evaluation_v1"
        assert evaluation.signals_seen == 1
        assert evaluation.quotes_attempted == 1
        assert evaluation.quotes_succeeded == 1
        assert evaluation.accepted_signals == 1
        assert evaluation.actionable_signals == 1
        assert evaluation.stale_signal_rejections == 0
        assert evaluation.rejected_signals == 0
        assert evaluation.evaluations_persisted == 0
        assert evaluation.evaluations[0]["wallet"] == address
        assert evaluation.evaluations[0]["accepted"] is True
        assert evaluation.evaluations[0]["actionable"] is True
        assert evaluation.evaluations[0]["actionability_reason"] == "actionable_quote"
        assert evaluation.evaluations[0]["best_ask"] == 0.55
        assert evaluation.evaluations[0]["executable_price"] == 0.55
        assert evaluation.evaluations[0]["observer_action"] == "external_paper_evaluate_no_order"
        assert count == 0
    finally:
        conn.close()


def test_paper_observer_evaluate_can_persist_quoteability_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("4")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)

        evaluation = evaluate_paper_observer(
            conn,
            limit=10,
            max_stake_usd=25,
            persist=True,
            client=BOOK_CLIENT,
        )
        order_count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
        evidence = conn.execute("SELECT * FROM paper_signal_evaluations WHERE wallet = ?", (address,)).fetchone()
        trial = conn.execute("SELECT * FROM paper_observer_trials WHERE wallet = ?", (address,)).fetchone()

        assert evaluation.evaluations_persisted == 1
        assert evaluation.trials_opened == 1
        assert order_count == 0
        assert evidence is not None
        assert trial is not None
        assert evidence["signal_id"] == evaluation.evaluations[0]["signal_id"]
        assert evidence["accepted"] == 1
        assert evidence["actionable"] == 1
        assert evidence["actionability_reason"] == "actionable_quote"
        assert evidence["decision_reason"] == "paper_clob_vwap"
        assert evidence["best_ask"] == 0.55
        assert evidence["executable_price"] == 0.55
        assert trial["entry_price"] == 0.55
        assert trial["stake_usd"] == 25
        assert trial["status"] == "open"
    finally:
        conn.close()


def test_paper_observer_persist_consumes_accepted_signal_once(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("a")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)

        first = evaluate_paper_observer(
            conn,
            limit=1,
            persist=True,
            client=BOOK_CLIENT,
        )
        second = evaluate_paper_observer(
            conn,
            limit=1,
            persist=True,
            client=BOOK_CLIENT,
        )

        assert first.signals_seen == 1
        assert first.evaluations_persisted == 1
        assert first.trials_opened == 1
        assert second.signals_seen == 0
        assert second.evaluations_persisted == 0
        assert second.trials_opened == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_signal_evaluations").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM paper_observer_trials").fetchone()[0] == 1
    finally:
        conn.close()


def test_paper_observer_retries_rejected_quote_after_cooldown(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("b")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
        now = int(time.time())

        first = evaluate_paper_observer(
            conn,
            limit=1,
            max_signal_age_sec=300,
            retry_cooldown_sec=60,
            now=now,
            persist=True,
            client=MissingBookClient(),
        )
        cooling_down = evaluate_paper_observer(
            conn,
            limit=1,
            max_signal_age_sec=300,
            retry_cooldown_sec=60,
            now=now + 30,
            persist=True,
            client=MissingBookClient(),
        )
        retried = evaluate_paper_observer(
            conn,
            limit=1,
            max_signal_age_sec=300,
            retry_cooldown_sec=60,
            now=now + 61,
            persist=True,
            client=MissingBookClient(),
        )

        assert first.signals_seen == 1
        assert first.accepted_signals == 0
        assert first.evaluations_persisted == 1
        assert cooling_down.signals_seen == 0
        assert retried.signals_seen == 1
        assert retried.accepted_signals == 0
        evidence = conn.execute(
            "SELECT accepted, evaluated_at FROM paper_signal_evaluations WHERE wallet = ?",
            (address,),
        ).fetchone()
        assert evidence["accepted"] == 0
        assert evidence["evaluated_at"] == now + 61
        assert conn.execute("SELECT COUNT(*) FROM paper_observer_trials").fetchone()[0] == 0
    finally:
        conn.close()


def test_paper_observer_prioritizes_unseen_market_before_newer_repeat(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("c")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
        conn.execute("DELETE FROM wallet_activity WHERE address = ?", (address,))
        now = int(time.time())
        history = [
            _activity(
                asset=f"asset-history-{index}",
                timestamp=now - 90_000 - index,
                idx=20_000 + index,
            )
            for index in range(99)
        ]
        seen = _activity(asset="asset-seen-first", timestamp=now - 120, idx=30_001)
        seen["conditionId"] = "condition-seen"
        seen["slug"] = "market-seen"
        persist_wallet_activity(conn, address, [*history, seen], ingested_at=now)

        first = evaluate_paper_observer(
            conn,
            limit=1,
            max_signal_age_sec=300,
            now=now,
            persist=True,
            client=BOOK_CLIENT,
        )
        assert first.evaluations[0]["market_slug"] == "market-seen"

        repeat = _activity(asset="asset-seen-repeat", timestamp=now + 10, idx=30_002)
        repeat["conditionId"] = "condition-seen"
        repeat["slug"] = "market-seen"
        unseen = _activity(asset="asset-unseen", timestamp=now + 5, idx=30_003)
        unseen["conditionId"] = "condition-unseen"
        unseen["slug"] = "market-unseen"
        persist_wallet_activity(conn, address, [repeat, unseen], ingested_at=now + 15)

        second = evaluate_paper_observer(
            conn,
            limit=1,
            max_signal_age_sec=300,
            now=now + 15,
            persist=True,
            client=BOOK_CLIENT,
        )

        assert second.signals_seen == 1
        assert second.evaluations[0]["market_slug"] == "market-unseen"
        assert conn.execute("SELECT COUNT(*) FROM paper_signal_evaluations").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM paper_observer_trials").fetchone()[0] == 2
    finally:
        conn.close()


def test_paper_observer_evaluate_marks_old_quoteable_signals_not_actionable(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("6")
        now = int(time.time())
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
        conn.execute("DELETE FROM wallet_activity WHERE address = ?", (address,))
        old_rows = [
            _activity(asset=f"asset-old-{idx}", timestamp=now - 90_000 - idx, idx=idx)
            for idx in range(1, 100)
        ]
        old = _activity(timestamp=now - 1_200, idx=777)
        persist_wallet_activity(conn, address, [*old_rows, old], ingested_at=now)

        evaluation = evaluate_paper_observer(
            conn,
            limit=10,
            max_stake_usd=25,
            max_actionable_signal_age_sec=300,
            now=now,
            persist=True,
            client=BOOK_CLIENT,
        )
        evidence = conn.execute("SELECT * FROM paper_signal_evaluations WHERE wallet = ?", (address,)).fetchone()
        trial_count = conn.execute("SELECT COUNT(*) FROM paper_observer_trials WHERE wallet = ?", (address,)).fetchone()[0]

        assert evaluation.accepted_signals == 1
        assert evaluation.actionable_signals == 0
        assert evaluation.trials_opened == 0
        assert evaluation.stale_signal_rejections == 1
        assert evaluation.evaluations[0]["accepted"] is True
        assert evaluation.evaluations[0]["actionable"] is False
        assert evaluation.evaluations[0]["actionability_reason"] == "signal_too_old"
        assert evidence["accepted"] == 1
        assert evidence["actionable"] == 0
        assert evidence["actionability_reason"] == "signal_too_old"
        assert trial_count == 0
    finally:
        conn.close()


def test_paper_observer_evaluate_cli_exports_json_without_writing_orders(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        address = _candidate_address("3")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
    finally:
        conn.close()

    out = tmp_path / "paper_observer_evaluation.json"
    monkeypatch.setattr(
        "pm_robot.orchestration.paper_runner.PublicPolymarketClient",
        lambda **kwargs: BOOK_CLIENT,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "paper-observer-evaluate",
            "--out",
            str(out),
            "--max-stake-usd",
            "25",
        ],
    )

    assert main() == 0
    captured = json.loads(capsys.readouterr().out)
    payload = json.loads(out.read_text(encoding="utf-8"))
    conn = connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
        evidence_count = conn.execute("SELECT COUNT(*) AS n FROM paper_signal_evaluations").fetchone()["n"]
        trial_count = conn.execute("SELECT COUNT(*) AS n FROM paper_observer_trials").fetchone()["n"]
    finally:
        conn.close()

    assert payload["schema_version"] == "paper_observer_evaluation_v1"
    assert payload["signals_seen"] == 1
    assert payload["accepted_signals"] == 1
    assert payload["actionable_signals"] == 1
    assert payload["evaluations"][0]["best_ask"] == 0.55
    assert captured["schema_version"] == payload["schema_version"]
    assert count == 0
    assert evidence_count == 0
    assert trial_count == 0


def test_paper_observer_evaluate_cli_can_persist_without_writing_orders(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        address = _candidate_address("5")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address, stage=CandidateStage.PAPER_APPROVED, score=83.11)
    finally:
        conn.close()

    out = tmp_path / "paper_observer_evaluation.json"
    monkeypatch.setattr(
        "pm_robot.orchestration.paper_runner.PublicPolymarketClient",
        lambda **kwargs: BOOK_CLIENT,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "paper-observer-evaluate",
            "--out",
            str(out),
            "--max-stake-usd",
            "25",
            "--persist",
        ],
    )

    assert main() == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    conn = connect(db_path)
    try:
        order_count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]
        evidence_count = conn.execute("SELECT COUNT(*) AS n FROM paper_signal_evaluations").fetchone()["n"]
        trial_count = conn.execute("SELECT COUNT(*) AS n FROM paper_observer_trials").fetchone()["n"]
    finally:
        conn.close()

    assert payload["evaluations_persisted"] == 1
    assert payload["trials_opened"] == 1
    assert payload["actionable_signals"] == 1
    assert payload["retry_cooldown_sec"] == 60
    assert payload["selection_mode"] == "incremental_unseen_market_first"
    assert order_count == 0
    assert evidence_count == 1
    assert trial_count == 1


def test_paper_runner_blocks_negative_settled_roi_wallet(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("c")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        _seed_paper_eligibility(conn, address)
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 10, 30, 40, 0, 1.0, 1000, -50, -0.05, 10, 0.01, 0, ?, ?)
            """,
            (address, json.dumps(["non_positive_settled_roi"]), int(time.time())),
        )
        summary = run_paper(conn, ledger_path=None, limit=10, client=BOOK_CLIENT)
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]

        assert summary.signals_seen == 0
        assert summary.orders_recorded == 0
        assert count == 0
    finally:
        conn.close()


def test_paper_runner_ignores_stale_backfill(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        address = _candidate_address("a")
        upsert_candidate(conn, CandidateAddress(address=address, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_CANDIDATE.value, address),
        )
        persist_wallet_activity(conn, address, [_activity()], ingested_at=int(time.time()))

        summary = run_paper(conn, ledger_path=None, limit=10, client=BOOK_CLIENT)
        count = conn.execute("SELECT COUNT(*) AS n FROM paper_orders").fetchone()["n"]

        assert summary.signals_seen == 0
        assert count == 0
    finally:
        conn.close()
