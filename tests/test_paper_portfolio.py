import json
from dataclasses import replace

from pm_robot.execution.paper_broker import PaperBroker
from pm_robot.execution.paper_portfolio import paper_readiness_rows, settle_paper_portfolio
from pm_robot.models import CandidateAddress, CandidateStage, TradeSignal, WalletFeatures
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import apply_paper_quality_blocks, upsert_candidate, upsert_wallet_feature


def _quoted(signal: TradeSignal, stake: float) -> TradeSignal:
    return replace(
        signal,
        best_bid=max(signal.price - 0.01, 0.01),
        best_ask=min(signal.price + 0.01, 0.99),
        executable_price=min(signal.price + 0.01, 0.99),
        fillable_stake_usd=stake,
        quote_snapshot_at=1,
        quote_source="test_book",
    )


def _production_features(conn, wallet: str) -> None:
    upsert_wallet_feature(
        conn,
        WalletFeatures(
            address=wallet,
            hygiene_status="clean",
            maker_fraction=0.1,
            copy_event_count=20,
            edge_retention_pct=80,
            walk_forward_consistency_pct=100,
            extra={"maker_fraction_source": "verified_test_fixture"},
        ),
    )


def _insert_l3_evidence(conn, wallet: str, *, updated_at: int = 20) -> None:
    conn.execute(
        """
        INSERT INTO wallet_processing_state(
            wallet, discovery_tier, evidence_status, evidence_depth,
            evidence_confidence, priority, current_stage, next_action,
            next_action_at, activity_count, distinct_markets,
            non_fast_trade_count, updated_at
        ) VALUES (?, 'l3_deep', 'summary_ready', 1000, 1.0, 10, 'deep_done',
                  'score_wallet', 0, 1000, 20, 200, ?)
        """,
        (wallet, updated_at),
    )


def test_paper_portfolio_marks_positions_from_gamma_cache(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        signal = TradeSignal(
            signal_id="signal-1",
            wallet="0x" + "1" * 40,
            market_slug="market-1",
            asset_id="asset-yes",
            outcome="YES",
            side="BUY",
            price=0.5,
            detected_at=1,
        )
        broker = PaperBroker(conn=conn, max_stake_usd=50)
        signal = _quoted(signal, 50)
        decision = broker.evaluate(signal)
        broker.submit(signal, decision)
        conn.execute(
            """
            INSERT INTO gamma_market_cache(
                market_slug, condition_id, event_slug, question, title, category,
                end_date, closed, active, archived, clob_token_ids_json,
                outcomes_json, outcome_prices_json, raw_json, fetched_at, expires_at
            ) VALUES (?, '', '', '', '', '', '', 0, 1, 0, ?, ?, ?, '{}', 10, 1000)
            """,
            (
                "market-1",
                json.dumps(["asset-yes", "asset-no"]),
                json.dumps(["YES", "NO"]),
                json.dumps(["0.70", "0.30"]),
            ),
        )
        conn.commit()

        summary = settle_paper_portfolio(conn, now=20)
        fill = conn.execute("SELECT * FROM paper_fills").fetchone()
        position = conn.execute("SELECT * FROM paper_positions").fetchone()
        performance = conn.execute("SELECT * FROM paper_wallet_performance").fetchone()

        assert summary.fills_created == 1
        assert summary.positions_written == 1
        assert summary.marks_written == 1
        assert summary.wallets_written == 1
        assert summary.missing_marks == 0
        assert round(fill["shares"], 6) == round(50 / 0.51, 6)
        assert fill["leader_price"] == 0.5
        assert fill["fill_price"] == 0.51
        assert fill["fee_usd"] == 0.5
        assert position["mark_price"] == 0.7
        assert round(position["unrealized_pnl_usd"], 2) == 18.13
        assert performance["orders"] == 1
        assert round(performance["roi"], 2) == 0.36
    finally:
        conn.close()


def test_paper_portfolio_falls_back_to_entry_price_when_mark_missing(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        signal = TradeSignal(
            signal_id="signal-2",
            wallet="0x" + "2" * 40,
            market_slug="missing-market",
            asset_id="asset",
            outcome="YES",
            side="BUY",
            price=0.25,
            detected_at=1,
        )
        broker = PaperBroker(conn=conn, max_stake_usd=25)
        signal = _quoted(signal, 25)
        broker.submit(signal, broker.evaluate(signal))

        summary = settle_paper_portfolio(conn, now=20)
        position = conn.execute("SELECT * FROM paper_positions").fetchone()

        assert summary.missing_marks == 1
        assert round(position["mark_price"], 4) == 0.2626
        assert abs(position["unrealized_pnl_usd"]) < 1e-10
    finally:
        conn.close()


def test_paper_portfolio_settles_closed_gamma_market(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        signal = TradeSignal(
            signal_id="signal-3",
            wallet="0x" + "3" * 40,
            market_slug="closed-market",
            asset_id="asset-yes",
            outcome="YES",
            side="BUY",
            price=0.4,
            detected_at=1,
        )
        broker = PaperBroker(conn=conn, max_stake_usd=40)
        signal = _quoted(signal, 40)
        broker.submit(signal, broker.evaluate(signal))
        conn.execute(
            """
            INSERT INTO gamma_market_cache(
                market_slug, condition_id, event_slug, question, title, category,
                end_date, closed, active, archived, clob_token_ids_json,
                outcomes_json, outcome_prices_json, raw_json, fetched_at, expires_at
            ) VALUES (?, '', '', '', '', '', '', 1, 0, 0, ?, ?, ?, '{}', 10, 4102444800)
            """,
            (
                "closed-market",
                json.dumps(["asset-yes", "asset-no"]),
                json.dumps(["YES", "NO"]),
                json.dumps(["1", "0"]),
            ),
        )
        conn.commit()

        summary = settle_paper_portfolio(conn, now=20)
        position = conn.execute("SELECT * FROM paper_positions").fetchone()
        settlement = conn.execute("SELECT * FROM paper_settlements").fetchone()
        performance = conn.execute("SELECT * FROM paper_wallet_performance").fetchone()
        quality = paper_readiness_rows(conn)[0]

        assert summary.settlements_written == 1
        assert position["status"] == "resolved"
        assert position["unrealized_pnl_usd"] == 0
        assert round(position["realized_pnl_usd"], 2) == 57.16
        assert settlement["settlement_price"] == 1
        assert round(settlement["payout_usd"], 2) == 97.56
        assert performance["open_positions"] == 0
        assert round(performance["realized_pnl_usd"], 2) == 57.16
        assert quality["settled_positions"] == 1
        assert quality["production_ready"] == 0
        assert "insufficient_paper_orders" in quality["blockers_json"]
        assert "market_concentration_exceeded" in quality["blockers_json"]
        assert "insufficient_validation_period" in quality["blockers_json"]
    finally:
        conn.close()


def test_paper_portfolio_blocks_negative_settled_roi_candidate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "4" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.PAPER_CANDIDATE.value, wallet),
        )
        signal = TradeSignal(
            signal_id="signal-4",
            wallet=wallet,
            market_slug="lost-market",
            asset_id="asset-yes",
            outcome="YES",
            side="BUY",
            price=0.4,
            detected_at=1,
        )
        broker = PaperBroker(conn=conn, max_stake_usd=40)
        signal = _quoted(signal, 40)
        broker.submit(signal, broker.evaluate(signal))
        conn.execute(
            """
            INSERT INTO gamma_market_cache(
                market_slug, condition_id, event_slug, question, title, category,
                end_date, closed, active, archived, clob_token_ids_json,
                outcomes_json, outcome_prices_json, raw_json, fetched_at, expires_at
            ) VALUES (?, '', '', '', '', '', '', 1, 0, 0, ?, ?, ?, '{}', 10, 4102444800)
            """,
            (
                "lost-market",
                json.dumps(["asset-yes", "asset-no"]),
                json.dumps(["YES", "NO"]),
                json.dumps(["0", "1"]),
            ),
        )
        conn.commit()

        summary = settle_paper_portfolio(conn, now=20)
        candidate = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()
        event = conn.execute("SELECT * FROM review_events WHERE address = ?", (wallet,)).fetchone()

        assert summary.wallets_blocked == 1
        assert candidate["candidate_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert event["from_stage"] == CandidateStage.PAPER_CANDIDATE.value
        assert event["to_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert event["reason"] == "paper_quality_risk_block"
    finally:
        conn.close()


def test_paper_quality_restores_ready_candidate_to_live_eligible(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "5" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        _production_features(conn, wallet)
        _insert_l3_evidence(conn, wallet)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_COPYABILITY.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                wallet,
                CandidateStage.PAPER_CANDIDATE.value,
                CandidateStage.BLOCKED_COPYABILITY.value,
                "paper_quality_non_positive_settled_roi",
                10,
            ),
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
            """,
            (wallet,),
        )
        for observed_at in (20, 1820, 3620):
            conn.execute(
                """
                INSERT INTO paper_readiness_observations(
                    wallet, observed_at, orders, settled_positions, mark_coverage,
                    settled_roi, total_roi, production_ready, blockers_json
                ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, 1, '[]')
                """,
                (wallet, observed_at),
            )
        conn.commit()

        blocked = apply_paper_quality_blocks(conn, now=30)
        candidate = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()
        event = conn.execute(
            "SELECT * FROM review_events WHERE address = ? ORDER BY created_at DESC LIMIT 1",
            (wallet,),
        ).fetchone()

        assert blocked == 0
        assert candidate["candidate_stage"] == CandidateStage.LIVE_ELIGIBLE.value
        assert event["from_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
        assert event["to_stage"] == CandidateStage.LIVE_ELIGIBLE.value
        assert event["reason"] == "paper_quality_production_ready"
    finally:
        conn.close()


def test_paper_quality_restores_ready_candidate_after_long_ready_streak(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "9" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        _production_features(conn, wallet)
        _insert_l3_evidence(conn, wallet)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.NEEDS_REVIEW.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
            """,
            (wallet,),
        )
        for observed_at, ready in ((1, 0), (100, 1), (1900, 1), (3600, 1), (3700, 1)):
            conn.execute(
                """
                INSERT INTO paper_readiness_observations(
                    wallet, observed_at, orders, settled_positions, mark_coverage,
                    settled_roi, total_roi, production_ready, blockers_json
                ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, ?, ?)
                """,
                (wallet, observed_at, ready, "[]" if ready else '["non_positive_total_roi"]'),
            )
        conn.commit()

        blocked = apply_paper_quality_blocks(conn, now=3800)
        candidate = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert blocked == 0
        assert candidate["candidate_stage"] == CandidateStage.LIVE_ELIGIBLE.value
    finally:
        conn.close()


def test_paper_quality_does_not_promote_without_l3_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "a" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        _production_features(conn, wallet)
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.NEEDS_REVIEW.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
            """,
            (wallet,),
        )
        for observed_at in (20, 1820, 3620):
            conn.execute(
                """
                INSERT INTO paper_readiness_observations(
                    wallet, observed_at, orders, settled_positions, mark_coverage,
                    settled_roi, total_roi, production_ready, blockers_json
                ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, 1, '[]')
                """,
                (wallet, observed_at),
            )
        conn.commit()

        apply_paper_quality_blocks(conn, now=3800)
        stage = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()["candidate_stage"]

        assert stage == CandidateStage.NEEDS_REVIEW.value
    finally:
        conn.close()


def test_paper_quality_does_not_restore_unstable_ready_candidate(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "6" * 40
        upsert_candidate(conn, CandidateAddress(address=wallet, sources="test"))
        conn.execute(
            "UPDATE candidate_wallets SET candidate_stage = ? WHERE address = ?",
            (CandidateStage.BLOCKED_COPYABILITY.value, wallet),
        )
        conn.execute(
            """
            INSERT INTO review_events(address, from_stage, to_stage, reason, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                wallet,
                CandidateStage.PAPER_CANDIDATE.value,
                CandidateStage.BLOCKED_COPYABILITY.value,
                "paper_quality_non_positive_settled_roi",
                10,
            ),
        )
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
            """,
            (wallet,),
        )
        for observed_at in (20, 30):
            conn.execute(
                """
                INSERT INTO paper_readiness_observations(
                    wallet, observed_at, orders, settled_positions, mark_coverage,
                    settled_roi, total_roi, production_ready, blockers_json
                ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, 1, '[]')
                """,
                (wallet, observed_at),
            )
        conn.commit()

        blocked = apply_paper_quality_blocks(conn, now=40)
        candidate = conn.execute(
            "SELECT candidate_stage FROM candidate_wallets WHERE address = ?",
            (wallet,),
        ).fetchone()

        assert blocked == 0
        assert candidate["candidate_stage"] == CandidateStage.BLOCKED_COPYABILITY.value
    finally:
        conn.close()


def test_paper_readiness_reports_stable_progress(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "7" * 40
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
            """,
            (wallet,),
        )
        for observed_at in (20, 1820, 3620):
            conn.execute(
                """
                INSERT INTO paper_readiness_observations(
                    wallet, observed_at, orders, settled_positions, mark_coverage,
                    settled_roi, total_roi, production_ready, blockers_json
                ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, 1, '[]')
                """,
                (wallet, observed_at),
            )
        conn.commit()

        rows = paper_readiness_rows(conn)

        assert rows[0]["stable_ready_observations"] == 3
        assert rows[0]["stable_observation_count"] == 3
        assert rows[0]["stable_ready_span_seconds"] == 3600
        assert rows[0]["stable_production_ready"] == 1
    finally:
        conn.close()


def test_paper_readiness_uses_consecutive_ready_streak(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        wallet = "0x" + "8" * 40
        conn.execute(
            """
            INSERT INTO paper_wallet_quality(
                wallet, orders, open_positions, settled_positions,
                gamma_marked_positions, fallback_marked_positions, mark_coverage,
                settled_cost_usd, settled_pnl_usd, settled_roi,
                total_pnl_usd, total_roi, production_ready, blockers_json, updated_at
            ) VALUES (?, 250, 20, 40, 60, 0, 1, 1000, 100, 0.1, 120, 0.12, 1, '[]', 20)
            """,
            (wallet,),
        )
        for observed_at, ready in ((1, 0), (100, 1), (1900, 1), (3600, 1), (3700, 1)):
            conn.execute(
                """
                INSERT INTO paper_readiness_observations(
                    wallet, observed_at, orders, settled_positions, mark_coverage,
                    settled_roi, total_roi, production_ready, blockers_json
                ) VALUES (?, ?, 250, 40, 1, 0.1, 0.12, ?, ?)
                """,
                (wallet, observed_at, ready, "[]" if ready else '["non_positive_total_roi"]'),
            )
        conn.commit()

        rows = paper_readiness_rows(conn)

        assert rows[0]["stable_ready_observations"] == 4
        assert rows[0]["stable_ready_span_seconds"] == 3600
        assert rows[0]["stable_production_ready"] == 1
    finally:
        conn.close()


def test_unchanged_marks_and_readiness_are_heartbeat_throttled(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        signal = _quoted(
            TradeSignal(
                signal_id="signal-throttle",
                wallet="0x" + "a" * 40,
                market_slug="market-throttle",
                asset_id="asset-throttle",
                outcome="YES",
                side="BUY",
                price=0.5,
                detected_at=1,
            ),
            10,
        )
        broker = PaperBroker(conn=conn, max_stake_usd=10)
        broker.submit(signal, broker.evaluate(signal))

        first = settle_paper_portfolio(conn, now=100)
        second = settle_paper_portfolio(conn, now=200)

        assert first.marks_written == 1
        assert second.marks_written == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_marks").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM paper_readiness_observations").fetchone()[0] == 1
    finally:
        conn.close()
