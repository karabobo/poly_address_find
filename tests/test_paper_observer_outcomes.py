import json
import sys

import pytest

from pm_robot.cli import main
from pm_robot.execution.market_marks import gamma_market_mark
from pm_robot.orchestration.paper_observer_outcomes import (
    paper_observer_trial_summary,
    settle_paper_observer_trials,
)
from pm_robot.storage.db import connect, run_migrations
from pm_robot.storage.repository import (
    list_gamma_market_backfill_targets,
    persist_paper_observer_trials,
    persist_paper_signal_evaluations,
    upsert_gamma_market_cache,
)


WALLET = "0x" + "a" * 40


def _evaluation(
    *,
    entry_price=0.55,
    signal_id="activity-1",
    market_slug="market-1",
    stake_usd=40.0,
):
    return {
        "signal_id": signal_id,
        "wallet": WALLET,
        "candidate_stage": "paper_approved",
        "validation_cohort": "validation",
        "market_slug": market_slug,
        "asset_id": "token-yes",
        "outcome": "Yes",
        "side": "BUY",
        "detected_at": 900,
        "signal_age_sec": 10,
        "leader_price": 0.5,
        "executable_price": entry_price,
        "stake_usd": stake_usd,
        "fee_usd": 0.04,
        "slippage_bps": 1_000.0,
        "accepted": True,
        "actionable": True,
    }


def _gamma_market(*, closed=False, yes_price="0.70"):
    return {
        "conditionId": "condition-1",
        "question": "Will this test pass?",
        "closed": closed,
        "active": not closed,
        "clobTokenIds": '["token-yes","token-no"]',
        "outcomes": '["Yes","No"]',
        "outcomePrices": f'["{yes_price}","{1 - float(yes_price):.2f}"]',
    }


def test_observer_trial_keeps_first_actionable_quote_and_never_writes_orders(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)

        assert persist_paper_observer_trials(conn, [_evaluation()], evaluated_at=1_000) == 1
        assert persist_paper_observer_trials(
            conn,
            [_evaluation(entry_price=0.75)],
            evaluated_at=1_100,
        ) == 0

        trial = conn.execute("SELECT * FROM paper_observer_trials WHERE signal_id = 'activity-1'").fetchone()
        assert trial["entry_price"] == 0.55
        assert trial["entry_evaluated_at"] == 1_000
        assert trial["shares"] == pytest.approx(40 / 0.55)
        assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 0
    finally:
        conn.close()


def test_observer_trial_rejects_invalid_probability(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)

        assert persist_paper_observer_trials(
            conn,
            [_evaluation(entry_price=1.01)],
            evaluated_at=1_000,
        ) == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_observer_trials").fetchone()[0] == 0
    finally:
        conn.close()


def test_gamma_raw_token_zero_price_is_a_valid_settlement_mark(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        upsert_gamma_market_cache(
            conn,
            market_slug="market-1",
            market={
                "closed": True,
                "active": False,
                "clobTokenIds": "[]",
                "outcomePrices": "[]",
                "tokens": [{"token_id": "token-yes", "price": 0}],
            },
            fetched_at=1_100,
            ttl_seconds=3_600,
        )
        row = conn.execute("SELECT * FROM gamma_market_cache WHERE market_slug = 'market-1'").fetchone()

        mark = gamma_market_mark(row, "token-yes")
        assert mark is not None
        assert mark.price == 0
        assert mark.is_settlement is True
    finally:
        conn.close()


def test_observer_trial_marks_then_resolves_from_gamma_without_execution_rows(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        persist_paper_observer_trials(conn, [_evaluation()], evaluated_at=1_000)
        upsert_gamma_market_cache(
            conn,
            market_slug="market-1",
            market=_gamma_market(),
            fetched_at=1_100,
            ttl_seconds=3_600,
        )

        marked = settle_paper_observer_trials(conn, now=1_200)
        open_trial = conn.execute("SELECT * FROM paper_observer_trials").fetchone()
        assert marked.trials_marked == 1
        assert marked.trials_resolved == 0
        assert open_trial["status"] == "open"
        assert open_trial["mark_price"] == 0.7
        assert open_trial["mark_source"] == "gamma_outcome_price"

        upsert_gamma_market_cache(
            conn,
            market_slug="market-1",
            market=_gamma_market(closed=True, yes_price="1"),
            fetched_at=1_300,
            ttl_seconds=3_600,
        )
        resolved = settle_paper_observer_trials(conn, now=1_400)
        trial = conn.execute("SELECT * FROM paper_observer_trials").fetchone()
        summary = paper_observer_trial_summary(conn)

        expected_pnl = (40 / 0.55) - 40.04
        assert resolved.trials_resolved == 1
        assert trial["status"] == "resolved"
        assert trial["mark_price"] == 1
        assert trial["mark_source"] == "gamma_settlement"
        assert trial["pnl_usd"] == pytest.approx(expected_pnl)
        assert trial["resolved_at"] == 1_400
        assert summary["resolved_trials"] == 1
        assert summary["resolved_markets"] == 1
        assert summary["settled_pnl_usd"] == pytest.approx(expected_pnl)
        assert summary["wallet_summaries"][0]["wallet"] == WALLET
        assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_fills").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0] == 0
    finally:
        conn.close()


def test_observer_summary_counts_repeated_trades_as_two_market_samples(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        evaluations = [
            _evaluation(signal_id=f"winning-{index}", market_slug="winning-market")
            for index in range(3)
        ]
        evaluations.append(_evaluation(signal_id="losing-1", market_slug="losing-market"))
        assert persist_paper_observer_trials(conn, evaluations, evaluated_at=1_000) == 4
        upsert_gamma_market_cache(
            conn,
            market_slug="winning-market",
            market=_gamma_market(closed=True, yes_price="1"),
            fetched_at=1_100,
            ttl_seconds=3_600,
        )
        upsert_gamma_market_cache(
            conn,
            market_slug="losing-market",
            market=_gamma_market(closed=True, yes_price="0"),
            fetched_at=1_100,
            ttl_seconds=3_600,
        )

        settle_paper_observer_trials(conn, now=1_200)
        summary = paper_observer_trial_summary(conn)
        wallet = summary["wallet_summaries"][0]

        assert summary["total_trials"] == 4
        assert summary["resolved_trials"] == 4
        assert summary["market_samples"] == 2
        assert summary["resolved_markets"] == 2
        assert summary["winning_markets"] == 1
        assert summary["win_rate_pct"] == 50.0
        assert summary["trial_win_rate_pct"] == 75.0
        assert wallet["market_samples"] == 2
        assert wallet["win_rate_pct"] == 50.0
        assert wallet["trial_win_rate_pct"] == 75.0
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("market_count", "win_count", "concentrated", "expected_status", "expected_direction"),
    [
        (19, 19, False, "collecting_outcomes", "positive"),
        (20, 15, False, "validated_promising", "positive"),
        (20, 10, False, "validated_mixed", "mixed"),
        (20, 5, False, "validated_negative", "negative"),
        (20, 15, True, "validation_concentrated", "positive"),
    ],
)
def test_observer_validation_requires_diverse_resolved_markets(
    tmp_path,
    market_count,
    win_count,
    concentrated,
    expected_status,
    expected_direction,
):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        evaluations = []
        for index in range(market_count):
            market_slug = f"market-{index:02d}"
            stake_usd = 800.0 if concentrated and index == 0 else 40.0
            evaluations.append(
                _evaluation(
                    entry_price=0.5,
                    signal_id=f"signal-{index:02d}",
                    market_slug=market_slug,
                    stake_usd=stake_usd,
                )
            )
            upsert_gamma_market_cache(
                conn,
                market_slug=market_slug,
                market=_gamma_market(
                    closed=True,
                    yes_price="1" if index < win_count else "0",
                ),
                fetched_at=1_100,
                ttl_seconds=3_600,
            )
        assert persist_paper_observer_trials(conn, evaluations, evaluated_at=1_000) == market_count

        settled = settle_paper_observer_trials(
            conn,
            now=1_200,
            policy={
                "observer_validation": {
                    "version": "test-market-samples",
                    "min_resolved_markets": 20,
                    "min_settled_cost_usd": 500,
                    "min_promising_roi_pct": 3,
                    "max_negative_roi_pct": -5,
                    "max_market_cost_share_pct": 25,
                }
            },
        )
        wallet = settled.wallet_summaries[0]

        assert wallet["resolved_markets"] == market_count
        assert wallet["validation_status"] == expected_status
        assert wallet["provisional_direction"] == expected_direction
        assert settled.validation_policy["version"] == "test-market-samples"
        assert settled.validation_counts == {expected_status: 1}
    finally:
        conn.close()


def test_paper_gamma_targets_include_open_observer_trials(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        persist_paper_observer_trials(conn, [_evaluation()], evaluated_at=1_000)

        targets = list_gamma_market_backfill_targets(
            conn,
            paper_only=True,
            now=1_100,
            limit=10,
        )
        assert targets == ["market-1"]

        upsert_gamma_market_cache(
            conn,
            market_slug="market-1",
            market=_gamma_market(closed=True, yes_price="1"),
            fetched_at=1_200,
            ttl_seconds=3_600,
        )
        settle_paper_observer_trials(conn, now=1_300)
        assert list_gamma_market_backfill_targets(
            conn,
            paper_only=True,
            now=1_400,
            limit=10,
        ) == []
    finally:
        conn.close()


def test_paper_gamma_targets_prioritize_observer_trials_over_legacy_fills(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        persist_paper_observer_trials(conn, [_evaluation()], evaluated_at=1_000)
        conn.execute(
            """
            INSERT INTO paper_orders(
                order_id, signal_id, wallet, market_slug, asset_id, outcome,
                side, price, stake_usd, route, accepted, reason, created_at
            ) VALUES ('legacy-order', 'legacy-signal', ?, 'legacy-market',
                      'legacy-token', 'Yes', 'BUY', 0.5, 10, 'paper', 1, 'test', 900)
            """,
            (WALLET,),
        )
        conn.execute(
            """
            INSERT INTO paper_fills(
                order_id, wallet, market_slug, asset_id, outcome, side,
                fill_price, stake_usd, shares, filled_at, source_order_created_at
            ) VALUES ('legacy-order', ?, 'legacy-market', 'legacy-token',
                      'Yes', 'BUY', 0.5, 10, 20, 900, 900)
            """,
            (WALLET,),
        )
        conn.commit()

        assert list_gamma_market_backfill_targets(
            conn,
            paper_only=True,
            now=1_100,
            limit=1,
        ) == ["market-1"]
    finally:
        conn.close()


def test_observer_trial_migration_seeds_existing_actionable_evidence(tmp_path):
    conn = connect(tmp_path / "robot.sqlite")
    try:
        run_migrations(conn)
        persist_paper_signal_evaluations(conn, [_evaluation()], evaluated_at=1_000)
        conn.execute("DROP TABLE paper_observer_trials")
        conn.execute("DELETE FROM schema_migrations WHERE version = 53")
        conn.commit()

        assert run_migrations(conn) == [53]
        trial = conn.execute("SELECT * FROM paper_observer_trials").fetchone()
        assert trial is not None
        assert trial["signal_id"] == "activity-1"
        assert trial["entry_price"] == 0.55
        assert trial["status"] == "open"
    finally:
        conn.close()


def test_paper_observer_settle_cli_exports_research_summary(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "robot.sqlite"
    conn = connect(db_path)
    try:
        run_migrations(conn)
        persist_paper_observer_trials(conn, [_evaluation()], evaluated_at=1_000)
        upsert_gamma_market_cache(
            conn,
            market_slug="market-1",
            market=_gamma_market(closed=True, yes_price="1"),
            fetched_at=1_100,
            ttl_seconds=3_600,
        )
    finally:
        conn.close()

    out = tmp_path / "paper_observer_outcomes.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pm-robot",
            "--env",
            str(tmp_path / "missing.env"),
            "--db",
            str(db_path),
            "paper-observer-settle",
            "--out",
            str(out),
        ],
    )

    assert main() == 0
    captured = json.loads(capsys.readouterr().out)
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["trials_resolved"] == 1
    assert payload["resolved_trials"] == 1
    assert captured == payload

    conn = connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM paper_orders").fetchone()[0] == 0
    finally:
        conn.close()
