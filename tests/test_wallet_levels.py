from pm_robot.wallet_levels import (
    CollectionAction,
    HistoryDepth,
    LevelFacts,
    WalletLevel,
    decide_wallet_level,
    next_collection_action,
)


def test_wallet_level_contract_is_exactly_l0_through_l6():
    assert tuple(level.value for level in WalletLevel) == (
        "l0",
        "l1",
        "l2",
        "l3",
        "l4",
        "l5",
        "l6",
    )


def test_history_depth_does_not_reuse_wallet_level_names():
    assert tuple(depth.value for depth in HistoryDepth) == (
        "none",
        "sample",
        "light",
        "deep",
    )


def test_l0_to_l1_admits_trusted_sources_or_one_hundred_usdc_sample():
    small_trade = decide_wallet_level(
        LevelFacts(
            current_level=WalletLevel.L0,
            verified_trade=True,
            sample_volume_usdc=5,
        )
    )
    qualifying_trade = decide_wallet_level(
        LevelFacts(
            current_level=WalletLevel.L0,
            verified_trade=True,
            sample_volume_usdc=100,
        )
    )
    trusted = decide_wallet_level(LevelFacts(current_level=WalletLevel.L0, trusted_source=True))

    assert small_trade.level is WalletLevel.L0
    assert qualifying_trade.level is WalletLevel.L1
    assert trusted.level is WalletLevel.L1


def test_unverified_l0_sighting_does_not_schedule_history():
    facts = LevelFacts(current_level=WalletLevel.L0)

    assert decide_wallet_level(facts).level is WalletLevel.L0
    assert next_collection_action(facts) is CollectionAction.NONE


def test_l1_screen_uses_up_to_ten_recent_trades_and_one_hundred_usdc():
    qualified = LevelFacts(
        current_level=WalletLevel.L1,
        screen_complete=True,
        sample_trade_count=7,
        sample_volume_usdc=125.0,
    )
    thin = LevelFacts(
        current_level=WalletLevel.L1,
        screen_complete=True,
        sample_trade_count=10,
        sample_volume_usdc=99.99,
    )

    assert decide_wallet_level(qualified).level is WalletLevel.L2
    assert decide_wallet_level(thin).level is WalletLevel.L1


def test_collection_budget_grows_only_after_the_previous_level_qualifies():
    l1 = LevelFacts(current_level=WalletLevel.L1, screen_complete=False)
    l2 = LevelFacts(current_level=WalletLevel.L2, light_history_complete=False)
    l3 = LevelFacts(current_level=WalletLevel.L3, deep_history_complete=False)

    assert next_collection_action(l1) is CollectionAction.SCREEN_RECENT
    assert next_collection_action(l2) is CollectionAction.COLLECT_LIGHT_HISTORY
    assert next_collection_action(l3) is CollectionAction.COLLECT_DEEP_HISTORY


def test_rank_selection_controls_expensive_promotions_without_fixed_score_cutoffs():
    light = LevelFacts(
        current_level=WalletLevel.L2,
        light_history_complete=True,
        selected_for_l3=True,
    )
    deep = LevelFacts(
        current_level=WalletLevel.L3,
        deep_history_complete=True,
        selected_for_l4=True,
    )
    elite = LevelFacts(current_level=WalletLevel.L4, selected_for_l5=True)

    assert decide_wallet_level(light).level is WalletLevel.L3
    assert decide_wallet_level(deep).level is WalletLevel.L4
    assert decide_wallet_level(elite).level is WalletLevel.L5


def test_l6_requires_independent_validation_after_l5_ranking():
    pending = LevelFacts(current_level=WalletLevel.L5)
    verified = LevelFacts(
        current_level=WalletLevel.L5,
        independent_validation_passed=True,
    )

    assert decide_wallet_level(pending).level is WalletLevel.L5
    assert decide_wallet_level(pending).reason == "independent_validation_pending"
    assert decide_wallet_level(verified).level is WalletLevel.L6
    assert decide_wallet_level(verified).reason == "independent_validation_passed"


def test_level_reconciliation_never_skips_or_automatically_demotes():
    flashy_l0 = LevelFacts(
        current_level=WalletLevel.L0,
        verified_trade=True,
        screen_complete=True,
        sample_trade_count=10,
        sample_volume_usdc=1_000_000.0,
        light_history_complete=True,
        selected_for_l3=True,
        deep_history_complete=True,
        selected_for_l4=True,
        selected_for_l5=True,
    )
    existing_l4 = LevelFacts(current_level=WalletLevel.L4, hard_risk_block=True)

    assert decide_wallet_level(flashy_l0).level is WalletLevel.L1
    assert decide_wallet_level(existing_l4).level is WalletLevel.L4


def test_l4_l5_and_l6_never_trigger_more_history_collection():
    assert next_collection_action(LevelFacts(current_level=WalletLevel.L4)) is CollectionAction.NONE
    assert next_collection_action(LevelFacts(current_level=WalletLevel.L5)) is CollectionAction.NONE
    assert next_collection_action(LevelFacts(current_level=WalletLevel.L6)) is CollectionAction.NONE
