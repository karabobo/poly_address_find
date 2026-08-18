CREATE INDEX IF NOT EXISTS idx_wallet_level_selections_reference_wallet_latest
    ON wallet_level_selections(
        target_level, policy_version, wallet, decided_at DESC
    )
    WHERE research_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_wallet_history_summaries_method_rank
    ON wallet_history_summaries(
        history_depth, methodology_version, research_score DESC, wallet
    );
