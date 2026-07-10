CREATE INDEX IF NOT EXISTS idx_leader_scores_address_score_id
    ON leader_scores(address, score_id DESC);

CREATE INDEX IF NOT EXISTS idx_candidate_wallets_stage_updated
    ON candidate_wallets(candidate_stage, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_copy_leader_stats_leader_wallet
    ON copy_leader_stats(leader_wallet);

CREATE INDEX IF NOT EXISTS idx_copy_leader_performance_leader_wallet
    ON copy_leader_performance(leader_wallet);
