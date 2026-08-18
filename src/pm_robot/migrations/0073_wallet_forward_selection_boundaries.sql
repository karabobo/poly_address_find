ALTER TABLE wallet_history_summaries
    ADD COLUMN diagnostic_score REAL;

ALTER TABLE wallet_history_summaries
    ADD COLUMN forward_selection_score REAL;

ALTER TABLE wallet_history_summaries
    ADD COLUMN forward_score_components_json TEXT NOT NULL DEFAULT '{}';

UPDATE wallet_history_summaries
SET diagnostic_score = research_score
WHERE diagnostic_score IS NULL;

ALTER TABLE wallet_level_selections
    ADD COLUMN forward_selection_score REAL;

ALTER TABLE wallet_level_selections
    ADD COLUMN score_status TEXT NOT NULL DEFAULT 'legacy'
        CHECK(score_status IN ('valid', 'legacy'));

CREATE TABLE IF NOT EXISTS wallet_level_review_state (
    wallet TEXT NOT NULL,
    target_level TEXT NOT NULL CHECK(target_level IN ('l3', 'l4', 'l5')),
    policy_version TEXT NOT NULL,
    methodology_version TEXT NOT NULL,
    review_state TEXT NOT NULL DEFAULT 'cooldown'
        CHECK(review_state IN ('active', 'cooldown', 'archived')),
    cooldown_until INTEGER NOT NULL DEFAULT 0,
    no_material_improvement_count INTEGER NOT NULL DEFAULT 0,
    last_evidence_artifact_id TEXT NOT NULL DEFAULT '',
    last_activity_count INTEGER NOT NULL DEFAULT 0,
    last_distinct_markets INTEGER NOT NULL DEFAULT 0,
    last_total_volume_usdc REAL NOT NULL DEFAULT 0,
    last_reviewed_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(wallet, target_level)
);

CREATE INDEX IF NOT EXISTS idx_wallet_level_review_state_due
    ON wallet_level_review_state(target_level, review_state, cooldown_until, wallet);

CREATE INDEX IF NOT EXISTS idx_wallet_levels_selection_candidates
    ON wallet_levels(level, hard_risk_block, wallet, level_updated_at);

CREATE INDEX IF NOT EXISTS idx_wallet_history_summaries_forward_rank
    ON wallet_history_summaries(
        history_depth, methodology_version, forward_selection_score DESC, wallet
    )
    WHERE forward_selection_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_wallet_level_selections_forward_reference
    ON wallet_level_selections(
        target_level, policy_version, score_status, wallet, decided_at DESC,
        evidence_artifact_id, forward_selection_score, research_score,
        updated_at, source_bucket, strategy_bucket
    )
    WHERE forward_selection_score IS NOT NULL AND score_status = 'valid';

CREATE INDEX IF NOT EXISTS idx_wallet_level_selections_artifact_policy
    ON wallet_level_selections(
        wallet, target_level, evidence_artifact_id, policy_version
    );
