ALTER TABLE wallet_level_selections
    ADD COLUMN research_score REAL;

UPDATE wallet_level_selections
SET research_score = (
    SELECT summary.research_score
    FROM wallet_history_summaries AS summary
    WHERE summary.wallet = wallet_level_selections.wallet
      AND summary.artifact_id = wallet_level_selections.evidence_artifact_id
)
WHERE research_score IS NULL;

CREATE INDEX IF NOT EXISTS idx_wallet_level_selections_reference
    ON wallet_level_selections(target_level, policy_version, decided_at DESC, wallet);
