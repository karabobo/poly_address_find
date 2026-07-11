CREATE INDEX IF NOT EXISTS idx_wallet_positions_captured_at
    ON wallet_positions(captured_at);

CREATE INDEX IF NOT EXISTS idx_leader_scores_scored_at
    ON leader_scores(scored_at);

CREATE INDEX IF NOT EXISTS idx_review_events_created_at
    ON review_events(created_at);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_started_at
    ON ingest_runs(started_at);

CREATE INDEX IF NOT EXISTS idx_paper_marks_marked_at
    ON paper_marks(marked_at);

CREATE INDEX IF NOT EXISTS idx_paper_readiness_observations_observed_at
    ON paper_readiness_observations(observed_at);
