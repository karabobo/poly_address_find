ALTER TABLE paper_signal_evaluations
    ADD COLUMN max_actionable_signal_age_sec INTEGER NOT NULL DEFAULT 300;

ALTER TABLE paper_signal_evaluations
    ADD COLUMN actionable INTEGER NOT NULL DEFAULT 0;

ALTER TABLE paper_signal_evaluations
    ADD COLUMN actionability_reason TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_paper_signal_evaluations_actionable_time
    ON paper_signal_evaluations(actionable, evaluated_at DESC);
