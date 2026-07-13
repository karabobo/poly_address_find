CREATE INDEX IF NOT EXISTS idx_paper_observer_trials_cohort_wallet_market
    ON paper_observer_trials(validation_cohort, wallet, market_slug, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_paper_signal_evaluations_cohort_wallet_time
    ON paper_signal_evaluations(validation_cohort, wallet, evaluated_at DESC);
