CREATE INDEX IF NOT EXISTS idx_paper_observer_trials_wallet_market
    ON paper_observer_trials(wallet, market_slug, detected_at DESC);
