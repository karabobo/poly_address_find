CREATE INDEX IF NOT EXISTS idx_wallet_levels_l1_screen_plan
    ON wallet_levels(last_seen_at DESC, wallet)
    WHERE level = 'l1' AND hard_risk_block = 0;

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_wallet_screen_active_lookup
    ON pipeline_jobs(wallet, status, attempts, max_attempts)
    WHERE job_type = 'wallet_recent_screen'
      AND status IN ('running', 'queued');
