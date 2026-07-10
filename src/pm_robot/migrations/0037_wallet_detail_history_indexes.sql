CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_wallet_updated
    ON pipeline_jobs(wallet, updated_at DESC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_updated
    ON pipeline_jobs(status, updated_at DESC, priority ASC, job_id DESC);

CREATE INDEX IF NOT EXISTS idx_review_events_address_created
    ON review_events(address, created_at DESC, event_id DESC);
