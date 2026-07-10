CREATE INDEX IF NOT EXISTS idx_wallet_processing_state_backlog
    ON wallet_processing_state(next_action, evidence_status, priority, updated_at, wallet);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_wallet_action_active
    ON pipeline_jobs(job_type, wallet, subject_key, status);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_completed
    ON pipeline_jobs(status, job_type, completed_at);
