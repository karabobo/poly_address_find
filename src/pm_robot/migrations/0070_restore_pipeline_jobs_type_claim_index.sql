CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_type_claim
    ON pipeline_jobs(job_type, status, shard, next_attempt_at, priority, updated_at);
