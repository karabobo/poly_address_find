CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_type_action_status_updated
ON pipeline_jobs(job_type, subject_key, status, updated_at);
