CREATE TABLE IF NOT EXISTS pipeline_scheduler_state (
    job_type TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    current_weight INTEGER NOT NULL DEFAULT 0,
    last_selected_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (job_type, subject_key)
);
