CREATE TABLE IF NOT EXISTS retention_cycle_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    database_id TEXT NOT NULL CHECK (length(database_id) = 32),
    mutation_generation INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO retention_cycle_state(
    singleton, database_id, mutation_generation, updated_at
) VALUES (1, lower(hex(randomblob(16))), 0, 0);
