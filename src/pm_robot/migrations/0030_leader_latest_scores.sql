CREATE TABLE IF NOT EXISTS leader_latest_scores (
    address TEXT PRIMARY KEY REFERENCES candidate_wallets(address) ON DELETE CASCADE,
    score_id INTEGER NOT NULL UNIQUE,
    leader_score REAL NOT NULL,
    review_stage TEXT NOT NULL,
    review_reason TEXT NOT NULL,
    components_json TEXT NOT NULL DEFAULT '{}',
    penalties_json TEXT NOT NULL DEFAULT '{}',
    policy_version TEXT NOT NULL DEFAULT '',
    scored_at INTEGER NOT NULL
);

INSERT INTO leader_latest_scores(
    address, score_id, leader_score, review_stage, review_reason,
    components_json, penalties_json, policy_version, scored_at
)
SELECT
    ls.address, ls.score_id, ls.leader_score, ls.review_stage, ls.review_reason,
    ls.components_json, ls.penalties_json, ls.policy_version, ls.scored_at
FROM leader_scores ls
JOIN (
    SELECT address, MAX(score_id) AS score_id
    FROM leader_scores
    GROUP BY address
) latest
  ON latest.score_id = ls.score_id
ON CONFLICT(address) DO UPDATE SET
    score_id = excluded.score_id,
    leader_score = excluded.leader_score,
    review_stage = excluded.review_stage,
    review_reason = excluded.review_reason,
    components_json = excluded.components_json,
    penalties_json = excluded.penalties_json,
    policy_version = excluded.policy_version,
    scored_at = excluded.scored_at
WHERE excluded.score_id > leader_latest_scores.score_id;

CREATE TRIGGER IF NOT EXISTS trg_leader_scores_latest_after_insert
AFTER INSERT ON leader_scores
BEGIN
    INSERT INTO leader_latest_scores(
        address, score_id, leader_score, review_stage, review_reason,
        components_json, penalties_json, policy_version, scored_at
    )
    VALUES (
        NEW.address, NEW.score_id, NEW.leader_score, NEW.review_stage, NEW.review_reason,
        NEW.components_json, NEW.penalties_json, NEW.policy_version, NEW.scored_at
    )
    ON CONFLICT(address) DO UPDATE SET
        score_id = excluded.score_id,
        leader_score = excluded.leader_score,
        review_stage = excluded.review_stage,
        review_reason = excluded.review_reason,
        components_json = excluded.components_json,
        penalties_json = excluded.penalties_json,
        policy_version = excluded.policy_version,
        scored_at = excluded.scored_at
    WHERE excluded.score_id > leader_latest_scores.score_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_leader_scores_latest_after_update
AFTER UPDATE ON leader_scores
BEGIN
    INSERT INTO leader_latest_scores(
        address, score_id, leader_score, review_stage, review_reason,
        components_json, penalties_json, policy_version, scored_at
    )
    VALUES (
        NEW.address, NEW.score_id, NEW.leader_score, NEW.review_stage, NEW.review_reason,
        NEW.components_json, NEW.penalties_json, NEW.policy_version, NEW.scored_at
    )
    ON CONFLICT(address) DO UPDATE SET
        score_id = excluded.score_id,
        leader_score = excluded.leader_score,
        review_stage = excluded.review_stage,
        review_reason = excluded.review_reason,
        components_json = excluded.components_json,
        penalties_json = excluded.penalties_json,
        policy_version = excluded.policy_version,
        scored_at = excluded.scored_at
    WHERE excluded.score_id >= leader_latest_scores.score_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_leader_scores_latest_after_delete
AFTER DELETE ON leader_scores
WHEN OLD.score_id = (SELECT score_id FROM leader_latest_scores WHERE address = OLD.address)
BEGIN
    DELETE FROM leader_latest_scores
    WHERE address = OLD.address;

    INSERT INTO leader_latest_scores(
        address, score_id, leader_score, review_stage, review_reason,
        components_json, penalties_json, policy_version, scored_at
    )
    SELECT
        ls.address, ls.score_id, ls.leader_score, ls.review_stage, ls.review_reason,
        ls.components_json, ls.penalties_json, ls.policy_version, ls.scored_at
    FROM leader_scores ls
    WHERE ls.address = OLD.address
    ORDER BY ls.score_id DESC
    LIMIT 1;
END;
