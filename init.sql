CREATE TABLE IF NOT EXISTS leaderboards (
    user_id VARCHAR(100) PRIMARY KEY,
    total_score BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_leaderboards_total_score ON leaderboards(total_score DESC);
