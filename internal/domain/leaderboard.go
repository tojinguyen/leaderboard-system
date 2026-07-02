package domain

import "context"

// ScoreEvent đại diện cho event thay đổi điểm số của player
type ScoreEvent struct {
	UserID     string `json:"user_id"`
	ScoreDelta int64  `json:"score_delta"`
}

// LeaderboardEntry đại diện cho một bản ghi trong bảng xếp hạng
type LeaderboardEntry struct {
	UserID string `json:"user_id"`
	Score  int64  `json:"score"`
	Rank   int64  `json:"rank"` // 1-indexed rank
}

// ScoreProducer định nghĩa interface để gửi event lên Kafka
type ScoreProducer interface {
	PublishScoreEvent(ctx context.Context, event ScoreEvent) error
	Close() error
}

// LeaderboardRepository định nghĩa interface cho DB Persistence (PostgreSQL)
type LeaderboardRepository interface {
	UpsertScore(ctx context.Context, userID string, scoreDelta int64) error
	Close() error
}

// LeaderboardCache định nghĩa interface cho Cache Engine (Redis)
type LeaderboardCache interface {
	IncrementScore(ctx context.Context, userID string, scoreDelta int64) error
	GetTopPlayers(ctx context.Context, limit int) ([]LeaderboardEntry, error)
	GetUserRank(ctx context.Context, userID string) (*LeaderboardEntry, error)
	Close() error
}
