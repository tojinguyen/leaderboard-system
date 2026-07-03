package domain

// ScoreEvent đại diện cho event thay đổi điểm số của player
type ScoreEvent struct {
	UserID     string `json:"user_id"`
	ScoreDelta int64  `json:"score_delta"`
	Timestamp  int64  `json:"timestamp"` // Unix timestamp tính bằng giây
}

// LeaderboardEntry đại diện cho một bản ghi trong bảng xếp hạng
type LeaderboardEntry struct {
	UserID string `json:"user_id"`
	Score  int64  `json:"score"`
	Rank   int64  `json:"rank"` // 1-indexed rank
}

