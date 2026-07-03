package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"leaderboard-system/internal/domain"
)

// LeaderboardRepository định nghĩa giao tiếp lưu trữ dữ liệu (Consumer side)
type LeaderboardRepository interface {
	UpsertScore(ctx context.Context, userID string, scoreDelta int64) error
}

// LeaderboardCache định nghĩa giao tiếp ghi cache (Consumer side)
type LeaderboardCache interface {
	IncrementScore(ctx context.Context, userID string, scoreDelta int64, timestamp int64) error
}

type ConsumerService struct {
	repo  LeaderboardRepository
	cache LeaderboardCache
}

// NewConsumerService khởi tạo Consumer Service chứa business logic của consumers
func NewConsumerService(repo LeaderboardRepository, cache LeaderboardCache) *ConsumerService {
	return &ConsumerService{
		repo:  repo,
		cache: cache,
	}
}

// ExecuteWithRetry thực thi một hàm nghiệp vụ với cơ chế Exponential Backoff
func ExecuteWithRetry(ctx context.Context, opName string, fn func() error) error {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		err := fn()
		if err == nil {
			return nil
		}

		log.Printf("Operation [%s] failed: %v. Retrying in %v...", opName, err, backoff)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry of [%s]: %w", opName, ctx.Err())
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// HandleDBConsumption xử lý logic nghiệp vụ cho Consumer Group A (Ghi vào PostgreSQL)
func (s *ConsumerService) HandleDBConsumption(ctx context.Context, msg kafka.Message) error {
	var event domain.ScoreEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("DB Consumer: corrupt message payload at offset %d: %v. Skipping...", msg.Offset, err)
		return nil // Trả về nil để skip message hỏng, tránh làm đứng cứng hệ thống
	}

	// Đảm bảo event hợp lệ
	if event.UserID == "" {
		log.Printf("DB Consumer: empty user_id at offset %d. Skipping...", msg.Offset)
		return nil
	}

	// Thực hiện upsert DB với cơ chế retry vô hạn (cho đến khi DB sống lại hoặc shutdown)
	err := ExecuteWithRetry(ctx, fmt.Sprintf("DB Upsert User %s", event.UserID), func() error {
		return s.repo.UpsertScore(ctx, event.UserID, event.ScoreDelta)
	})
	if err != nil {
		return err
	}

	log.Printf("DB Consumer: saved offset %d (user: %s, delta: %d)", msg.Offset, event.UserID, event.ScoreDelta)
	return nil
}

// HandleRedisConsumption xử lý logic nghiệp vụ cho Consumer Group B (Cập nhật Redis ZSET)
func (s *ConsumerService) HandleRedisConsumption(ctx context.Context, msg kafka.Message) error {
	var event domain.ScoreEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Redis Consumer: corrupt message payload at offset %d: %v. Skipping...", msg.Offset, err)
		return nil // Skip message hỏng
	}

	if event.UserID == "" {
		log.Printf("Redis Consumer: empty user_id at offset %d. Skipping...", msg.Offset)
		return nil
	}

	// Thực hiện increment Redis score với cơ chế retry vô hạn
	err := ExecuteWithRetry(ctx, fmt.Sprintf("Redis ZINCRBY User %s", event.UserID), func() error {
		return s.cache.IncrementScore(ctx, event.UserID, event.ScoreDelta, event.Timestamp)
	})
	if err != nil {
		return err
	}

	log.Printf("Redis Consumer: saved offset %d (user: %s, delta: %d)", msg.Offset, event.UserID, event.ScoreDelta)
	return nil
}
