package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"leaderboard-system/internal/domain"
)

var ErrUserNotFound = errors.New("user not found in leaderboard")

const LeaderboardKey = "global_leaderboard"

type RedisCache struct {
	client *redis.Client
}

// NewRedisCache khởi tạo kết nối Redis Client với cơ chế Retry Exponential Backoff
func NewRedisCache(ctx context.Context, addr string, password string, db int) (domain.LeaderboardCache, error) {
	var client *redis.Client
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		client = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		})

		_, err := client.Ping(ctx).Result()
		if err == nil {
			log.Println("Successfully connected to Redis")
			break
		}

		log.Printf("Failed to connect to Redis, retrying in %v: %v", backoff, err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled during Redis connection retry: %w", ctx.Err())
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	return &RedisCache{client: client}, nil
}

// IncrementScore cập nhật điểm số cho user trong Redis Sorted Set
func (c *RedisCache) IncrementScore(ctx context.Context, userID string, scoreDelta int64) error {
	_, err := c.client.ZIncrBy(ctx, LeaderboardKey, float64(scoreDelta), userID).Result()
	if err != nil {
		return fmt.Errorf("failed to increment score in Redis: %w", err)
	}
	return nil
}

// GetTopPlayers trả về top N players trên toàn thế giới (1-indexed rank)
func (c *RedisCache) GetTopPlayers(ctx context.Context, limit int) ([]domain.LeaderboardEntry, error) {
	zList, err := c.client.ZRevRangeWithScores(ctx, LeaderboardKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get top players: %w", err)
	}

	entries := make([]domain.LeaderboardEntry, len(zList))
	for i, z := range zList {
		entries[i] = domain.LeaderboardEntry{
			UserID: z.Member.(string),
			Score:  int64(z.Score),
			Rank:   int64(i + 1), // 1-indexed
		}
	}
	return entries, nil
}

// GetUserRank trả về rank (1-indexed) và score của một user cụ thể
func (c *RedisCache) GetUserRank(ctx context.Context, userID string) (*domain.LeaderboardEntry, error) {
	// Sử dụng pipeline để gộp hai lệnh thành một request nhằm giảm latency
	pipe := c.client.Pipeline()
	rankCmd := pipe.ZRevRank(ctx, LeaderboardKey, userID)
	scoreCmd := pipe.ZScore(ctx, LeaderboardKey, userID)

	_, err := pipe.Exec(ctx)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to execute redis pipeline: %w", err)
	}

	rank, err := rankCmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	score, err := scoreCmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	return &domain.LeaderboardEntry{
		UserID: userID,
		Score:  int64(score),
		Rank:   rank + 1, // Đổi từ 0-indexed sang 1-indexed
	}, nil
}

// Close đóng Redis client connection
func (c *RedisCache) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
