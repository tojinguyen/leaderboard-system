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

const (
	PrefixDaily   = "leaderboard:daily:"
	PrefixWeekly  = "leaderboard:weekly:"
	PrefixMonthly = "leaderboard:monthly:"
	KeyAllTime    = "leaderboard:alltime"
)

type RedisCache struct {
	client *redis.Client
}

// getLeaderboardKeys sinh ra 4 key Redis tương ứng với thời gian truyền vào
func getLeaderboardKeys(t time.Time) (dailyKey, weeklyKey, monthlyKey, allTimeKey string) {
	dailyKey = PrefixDaily + t.Format("2006-01-02")
	
	year, week := t.ISOWeek()
	weeklyKey = fmt.Sprintf("%s%d-W%02d", PrefixWeekly, year, week)
	
	monthlyKey = PrefixMonthly + t.Format("2006-01")
	
	return dailyKey, weeklyKey, monthlyKey, KeyAllTime
}

// getLeaderboardKeyByMode trả về key Redis cụ thể dựa trên mode và thời gian
func getLeaderboardKeyByMode(t time.Time, mode string) string {
	dailyKey, weeklyKey, monthlyKey, allTimeKey := getLeaderboardKeys(t)
	switch mode {
	case "daily":
		return dailyKey
	case "weekly":
		return weeklyKey
	case "monthly":
		return monthlyKey
	default:
		return allTimeKey
	}
}

// NewRedisCache khởi tạo kết nối Redis Client với cơ chế Retry Exponential Backoff
func NewRedisCache(ctx context.Context, addr string, password string, db int) (*RedisCache, error) {
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

// IncrementScore cập nhật điểm số cho user trong 4 time bucket song song qua Redis Pipeline
func (c *RedisCache) IncrementScore(ctx context.Context, userID string, scoreDelta int64, timestamp int64) error {
	t := time.Unix(timestamp, 0).UTC()
	dailyKey, weeklyKey, monthlyKey, allTimeKey := getLeaderboardKeys(t)

	// Sử dụng pipeline để gộp 4 lệnh ZINCRBY gửi đi trong 1 RTT mạng
	pipe := c.client.Pipeline()
	pipe.ZIncrBy(ctx, dailyKey, float64(scoreDelta), userID)
	pipe.ZIncrBy(ctx, weeklyKey, float64(scoreDelta), userID)
	pipe.ZIncrBy(ctx, monthlyKey, float64(scoreDelta), userID)
	pipe.ZIncrBy(ctx, allTimeKey, float64(scoreDelta), userID)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to increment score in Redis pipeline: %w", err)
	}
	return nil
}

// GetTopPlayers trả về top N players dựa trên mode (daily, weekly, monthly, alltime)
func (c *RedisCache) GetTopPlayers(ctx context.Context, limit int, mode string) ([]domain.LeaderboardEntry, error) {
	key := getLeaderboardKeyByMode(time.Now().UTC(), mode)
	zList, err := c.client.ZRevRangeWithScores(ctx, key, 0, int64(limit-1)).Result()
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

// GetUserRank trả về rank (1-indexed) và score của một user cụ thể dựa trên mode
func (c *RedisCache) GetUserRank(ctx context.Context, userID string, mode string) (*domain.LeaderboardEntry, error) {
	key := getLeaderboardKeyByMode(time.Now().UTC(), mode)

	// Sử dụng pipeline để gộp hai lệnh thành một request nhằm giảm latency
	pipe := c.client.Pipeline()
	rankCmd := pipe.ZRevRank(ctx, key, userID)
	scoreCmd := pipe.ZScore(ctx, key, userID)

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
