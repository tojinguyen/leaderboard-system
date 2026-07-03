package cache

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"leaderboard-system/internal/config"
	"leaderboard-system/internal/domain"
)

func TestShardedRedisCache_Integration(t *testing.T) {
	// Chỉ chạy test nếu Redis local đang chạy (cổng mặc định 6379)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Khởi tạo các shard dùng DB 10, 11, 12 để tránh ảnh hưởng đến môi trường dev chính (DB 0, 1, 2)
	shardConfigs := []config.RedisShardConfig{
		{MinScore: 0, MaxScore: 99, Addr: "localhost:6379", Password: "", DB: 10},
		{MinScore: 100, MaxScore: 499, Addr: "localhost:6379", Password: "", DB: 11},
		{MinScore: 500, MaxScore: 9223372036854775807, Addr: "localhost:6379", Password: "", DB: 12},
	}

	// Kiểm tra xem có kết nối được Redis không trước khi chạy test thực tế
	testClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := testClient.Ping(ctx).Err(); err != nil {
		t.Skip("Skipping integration test: local Redis is not running on localhost:6379")
		return
	}
	_ = testClient.Close()

	// 1. Khởi tạo ShardedRedisCache
	shardedCache, err := NewShardedRedisCache(ctx, shardConfigs)
	if err != nil {
		t.Fatalf("Failed to create ShardedRedisCache: %v", err)
	}
	defer func() {
		_ = shardedCache.Close()
	}()

	// 2. Dọn sạch DB test (DB 10, 11, 12) trước khi chạy test
	for _, shard := range shardedCache.shards {
		if err := shard.Cache.client.FlushDB(ctx).Err(); err != nil {
			t.Fatalf("Failed to flush test DB %d: %v", shard.Cache.client.Options().DB, err)
		}
	}

	now := time.Now().Unix()

	// TÌNH HUỐNG 1: Ghi mới điểm số vào dải thấp (Shard 0 - DB 10)
	t.Log("Scenario 1: Adding 50 score to player_1 (should go to Shard 0/DB 10)")
	err = shardedCache.IncrementScore(ctx, "player_1", 50, now)
	if err != nil {
		t.Fatalf("Failed to increment score: %v", err)
	}

	// Xác nhận player_1 nằm ở DB 10
	scoreInShard0, err := shardedCache.shards[0].Cache.client.ZScore(ctx, KeyAllTime, "player_1").Result()
	if err != nil {
		t.Errorf("player_1 should be in Shard 0: %v", err)
	}
	if scoreInShard0 != 50 {
		t.Errorf("Expected score in Shard 0 to be 50, got %f", scoreInShard0)
	}

	// Xác nhận player_1 KHÔNG nằm ở DB 11, 12
	if err := shardedCache.shards[1].Cache.client.ZScore(ctx, KeyAllTime, "player_1").Err(); err == nil {
		t.Error("player_1 should NOT exist in Shard 1")
	}

	// TÌNH HUỐNG 2: Cộng điểm vượt ngưỡng để kích hoạt DI TRÚ (từ Shard 0 sang Shard 1)
	t.Log("Scenario 2: Adding 80 score to player_1 (total 130 -> migrate to Shard 1/DB 11)")
	err = shardedCache.IncrementScore(ctx, "player_1", 80, now)
	if err != nil {
		t.Fatalf("Failed to increment score: %v", err)
	}

	// Check xem đã bị xoá khỏi Shard 0 chưa
	if err := shardedCache.shards[0].Cache.client.ZScore(ctx, KeyAllTime, "player_1").Err(); err == nil {
		t.Error("player_1 should be removed from Shard 0 after migration")
	}

	// Check xem đã được đưa sang Shard 1 chưa
	scoreInShard1, err := shardedCache.shards[1].Cache.client.ZScore(ctx, KeyAllTime, "player_1").Result()
	if err != nil {
		t.Errorf("player_1 should be migrated to Shard 1: %v", err)
	}
	if scoreInShard1 != 130 {
		t.Errorf("Expected score in Shard 1 to be 130, got %f", scoreInShard1)
	}

	// TÌNH HUỐNG 3: Cộng điểm tiếp để di trú lên Shard 2 (DB 12)
	t.Log("Scenario 3: Adding 400 score to player_1 (total 530 -> migrate to Shard 2/DB 12)")
	err = shardedCache.IncrementScore(ctx, "player_1", 400, now)
	if err != nil {
		t.Fatalf("Failed to increment score: %v", err)
	}

	// Check Shard 1 phải trống
	if err := shardedCache.shards[1].Cache.client.ZScore(ctx, KeyAllTime, "player_1").Err(); err == nil {
		t.Error("player_1 should be removed from Shard 1 after second migration")
	}

	// Check Shard 2 phải có user
	scoreInShard2, err := shardedCache.shards[2].Cache.client.ZScore(ctx, KeyAllTime, "player_1").Result()
	if err != nil {
		t.Errorf("player_1 should be migrated to Shard 2: %v", err)
	}
	if scoreInShard2 != 530 {
		t.Errorf("Expected score in Shard 2 to be 530, got %f", scoreInShard2)
	}

	// TÌNH HUỐNG 4: Thêm các players khác ở các shard khác nhau để test GetTop và GetRank
	t.Log("Scenario 4: Adding other players for ranking tests")
	_ = shardedCache.IncrementScore(ctx, "player_2", 600, now) // Shard 2 (600đ)
	_ = shardedCache.IncrementScore(ctx, "player_3", 300, now) // Shard 1 (300đ)
	_ = shardedCache.IncrementScore(ctx, "player_4", 40, now)  // Shard 0 (40đ)

	// 4.1. Test GetTopPlayers (Top 3)
	// Dự kiến thứ tự: player_2 (600đ) -> player_1 (530đ) -> player_3 (300đ)
	topPlayers, err := shardedCache.GetTopPlayers(ctx, 3, "alltime")
	if err != nil {
		t.Fatalf("Failed to GetTopPlayers: %v", err)
	}
	if len(topPlayers) != 3 {
		t.Fatalf("Expected 3 top players, got %d", len(topPlayers))
	}

	expectedOrder := []string{"player_2", "player_1", "player_3"}
	expectedScores := []int64{600, 530, 300}
	for i, entry := range topPlayers {
		if entry.UserID != expectedOrder[i] {
			t.Errorf("Rank %d expected user %s, got %s", i+1, expectedOrder[i], entry.UserID)
		}
		if entry.Score != expectedScores[i] {
			t.Errorf("Rank %d expected score %d, got %d", i+1, expectedScores[i], entry.Score)
		}
		if entry.Rank != int64(i+1) {
			t.Errorf("Expected Rank metadata to be %d, got %d", i+1, entry.Rank)
		}
	}

	// 4.2. Test GetUserRank (Rank Global)
	// player_2 (600đ) -> Hạng 1 (ở Shard 2)
	// player_1 (530đ) -> Hạng 2 (ở Shard 2)
	// player_3 (300đ) -> Hạng 3 (ở Shard 1, trong Shard 2 có 2 người)
	// player_4 (40đ)  -> Hạng 4 (ở Shard 0, các shard cao hơn có 3 người)
	rankTests := []struct {
		userID       string
		expectedRank int64
		expectedSc   int64
	}{
		{"player_2", 1, 600},
		{"player_1", 2, 530},
		{"player_3", 3, 300},
		{"player_4", 4, 40},
	}

	for _, rt := range rankTests {
		entry, err := shardedCache.GetUserRank(ctx, rt.userID, "alltime")
		if err != nil {
			t.Errorf("Failed to GetUserRank for %s: %v", rt.userID, err)
			continue
		}
		if entry.Rank != rt.expectedRank {
			t.Errorf("User %s rank expected %d, got %d", rt.userID, rt.expectedRank, entry.Rank)
		}
		if entry.Score != rt.expectedSc {
			t.Errorf("User %s score expected %d, got %d", rt.userID, rt.expectedSc, entry.Score)
		}
	}
}

type mockRebuildRepo struct {
	entries []domain.LeaderboardEntry
}

func (m *mockRebuildRepo) GetUsersBatch(ctx context.Context, lastUserID string, limit int) ([]domain.LeaderboardEntry, error) {
	var result []domain.LeaderboardEntry
	for _, entry := range m.entries {
		if entry.UserID > lastUserID {
			result = append(result, entry)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func TestShardedRedisCache_Rebuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shardConfigs := []config.RedisShardConfig{
		{MinScore: 0, MaxScore: 99, Addr: "localhost:6379", Password: "", DB: 10},
		{MinScore: 100, MaxScore: 499, Addr: "localhost:6379", Password: "", DB: 11},
		{MinScore: 500, MaxScore: 9223372036854775807, Addr: "localhost:6379", Password: "", DB: 12},
	}

	testClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := testClient.Ping(ctx).Err(); err != nil {
		t.Skip("Skipping integration test: local Redis is not running on localhost:6379")
		return
	}
	_ = testClient.Close()

	shardedCache, err := NewShardedRedisCache(ctx, shardConfigs)
	if err != nil {
		t.Fatalf("Failed to create ShardedRedisCache: %v", err)
	}
	defer func() {
		_ = shardedCache.Close()
	}()

	// Mock DB data
	mockData := []domain.LeaderboardEntry{
		{UserID: "user_a", Score: 50},  // Shard 0
		{UserID: "user_b", Score: 250}, // Shard 1
		{UserID: "user_c", Score: 750}, // Shard 2
	}
	mockRepo := &mockRebuildRepo{entries: mockData}

	// Chạy Rebuild
	err = shardedCache.RebuildCache(ctx, mockRepo)
	if err != nil {
		t.Fatalf("RebuildCache failed: %v", err)
	}

	// Kiểm tra xem dữ liệu đã được nạp đúng vào các shard chưa
	// Shard 0 (DB 10) phải có user_a (50đ)
	scoreA, err := shardedCache.shards[0].Cache.client.ZScore(ctx, KeyAllTime, "user_a").Result()
	if err != nil || scoreA != 50 {
		t.Errorf("user_a (50) should be in Shard 0, got error: %v or score: %f", err, scoreA)
	}

	// Shard 1 (DB 11) phải có user_b (250đ)
	scoreB, err := shardedCache.shards[1].Cache.client.ZScore(ctx, KeyAllTime, "user_b").Result()
	if err != nil || scoreB != 250 {
		t.Errorf("user_b (250) should be in Shard 1, got error: %v or score: %f", err, scoreB)
	}

	// Shard 2 (DB 12) phải có user_c (750đ)
	scoreC, err := shardedCache.shards[2].Cache.client.ZScore(ctx, KeyAllTime, "user_c").Result()
	if err != nil || scoreC != 750 {
		t.Errorf("user_c (750) should be in Shard 2, got error: %v or score: %f", err, scoreC)
	}
}
