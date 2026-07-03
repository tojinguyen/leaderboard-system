package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"leaderboard-system/internal/config"
	"leaderboard-system/internal/domain"
)

type Shard struct {
	MinScore int64
	MaxScore int64
	Cache    *RedisCache
}

type ShardedRedisCache struct {
	shards []Shard
}

// NewShardedRedisCache khởi tạo ShardedRedisCache chứa danh sách các shard.
func NewShardedRedisCache(ctx context.Context, shardConfigs []config.RedisShardConfig) (*ShardedRedisCache, error) {
	if len(shardConfigs) == 0 {
		return nil, errors.New("no redis shard configurations provided")
	}

	shards := make([]Shard, 0, len(shardConfigs))
	for _, sc := range shardConfigs {
		rc, err := NewRedisCache(ctx, sc.Addr, sc.Password, sc.DB)
		if err != nil {
			// Đóng các shard đã mở thành công trước đó trước khi trả về lỗi
			for _, s := range shards {
				_ = s.Cache.Close()
			}
			return nil, fmt.Errorf("failed to initialize redis cache for shard %d (%s): %w", sc.DB, sc.Addr, err)
		}
		shards = append(shards, Shard{
			MinScore: sc.MinScore,
			MaxScore: sc.MaxScore,
			Cache:    rc,
		})
	}

	// Sắp xếp các shard theo dải điểm tăng dần để thuận tiện cho việc tìm kiếm
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].MinScore < shards[j].MinScore
	})

	return &ShardedRedisCache{shards: shards}, nil
}

// getShardIndexByScore tìm shard phù hợp dựa trên điểm số
func (sc *ShardedRedisCache) getShardIndexByScore(score int64) int {
	for i, shard := range sc.shards {
		if score >= shard.MinScore && score <= shard.MaxScore {
			return i
		}
	}
	// Fallback về shard cuối cùng nếu điểm quá lớn hoặc không khớp
	return len(sc.shards) - 1
}

// findUserScoreAndShard tìm kiếm song song trên tất cả các shard để xác định xem user hiện đang ở shard nào và có score bao nhiêu.
// Trả về: index của shard, score của user, và boolean cho biết có tìm thấy user hay không.
func (sc *ShardedRedisCache) findUserScoreAndShard(ctx context.Context, userID string, mode string) (int, int64, bool) {
	type searchResult struct {
		shardIdx int
		score    int64
		found    bool
		err      error
	}

	ch := make(chan searchResult, len(sc.shards))
	var wg sync.WaitGroup

	keyMode := mode
	if keyMode == "" {
		keyMode = "alltime"
	}

	for i := range sc.shards {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := getLeaderboardKeyByMode(time.Now().UTC(), keyMode)

			// Gọi ZScore trực tiếp từ client Redis của shard
			val, err := sc.shards[idx].Cache.client.ZScore(ctx, key, userID).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					ch <- searchResult{shardIdx: idx, score: 0, found: false, err: nil}
				} else {
					ch <- searchResult{shardIdx: idx, score: 0, found: false, err: err}
				}
				return
			}
			ch <- searchResult{shardIdx: idx, score: int64(val), found: true, err: nil}
		}(i)
	}

	wg.Wait()
	close(ch)

	for res := range ch {
		if res.err != nil {
			log.Printf("Error searching user %s on shard %d: %v", userID, res.shardIdx, res.err)
			continue
		}
		if res.found {
			return res.shardIdx, res.score, true
		}
	}

	// Mặc định trả về Shard 0, score 0, found false
	return 0, 0, false
}

// IncrementScore cập nhật điểm số cho user, thực hiện chuyển shard (di trú) nếu điểm mới vượt dải điểm.
func (sc *ShardedRedisCache) IncrementScore(ctx context.Context, userID string, scoreDelta int64, timestamp int64) error {
	t := time.Unix(timestamp, 0).UTC()
	dailyKey, weeklyKey, monthlyKey, allTimeKey := getLeaderboardKeys(t)

	// Vì event có thể thay đổi điểm số trong cả 4 key, ta sẽ thực hiện di trú dựa trên điểm số All-Time.
	// 1. Tìm thông tin All-Time hiện tại của user
	oldShardIdx, oldScore, found := sc.findUserScoreAndShard(ctx, userID, "alltime")
	newScore := oldScore + scoreDelta
	newShardIdx := sc.getShardIndexByScore(newScore)

	if !found {
		// User chưa có điểm, ghi mới vào shard tương ứng của điểm mới
		destShard := sc.shards[newShardIdx].Cache
		pipe := destShard.client.Pipeline()
		pipe.ZIncrBy(ctx, dailyKey, float64(scoreDelta), userID)
		pipe.ZIncrBy(ctx, weeklyKey, float64(scoreDelta), userID)
		pipe.ZIncrBy(ctx, monthlyKey, float64(scoreDelta), userID)
		pipe.ZIncrBy(ctx, allTimeKey, float64(scoreDelta), userID)
		_, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to init score in new shard: %w", err)
		}
		return nil
	}

	if oldShardIdx == newShardIdx {
		// Cùng shard, chỉ việc ZIncrBy
		destShard := sc.shards[oldShardIdx].Cache
		pipe := destShard.client.Pipeline()
		pipe.ZIncrBy(ctx, dailyKey, float64(scoreDelta), userID)
		pipe.ZIncrBy(ctx, weeklyKey, float64(scoreDelta), userID)
		pipe.ZIncrBy(ctx, monthlyKey, float64(scoreDelta), userID)
		pipe.ZIncrBy(ctx, allTimeKey, float64(scoreDelta), userID)
		_, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to increment score in same shard: %w", err)
		}
	} else {
		// Khác shard -> Thực hiện Migration
		log.Printf("[Migration] Migrating user %s from shard %d (score %d) to shard %d (new score %d)", userID, oldShardIdx, oldScore, newShardIdx, newScore)

		// Để cập nhật chính xác cho các khoảng thời gian khác (Daily, Weekly, Monthly), ta cần lấy điểm số hiện tại của user trong các key đó ở shard cũ.
		oldShard := sc.shards[oldShardIdx].Cache
		pipeOldGet := oldShard.client.Pipeline()
		dailyScoreCmd := pipeOldGet.ZScore(ctx, dailyKey, userID)
		weeklyScoreCmd := pipeOldGet.ZScore(ctx, weeklyKey, userID)
		monthlyScoreCmd := pipeOldGet.ZScore(ctx, monthlyKey, userID)

		_, _ = pipeOldGet.Exec(ctx) // Chấp nhận lỗi nil nếu user không tồn tại trong một số time bucket

		var dailyDelta, weeklyDelta, monthlyDelta int64

		if val, err := dailyScoreCmd.Result(); err == nil {
			dailyDelta = int64(val) + scoreDelta
		} else {
			dailyDelta = scoreDelta
		}
		if val, err := weeklyScoreCmd.Result(); err == nil {
			weeklyDelta = int64(val) + scoreDelta
		} else {
			weeklyDelta = scoreDelta
		}
		if val, err := monthlyScoreCmd.Result(); err == nil {
			monthlyDelta = int64(val) + scoreDelta
		} else {
			monthlyDelta = scoreDelta
		}

		// 1. Xóa khỏi shard cũ
		pipeOldDel := oldShard.client.Pipeline()
		pipeOldDel.ZRem(ctx, dailyKey, userID)
		pipeOldDel.ZRem(ctx, weeklyKey, userID)
		pipeOldDel.ZRem(ctx, monthlyKey, userID)
		pipeOldDel.ZRem(ctx, allTimeKey, userID)
		_, err := pipeOldDel.Exec(ctx)
		if err != nil {
			log.Printf("[Migration Warning] failed to remove user %s from old shard %d: %v", userID, oldShardIdx, err)
		}

		// 2. Thêm vào shard mới
		newShard := sc.shards[newShardIdx].Cache
		pipeNewAdd := newShard.client.Pipeline()
		pipeNewAdd.ZAdd(ctx, dailyKey, redis.Z{Score: float64(dailyDelta), Member: userID})
		pipeNewAdd.ZAdd(ctx, weeklyKey, redis.Z{Score: float64(weeklyDelta), Member: userID})
		pipeNewAdd.ZAdd(ctx, monthlyKey, redis.Z{Score: float64(monthlyDelta), Member: userID})
		pipeNewAdd.ZAdd(ctx, allTimeKey, redis.Z{Score: float64(newScore), Member: userID})
		_, err = pipeNewAdd.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to add user to new shard during migration: %w", err)
		}
	}

	return nil
}

// GetTopPlayers trả về top N players bằng cách duyệt từ shard cao xuống shard thấp.
func (sc *ShardedRedisCache) GetTopPlayers(ctx context.Context, limit int, mode string) ([]domain.LeaderboardEntry, error) {
	var result []domain.LeaderboardEntry
	needed := limit

	// Quét các shard theo thứ tự điểm giảm dần (từ shard có dải điểm cao nhất về thấp nhất)
	for i := len(sc.shards) - 1; i >= 0; i-- {
		if needed <= 0 {
			break
		}

		shardEntries, err := sc.shards[i].Cache.GetTopPlayers(ctx, needed, mode)
		if err != nil {
			return nil, fmt.Errorf("failed to get top players from shard %d: %w", i, err)
		}

		result = append(result, shardEntries...)
		needed -= len(shardEntries)
	}

	// Gán lại Rank toàn cục cho các entry trong mảng kết quả
	for idx := range result {
		result[idx].Rank = int64(idx + 1)
	}

	return result, nil
}

// GetUserRank trả về rank global (1-indexed) và score của một user cụ thể.
func (sc *ShardedRedisCache) GetUserRank(ctx context.Context, userID string, mode string) (*domain.LeaderboardEntry, error) {
	// 1. Xác định xem user nằm ở shard nào
	shardIdx, score, found := sc.findUserScoreAndShard(ctx, userID, mode)
	if !found {
		return nil, ErrUserNotFound
	}

	key := getLeaderboardKeyByMode(time.Now().UTC(), mode)

	// 2. Lấy local rank của user trong shard đó (0-indexed)
	localRank, err := sc.shards[shardIdx].Cache.client.ZRevRank(ctx, key, userID).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to get local rank: %w", err)
	}

	// 3. Tính toán rank toàn cục: Local Rank + ZCARD của tất cả các shard cao hơn
	var higherPlayersCount int64
	for i := shardIdx + 1; i < len(sc.shards); i++ {
		count, err := sc.shards[i].Cache.client.ZCard(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get ZCARD for shard %d: %w", i, err)
		}
		higherPlayersCount += count
	}

	globalRank := localRank + 1 + higherPlayersCount // Chuyển sang 1-indexed

	return &domain.LeaderboardEntry{
		UserID: userID,
		Score:  score,
		Rank:   globalRank,
	}, nil
}

type RebuildRepository interface {
	GetUsersBatch(ctx context.Context, lastUserID string, limit int) ([]domain.LeaderboardEntry, error)
}

// RebuildCache thực hiện làm sạch và nạp lại toàn bộ dữ liệu từ PostgreSQL vào các shard Redis
func (sc *ShardedRedisCache) RebuildCache(ctx context.Context, repo RebuildRepository) error {
	log.Println("[Rebuild] Starting Redis cache rebuild from PostgreSQL...")
	startTime := time.Now()

	// 1. Dọn sạch dữ liệu trên toàn bộ các shard trước khi rebuild
	for i, shard := range sc.shards {
		log.Printf("[Rebuild] Flushing database for shard %d...", i)
		if err := shard.Cache.client.FlushDB(ctx).Err(); err != nil {
			return fmt.Errorf("failed to flush shard %d: %w", i, err)
		}
	}

	batchSize := 10000
	lastUserID := ""
	totalRebuilt := 0
	nowTime := time.Now().UTC()

	// Tạo các key thời gian
	dailyKey, weeklyKey, monthlyKey, allTimeKey := getLeaderboardKeys(nowTime)

	for {
		entries, err := repo.GetUsersBatch(ctx, lastUserID, batchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch user batch from database: %w", err)
		}

		if len(entries) == 0 {
			break // Hết dữ liệu
		}

		// Phân nhóm thành viên theo từng shard
		shardPipelines := make(map[int][]redis.Z)
		for _, entry := range entries {
			shardIdx := sc.getShardIndexByScore(entry.Score)
			shardPipelines[shardIdx] = append(shardPipelines[shardIdx], redis.Z{
				Score:  float64(entry.Score),
				Member: entry.UserID,
			})
			lastUserID = entry.UserID
		}

		// Sử dụng pipeline để ZADD hàng loạt vào từng shard
		for shardIdx, zMembers := range shardPipelines {
			destShard := sc.shards[shardIdx].Cache
			pipe := destShard.client.Pipeline()

			pipe.ZAdd(ctx, dailyKey, zMembers...)
			pipe.ZAdd(ctx, weeklyKey, zMembers...)
			pipe.ZAdd(ctx, monthlyKey, zMembers...)
			pipe.ZAdd(ctx, allTimeKey, zMembers...)

			_, err := pipe.Exec(ctx)
			if err != nil {
				return fmt.Errorf("failed to execute rebuild pipeline on shard %d: %w", shardIdx, err)
			}
		}

		totalRebuilt += len(entries)
		log.Printf("[Rebuild] Processed batch of %d users (Total: %d)", len(entries), totalRebuilt)
	}

	log.Printf("[Rebuild] Cache rebuild complete! Total rebuilt: %d in %v", totalRebuilt, time.Since(startTime))
	return nil
}

// Close đóng tất cả các kết nối Redis shards
func (sc *ShardedRedisCache) Close() error {
	var errs []error
	for _, s := range sc.shards {
		if err := s.Cache.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing shards: %v", errs)
	}
	return nil
}
